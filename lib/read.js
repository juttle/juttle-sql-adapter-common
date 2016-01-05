var _ = require('underscore');
var db = require('./db');
var SQLFilter = require('./sql_filter');
var Juttle = require('juttle/lib/runtime').Juttle;
var JuttleUtils = require('juttle/lib/runtime/juttle-utils');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var Promise = require('bluebird');

var logger = require('juttle/lib/logger').getLogger('sql-db-read');

var Read = Juttle.proc.source.extend({
    procName: 'read-sql',
    FETCH_SIZE_LIMIT: 10000,

    allowed_options: ['table', 'raw', 'debug', 'timeField', 'fetchSize', 'from', 'to', 'optimize'],

    //initialization functions

    initialize: function(options, params, pname, location, program, juttle) {
        logger.debug('init proc name:', this.procName);
        logger.debug('options:', options);
        this.validateOptions(options);
        this.setOptions(options);

        this.baseQuery = db.getDbConnection();
        this.total_emitted_points = 0;

        this.toRaw = this.baseQuery.raw;

        if (options.raw) {
            this.baseQuery = this.toRaw(options.raw);
            return;
        }

        if (params.filter_ast) {
            this.baseQuery = this.addFilters(params.filter_ast);
        }

        this.handleTimeOptions(options);
        this.addOptimizations(params.optimization_info);

        this.baseQuery = this.baseQuery.from(options.table).limit(this.getNextLimit());

        //cannot order by time with any reduces
        if (this.timeField && !(params.optimization_info && params.optimization_info.type === 'reduce')) {
            this.baseQuery = this.baseQuery.orderBy(this.timeField || 'time');
        }
    },

    validateOptions: function(options) {
        var unknown = _.difference(_.keys(options), this.allowed_options);
        if (unknown.length > 0) {
            throw this.compile_error('RT-UNKNOWN-OPTION-ERROR', {
                proc: 'read-sql',
                option: unknown[0]
            });
        }

        if (_.has(options, 'table') === _.has(options, 'raw')) {
            throw this.compile_error('RT-MISSING-OPTION-ERROR', { option: "choose only one: table,raw" });
        }

        //XXX also disallow ANY other options with raw
    },

    setOptions: function(options) {
        this.debugOption = options.debug;
        this.timeField = options.timeField;
        this.fetchSize = options.fetchSize || this.FETCH_SIZE_LIMIT;
        this.raw = options.raw;
    },

    addFilters: function(filter_ast) {
        var FilterSQLCompiler = Juttle.FilterJSCompiler.extend(SQLFilter);
        var compiler = new FilterSQLCompiler({ baseQuery: this.baseQuery });
        return compiler.compile(filter_ast);
    },

    handleTimeOptions: function(options) {
        if (options.timeField || options.from || options.to) {
            var timeField = this.timeField || 'time';
            logger.debug('sorting by time on field ', timeField);

            if (options.from) {
                this.from = options.from;
                this.baseQuery = this.baseQuery.where(timeField, '>=', new Date(options.from.valueOf()));
                logger.debug('start time', new Date(options.from.valueOf()).toISOString());
            }
            if (options.to) {
                this.to = options.to;
                this.baseQuery = this.baseQuery.where(timeField, '<', new Date(options.to.valueOf()));
                logger.debug('end time', new Date(options.to.valueOf()).toISOString());
            }
        }
    },

    addOptimizations: function(optimization_info) {
        var self = this;
        if (optimization_info && !_.isEmpty(optimization_info)) {
            logger.debug('adding optimizations: ', optimization_info);
            if (optimization_info.type === 'head') {
                logger.debug('head optimization, new max size: ', optimization_info.limit);
                self.maxSize = optimization_info.limit;
            }
            if (optimization_info.type === 'reduce') {
                var groupby = optimization_info.groupby;
                if (groupby && groupby.length > 0) {
                    logger.debug('adding groupby: ', groupby);
                    self.baseQuery = self.baseQuery.select(groupby).groupBy(groupby);
                }
                self.aggregation_targets = [];
                _.each(optimization_info.aggregations, function(aggregation, target_field) {
                    logger.debug('adding aggregation: ', {
                        func: aggregation.name,
                        by_field: aggregation.field,
                        target_field: target_field
                    });

                    if (aggregation.raw) {
                        self.baseQuery = self.baseQuery.select(self.toRaw(aggregation.raw));
                    } else {
                        self.baseQuery = self.baseQuery[aggregation.name](aggregation.field + ' as ' + target_field);
                    }

                    self.aggregation_targets.push(target_field);
                });
                if (optimization_info.reduce_every) {
                    if (self.aggregation_targets.length > 0) {
                        self.empty_aggregate = optimization_info.empty_aggregate;
                        self.bufferEmptyAggregates = [];
                    }
                    this.getBatchBuckets(optimization_info);
                }
            }
        }
    },

    getBatchBuckets: function(optimization_info) {
        var reduce_every = optimization_info.reduce_every;
        var reduce_on = optimization_info.reduce_on;
        var query_start = this.from;
        var query_end = this.to;

        function get_batch_offset_as_duration(every_duration) {
            try {
                return new JuttleMoment.duration(reduce_on);
            } catch (err) {
                // translate a non-duration -on into the equivalent duration
                // e.g. if we're doing -every :hour: -on :2015-03-16T18:32:00.000Z:
                // then that's equivalent to -every :hour: -on :32 minutes:
                var moment = new JuttleMoment(reduce_on);
                return JuttleMoment.subtract(moment, JuttleMoment.quantize(moment, every_duration));
            }
        }

        function get_buckets() {
            var buckets = [query_start];

            var duration = new JuttleMoment.duration(reduce_every);
            var zeroth_bucket = JuttleMoment.quantize(query_start, duration);
            var last_bucket = JuttleMoment.quantize(query_end, duration);

            if (reduce_on) {
                var offset = get_batch_offset_as_duration(duration);
                zeroth_bucket = JuttleMoment.add(zeroth_bucket, offset);
                if (zeroth_bucket.gt(query_start)) {
                    buckets.push(zeroth_bucket);
                }
                last_bucket = JuttleMoment.add(last_bucket, offset);
            }

            var intermediate_bucket = JuttleMoment.add(zeroth_bucket, duration);

            while (intermediate_bucket.lte(last_bucket)) {
                buckets.push(intermediate_bucket);
                intermediate_bucket = JuttleMoment.add(intermediate_bucket, duration);
            }

            //this replaces end_query as the final aggregation time
            //end_query right endpoint already in the SQL where clause
            buckets.push(intermediate_bucket);

            _.each(buckets, function(b, i) {
                logger.debug('time bucket ' + i, new Date(b.valueOf()).toISOString());
            });
            return buckets;
        }
        this.batchBuckets = get_buckets();
    },

    getNextLimit: function () {
        return this.maxSize === undefined ? this.fetchSize : Math.min(this.fetchSize, this.maxSize - this.total_emitted_points);
    },

    //Query execution

    start: function() {
        var self = this;

        if (this.debugOption) {
            this.handleDebug();
            return;
        }

        function executeQuery() {
            if (self.raw) {
                return self.baseQuery
                .then(function(res) {
                    return self.handleRawResponse(res);
                })
                .then(function(points) {
                    return self.formatAndSend(points);
                });
            }

            if (self.batchBuckets) {
                return self.paginateBuckets();
            }

            self.offsetCount = 0;
            return self.paginate(self.baseQuery.clone());
        }

        executeQuery()
        .catch(function(err) {
            if (/Pool (is|was) destroyed/.test(err.message)) {
                var connectionInfo = self.baseQuery.client.connectionSettings;

                self.trigger('error', self.runtime_error('RT-INTERNAL-ERROR', {
                    error: 'could not connect to database: ' + JSON.stringify(connectionInfo)
                }));
            } else {
                self.trigger('error', self.runtime_error('RT-INTERNAL-ERROR', { error: err.message.toString() }));
            }
        }).finally(function() {
            self.eof();
        });
    },

    paginateBuckets: function() {
        var self = this;
        var timeField = self.timeField || 'time';
        var buckets = self.batchBuckets;
        return Promise.mapSeries(buckets.slice(1), function(bucket, i, len) {
            var query = self.baseQuery.clone();
            query = query.where(timeField, '>=', new Date(buckets[i].valueOf()));
            query = query.where(timeField, '<', new Date(bucket.valueOf()));

            var additionalFields = {};
            bucket.epsilon = true;
            additionalFields[timeField] = bucket;

            return self.paginate(query, additionalFields);
        });
    },

    //the raw query response format can be different based on client
    handleRawResponse: function (points) {
        return points;
    },

    paginate: function(query, additionalFields) {
        var self = this;

        logger.debug('executing paginated query');
        return query.then(function(points) {
            var nextQuery;

            if (additionalFields) {
                _.each(points, function(pt) {
                    _.extend(pt, additionalFields);
                });
            }

            if (points.length < self.fetchSize || self.getNextLimit() <= 0) {
                //no more pagination necessary
                return self.formatAndSend(points);
            }

            //perform time-based pagination if timeField is indicated
            if (self.timeField) {
                nextQuery = self.processPaginatedResults(query, points, self.timeField);
            } else {
                self.formatAndSend(points);
                self.offsetCount += points.length;
                logger.debug('next pagination based on offset ', self.offsetCount);
                nextQuery = query.offset(self.offsetCount).limit(self.getNextLimit());
            }

            if (nextQuery) {
                return self.paginate(nextQuery);
            }
        });
    },

    // utility function to help with pagination:
    //      - pass in an page of sorted results and specify the sort field
    //      - returns paginated query
    processPaginatedResults: function(query, resultsPage, sortField) {
        var nonBorderPoints = [];
        var len = resultsPage.length;
        var borderValue = _.last(resultsPage)[sortField];
        if (!borderValue) {
            this.trigger('error', this.runtime_error('RT-INTERNAL-ERROR', {
                error: 'value of field "' + sortField + '" is ' + typeof borderValue
            }));
            return;
        }

        for (var i = len - 2; i >= 0 ; i--) {
            //use isEqual in order to compare Date objects correctly
            if (!_.isEqual(resultsPage[i][sortField], borderValue)) {
                nonBorderPoints = resultsPage.slice(0, i + 1);
                break;
            }
        }
        if (nonBorderPoints.length === 0) {
            this.trigger('error', this.runtime_error('RT-INTERNAL-ERROR', {
                error: 'unable to paginate because all of fetchSize ' +
                    this.fetchSize + ' has the same timeField. Consider increasing fetchSize.'
            }));
            return;
        }
        this.formatAndSend(nonBorderPoints);

        logger.debug('next pagination based on timefield ', sortField, ' with border value ', borderValue);
        return query.where(sortField, '>=', new Date(borderValue));
    },

    formatAndSend: function(points) {
        var formattedPoints = this.formatPoints(points);
        this.total_emitted_points += points.length;
        this.emit(formattedPoints);
    },

    formatPoints: function(points) {
        var self = this;

        if (this.addPointFormatting) {
            this.addPointFormatting(points, this.aggregation_targets);
        }

        if (this.empty_aggregate) {
            points = this.handleNullAggregates(points);
        }

        _.each(points, function(pt) {
            //ensure time is in the "time" key of each point.
            if (self.timeField && self.timeField !== 'time') {
                pt.time = pt[self.timeField];
                pt[self.timeField] = undefined;
            }
        });

        return JuttleUtils.toNative(points);
    },

    //handle trailing and leading null aggreates
    handleNullAggregates: function(points) {
        if (_.isMatch(points[0], this.empty_aggregate)) {
            if (this.total_emitted_points > 0) {
                this.bufferEmptyAggregates.push(points[0]);
            }
            return [];
        } else if (this.bufferEmptyAggregates.length > 0) {
            points.unshift.apply(null, this.bufferEmptyAggregates);
        }
        return points;
    },

    handleDebug: function() {
        logger.debug('returning query string as point');
        this.emit([{
            query: this.baseQuery.toString()
        }]);
        this.eof();
    }
});

module.exports  = Read;
