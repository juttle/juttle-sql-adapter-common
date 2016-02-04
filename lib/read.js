'use strict';

var _ = require('underscore');
var db = require('./db');
var SQLFilter = require('./sql_filter');
var FilterJSCompiler = require('juttle/lib/compiler/filters/filter-js-compiler');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var Promise = require('bluebird');
var AdapterRead = require('juttle/lib/runtime/adapter-read');
var errors = require('juttle/lib/errors');

const ALLOWED_OPTIONS = AdapterRead.commonOptions.concat(['table', 'raw', 'debug', 'fetchSize', 'optimize']);

class ReadSql extends AdapterRead {

    static get FETCH_SIZE_LIMIT() { return 10000; }

    //initialization functions

    constructor(options, params) {
        super(options, params);
        this.validateOptions(options);
        this.setOptions(options);

        this.pointsBuffer = [];
        this.baseQuery = db.getDbConnection();

        this.total_emitted_points = 0;
        this.maxSize = Infinity;
        this.queueSize = Infinity;

        this.toRaw = this.baseQuery.raw;

        if (options.raw) {
            this.baseQuery = this.toRaw(options.raw);
            return;
        }

        if (params.filter_ast) {
            this.baseQuery = this.addFilters(params.filter_ast);
        }

        this.addOptimizations(params.optimization_info);

        this.baseQuery = this.baseQuery.from(options.table).limit(this.getNextLimit());

        if (this.timeField && this.optimization_info.type !== 'reduce') {
            this.baseQuery = this.baseQuery.orderBy(this.timeField, this.tailOptimized ? 'desc' : 'asc');
        }
    }

    periodicLiveRead() {
        return !!this.timeField;
    }

    validateOptions(options) {
        var unknown = _.difference(_.keys(options), ALLOWED_OPTIONS);
        if (unknown.length > 0) {
            throw new errors.runtimeError('RT-UNKNOWN-OPTION-ERROR', {
                proc: 'read-sql',
                option: unknown[0]
            });
        }

        if (_.has(options, 'table') && _.has(options, 'raw') ||
            (_.has(options, 'debug') && _.has(options, 'raw'))
        ) {
            throw new errors.runtimeError('RT-INCOMPATIBLE-OPTION-ERROR', {
                option: 'raw',
                other: 'table or -debug',
            });
        }

        if (!_.has(options, 'table') && !_.has(options, 'raw')) {
            throw new errors.runtimeError('RT-MISSING-OPTION-ERROR', {
                option: '-table or -raw',
                proc: 'read-sql'
            });
        }
    }

    setOptions(options) {
        this.debugOption = options.debug;
        this.fetchSize = options.fetchSize || ReadSql.FETCH_SIZE_LIMIT;
        this.raw = options.raw;

        // set timeField IFF options reference time field or interval
        this.timeField = options.timeField ||
            (options.from || options.to || options.last ? 'time' : undefined);
    }

    addFilters(filter_ast) {
        var FilterSQLCompiler = FilterJSCompiler.extend(SQLFilter);
        var compiler = new FilterSQLCompiler({ baseQuery: this.baseQuery });
        return compiler.compile(filter_ast);
    }

    addOptimizations(optimization_info) {
        var self = this;
        this.optimization_info = optimization_info || {};

        if (!_.isEmpty(this.optimization_info)) {
            this.logger.debug('adding optimizations: ', optimization_info);
            if (optimization_info.type === 'head' || optimization_info.type === 'tail') {
                this.logger.debug(optimization_info.type + ' optimization, new max size: ', optimization_info.limit);
                self.maxSize = optimization_info.limit;
                self.tailOptimized = optimization_info.type === 'tail';
            }
            if (optimization_info.type === 'reduce') {
                var groupby = optimization_info.groupby;
                if (groupby && groupby.length > 0) {
                    this.logger.debug('adding groupby: ', groupby);
                    self.baseQuery = self.baseQuery.select(groupby).groupBy(groupby);
                }
                self.aggregation_targets = [];
                _.each(optimization_info.aggregations, function(aggregation, target_field) {
                    self.logger.debug('adding aggregation: ', {
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
                }
            }
        }
    }

    getBatchBuckets(optimization_info, query_start, query_end) {
        var self = this;

        var reduce_every = optimization_info.reduce_every;
        var reduce_on = optimization_info.reduce_on;

        function get_batch_offset_as_duration(every_duration) {
            try {
                return JuttleMoment.duration(reduce_on);
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

            var duration = JuttleMoment.duration(reduce_every);
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
                self.logger.debug('time bucket ' + i, new Date(b.valueOf()).toISOString());
            });
            return buckets;
        }
        this.batchBuckets = get_buckets();
    }

    getNextLimit() {
        return Math.min(this.fetchSize, this.queueSize, this.maxSize - this.total_emitted_points);
    }

    //Query execution

    read(from, to, limit, state) {
        var queryExecutionPromise;

        this.queueSize = limit;

        if (this.raw) {
            queryExecutionPromise = this.baseQuery
            .then((res) => {
                return this.handleRawResponse(res);
            })
            .then((points) => {
                return this.formatAndBuffer(points);
            });
        } else {
            var query = this.baseQuery.clone();
            if (this.timeField) {
                if (from) {
                    query = query.where(this.timeField, '>=', new Date(from.valueOf()));
                    this.logger.debug('start time', new Date(from.valueOf()).toISOString());
                }
                if (to) {
                    query = query.where(this.timeField, '<', new Date(to.valueOf()));
                    this.logger.debug('end time', new Date(to.valueOf()).toISOString());
                }
            }

            if (this.debugOption) {
                queryExecutionPromise = Promise.try(() => { return this.handleDebug(query); });
            } else if (this.optimization_info.reduce_every) {
                this.getBatchBuckets(this.optimization_info, from, to);
                queryExecutionPromise = this.paginateBuckets(query);
            } else {
                this.offsetCount = 0;
                queryExecutionPromise = this.paginate(query);
            }
        }

        return queryExecutionPromise
        .catch(err => {
            if (/Pool (is|was) destroyed/.test(err.message)) {
                var connectionInfo = this.baseQuery.client.connectionSettings;
                this.trigger('error', errors.runtimeError('RT-INTERNAL-ERROR', {
                    error: 'could not connect to database: ' + JSON.stringify(connectionInfo)
                }));
            } else {
                this.trigger('error', err);
            }
        })
        .then(() => {
            var points = this.pointsBuffer;
            this.pointsBuffer = [];
            return {
                points: points,
                readEnd: this.timeField && !this.debugOption ? to : new JuttleMoment(Infinity)
            };
        });
    }

    paginateBuckets(original_query) {
        var self = this;
        var timeField = self.timeField || 'time';
        var buckets = self.batchBuckets;
        return Promise.mapSeries(buckets.slice(1), function(bucket, i, len) {
            var query = original_query.clone();
            query = query.where(timeField, '>=', new Date(buckets[i].valueOf()));
            query = query.where(timeField, '<', new Date(bucket.valueOf()));

            var additionalFields = {};
            bucket.epsilon = true;
            additionalFields[timeField] = bucket;

            return self.paginate(query, additionalFields);
        });
    }

    //the raw query response format can be different based on client
    handleRawResponse(points) {
        return points;
    }

    paginate(query, additionalFields) {
        this.logger.debug('executing paginated query');

        return query.then((points) => {
            var nextQuery;

            if (additionalFields) {
                _.each(points, function(pt) {
                    _.extend(pt, additionalFields);
                });
            }
            if (points.length < this.fetchSize) {
                //no more pagination necessary
                return this.formatAndBuffer(points);
            }

            //perform time-based pagination if timeField is indicated
            if (this.timeField) {
                nextQuery = this.processPaginatedResults(query, points, this.timeField);
            } else {
                this.formatAndBuffer(points);
                this.offsetCount += points.length;
                this.logger.debug('next pagination based on offset ', this.offsetCount);
                nextQuery = query.offset(this.offsetCount).limit(this.getNextLimit());
            }

            if (nextQuery && this.getNextLimit() > 0) {
                return this.paginate(nextQuery);
            }
        });
    }

    // utility function to help with pagination:
    //      - pass in an page of sorted results and specify the sort field
    //      - returns paginated query
    processPaginatedResults(query, resultsPage, sortField) {
        var nonBorderPoints = [];
        var len = resultsPage.length;
        var borderValue = _.last(resultsPage)[sortField];
        if (!borderValue) {
            this.trigger('error', new Error(`value of field "${sortField}" is ${typeof borderValue}`));
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
            this.trigger('error', new Error(
                `unable to paginate because all of fetchSize ` +
                `${this.fetchSize} has the same timeField. Consider increasing fetchSize.`
            ));
            return;
        }

        this.formatAndBuffer(nonBorderPoints);

        this.logger.debug('next pagination based on timefield ', sortField, ' with border value ', borderValue);
        return query.where(sortField, this.tailOptimized ? '<=' : '>=', new Date(borderValue));
    }

    formatAndBuffer(points) {
        var formattedPoints = this.formatPoints(points);
        this.total_emitted_points += formattedPoints.length;
        this.pointsBuffer = this.pointsBuffer.concat(formattedPoints);
    }

    formatPoints(points) {
        if (this.addPointFormatting) {
            this.addPointFormatting(points, this.aggregation_targets);
        }

        if (this.empty_aggregate) {
            points = this.handleNullAggregates(points);
        }

        if (this.tailOptimized) {
            points.reverse();
        }
        this.convertDatesToMoments(points);

        // optimized reduce without reduce_every requires no timestamp
        if (this.timeField &&
            (this.optimization_info.type !== 'reduce' || this.optimization_info.reduce_every)
        ) {
            points = this.parseTime(points, this.timeField);
        }

        return points;
    }

    convertDatesToMoments(points) {
        var timeField = this.timeField || 'time';
        _.each(points, function(pt) {
            if (_.isNumber(pt[timeField])) {
                //sqlite return milliseconds in time field
                pt[timeField] = new Date(pt[timeField]);
            }
            _.each(pt, function(v, k) {
                if (v instanceof Date && v.getTime() === v.getTime()) {
                    pt[k] = new JuttleMoment({ rawDate: v });
                }
            });
        });
    }

    //handle trailing and leading null aggreates
    handleNullAggregates(points) {
        if (_.isMatch(points[0], this.empty_aggregate)) {
            if (this.total_emitted_points > 0) {
                this.bufferEmptyAggregates.push(points[0]);
            }
            return [];
        } else if (this.bufferEmptyAggregates.length > 0) {
            points.unshift.apply(points, this.bufferEmptyAggregates);
            this.bufferEmptyAggregates = [];
        }
        return points;
    }

    handleDebug() {
        this.logger.debug('returning query string as point');
        this.pointsBuffer = [{
            query: this.baseQuery.toString()
        }];
    }
}

module.exports  = ReadSql;
