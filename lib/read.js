'use strict';

/* global JuttleAdapterAPI */
let AdapterRead = JuttleAdapterAPI.AdapterRead;
let JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
let errors = JuttleAdapterAPI.errors;

let _ = require('underscore');
let Promise = require('bluebird');

let db = require('./db');
let FilterSQLCompiler = require('./sql-filter');

const BATCHING_CONCURRENCY = 3;

class ReadSql extends AdapterRead {

    static get FETCH_SIZE_LIMIT() { return 10000; }

    static allowedOptions() {
        return AdapterRead.commonOptions().concat(['table', 'raw', 'debug', 'fetchSize', 'optimize', 'db', 'id']);
    }

    //initialization functions

    constructor(options, params) {
        super(options, params);
        this.validateOptions(options);
        this.setOptions(options);

        this.pointsBuffer = [];
        this.baseQuery = db.getDbConnection(_.pick(options, 'db', 'id'));

        this.total_emitted_points = 0;
        this.maxSize = Infinity;
        this.queueSize = Infinity;
        this.offsetCount = 0;

        this.toRaw = this.baseQuery.raw;

        if (options.raw) {
            this.baseQuery = this.toRaw(options.raw);
            return;
        }

        if (params.filter_ast) {
            this.baseQuery = this.addFilters(params.filter_ast);
        }

        this.addOptimizations(params.optimization_info);

        this.baseQuery = this.baseQuery.from(options.table);

        if (this.timeField && this.optimization_info.type !== 'reduce') {
            this.baseQuery = this.baseQuery.orderBy(this.timeField, this.tailOptimized ? 'desc' : 'asc');
        }
    }

    periodicLiveRead() {
        return !!this.timeField;
    }

    validateOptions(options) {
        if (_.has(options, 'table') && _.has(options, 'raw') ||
            (_.has(options, 'debug') && _.has(options, 'raw'))
        ) {
            throw new errors.runtimeError('INCOMPATIBLE-OPTION', {
                option: 'raw',
                other: 'table or -debug',
            });
        }

        if (!_.has(options, 'table') && !_.has(options, 'raw')) {
            throw new errors.runtimeError('MISSING-OPTION', {
                option: '-table or -raw',
                proc: 'read-sql'
            });
        }
        if (options.timeField && !options.from && !options.to) {
            throw new errors.runtimeError('MISSING-OPTION', {
                option: '-from, -to, or -last when -timeField is specified',
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
        let compiler = new FilterSQLCompiler({ baseQuery: this.baseQuery });
        return compiler.compile(filter_ast);
    }

    addOptimizations(optimization_info) {
        this.optimization_info = optimization_info || {};

        if (!_.isEmpty(this.optimization_info)) {
            this.logger.debug('adding optimizations: ', optimization_info);
            if (optimization_info.type === 'head' || optimization_info.type === 'tail') {
                this.logger.debug(optimization_info.type + ' optimization, new max size: ', optimization_info.limit);
                this.maxSize = optimization_info.limit;
                this.tailOptimized = optimization_info.type === 'tail';
            }
            if (optimization_info.type === 'reduce') {
                let groupby = optimization_info.groupby;
                if (groupby && groupby.length > 0) {
                    this.logger.debug('adding groupby: ', groupby);
                    this.baseQuery = this.baseQuery.select(groupby).groupBy(groupby);
                }
                this.aggregation_targets = [];
                _.each(optimization_info.aggregations, (aggregation, target_field) => {
                    this.logger.debug('adding aggregation: ', {
                        func: aggregation.name,
                        by_field: aggregation.field,
                        target_field: target_field
                    });

                    if (aggregation.raw) {
                        this.baseQuery = this.baseQuery.select(this.toRaw(aggregation.raw));
                    } else {
                        this.baseQuery = this.baseQuery[aggregation.name](aggregation.field + ' as ' + target_field);
                    }

                    this.aggregation_targets.push(target_field);
                });
                if (optimization_info.reduce_every) {
                    if (this.aggregation_targets.length > 0) {
                        this.empty_aggregate = optimization_info.empty_aggregate;
                        this.expect_empty_aggregate = optimization_info.expect_empty_aggregate;
                        this.bufferEmptyAggregates = [];
                    }
                }
            }
        }
    }

    getNextLimit() {
        return Math.min(this.fetchSize, this.queueSize, this.maxSize - this.total_emitted_points);
    }

    //Query execution

    read(from, to, limit, state) {
        return Promise.try(() => {
            this.queueSize = limit;

            if (this.raw) {
                return this.getRawResult();
            }

            let query = this.getPaginationQuery(from, to);

            if (this.debugOption) {
                return this.handleDebug(query);
            }
            if (this.optimization_info.reduce_every) {
                return this.getReduceEveryResults(query, from, to);
            }
            return this.executeQuery(query, to);
        })
        .catch((err) => {
            if (/Pool (is|was) destroyed/.test(err.message)) {
                let connectionInfo = this.baseQuery.client.connectionSettings;
                throw errors.runtimeError('INTERNAL-ERROR', {
                    error: 'could not connect to database: ' + JSON.stringify(connectionInfo)
                });
            } else {
                throw err;
            }
        });
    }

    // raw query

    getRawResult() {
        return this.baseQuery
        .then((res) => {
            return this.handleRawResponse(res);
        })
        .then((points) => {
            return {
                points: this.formatPoints(points),
                readEnd: new JuttleMoment(Infinity)
            };
        });
    }

    //the raw query response format can be different based on client
    handleRawResponse(points) {
        return points;
    }

    getPaginationQuery(from, to) {
        let query = this.baseQuery.clone();

        query = query.limit(this.getNextLimit());

        if (this.timeField) {
            if (from) {
                query = query.where(this.timeField, '>=', new Date(from.valueOf()));
                this.logger.debug('start time', new Date(from.valueOf()).toISOString());
            }
            if (to) {
                query = query.where(this.timeField, '<', new Date(to.valueOf()));
                this.logger.debug('end time', new Date(to.valueOf()).toISOString());
            }
        } else {
            query = query.offset(this.offsetCount);
        }
        return query;
    }

    // debug option handler

    handleDebug(query) {
        this.logger.debug('returning query string as point');
        return  {
            points: [{ query: query.toString() }],
            readEnd: new JuttleMoment(Infinity)
        };
    }

    // "reduce -every" batch query

    getReduceEveryResults(original_query, from, to) {
        let timeField = this.timeField || 'time';
        let buckets = this.getBatchBuckets(from, to);

        return Promise.map(buckets, (bucket, i, len) => {
            let start = bucket;
            let end = buckets[i + 1];
            if (!end) {
                return [];
            }

            let query = original_query.clone();
            query = query.where(timeField, '>=', new Date(start.valueOf()));
            query = query.where(timeField, '<', new Date(end.valueOf()));

            let additionalFields = {};
            bucket.epsilon = true;
            additionalFields[timeField] = end;

            return query.then((points) => {
                _.each(points, function(pt) {
                    _.extend(pt, additionalFields);
                });
                return points;
            });
        }, { concurrency: BATCHING_CONCURRENCY })
        .then((res) => {
            let sortedPoints = _.chain(res)
            .flatten()
            .sortBy(timeField)
            .value();

            this.formatPoints(sortedPoints);

            return {
                points: this.omitNullAggregates(sortedPoints),
                readEnd: to
            };
        });
    }

    getBatchBuckets(query_start, query_end) {
        let reduce_every = this.optimization_info.reduce_every;
        let reduce_on = this.optimization_info.reduce_on;

        function get_batch_offset_as_duration(every_duration) {
            try {
                return JuttleMoment.duration(reduce_on);
            } catch (err) {
                // translate a non-duration -on into the equivalent duration
                // e.g. if we're doing -every :hour: -on :2015-03-16T18:32:00.000Z:
                // then that's equivalent to -every :hour: -on :32 minutes:
                let moment = new JuttleMoment(reduce_on);
                return JuttleMoment.subtract(moment, JuttleMoment.quantize(moment, every_duration));
            }
        }

        let buckets = [query_start];

        let duration = JuttleMoment.duration(reduce_every);
        let zeroth_bucket = JuttleMoment.quantize(query_start, duration);
        let last_bucket = JuttleMoment.quantize(query_end, duration);

        if (reduce_on) {
            let offset = get_batch_offset_as_duration(duration);
            zeroth_bucket = JuttleMoment.add(zeroth_bucket, offset);
            if (zeroth_bucket.gt(query_start)) {
                buckets.push(zeroth_bucket);
            }
            last_bucket = JuttleMoment.add(last_bucket, offset);
        }

        let intermediate_bucket = JuttleMoment.add(zeroth_bucket, duration);

        while (intermediate_bucket.lte(last_bucket)) {
            buckets.push(intermediate_bucket);
            intermediate_bucket = JuttleMoment.add(intermediate_bucket, duration);
        }

        //this replaces end_query as the final aggregation time
        //end_query right endpoint already in the SQL where clause
        buckets.push(intermediate_bucket);

        _.each(buckets, (b, i) => {
            this.logger.debug('time bucket ' + i, new Date(b.valueOf()).toISOString());
        });

        return buckets;
    }

    // query execution for timed/offset pagination

    executeQuery(query, to) {
        this.logger.debug('executing query', query.toString());

        return query.then((points) => {
            if (points.length < this.fetchSize) {
                //no more pagination necessary
                return {
                    points: points,
                    readEnd: this.timeField ? to : new JuttleMoment(Infinity)
                };
            }
            //perform time-based pagination if timeField is indicated
            if (this.timeField) {
                let res = this.processPaginatedResults(points, this.timeField);
                res.readEnd = new JuttleMoment({rawDate: new Date(res.borderValue)});
                return res;
            }

            //offset pagination
            this.offsetCount += points.length;
            return {
                points: points,
                readEnd: null //read again
            };
        })
        .then((res) => {
            this.formatPoints(res.points);
            return res;
        });
    }

    // utility function to help with pagination:
    //      - pass in an page of sorted results and specify the sort field
    //      - returns formatted points used and border value
    processPaginatedResults(resultsPage, sortField) {
        let nonBorderPoints = [];
        let len = resultsPage.length;
        let borderValue = _.last(resultsPage)[sortField];
        if (!borderValue) {
            throw new Error(`value of field "${sortField}" is ${typeof borderValue}`);
        }

        for (let i = len - 2; i >= 0 ; i--) {
            //use isEqual in order to compare Date objects correctly
            if (!_.isEqual(resultsPage[i][sortField], borderValue)) {
                nonBorderPoints = resultsPage.slice(0, i + 1);
                break;
            }
        }
        if (nonBorderPoints.length === 0) {
            throw new Error(
                `unable to paginate because all of fetchSize ` +
                `${this.fetchSize} has the same ${sortField}. Consider increasing fetchSize.`
            );
        }

        return {
            points: nonBorderPoints,
            borderValue: borderValue
        };
    }

    // shared code between all query types

    formatPoints(points) {
        if (this.addPointFormatting) {
            this.addPointFormatting(points, this.aggregation_targets);
        }

        if (this.tailOptimized) {
            points.reverse();
        }
        this.convertDatesToMoments(points);

        // optimized reduce without reduce_every requires no timestamp
        if (this.timeField &&
            (this.optimization_info.type !== 'reduce' || this.optimization_info.reduce_every)
        ) {
            points = this.parseTime(points, { timeField: this.timeField });
        }

        this.total_emitted_points += points.length;
        return points;
    }

    convertDatesToMoments(points) {
        let timeField = this.timeField || 'time';
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

    omitNullAggregates(points) {
        let result = [];
        let bufferEmptyAggregates = [];

        _.each(points, (pt) => {
            if (_.isMatch(pt, this.empty_aggregate)) {
                if (result.length > 0) {
                    _.extend(pt, this.expect_empty_aggregate);
                    bufferEmptyAggregates.push(pt);
                }
                return;
            } else if (bufferEmptyAggregates.length > 0) {
                result = result.concat(bufferEmptyAggregates);
                bufferEmptyAggregates = [];
            }
            result.push(pt);
        });
        return result;
    }
}

module.exports  = ReadSql;
