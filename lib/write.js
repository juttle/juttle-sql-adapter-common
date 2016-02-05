'use strict';

var _ = require('underscore');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');
var errors = require('juttle/lib/errors');
var db = require('./db');
var values = require('juttle/lib/runtime/values');
var Promise = require('bluebird');
var logger = require('juttle/lib/logger').getLogger('sql-db-write');

const ALLOWED_OPTIONS = ['table'];

class WriteSql extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.handleOptions(options);
        this.inserts_in_progress = 0;
        this.eof_received = false;
        this.writePromise = Promise.resolve();
    }

    handleOptions(options) {
        logger.debug('init options:', options);
        var unknown = _.difference(_.keys(options), ALLOWED_OPTIONS);
        if (unknown.length > 0) {
            throw new errors.compileError('RT-UNKNOWN-OPTION-ERROR', {
                proc: 'write-sql',
                option: unknown[0]
            });
        }

        if (!_.has(options, 'table')) {
            throw new errors.compileError('RT-REQUIRED-OPTION-ERROR', {
                proc: 'write-sql',
                option: "table"
            });
        }

        this.table = options.table;
    }

    write(points) {
        if (points.length === 0) {
            return;
        }
        this.writePromise = this.writePromise.then(() => {
            var formattedPoints = this.formatPoints(points);
            return this.insertPoints(formattedPoints);
        });
    }

    formatPoints(points) {
        points = _.filter(points, (pt) => {
            var deep = _.some(pt, function(value, key) {
                return values.isObject(value) || values.isArray(value);
            });

            if (deep) {
                this.trigger('warning', new errors.runtimeError('RT-INTERNAL-ERROR', {
                    error: 'Serializing array and object fields is not supported.'
                }));
            }

            return !deep;
        });

        _.each(points, function(pt) {
            _.each(pt, function(value, key) {
                if (values.isDate(value) || values.isDuration(value)) {
                    pt[key] = new Date(value.valueOf());
                }
            });
        });

        return points;
    }

    insertPoints(points) {
        if (points.length === 0) { return; }

        var knex = db.getDbConnection();

        //XXX splice is a temporary fix until we have shared concurrency handling logic
        return knex(this.table).insert(points.splice(0, 1000))
        .catch((err) => {
            this.trigger('error', new errors.runtimeError('RT-INTERNAL-ERROR', {
                error: err.toString()
            }));
        })
        .then(() => {
            return this.insertPoints(points);
        });
    }

    eof() {
        return this.writePromise;
    }
}

module.exports = WriteSql;
