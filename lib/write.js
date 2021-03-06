'use strict';

/* global JuttleAdapterAPI */
let AdapterWrite = JuttleAdapterAPI.AdapterWrite;
let values = JuttleAdapterAPI.runtime.values;
let logger = JuttleAdapterAPI.getLogger('sql-db-write');

let _ = require('underscore');
let Promise = require('bluebird');

class WriteSql extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.handleOptions(options);
        this.knex = this.getDbConnection(_.pick(options, 'db', 'id'));
        this.inserts_in_progress = 0;
        this.eof_received = false;
        this.writePromise = Promise.resolve();
    }

    static allowedOptions() {
        return ['table', 'db', 'id'];
    }

    getDbConnection() {
        throw new Error('getDBConnection not implemented by adapter');
    }

    handleOptions(options) {
        logger.debug('init options:', options);

        if (!_.has(options, 'table')) {
            throw this.compileError('REQUIRED-OPTION', {
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
            let formattedPoints = this.formatPoints(points);
            return this.insertPoints(formattedPoints);
        }).catch((err) => {
            if (/Pool (is|was) destroyed/.test(err.message)) {
                let connectionInfo = this.knex.client.connectionSettings;
                err = this.runtimeError('INTERNAL-ERROR', {
                    error: 'could not connect to database: ' + JSON.stringify(connectionInfo)
                });
            }
            this.trigger('error', err);
        });
    }

    formatPoints(points) {
        points = _.filter(points, (pt) => {
            let deep = _.some(pt, function(value, key) {
                return values.isObject(value) || values.isArray(value);
            });

            if (deep) {
                this.trigger('warning', this.runtimeError('INTERNAL-ERROR', {
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

        //XXX splice is a temporary fix until we have shared concurrency handling logic
        return this.knex(this.table).insert(points.splice(0, 1000))
        .then(() => {
            return this.insertPoints(points);
        });
    }

    eof() {
        return this.writePromise;
    }
}

module.exports = WriteSql;
