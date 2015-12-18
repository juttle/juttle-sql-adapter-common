var _ = require('underscore');
var db = require('./db');
var Juttle = require('juttle/lib/runtime').Juttle;
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

var logger = require('juttle/lib/logger').getLogger('sql-db-write');

var Write = Juttle.proc.sink.extend({
    procName: 'write-sql',

    allowed_options: ['table'],

    initialize: function(options, params) {
        this.handleOptions(options);
        this.inserts_in_progress = 0;
        this.eof_received = false;
    },

    handleOptions: function(options) {
        logger.debug('init options:', options);
        var unknown = _.difference(_.keys(options), this.allowed_options);
        if (unknown.length > 0) {
            throw this.compile_error('RT-UNKNOWN-OPTION-ERROR', {
                proc: this.procName,
                option: unknown[0]
            });
        }

        if (!_.has(options, 'table')) {
            throw this.compile_error('RT-REQUIRED-OPTION-ERROR', {
                proc: this.procName,
                option: "table"
            });
        }

        this.table = options.table;
    },

    process: function(points) {
        if (points.length === 0) {
            return;
        }

        var formattedPoints = this.formatPoints(points);
        this.insertPoints(formattedPoints);
    },

    formatPoints: function(points) {
        _.each(points, function(pt) {
            _.each(pt, function(value, key) {
                if (value instanceof JuttleMoment) {
                    pt[key] = new Date(value.valueOf());
                }
            });
        });

        return points;
    },

    insertPoints: function(points) {
        var self = this;

        this.inserts_in_progress++;

        var knex = db.getDbConnection();

        return knex(this.table).insert(points)
        .catch(function(err) {
            self.trigger('error', self.runtime_error('RT-INTERNAL-ERROR', {
                 error: err.toString()
             }));
        }).finally(function() {
            self.tryFinish(true);
        });
    },

    eof: function() {
        logger.debug('eof fired');
        this.eof_received = true;
        this.tryFinish();
    },
    //ensure no query is in progess when "done" is called.
    tryFinish: function(batchProcessed) {
        if (batchProcessed) {
            this.inserts_in_progress--;
        }
        if (this.eof_received && this.inserts_in_progress <= 0) {
            logger.debug('proc is done');
            this.done();
        }
    }
});

module.exports = Write;
