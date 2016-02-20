/* global JuttleAdapterAPI */
var logger = JuttleAdapterAPI.getLogger('sql-db-init');

var _ = require('underscore');
var knex;
var savedDbId;
var confArr;

var DB = {
    init: function(config) {
        confArr = _.isArray(config) ? config : [config];
    },

    getDbConnection: function (options) {
        options = options || {};

        var dbid = `${options.id}-${options.db}`;
        if (knex && dbid === savedDbId) {
            return knex;
        }

        var conf = _.findWhere(confArr, {id: options.id}) || confArr[0];

        //ensure that no changes are permanently made to the config in getKnex function.
        conf = _.clone(conf);

        knex = DB.getKnex(conf, options);
        savedDbId = dbid;

        if (knex.client) {
            logger.info('initializing db connection with conf:', knex.client.config);
        } else {
            knex = null;
            throw new Error('knex config for sql adapter not found:' + JSON.stringify(options));
        }
        return knex;
    },

    getKnex: function(config, options) {
        throw new Error('getKnex function not implemented by adapter');
    },

    closeConnection: function() {
        if (knex) {
            knex.destroy();
            knex = null;
        }
    }
};

module.exports = DB;
