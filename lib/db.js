'use strict';

/* global JuttleAdapterAPI */
let logger = JuttleAdapterAPI.getLogger('sql-db-init');

let _ = require('underscore');

let knex;
let savedDbId;
let confArr;

class DB {
    static init(config) {
        confArr = _.isArray(config) ? config : [config];
    }

    static getDbConnection(options) {
        options = options || {};

        let dbid = `${options.id}-${options.db}`;
        if (knex && dbid === savedDbId) {
            return knex;
        }

        let conf = _.findWhere(confArr, {id: options.id}) || confArr[0];

        //ensure that no changes are permanently made to the config in getKnex function.
        conf = _.clone(conf);

        knex = this.getKnex(conf, options);
        savedDbId = dbid;

        if (knex.client) {
            logger.info('initializing db connection with conf:', knex.client.config);
        } else {
            knex = null;
            throw new Error('knex config for sql adapter not found:' + JSON.stringify(options));
        }
        return knex;
    }

    static getKnex(config, options) {
        throw new Error('getKnex function not implemented by adapter');
    }

    static closeConnection() {
        if (knex) {
            knex.destroy();
            knex = null;
        }
    }
}

module.exports = DB;
