// Db connection retreival for sqlite.
// This class must live in this repo because it used for unit tests.
// This class is also used by the juttle-sqlite-adapter for code-reuse purposes.

'use strict';

let _ = require('underscore');
let Knex = require('knex');

let SqlCommonDB = require('./lib/db');

var REQUIRED_CONFIG_PROPERTIES = ['filename'];

class DB extends SqlCommonDB {

    static getKnex(singleDBConfig, options) {

        options = options || {};
        if (options.db) {
            singleDBConfig.filename = options.db;
        }

        _.each(REQUIRED_CONFIG_PROPERTIES, function(prop) {
            if (!singleDBConfig.hasOwnProperty(prop)) {
                throw new Error('Each configuration must contain a field: ' + prop);
            }
        });

        var connection = {
            filename: singleDBConfig.filename
        };

        return Knex({
            "client": "sqlite3",
            "connection": connection
        });
    }
}
module.exports = DB;
