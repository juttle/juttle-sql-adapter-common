// Configuration for testing common code only.

var _ = require('underscore');
var juttle_test_utils = require('juttle/test').utils;

var REQUIRED_CONFIG_PROPERTIES = ['filename'];

juttle_test_utils.withAdapterAPI(function() {

    var db = require('../lib/db');

    db.getKnex = function(singleDBConfig, options) {
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

        return require('knex')({
            "client": "sqlite3",
            "connection": connection
        });
    };
});

function getAdapterName() {
    return 'sql';
}

function getAdapterConfig() {
    var config = [
        {
            id: 'default',
            filename: "./unit-test.sqlite"
        }, {
            id: 'fake',
            filename: "./not_dir/should_not_work/not_db.sqlite"
        }
    ];
    config.path = "./";

    return config;
}

module.exports = {
    getAdapterName: getAdapterName,
    getAdapterConfig: getAdapterConfig
};
