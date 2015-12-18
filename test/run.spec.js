var SharedSqlTests = require('./shared-sql.spec');
var sql = require('../');
var TestUtils = require("./utils");

var config = {
    "knex_conf" : {
        "client": "sqlite3",
        "connection": ":memory:"
    }
};
TestUtils.init(config, sql);
SharedSqlTests();
