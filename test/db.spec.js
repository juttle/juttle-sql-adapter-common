var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var AdapterClass = require('../');

describe('test db connection error', function () {
    before(function() {
        var config = {
            "knex_conf" : {
                "client": "sqlite3",
                "connection": {
                    filename: "./not_dir/not_dir/not_db.sqlite"
                }
            }
        };
        var sqlTest = function(conf) {
            var sql = AdapterClass.call(this, conf);
            sql.name = 'sqltest';
            return sql;
        };
        return TestUtils.init(config, sqlTest);
    });
    it('error on incorrect connection string or credentials', function() {
        return check_juttle({
            program: 'read sql -table "fake"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.contain('could not connect to database');
        });
    });
});
