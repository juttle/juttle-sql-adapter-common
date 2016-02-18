var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;

describe('test db connection error', function () {
    it('incorrect connection string or credentials', function() {
        return check_juttle({
            program: 'read sql -id "fake" -table "fake"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.contain('could not connect to database');
            expect(result.errors[0]).to.contain('should_not_work');
        });
    });
    it('choose db with option', function() {
        var fake_db_name = "./fake/database";
        return check_juttle({
            program: `read sql -db "${fake_db_name}" -raw "SELECT * FROM hello"`
        })
        .then(function(result) {
            expect(result.errors[0]).to.contain('could not connect to database');
            expect(result.errors[0]).to.match(/(db|filename|database)":".\/fake\/database"/);
        });
    });
});
