var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;

describe('test db connection error', function () {
    before(function() {
        TestUtils.clearState();
        return TestUtils.init(true);
    });
    after(function() {
        TestUtils.clearState();
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
