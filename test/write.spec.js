require('./shared');
var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var check_success = TestUtils.check_juttle_success;

describe('write proc', function () {
    before(function() {
        return TestUtils.createTables(['sqlwriter']);
    });

    it('write points', function() {
        return check_success({
            program: 'emit -limit 1 | put a = "temp_str" | put b = "temp_str2" | write sql -table "sqlwriter"'
        })
        .then(function(result) {
            expect(result.sinks).to.not.include.keys('table', 'logger');
        })
        .then(function() {
            return check_success({
                program: 'read sql -table "sqlwriter" a = "temp_str"'
            });
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);

            var pt = result.sinks.table[0];

            expect(pt.a).equals('temp_str');
            expect(pt.b).equals('temp_str2');

            var pt_time = Date.parse(pt.time);
            expect(isNaN(pt_time)).to.be.false;

            //test if date makes sense.
            var today = new Date();
            var yesterday = new Date();
            yesterday.setDate(today.getDate() - 1);
            expect(pt_time).gt(Date.parse(yesterday));
        });
    });

    it('write array/object fields triggers a warning', function() {
        return check_juttle({
            program: 'emit -limit 1 | put a = { key: "val", arr: [1,2,3] } | put b = "test" | write sql -table "sqlwriter"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.equal(undefined);
            expect(result.warnings[0]).to.include('not supported');
        });
    });

    it('write proc error when fields do not match columns', function() {
        return check_juttle({
            program: 'emit -limit 1 | put c = "hi" | put d = "yes" | write sql -table "sqlwriter"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.match(/(has no column|column .* does not exist|Unknown column 'c')/);
        });
    });
});
