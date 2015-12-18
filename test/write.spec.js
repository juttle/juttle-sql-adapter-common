var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;

describe('write proc', function () {
    before(function() {
        return TestUtils.createWritingTable();
    });

    it('write points', function() {
        return check_juttle({
            program: 'emit -limit 1 | put a = "temp_str" | put b = "temp_str2" | write sql -table "sqlwriter"'
        })
        .then(function(result) {
            //TODO make all errors do this so that it's easier to see the error right away.
            expect(result.errors[0]).equals(undefined);
            expect(result.warnings).to.have.length(0);
            expect(result.sinks).to.not.include.keys('table', 'logger');
        })
        .then(function() {
            return check_juttle({
                program: 'read sql -table "sqlwriter" a = "temp_str"'
            });
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);
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
    it('write proc error when fields do not match columns', function() {
        return check_juttle({
            program: 'emit -limit 1 | put c = "hi" | put d = "yes" | write sql -table "sqlwriter"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.match(/(has no column|column .* does not exist|Unknown column 'c')/);
        });
    });
});
