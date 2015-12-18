var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var sampleData = TestUtils.getSampleData();

describe('test options', function () {
    it('-raw', function() {
        return check_juttle({
            program: 'read sql -raw "select * from logs WHERE level = \'error\'"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).equals("error");
            });
        });
    });
    it('-debug', function() {
        return check_juttle({
            program: 'read sql -debug true -fetchSize 100 -table "logs" level !~ "e_ror"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length(1);
            var no_quotes_query = result.sinks.table[0].query.replace(/\'|\"|\`/g, '');
            expect(no_quotes_query).equals('select * from logs where level NOT LIKE e_ror limit 100');
        });
    });
    it('-table', function() {
        return check_juttle({
            program: 'read sql -table "logs"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length(sampleData.logs.length);
        });
    });
    it('-fetchSize', function() {
        return check_juttle({
            program: 'read sql -fetchSize 100 -table "logs"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length(sampleData.logs.length);
        });
    });
});
