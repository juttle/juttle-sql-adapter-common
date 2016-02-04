var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var sampleData = TestUtils.getSampleData();

describe('test options', function () {
    before(function() {
        TestUtils.init();
        return TestUtils.loadTables();
    });
    it('-raw', function() {
        return check_juttle({
            program: 'read sql -raw "select * from logs WHERE level = \'error\'"'
        })
        .then(function(result) {
            expect(result.errors[0]).to.equal(undefined);
            expect(result.warnings[0]).to.equal(undefined);

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
    it('sql read from table that is not there', function() {
        return check_juttle({
            program: 'read sql -table "not_there"'
        })
        .then(function(result) {
            expect(result.warnings).to.have.length(0);
            expect(result.sinks.table).to.have.length(0);

            expect(result.errors).to.have.length(1);
            expect(result.errors[0])
                .to.match(/not_there/)
                .and.to.match(/no such table|does(n't| not) exist/);
        });
    });
    it('sql options incorrect error', function() {
        return check_juttle({
            program: 'read sql level = "error"'
        })
        .then(function() {
            throw new Error('We should not get this error');
        })
        .catch(function(result) {
            expect(result.message).to.contain("required option -table or -raw.");
        });
    });
    it('incompatable option error', function() {
        return check_juttle({
            program: 'read sql -table "test" -raw "RAW SQL" level = "error"'
        })
        .then(function() {
            throw new Error('We should not get this error');
        })
        .catch(function(result) {
            expect(result.message).to.contain("-raw option should not be combined with");
        });
    });
});
