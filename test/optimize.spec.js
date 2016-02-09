require('./shared');
var expect = require('chai').expect;
var TestUtils = require("./utils");
var sampleData = TestUtils.getSampleData();
var check_success = TestUtils.check_juttle_success;
var check_optimization_juttle = TestUtils.check_sql_optimization_juttle;

describe('test optimizations', function() {
    before(function() {
        return TestUtils.createTables(['logs']);
    });

    it('head with positive number', function() {
        return check_optimization_juttle({
            program: 'read sql -table "logs" level = "info" | head 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(5);
        });
    });
    it('head with positive number shows initial limit of 5 in query', function() {
        return check_success({
            program: 'read sql -from :200 days ago: -debug true -table "logs" level = "info" | head 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(1);
            expect(result.sinks.table[0].query)
                .to.match(/limit '?5'?/);
        });
    });
    it('head 0', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -table "logs" level = "info" | head 0'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(0);
        });
    });
    it('head with limit greater than fetchSize', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -fetchSize 2 -table "logs" level = "info" | head 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(5);
        });
    });

    it('tail with positive number', function() {
        return check_optimization_juttle({
            program: 'read sql -table "logs" -from :200 days ago: -timeField "time" level = "info" | tail 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(5);
        });
    });
    it('tail with positive number shows initial limit of 5 in query', function() {
        return check_success({
            program: 'read sql -debug true -timeField "time" -table "logs" level = "info" | tail 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(1);
            expect(result.sinks.table[0].query)
                .to.match(/limit '?5'?/);
            expect(result.sinks.table[0].query)
                .to.match(/ORDER BY ("|`)time("|`) desc/i);
        });
    });
    it('tail 0', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -timeField "time" -table "logs" level = "info" | tail 0'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(0);
        });
    });
    it('tail without a limit defaults to 1', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -timeField "time" -table "logs" level = "info" | tail'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(1);
        });
    });
    it('tail by unoptimized', function() {
        return check_success({
            program: 'read sql -from :200 days ago: -timeField "time" -table "logs" level = "info" | tail 5 by code'
        }).then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
        });
    });
    it('tail with limit greater than fetchSize is not optimized', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -fetchSize 2 -timeField "time" -table "logs" level = "info" | tail 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(5);
        })
        .then(function(result) {
            return check_optimization_juttle({
                program: 'read sql -debug true -to :yesterday: -fetchSize 2 -timeField "time" -table "logs" level = "info" | tail 5'
            });
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(1);
            expect(result.sinks.table[0].query)
                .to.not.match(/limit '?5'?/);
        });
    });

    it('reduce count()', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -table "logs" | reduce count()'
        })
        .then(function(result) {
            expect(result.sinks.table[0].count).to.equal(sampleData.logs.length);
        });
    });
    it('reduce avg, count, max, min, sum (as target s) by field aggregation', function() {
        return check_optimization_juttle({
            program: 'read sql -table "logs" | reduce avg(code), count(level), max(code), min(code), s = sum(code)',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(1);
            var aggr_res = result.sinks.table[0];
            expect(aggr_res.avg).to.be.gt(1);
            expect(aggr_res.count).to.equal(sampleData.logs.length);
            expect(aggr_res.max).to.be.gte(10);
            expect(aggr_res.min).to.be.lte(2);
            expect(aggr_res.s).to.be.gt(sampleData.logs.length);
        });
    });
    it('reduce count_unique (as target s) by field aggregation', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -table "logs" | reduce count_unique(level)'
        })
        .then(function(result) {
            expect(result.sinks.table[0].count_unique).to.equal(2);
        });
    });
    it('ensure fetchSize does not affect outcome of reduce', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -fetchSize 2 -table "logs" | reduce sum(code)'
        })
        .then(function(result) {
            expect(result.sinks.table[0].sum).gte(1);
        });
    });
    it('groupby', function() {
        return check_optimization_juttle({
            program: 'read sql -from :200 days ago: -table "logs" | reduce by level',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(2);
            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('level');
            });
        });
    });
    it('groupby and count', function() {
        return check_optimization_juttle({
            program: 'read sql -table "logs" | reduce count() by level',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(2);
            result.sinks.table.forEach(function(row) {
                expect(row.count).to.be.within(55,95);
            });
        });
    });
    it('multiple groupby count', function() {
        return check_optimization_juttle({
            program: 'read sql -table "logs" | reduce count() by level,code',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(5);

            var unique_set = {};
            result.sinks.table.forEach(function(row) {
                expect(row.count).to.be.gte(1);
                var uniqueStr = row.level + row.code;
                expect(unique_set[uniqueStr]).to.equal(undefined);
                unique_set[uniqueStr] = 1;
            });
        });
    });
    it('reduce every', function() {
        return check_optimization_juttle({
            program: 'read sql -from :20 days ago: -to :3 days ago: -table "logs" | reduce -every :week: count()',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(3);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('count', 'time');
            });
        });
    });
    it('reduce every with timeframe smaller than every param', function() {
        return check_optimization_juttle({
            program: 'read sql -from :8 days ago: -to :4 days ago: -table "logs" | reduce -every :week: count(), a = avg(code)',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(1);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('count', 'time');
            });
        });
    });
    it('reduce every on', function() {
        //first opt elem has count = 0 (not included in unopt) and last time is -3d not full week. which is right?
        return check_optimization_juttle({
            program: 'read sql -from :20 days ago: -to :3 days ago: -table "logs" | reduce -every :week: -on :day 1: count()',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(3);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('count', 'time');
            });
        });
    });
    it('reduce every multi-aggr', function() {
        return check_optimization_juttle({
            program: 'read sql -from :20 days ago: -to :3 days ago: -table "logs" | reduce -every :week: c = count(), a = avg(code)',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(3);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('c', 'a', 'time');
            });
        });
    });
    it('reduce every empty aggregates on before and after', function() {
        return check_optimization_juttle({
            program: 'read sql -from :170 days ago: -to :104 days ago: -table "logs" |' +
                'reduce -every :week: count(), a = avg(code), max(code), min(code), s = sum(code), count_unique(code)',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(3);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('count', 'a', 'max', 'min', 's', 'count_unique');
            });
        });
    });
    it('reduce every with aggregation and groupby', function() {
        return check_optimization_juttle({
            program: 'read sql -from :20 days ago: -to :3 days ago: -table "logs" | reduce -every :week: a = avg(code) by level',
            massage: true
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(3);

            result.sinks.table.forEach(function(row) {
                expect(row).to.include.keys('a', 'level', 'time');
            });
        });
    });
});
