var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var sampleData = TestUtils.getSampleData();

describe('test time usage', function () {
    it('sql time usage', function() {
        return check_juttle({
            program: 'read sql -table "logs" | reduce -every :3 days: avg = avg(code)'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            //catch single element reduce output
            expect(result.sinks.table).to.have.length.gt(1);

            var time;
            result.sinks.table.forEach(function(pt) {
                if (time) {
                    expect(pt.time).gt(time);
                }
                expect(!!pt.avg).to.be.true;
                expect(isNaN(Date.parse(pt.time))).to.be.false;
                time = pt.time;
            });
        });
    });
    it('sql -to and -from option', function() {
        return check_juttle({
            program: 'read sql -from :20 days ago: -to :10 days ago: -timeField "create_time" -table "logs_create"'
        })
        .then(function(result) {
            expect(result.errors.length).equal(0);
            expect(result.warnings.length).equal(0);

            //10 days difference between from and to
            expect(result.sinks.table.length).equals(10);

            var time;
            result.sinks.table.forEach(function(pt) {
                if (time) {
                    expect(pt.time).gt(time);
                }
                time = pt.time;
            });
        });
    });
    it('sql -from and -to without timeField indicated', function() {
        return check_juttle({
            program: 'read sql -from :20 days ago: -to :10 days ago: -table "logs"'
        })
        .then(function(result) {
            expect(result.errors.length).equal(0);
            expect(result.warnings.length).equal(0);

            //10 days difference between from and to
            expect(result.sinks.table.length).equals(10);

            var time;
            result.sinks.table.forEach(function(pt) {
                if (time) {
                    expect(pt.time).gt(time);
                }
                time = pt.time;
            });
        });
    });
    it('sql -from and -to with time over multiple fetches', function() {
        return check_juttle({
            program: 'read sql -timeField "time" -fetchSize 30 -from :130 days ago: -to :10 days ago: -table "logs"'
        })
        .then(function(result) {
            expect(result.errors.length).equal(0);
            expect(result.warnings.length).equal(0);

            //10 days difference between from and to
            expect(result.sinks.table.length).equals(120);

            var time;
            result.sinks.table.forEach(function(pt) {
                if (time) {
                    expect(pt.time).gt(time);
                }
                time = pt.time;
            });
        });
    });
    it('sql time usage with timeField', function() {
        return check_juttle({
            program: 'read sql -fetchSize 100 -timeField "create_time" -table "logs_create" | reduce -every :3 days: avg = avg(code)'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gte(sampleData.logsCreateTime.length / 3);

            var time;
            result.sinks.table.forEach(function(pt) {
                if (time) {
                    expect(pt.time).gt(time);
                }
                expect(!!pt.avg).to.be.true;
                expect(isNaN(Date.parse(pt.time))).to.be.false;
                time = pt.time;
            });
        });
    });
    it('sql time usage with same timeField err', function() {
        return check_juttle({
            program: 'read sql -fetchSize 100 -timeField "create_time" -table "logs_same_time"'
        })
        .then(function(result) {
            expect(result.warnings).to.have.length(0);
            expect(result.sinks.table).to.have.length(0);

            expect(result.errors).to.have.length(1);
            expect(result.errors[0])
                .to.match(/.* unable to paginate because all of fetchSize 100 has the same timeField.*/);
        });
    });
    it('sql time usage with same timeField without pagination', function() {
        return check_juttle({
            program: 'read sql -timeField "create_time" -table "logs_same_time"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);
            expect(result.sinks.table).to.have.length(sampleData.logsSameTime.length);
        });
    });
    it('sql time usage with invalid timeField when paginating', function() {
        return check_juttle({
            program: 'read sql -fetchSize 100 -timeField "wrong_time_field" -table "logs_create" | reduce -every :3 days: avg = avg(code)'
        })
        .then(function(result) {
            expect(result.warnings).to.have.length(0);
            expect(result.sinks.table).to.have.length(0);

            expect(result.errors).to.have.length(1);
            expect(result.errors[0])
                .to.match(/"wrong_time_field" (is undefined|does not exist)| Unknown column 'wrong_time_field'/);
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
            expect(result.message).equal("Error: missing  required option choose only one: table,raw.");
        });
    });
});
