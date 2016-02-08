require('./shared');
var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;
var check_success = TestUtils.check_juttle_success;
var sampleData = TestUtils.getSampleData();
var logger = require('juttle/lib/logger').getLogger('sql-time-test');

describe('test time usage', function () {
    before(function() {
        return TestUtils.createTables(['logs', 'logs_create', 'logs_same_time', 'simple']);
    });

    it('sql time usage', function() {
        return check_success({
            program: 'read sql -from :100 days ago: -to :now: -table "logs" | reduce -every :3 days: avg = avg(code)'
        })
        .then(function(result) {
            //catch single element reduce output
            expect(result.sinks.table).to.have.length.gt(1);

            TestUtils.expectTimeSorted(result);
            result.sinks.table.forEach(function(pt) {
                expect(!!pt.avg).to.be.true;
            });
        });
    });
    it('sql -to and -from option', function() {
        return check_success({
            program: 'read sql -from :20 days ago: -to :10 days ago: -timeField "create_time" -table "logs_create"'
        })
        .then(function(result) {
            //10 days difference between from and to
            expect(result.sinks.table.length).equals(10);

            TestUtils.expectTimeSorted(result);
        });
    });
    it('sql -from and -to without timeField indicated', function() {
        return check_success({
            program: 'read sql -from :20 days ago: -to :10 days ago: -table "logs"'
        })
        .then(function(result) {
            //10 days difference between from and to
            expect(result.sinks.table.length).equals(10);

            TestUtils.expectTimeSorted(result);
        });
    });
    it('sql -from and -to with time over multiple fetches', function() {
        return check_success({
            program: 'read sql -timeField "time" -fetchSize 30 -from :130 days ago: -to :10 days ago: -table "logs"'
        })
        .then(function(result) {
            //10 days difference between from and to
            expect(result.sinks.table.length).equals(120);

            TestUtils.expectTimeSorted(result);
        });
    });
    it('sql time usage with timeField', function() {
        return check_success({
            program: 'read sql -fetchSize 100 -from :200 days ago: -timeField "create_time" -table "logs_create" | reduce -every :3 days: avg = avg(code)'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gte(sampleData.logsCreateTime.length / 3);

            TestUtils.expectTimeSorted(result);
            result.sinks.table.forEach(function(pt) {
                expect(!!pt.avg).to.be.true;
            });
        });
    });
    it('sql time usage with same timeField err', function() {
        return check_juttle({
            program: 'read sql -fetchSize 100 -from :200 days ago: -timeField "create_time" -table "logs_same_time"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(1);
            expect(result.errors[0])
                .to.match(/.*unable to paginate because all of fetchSize 100 has the same timeField.*/);
        });
    });
    it('sql time usage with same timeField without pagination', function() {
        return check_success({
            program: 'read sql -from :200 days ago: -timeField "create_time" -table "logs_same_time"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(sampleData.logsSameTime.length);
        });
    });
    it('sql live', function() {
        this.timeout(10000);
        var table_name = 'live';
        var numFuturePoints;
        return TestUtils.addFuturePoints(table_name)
        .then(function(num_future_points) {
            expect(num_future_points).to.be.gt(0);
            numFuturePoints = num_future_points;

            var wait = numFuturePoints * 1000 + 2000;
            logger.info('Performing live query, waiting ms:', wait);


            return check_success({
                program: `read sql -from :now: -to :end: -table "${table_name}"`,
                realtime: true
            }, wait);
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(numFuturePoints);
        });
    });
    it('sql live super query', function() {
        this.timeout(10000);
        var table_name = 'live_super';
        var numFuturePoints;
        return TestUtils.addFuturePoints(table_name)
        .then(function(num_future_points) {
            expect(num_future_points).to.be.gt(0);
            numFuturePoints = num_future_points;

            var wait = numFuturePoints * 1000 + 2000;
            logger.info('Performing super live query, waiting ms:', wait);

            expect(num_future_points).to.be.gt(0);
            numFuturePoints = num_future_points;

            return check_success({
                program: `read sql -from :200 days ago: -to :end: -table "${table_name}"`,
                realtime: true
            }, wait);
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(numFuturePoints + sampleData.logs.length);
        });
    });
});
