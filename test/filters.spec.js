require('./shared');
var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_success = TestUtils.check_juttle_success;
var check_juttle_error = TestUtils.check_juttle_error;

describe('test filters', function () {
    before(function() {
        return TestUtils.createTables(['logs']);
    });

    it('sql works AND', function() {
        return check_success({
            program: 'read sql -table "logs" level="error" host="127.0.0.2"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).equals("error");
                expect(pt.host).equals("127.0.0.2");
            });
        });
    });
    it('sql OR', function() {
        return check_success({
            program: 'read sql -table "logs" level="error" OR host="127.0.0.1" OR host="127.0.0.2"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level === "error" || pt.host === "127.0.0.1" || pt.host === "127.0.0.2").to.be.true;
            });
        });
    });
    it('sql OR with other post-filter conditions', function() {
        return check_success({
            program: 'read sql -table "logs" -from :10 days ago: host="127.0.0.1" OR level="error" '
        })
        .then(function(result) {
            var ten_days_ago = new Date();
            ten_days_ago.setDate(new Date().getDate() - 10);
            ten_days_ago = ten_days_ago.toISOString();

            result.sinks.table.forEach(function(pt) {
                expect(pt.time).gt(ten_days_ago);
                expect(pt.level === "error" || pt.host === "127.0.0.1").to.be.true;
            });
        });
    });
    it('sql OR -> AND', function() {
        return check_success({
            program: 'read sql -table "logs" (level="error" AND host="127.0.0.2") OR (host="127.0.0.1" AND level="info")'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(
                    (pt.level === "error" && pt.host === "127.0.0.2") ||
                    (pt.host === "127.0.0.1" && pt.level === "info")
                ).to.be.true;
            });
        });
    });
    it('sql AND -> OR ', function() {
        return check_success({
            program: 'read sql -table "logs" (host="127.0.0.1" OR host="127.0.0.2") AND (level="error" OR level="info")'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(
                    (pt.host === "127.0.0.1" || pt.host === "127.0.0.2") &&
                    (pt.level === "error"  || pt.level === "info")
                ).to.be.true;
            });
        });
    });
    it('sql LIKE *', function() {
        return check_success({
            program: 'read sql -table "logs" level =~ "*r*"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).equals("error");
            });
        });
    });
    it('sql NOT LIKE _', function() {
        return check_success({
            program: 'read sql -table "logs" level !~ "e_ror"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).not.equals("error");
            });
        });
    });
    it('sql LIKE _', function() {
        return check_success({
            program: 'read sql -table "logs" level =~ "e_o*"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length(0);
        });
    });
    it('sql >', function() {
        return check_success({
            program: 'read sql -table "logs" code > 2'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.code).gt(2);
            });
        });
    });
    it('sql <', function() {
        return check_success({
            program: 'read sql -table "logs" code < 5'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.code).lt(5);
            });
        });
    });
    it('sql <=', function() {
        return check_success({
            program: 'read sql -table "logs" code <= 3'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.code).lte(3);
            });
        });
    });
    it('sql in', function() {
        return check_success({
            program: 'read sql -table "logs" level in ["error","debug"]'
        })
      .then(function(result) {
          expect(result.sinks.table).to.have.length.gt(0);
          result.sinks.table.forEach(function(pt) {
              expect(['debug', 'error']).to.include(pt.level);
          });
      });
    });
    it('sql NOT unary', function() {
        return check_success({
            program: 'read sql -table "logs" NOT level = "error"'
        })
        .then(function(result) {
            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).not.equals('error');
            });
        });
    });
    it('sql filters with dates errors', function() {
        return check_juttle_error({
            program: 'read sql -table "logs" time > :10 days ago:'
        })
        .catch(function(err) {
            expect(err.message).to.contain('Cannot filter on "time" in read');
        });
    });

    it('sql filters with Infinity', function() {
        return check_juttle_error({
            program: 'read sql -table "logs" code > Infinity'
        })
        .catch(function(err) {
            expect(err.message).to.contain('Filters do not support Infinity');
        });
    });
    it('sql filters with NaN', function() {
        return check_juttle_error({
            program: 'read sql -table "logs" code > NaN'
        })
        .catch(function(err) {
            expect(err.message).to.contain('Filters do not support NaN');
        });
    });
    it('sql filters with RegExp', function() {
        return check_juttle_error({
            program: 'read sql -table "logs" code =~ /2/'
        })
        .catch(function(err) {
            expect(err.message).to.contain('Filters do not support regular expressions');
        });
    });
});
