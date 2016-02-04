var expect = require('chai').expect;
var TestUtils = require("./utils");
var check_juttle = TestUtils.check_sql_juttle;

describe('test filters', function () {
    before(function() {
        TestUtils.init();
        return TestUtils.loadTables();
    });
    it('sql works AND', function() {
        return check_juttle({
            program: 'read sql -table "logs" level="error" host="127.0.0.2"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).equals("error");
                expect(pt.host).equals("127.0.0.2");
            });
        });
    });
    it('sql OR', function() {
        return check_juttle({
            program: 'read sql -table "logs" level="error" OR host="127.0.0.1" OR host="127.0.0.2"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level === "error" || pt.host === "127.0.0.1" || pt.host === "127.0.0.2").to.be.true;
            });
        });
    });
    it('sql OR -> AND', function() {
        return check_juttle({
          program: 'read sql -table "logs" (level="error" AND host="127.0.0.2") OR (host="127.0.0.1" AND level="info")'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

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
        return check_juttle({
          program: 'read sql -table "logs" (host="127.0.0.1" OR host="127.0.0.2") AND (level="error" OR level="info")'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

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
        return check_juttle({
          program: 'read sql -table "logs" level =~ "*r*"'
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
    it('sql NOT LIKE _', function() {
        return check_juttle({
            program: 'read sql -table "logs" level !~ "e_ror"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).not.equals("error");
            });
        });
    });
    it('sql LIKE _', function() {
        return check_juttle({
            program: 'read sql -table "logs" level =~ "e_o*"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length(0);
        });
    });
    it('sql >', function() {
        return check_juttle({
          program: 'read sql -table "logs" code > 2'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

          expect(result.sinks.table).to.have.length.gt(0);
          result.sinks.table.forEach(function(pt) {
              expect(pt.code).gt(2);
          });
      });
    });
    it('sql <', function() {
        return check_juttle({
          program: 'read sql -table "logs" code < 5'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

          expect(result.sinks.table).to.have.length.gt(0);
          result.sinks.table.forEach(function(pt) {
              expect(pt.code).lt(5);
          });
      });
    });
    it('sql <=', function() {
        return check_juttle({
          program: 'read sql -table "logs" code <= 3'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

          expect(result.sinks.table).to.have.length.gt(0);
          result.sinks.table.forEach(function(pt) {
              expect(pt.code).lte(3);
          });
      });
    });
    it('sql in', function() {
        return check_juttle({
          program: 'read sql -table "logs" level in ["error","debug"]'
      })
      .then(function(result) {
          expect(result.errors).to.have.length(0);
          expect(result.warnings).to.have.length(0);

          expect(result.sinks.table).to.have.length.gt(0);
          result.sinks.table.forEach(function(pt) {
              expect(['debug', 'error']).to.include(pt.level);
          });
      });
    });
    it('sql NOT unary', function() {
        return check_juttle({
            program: 'read sql -table "logs" NOT level = "error"'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.gt(0);
            result.sinks.table.forEach(function(pt) {
                expect(pt.level).not.equals('error');
            });
        });
    });
    it('sql filters with dates', function() {
        return check_juttle({
            program: 'read sql -table "logs" time > :10 days ago:'
        })
        .then(function(result) {
            expect(result.errors).to.have.length(0);
            expect(result.warnings).to.have.length(0);

            expect(result.sinks.table).to.have.length.within(9,11);
        });
    });
});
