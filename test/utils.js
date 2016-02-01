var sampleData = require("./sample_data");
var Promise = require('bluebird');
var _ = require('underscore');
var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var expect = require('chai').expect;
var Juttle = require('juttle/lib/runtime').Juttle;

var knex;
var adapter;
var defaultTablesCreated;

var TestUtils = {
    init: function (adapterConfig, adapterClass) {
        adapter = adapterClass(adapterConfig);

        knex = adapter.knex;
        Juttle.adapters.register(adapter.name, adapter);
    },
    getSampleData: function () {
        return sampleData;
    },
    getAdapterName: function () {
        return adapter.name;
    },
    loadTables: function (arrTablesNames) {
        return Promise.try(function() {
            if (defaultTablesCreated) {

                return sampleData;
            }
            defaultTablesCreated = true;

            return TestUtils.createLogTable('logs', 'time', sampleData.logs)
            .then(function() {
                return TestUtils.createLogTable('logs_create', 'create_time', sampleData.logsCreateTime);
            }).then(function() {
                return TestUtils.createLogTable('logs_same_time', 'create_time', sampleData.logsSameTime);
            }).then(function() {
                return TestUtils.createSimpleTable();
            }).then(function() {
                return sampleData;
            });
        });
    },
    endConnection: function() {
        knex.destroy();
    },
    createLogTable: function(table_name, time_field, data) {
        return knex.schema.dropTableIfExists(table_name)
        .then(function() {
            return knex.schema.createTable(table_name, function (table) {
                table.timestamp(time_field);
                table.string('host');
                table.string('msg');
                table.string('level');
                table.integer('code');
            });
        })
        .then(function() {
            // randomize data to ensure proper sorting
            var randomOrderData = data.sort(function() {
                return 0.5 - Math.random();
            });
            return knex(table_name).insert(randomOrderData);
        });
    },
    addFuturePoints: function(table_name) {
        var sample_data_arr = sampleData.logs;

        return TestUtils.createLogTable(table_name, 'time', sample_data_arr)
        .then(() => {
            var sample_pt = sample_data_arr[0];
            var future_data = _.map(_.range(3), function(n) {
                var pt = _.clone(sample_pt);
                pt.time = new Date(Date.now() + (n * 1000) + 2000);
                return pt;
            });
            return knex(table_name).insert(future_data)
            .then(() => future_data.length);
        });
    },
    createSimpleTable: function () {
        return knex.schema.dropTableIfExists('simple')
        .then(function() {
            return knex.schema.createTable('simple', function (table) {
                table.string('name');
                table.integer('id');
            });
        })
        .then(function() {
            return knex('simple').insert(sampleData.simple);
        });
    },
    createWritingTable: function () {
        return knex.schema.dropTableIfExists('sqlwriter')
        .then(function() {
            return knex.schema.createTable('sqlwriter', function (table) {
                table.timestamp('time');
                table.string('a');
                table.string('b');
            });
        });
    },
    massage: function(arr, shouldMassage) {
        if (!shouldMassage) {
            return arr;
        }
        return _.chain(arr)
        .sortBy('level')
        .sortBy('code')
        .each(function(pt) {
            var k, v;
            for (k in pt) {
                v = pt[k];
                if (_.isNumber(v) && (v % 1)) {
                    pt[k] = Math.round(v * 10000) / 10000;
                }
            }
        })
        .compact()
        .value();
    },
    expectTimeSorted: function(result) {
        var time;
        _.each(result.sinks.table, function(pt) {
            if (time) {
                expect(pt.time).gt(time);
            }
            expect(isNaN(Date.parse(pt.time))).to.be.false;
            time = pt.time;
        });
    },
    check_sql_juttle: function(params, deactivateAfter) {
        params.program = params.program.replace(' sql ', ' ' + adapter.name + ' ');
        return check_juttle(params, deactivateAfter);
    },
    check_sql_optimization_juttle: function(params, deactivateAfter) {
        var unopt_params = _.clone(params);
        unopt_params.program = params.program.replace('sql', 'sql -optimize false');

        return Promise.props({
            unopt: TestUtils.check_sql_juttle(unopt_params, deactivateAfter),
            opt: TestUtils.check_sql_juttle(params, deactivateAfter)
        }).then(function(res) {
            expect(res.opt.errors[0]).to.equal(undefined);
            expect(res.opt.warnings[0]).to.equal(undefined);

            var unopt = TestUtils.massage(res.unopt.sinks.table, params.massage);
            var opt = TestUtils.massage(res.opt.sinks.table, params.massage);
            expect(opt).deep.equal(unopt);

            return res.opt;
        });
    }
};

module.exports = TestUtils;
