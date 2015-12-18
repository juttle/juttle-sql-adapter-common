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
            return knex(table_name).insert(data);
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
    check_sql_juttle: function(params) {
        var program = params.program.replace(' sql ', ' ' + adapter.name + ' ');
        return check_juttle({
            program: program
        });
    },
    check_sql_optimization_juttle: function(params) {
        var program = params.program;
        var unopt_program = program.replace('sql', 'sql -optimize false');

        return Promise.props({
            unopt: TestUtils.check_sql_juttle({
                program: unopt_program
            }),
            opt: TestUtils.check_sql_juttle({
                program: program
            })
        }).then(function(res) {
            expect(res.opt.errors).to.have.length(0);
            expect(res.opt.warnings).to.have.length(0);

            var unopt = TestUtils.massage(res.unopt.sinks.table, params.massage);
            var opt = TestUtils.massage(res.opt.sinks.table, params.massage);
            expect(opt).deep.equal(unopt);

            return res.opt;
        });
    }
};

module.exports = TestUtils;
