var sampleData = require("./sample_data");
var Promise = require('bluebird');
var _ = require('underscore');
var juttle_test_utils = require('juttle/test').utils;
var check_juttle = juttle_test_utils.checkJuttle;
var expect = require('chai').expect;
var logger = require('juttle/lib/logger').getLogger('sql-test-util');

var db;
var knex;
var adapterName;
var createdTables = [];
var tableCreationMap;

var TestUtils = {
    init: function () {
        if (adapterName) { return; }

        var config = TestUtils.getAdapterConfig();
        adapterName = TestUtils.getAdapterName();

        logger.info('Testing ' + adapterName + ' adapter.');

        TestUtils.initTestDbConnection();

        try {
            var confParams = {};
            confParams[adapterName] = config;

            juttle_test_utils.configureAdapter(confParams);
        } catch (err) {
            if (!err.message.includes('already registered')) {
                throw err;
            }
        }
        tableCreationMap = TestUtils.getTableCreationMap();
    },
    initTestDbConnection: function() {
        juttle_test_utils.withAdapterAPI(() => {
            db = this.getDBClass();
            db.init(TestUtils.getAdapterConfig());
            knex = db.getDbConnection();
        });
    },
    getTableCreationMap: function() {
        return {
            logs: function() { return TestUtils.createLogTable('logs', 'time', sampleData.logs); },
            logs_create: function() { return TestUtils.createLogTable('logs_create', 'create_time', sampleData.logsCreateTime); },
            logs_same_time: function() { return TestUtils.createLogTable('logs_same_time', 'create_time', sampleData.logsSameTime); },
            simple: function() { return TestUtils.createSimpleTable(); },
            sqlwriter: function() { return TestUtils.createWritingTable(); }
        };
    },
    getSampleData: function () {
        return sampleData;
    },
    createTables: function (createTableNames) {
        return Promise.try(function() {
            createTableNames = _.difference(createTableNames, createdTables);
            if (createTableNames.length === 0) {
                return;
            }

            var tableCreateFunctions = [];
            _.each(createTableNames, function(tableName) {
                if (!tableCreationMap[tableName]) {
                    throw new Error('table create does not exist for ' + tableName);
                }
                logger.info('creating table', tableName);
                tableCreateFunctions.push(tableCreationMap[tableName]());
                createdTables.push(tableName);
            });
            return Promise.all(tableCreateFunctions);
        });
    },
    removeTables: function () {
        return Promise.map(createdTables, function(tableName) {
            return knex.schema.dropTableIfExists(tableName);
        }).then(function() {
            createdTables = [];
        });
    },
    clearState: function() {
        adapterName = '';
        if (knex) {
            return TestUtils.removeTables()
            .then(function() {
                db.closeConnection();
                knex = null;
            });
        }
        return Promise.resolve();
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
    // What is performed here:
    // - round values
    massage: function(arr, massageOptions) {
        if (!massageOptions) {
            return arr;
        }
        _.each(arr, function(pt) {
            var k, v;
            for (k in pt) {
                v = pt[k];
                if (v === Infinity || v === -Infinity) {
                    pt[k] = null;
                }
                if (_.isNumber(v) && (v % 1)) {
                    pt[k] = Math.round(v * 10000) / 10000;
                }
            }
        });
        
        if (massageOptions.sort) {
            massageOptions.sort.forEach(function(sortField) {
                arr = _.sortBy(arr, sortField);
            });
        }
        
        return arr;
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
        params.program = params.program.replace(' sql ', ' ' + adapterName + ' ');
        params.deactivateAfter = deactivateAfter;
        return check_juttle(params);
    },
    check_juttle_success: function(params, deactivateAfter) {
        return TestUtils.check_sql_juttle(params, deactivateAfter)
        .then(function(res) {
            expect(res.errors[0]).to.equal(undefined);
            expect(res.warnings[0]).to.equal(undefined);
            return res;
        });
    },
    check_juttle_error: function(params, deactivateAfter) {
        return TestUtils.check_sql_juttle(params, deactivateAfter)
        .then(function() {
            throw new Error('We should not get this error');
        });
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

            if (params.optimize_param) {
                var res_opt_param = res.opt.prog.graph.params.optimization_info;
                res_opt_param = _.omit(res_opt_param, 'empty_aggregate', 'expect_empty_aggregate');
                expect(res_opt_param)
                    .to.deep.equal(params.optimize_param);
            }

            var unopt = TestUtils.massage(res.unopt.sinks.table, params.massage);
            var opt = TestUtils.massage(res.opt.sinks.table, params.massage);
            expect(opt).deep.equal(unopt);

            return res.opt;
        });
    }
};

_.extend(TestUtils, require('./conf'));
module.exports = TestUtils;
