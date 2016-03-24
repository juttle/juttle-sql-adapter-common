var _ = require('underscore');
var moment = require('moment');
var tableData = {};

var logs = [];
var isErrLog;
var today = new Date();
var todayStr = today;
var temp;
for (var i = 150 ; i > 0 ; i--) {
    isErrLog = Math.round(Math.random()); // 0 or 1

    temp = moment.utc().subtract(i, 'days').toDate();
    logs.push({
        time: temp,
        host: "127.0.0." + (Math.round(Math.random()) + 1),
        msg: "this is a test " + isErrLog ? 'err' : 'msg',
        level: isErrLog ? 'error' : 'info',
        code: Math.round(Math.random() * 10) + 1
    });

}

var logsCreateTime = logs.map(function(originalLog) {
    var log = _.clone(originalLog);
    log.create_time = log.time;
    delete log.time;
    return log;
}).reverse();

tableData.simple = [
    {
        id: 1,
        name: 'timmy'
    },
    {
        id: 2,
        name: 'sammy'
    }
];

var logsSameTime = logsCreateTime.map(function(originalLog) {
    var log = _.clone(originalLog);
    log.create_time = todayStr;
    return log;
});

tableData.logs = logs;
tableData.logsCreateTime = logsCreateTime;
tableData.logsSameTime = logsSameTime;

module.exports = tableData;
