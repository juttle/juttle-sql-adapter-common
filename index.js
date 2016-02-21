/*
    SQL Adapter Common Code
    Note: This file is only used for testing.
*/
'use strict';

let db = require('./sqlite-db');
let Read = require('./lib/read');
let Write = require('./lib/write');

class SqliteRead extends Read {
    getDbConnection(options) {
        return db.getDbConnection(options);
    }
}

class SqliteWrite extends Write {
    getDbConnection(options) {
        return db.getDbConnection(options);
    }
}

function SqlAdapter(config) {
    db.init(config);

    return {
        name: 'sql',
        read: SqliteRead,
        write: SqliteWrite,
        optimizer: require('./lib/optimize')
    };
}
module.exports = SqlAdapter;
