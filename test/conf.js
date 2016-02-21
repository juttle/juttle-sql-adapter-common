// Configuration for testing common code only.

function getDBClass() {
    return require('../sqlite-db');
}

function getAdapterName() {
    return 'sql';
}

function getAdapterConfig() {
    var config = [
        {
            id: 'default',
            filename: "./unit-test.sqlite"
        }, {
            id: 'fake',
            filename: "./not_dir/should_not_work/not_db.sqlite"
        }
    ];
    config.path = "./";

    return config;
}

module.exports = {
    getDBClass: getDBClass,
    getAdapterName: getAdapterName,
    getAdapterConfig: getAdapterConfig
};
