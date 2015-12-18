var logger = require('juttle/lib/logger').getLogger('sql-db-init');

var knex;

var DB = {
    init: function(config) {
        if (config.knex) {
            knex =  config.knex;
            logger.info('using knex db connection object specified by conf file');
        } else if (config.knex_conf) {
            logger.info('using knex_conf to connect to db', config.knex_conf);
            knex =  require('knex')(
                config.knex_conf
            );
        } else {
            throw new Error('knex config for sql adapter not found.');
        }
    },
    getDbConnection: function () {
        return knex;
    }
};

module.exports = DB;
