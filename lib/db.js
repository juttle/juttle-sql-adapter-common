var logger = require('juttle/lib/logger').getLogger('sql-db-init');

var knex;

var DB = {
    init: function(config) {
        if (config.knex) {
            knex = config.knex;
            logger.info('initializing db connection with conf:', knex.client.config);
        } else {
            throw new Error('knex config for sql adapter not found.');
        }
    },
    getDbConnection: function () {
        return knex;
    }
};

module.exports = DB;
