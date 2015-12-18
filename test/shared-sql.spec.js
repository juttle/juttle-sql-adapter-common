var TestUtils = require("./utils");

function SharedSqlTests() {
    describe(TestUtils.getAdapterName() + ' adapter API tests', function () {
        after(function() {
            return TestUtils.endConnection();
        });

        describe('read proc', function () {
            before(function() {
                return TestUtils.loadTables();
            });

            require('./options.spec');
            require('./filters.spec');
            require('./time.spec');
            require('./optimize.spec');
        });

        require('./write.spec');
    });
}
module.exports = SharedSqlTests;
