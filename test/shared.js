var TestUtils = require("./utils");
var juttle_test_utils = require('juttle/test').utils;

before(function() {
    return juttle_test_utils.withAdapterAPI(function() {
        TestUtils.init();
    });
});
after(function() {
    return TestUtils.removeTables();
});
