var TestUtils = require("./utils");

before(function() {
    return TestUtils.init();
});
after(function() {
    return TestUtils.removeTables();
});
