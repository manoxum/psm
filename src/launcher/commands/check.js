"use strict";
const check_1 = require("../../tools/check");
const command = {
    command: "check",
    builder: args => {
        return args;
    },
    handler: args => {
        (0, check_1.check)();
    }
};
module.exports = command;
//# sourceMappingURL=check.js.map