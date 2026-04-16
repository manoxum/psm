"use strict";
const generate_1 = require("../../tools/generate");
const command = {
    command: "generate",
    describe: "Generate pre-migrate archives",
    builder: args => {
        return args;
    },
    handler: args => {
        try {
            (0, generate_1.generate)();
        }
        catch (e) {
            console.error(e);
            console.log(e);
            throw e;
        }
    }
};
module.exports = command;
//# sourceMappingURL=generate.js.map