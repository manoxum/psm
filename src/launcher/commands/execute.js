"use strict";
// filename: src/launcher/commands/execute.ts
const execute_1 = require("../../tools/execute");
const command = {
    command: "execute",
    describe: "Execute customs scripts into database",
    builder: args => {
        args.options("schema", {
            type: "string",
            alias: "s"
        }).options("label", {
            type: "string",
            alias: "l",
        }).options("groups", {
            type: "string",
            alias: "g",
            array: true
        });
        return args;
    },
    handler: (argv) => {
        (0, execute_1.execute)(argv).then(value => {
            console.log("Operation finished with success!");
        }).catch(reason => {
            console.error("Operation finished with error!");
            console.error(reason);
        });
    }
};
module.exports = command;
//# sourceMappingURL=execute.js.map