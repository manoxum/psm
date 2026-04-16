"use strict";
const commit_1 = require("../../tools/commit");
const command = {
    command: "commit",
    describe: "Migrate next schema structure into dev environment",
    builder: args => {
        args.options("schema", {
            type: "string",
            alias: "s"
        }).options("generate", {
            type: "boolean",
            alias: "g",
        }).options("label", {
            type: "string",
            alias: "l",
        }).options("generate-command", {
            type: "string",
            alias: "c"
        });
        return args;
    },
    handler: (argv) => {
        (0, commit_1.commit)(argv).then(value => {
            console.log("Operation finished with success!");
        }).catch(reason => {
            console.error("Operation finished with error!");
            console.error(reason);
        });
    }
};
module.exports = command;
//# sourceMappingURL=commit.js.map