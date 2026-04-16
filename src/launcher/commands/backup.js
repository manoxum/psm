"use strict";
const backup_1 = require("../../tools/backup");
const command = {
    command: "backup",
    describe: "Backup current database",
    builder: args => {
        args.options("schema", {
            type: "string",
            alias: "s"
        }).options("add", {
            type: "boolean",
            describe: "Adicionar no git"
        }).options("label", {
            type: "string",
            alias: "l",
        });
        return args;
    },
    handler: (argv) => {
        (0, backup_1.backup)(argv).then(value => {
            console.log("Operation finished with success!");
        }).catch(reason => {
            console.error("Operation finished with error!");
            console.error(reason);
        });
    }
};
module.exports = command;
//# sourceMappingURL=backup.js.map