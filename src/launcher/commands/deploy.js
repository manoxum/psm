"use strict";
const deploy_1 = require("../../tools/deploy");
const command = {
    command: "deploy",
    describe: "Deploy all commited revision into server",
    builder: args => {
        args.options("schema", {
            type: "string",
            alias: "s"
        });
        return args;
    },
    handler: (argv) => {
        (0, deploy_1.deploy)(argv).then(value => {
            console.log("Operation finished with success!");
        }).catch(reason => {
            console.error("Operation finished with error!");
            console.error(reason);
        });
    }
};
module.exports = command;
//# sourceMappingURL=deploy.js.map