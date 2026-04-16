"use strict";
const project_migration_1 = require("../../tools/project-migration");
const command = {
    command: "rename column <model> <from> <to>",
    describe: "Register a versioned column rename rule in psm.migration.yml",
    builder: (args) => {
        args
            .positional("model", { type: "string" })
            .positional("from", { type: "string" })
            .positional("to", { type: "string" })
            .option("schema", { type: "string", alias: "s" })
            .option("revision", { type: "string", alias: "r" })
            .option("description", { type: "string", alias: "d" })
            .option("once", { type: "boolean", default: true })
            .option("references", {
            type: "string",
            choices: ["preserve", "drop"],
            default: "preserve",
        });
        return args;
    },
    handler: (argv) => {
        try {
            const result = (0, project_migration_1.addRenameColumnRule)(argv);
            console.log(`Migration rule created: ${result.revision}`);
        }
        catch (error) {
            console.error(error);
            process.exitCode = 1;
        }
    },
};
module.exports = command;
//# sourceMappingURL=rename.js.map