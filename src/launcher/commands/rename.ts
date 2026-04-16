import { CommandModule } from "yargs";
import { addRenameColumnRule, RenameColumnOptions } from "../../tools/project-migration";

const command: CommandModule<RenameColumnOptions, RenameColumnOptions> = {
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
                choices: ["preserve", "drop"] as const,
                default: "preserve",
            });

        return args;
    },
    handler: (argv) => {
        try {
            const result = addRenameColumnRule(argv);
            console.log(`Migration rule created: ${result.revision}`);
        } catch (error) {
            console.error(error);
            process.exitCode = 1;
        }
    },
};

export = command;
