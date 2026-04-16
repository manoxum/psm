import { CommandModule } from "yargs";
import { addMoveColumnRule, MoveColumnOptions } from "../../tools/project-migration";

const command: CommandModule<MoveColumnOptions, MoveColumnOptions> = {
    command: "move column <model> <column>",
    describe: "Register a versioned column move rule in psm.migration.yml",
    builder: (args) => {
        args
            .positional("model", { type: "string" })
            .positional("column", { type: "string" })
            .option("after", { type: "string" })
            .option("first", { type: "boolean", default: false })
            .option("schema", { type: "string", alias: "s" })
            .option("revision", { type: "string", alias: "r" })
            .option("description", { type: "string", alias: "d" })
            .option("once", { type: "boolean", default: true });

        return args;
    },
    handler: (argv) => {
        try {
            const result = addMoveColumnRule(argv);
            console.log(`Migration rule created: ${result.revision}`);
        } catch (error) {
            console.error(error);
            process.exitCode = 1;
        }
    },
};

export = command;
