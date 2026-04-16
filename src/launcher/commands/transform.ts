import { CommandModule } from "yargs";
import { addTransformColumnRule, TransformColumnOptions } from "../../tools/project-migration";

const command: CommandModule<TransformColumnOptions, TransformColumnOptions> = {
    command: "transform column <model> <column> <to>",
    describe: "Register a versioned column transform rule in psm.migration.yml",
    builder: (args) => {
        args
            .positional("model", { type: "string" })
            .positional("column", { type: "string" })
            .positional("to", { type: "string" })
            .option("from", { type: "string" })
            .option("using", { type: "string" })
            .option("schema", { type: "string", alias: "s" })
            .option("revision", { type: "string", alias: "r" })
            .option("description", { type: "string", alias: "d" })
            .option("once", { type: "boolean", default: true });

        return args;
    },
    handler: (argv) => {
        try {
            const result = addTransformColumnRule(argv);
            console.log(`Migration rule created: ${result.revision}`);
        } catch (error) {
            console.error(error);
            process.exitCode = 1;
        }
    },
};

export = command;
