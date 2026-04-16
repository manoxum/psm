import { CommandModule } from "yargs";
import { addRlsPolicyRule, RlsPolicyOptions } from "../../tools/project-migration";

const command: CommandModule<RlsPolicyOptions, RlsPolicyOptions> = {
    command: "rls policy <table> <name>",
    describe: "Register a versioned row-level security policy rule in psm.migration.yml",
    builder: (args) => {
        args
            .positional("table", { type: "string" })
            .positional("name", { type: "string" })
            .option("schema", { type: "string", alias: "s" })
            .option("schema_name", { type: "string" })
            .option("revision", { type: "string", alias: "r" })
            .option("description", { type: "string", alias: "d" })
            .option("once", { type: "boolean", default: true })
            .option("command", {
                type: "string",
                choices: ["ALL", "SELECT", "INSERT", "UPDATE", "DELETE"] as const,
                default: "ALL",
            })
            .option("to", {
                type: "string",
                array: true,
            })
            .option("using", { type: "string" })
            .option("check", { type: "string" });

        return args;
    },
    handler: (argv) => {
        try {
            const result = addRlsPolicyRule(argv);
            console.log(`Migration rule created: ${result.revision}`);
        } catch (error) {
            console.error(error);
            process.exitCode = 1;
        }
    },
};

export = command;
