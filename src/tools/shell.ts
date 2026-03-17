// filename: src/launcher/commands/shell.ts

import { CommandModule } from "yargs";
import { shell, ShellOptions } from "../../tools/shell";

const command: CommandModule<ShellOptions, ShellOptions> = {
    command: "shell",
    describe: "Open interactive SQL shell with command accumulation (execute with \\execute)",
    builder: args => {
        return args
            .options("schema", {
                type: "string",
                alias: "s",
                describe: "Caminho para o schema.prisma (opcional)",
            })
            .options("label", {
                type: "string",
                alias: "l",
                describe: "Label para a revisão gerada ao commitar",
            })
            .options("add", {
                type: "boolean",
                describe: "Adicionar o arquivo .tar.gz ao git após gerar revisão",
            });
    },
    handler: (argv) => {
        shell(argv).catch((reason) => {
            console.error("Erro no shell:", reason);
            process.exit(1);
        });
    },
};

export = command;