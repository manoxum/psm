// filename: src/tools/commit.ts

import * as fs from "node:fs";
import * as Path from "node:path";
import * as os from "node:os";                // <-- adicionado para diretório temporário
import * as yaml from "yaml";
import * as tar from "tar";
import chalk from "chalk";
import { PSMConfigFile } from "../configs";
import * as cp from "node:child_process";
import { fetch } from "./deploy";
import { psmLockup, resolveDatabaseUrl } from "./common";
import { getLatestFolder, gitAddPath, sanitizeLabel } from "../utils/fs";
import { CreateCustom } from "./execute";

async function readRevisionConfig(archivePath: string): Promise<PSMConfigFile | undefined> {
    const tempDir = fs.mkdtempSync(Path.join(os.tmpdir(), "psm-preview-"));
    try {
        await tar.x({
            file: archivePath,
            cwd: tempDir,
            filter: (pathname) => pathname.endsWith("/psm.yml"),
        });
        const extractedItems = fs.readdirSync(tempDir);
        if (!extractedItems.length) return undefined;
        const psmYamlPath = Path.join(tempDir, extractedItems[0], "psm.yml");
        if (!fs.existsSync(psmYamlPath)) return undefined;
        return yaml.parse(fs.readFileSync(psmYamlPath, "utf-8")) as PSMConfigFile;
    } finally {
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
}

export interface MigrateOptions {
    schema?: string;
    generate?: string;
    label?: string;
    "generate-command": string;
}

export async function commit(opts: MigrateOptions) {
    const moment = require('moment');

    if (opts.generate) {
        let command = opts["generate-command"];
        if (!command) command = "prisma generate";
        cp.spawnSync("npx", [...command.split(" ")], {
            cwd: process.cwd(),
        });
    }

    const { psm, psm_sql, driver, home } = await psmLockup({ schema: opts.schema });
    const next = Path.join(psm.psm.output, "next/migration.next.sql");
    const check = Path.join(psm.psm.output, "next/migration.next.check.sql");

    // -------------------- Leitura do preview corrigida --------------------
    let preview: PSMConfigFile | undefined;
    const revisionsDir = Path.join(home, "psm/revisions/schema");
    const lastFile = getLatestFolder(revisionsDir);

    if (lastFile) {
        const tarPath = Path.join(revisionsDir, lastFile);
        preview = await readRevisionConfig(tarPath);
    }
    // ---------------------------------------------------------------------

    psm.migration = {
        revision: `${moment().format('YYYYMMDDHHmmss')} - ${psm.psm.migration}`,
        instante: moment().format('YYYYMMDDHHmmss'),
        preview: preview?.migration?.revision,
        label: opts.label,
    };

    let label = "";
    if (!!opts.label) label = ` - ${sanitizeLabel(opts.label)}`;
    const nextRev = Path.join(home, `psm/revisions/schema/${psm.migration.instante}${label}`);

    if (!fs.existsSync(check)) {
        throw new Error("Migrate error: next/migration.next.check.sql file not found!");
    }
    if (!fs.existsSync(next)) {
        throw new Error("Migrate error: next/migration.next.sql file not found!");
    }

    const migrator = driver.migrator({
        url: resolveDatabaseUrl(psm.psm.url),
        migrate: fs.readFileSync(next).toString(),
        check: fs.readFileSync(check).toString(),
        core: fs.readFileSync(psm_sql).toString(),
    });

    let result = await migrator.core();
    if (!result.success) {
        console.error(result.error);
        result.messages.forEach(error => {
            console.error(error);
        });
        throw new Error("Migrate error: Core failed!");
    }

    const fetchResponse = await fetch({
        psm: psm,
        driver: driver,
        home: home,
    });
    try {
        if (fetchResponse.error) {
            throw fetchResponse.error;
        }

        const noPulled = fetchResponse.revs.filter(n => !n.pulled);

        if (noPulled.length) {
            throw new Error(`Commit not pulled already exists! Please run ${chalk.bold("psm deploy")} first!`);
        }
    } finally {
        fetchResponse.clean();
    }

    result = await migrator.test();
    if (!result.success) {
        console.error(result.error);
        result.messages.forEach(error => {
            console.error(error);
        });
        throw new Error("Migrate error: Check shadow failed!");
    }

    const dump = await migrator.dump();
    if (dump.error) {
        throw dump.error;
    }

    fs.mkdirSync(nextRev, { recursive: true });
    const custom = CreateCustom(
        {
            home: home,
            nextRev: nextRev,
            migrator: migrator,
        },
        "functions",
        "triggers",
        "views"
    );

    result = await migrator.migrate(custom).catch(reason => {
        return { error: reason, success: false };
    });

    if (!result.success) {
        console.error(result.error);
        result.messages.forEach(error => {
            console.error(error);
        });
        if (fs.existsSync(nextRev)) {
            fs.rmdirSync(nextRev);
        }
        throw new Error(`Migrate error: Commit migration failed! ${home}`);
    }

    custom.createFiles();

    fs.writeFileSync(Path.join(nextRev, "migration.sql"), migrator.migrateRaw(custom));
    fs.writeFileSync(Path.join(nextRev, "psm.yml"), yaml.stringify(psm));
    if (dump.file) {
        fs.copyFileSync(dump.file, Path.join(nextRev, "backup.sql"));
        fs.rmSync(Path.dirname(dump.file), { recursive: true, force: true });
    } else {
        fs.writeFileSync(Path.join(nextRev, "backup.sql"), dump.output || "");
    }

    fs.unlinkSync(check);

    const archiveName = Path.join(
        home || process.cwd(),
        `psm/revisions/schema/${psm.migration.instante}${label}.tar.gz`
    );

    await tar.c(
        {
            gzip: {
                level: 9,
            },
            file: archiveName,
            cwd: Path.dirname(nextRev),
        },
        [Path.basename(nextRev)]
    );

    console.log(chalk.green(`✔ Migration compactada: ${archiveName}`));

    fs.rmSync(nextRev, { recursive: true, force: true });
    fs.unlinkSync(next);

    gitAddPath(home || process.cwd(), archiveName);
}
