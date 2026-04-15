// filename: src/tools/deploy.ts

import * as tar from "tar";
import * as os from "node:os";
import * as fs from "node:fs";
import * as yaml from "yaml";
import * as Path from "node:path";
import { PSMConfigFile } from "../configs";
import chalk from "chalk";
import { PSMDriver } from "../driver";
import { psmLockup, resolveDatabaseUrl } from "./common";

export interface DeployOptions {
    schema?: string;
    generate?: string;
    label?: string;
    "generate-command": string;
}

const TAG = "PSM DEPLOY >";

type RevisionEntry = {
    archive: string;
    temp: string;
    extractedDir: string;
    psm: PSMConfigFile;
    migrate: string;
    pulled: boolean;
    label: string;
    message: string[];
    date: Date | null;
    backup?: string;
};

async function extractRevisionFiles(archive: string, tempDir: string, files: string[]) {
    const normalized = new Set(files);
    await tar.x({
        file: archive,
        cwd: tempDir,
        filter: (pathname) => normalized.has(Path.basename(pathname)),
    });
}

function resolveExtractedDir(tempDir: string) {
    const extractedDir = fs.readdirSync(tempDir)[0];
    if (!extractedDir) {
        throw new Error(`Invalid migration archive: ${tempDir}`);
    }
    return Path.join(tempDir, extractedDir);
}

async function loadRevisionArchive(archive: string): Promise<RevisionEntry> {
    const tempDir = fs.mkdtempSync(Path.join(os.tmpdir(), "psm-"));
    await extractRevisionFiles(archive, tempDir, ["psm.yml", "migration.sql"]);

    const fullPath = resolveExtractedDir(tempDir);
    const psmPath = Path.join(fullPath, "psm.yml");
    const migrationPath = Path.join(fullPath, "migration.sql");

    if (!fs.existsSync(psmPath) || !fs.existsSync(migrationPath)) {
        fs.rmSync(tempDir, { recursive: true, force: true });
        throw new Error(`Invalid migration archive: ${Path.basename(archive)}`);
    }

    return {
        archive,
        temp: tempDir,
        extractedDir: fullPath,
        psm: yaml.parse(fs.readFileSync(psmPath, "utf-8")) as PSMConfigFile,
        migrate: fs.readFileSync(migrationPath, "utf-8"),
        pulled: false,
        label: "",
        message: [],
        date: null,
    };
}

async function ensureBackupExtracted(revision: RevisionEntry) {
    if (revision.backup) return revision.backup;
    await extractRevisionFiles(revision.archive, revision.temp, ["backup.sql"]);
    const backup = Path.join(revision.extractedDir, "backup.sql");
    if (fs.existsSync(backup)) {
        revision.backup = backup;
    }
    return revision.backup;
}

export async function deploy(opts: DeployOptions) {
    const { psm, psm_sql, driver, home } = await psmLockup({ schema: opts.schema });
    const currentUrl = resolveDatabaseUrl(psm.psm.url);

    let migrator = driver.migrator({
        url: currentUrl,
        migrate: "",
        check: "",
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

    const pullResponse = await fetch({
        home: home,
        driver: driver,
        psm: psm,
    });

    if (pullResponse.error) {
        pullResponse.clean();
        throw pullResponse.error;
    }

    if (!pullResponse.revs?.length) {
        pullResponse.clean();
        throw new Error(`No migrate commited! Use ${chalk.bold("psm migrate commit")} first!`);
    }

    let revs = pullResponse.revs;

    // Ordena as revisões por instante (cronologicamente)
    revs.sort((a, b) => a.psm.migration.instante.localeCompare(b.psm.migration.instante));
    let restored = false;

    for (let i = 0; i < revs.length; i++) {
        let next = revs[i];
        const nextUrl = resolveDatabaseUrl(next.psm?.psm?.url, psm.psm.url);
        const migrator = driver.migrator({
            url: nextUrl,
            migrate: next.migrate,
            check: "",
            core: "",
        });
        next.message.forEach(console.log);

        // CORREÇÃO: se já foi migrada, apenas continua para a próxima
        if (next.pulled) continue;

        // Restaura backup apenas na primeira revisão não aplicada (se houver)
        if (!restored) {
            const backup = await ensureBackupExtracted(next);
            if (backup) {
                await migrator.restore(backup);
                restored = true;
            }
        }

        const result = await migrator.migrate();

        if (!result.success) {
            console.error(result.error);
            result.messages.forEach(error => {
                console.error(error);
            });
            console.error(`${next.label} is migrated at ${next.date} - ${chalk.redBright.bold("FAILED")}`);
            throw new Error("Migrate error: Push migration failed!");
        }
        console.log(`${next.label} is migrated at ${next.date} - ${chalk.greenBright.bold("SUCCESS")}`);
    }

    pullResponse.clean();
}

export interface FetchOptions {
    home: string;
    driver: PSMDriver;
    psm: PSMConfigFile;
}

export async function fetch(opts: FetchOptions) {
    const revisionsDir = Path.join(opts.home, "psm/revisions/schema");
    fs.mkdirSync(revisionsDir, {recursive:true})
    const revFiles = fs.readdirSync(revisionsDir).filter(n => n.endsWith(".tar.gz"));

    const revs: RevisionEntry[] = [];

    for (const file of revFiles) {
        revs.push(await loadRevisionArchive(Path.join(revisionsDir, file)));
    }

    // verificar previews faltantes
    const missedPreviews = revs.filter((n, i) => {
        return (!!n.psm.migration.preview && !revs.find(p => p.psm.migration.revision === n.psm.migration.preview))
            || (i > 0 && !n.psm.migration.preview);
    });

    if (missedPreviews.length) {
        const missed = missedPreviews.map(n => `${n.psm.psm.migration} - ${n.psm.migration.label} at ${n.psm.migration.instante}`);
        return {
            revs,
            error: new Error(`MISSING PREVIEW migration for...\n${missed.join(", ")}`),
            clean() {
                revs.forEach(value => {
                    fs.rmSync(value.temp, { recursive: true, force: true });
                });
            },
        };
    }

    // marcar revisões já migradas
    const migrated = await opts.driver.migrated({
        url: resolveDatabaseUrl(opts.psm.psm.url),
        sys: opts.psm.psm.sys,
        sids: revs.map((value) => value.psm.psm.migration),
    });

    if (!migrated.success) {
        console.error(migrated.error);
        migrated.messages.forEach(console.error);
        return {
            revs,
            error: new Error("Load Migrated failed!"),
            clean() {
                revs.forEach(value => {
                    fs.rmSync(value.temp, { recursive: true, force: true });
                });
            },
        };
    }

    for (const next of revs) {
        const pl = next.psm.migration.label?.length ? ` - ${next.psm.migration.label} ` : " ";
        next.label = `RevNo ${next.psm.migration.revision}${pl}commited at ${next.psm.migration.instante}`;
        const mig = migrated.migrated.find(v => v.sid === next.psm.psm.migration);
        if (mig) {
            next.date = mig.date;
            next.message.push(`${next.label} is migrated at ${mig.date}`);
            next.pulled = true;
        }
    }

    return {
        revs,
        clean() {
            revs.forEach(value => {
                fs.rmSync(value.temp, { recursive: true, force: true });
            });
        },
    };
}
