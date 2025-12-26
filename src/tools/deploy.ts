// filename: src/tools/deploy.ts

import tar from "tar";
import * as os from "node:os";
import * as fs from "node:fs";
import * as yaml from "yaml";
import * as Path from "node:path";
import {PSMConfigFile} from "../configs";
import chalk from "chalk";
import {PSMDriver} from "../driver";
import {psmLockup} from "./common";

export interface DeployOptions {
    schema?:string
    generate?:string
    label?:string
    "generate-command":string
}

const TAG = "PSM DEPLOY >"

export async function deploy( opts: DeployOptions) {
    require('dotenv').config();

    const { psm, psm_sql, driver, home} = await psmLockup({ schema: opts.schema });

    let migrator = driver.migrator({
        url: process.env[psm.psm.url],
        migrate: "",
        check: "",
        core: fs.readFileSync( psm_sql ).toString(),
    });

    let result = await migrator.core();
    if( !result.success ) {
        console.error( result.error );
        result.messages.forEach( error => {
            console.error( error );
        });
        throw new Error( "Migrate error: Core failed!" );
    }

    const pullResponse = await fetch({
        home: home,
        driver: driver,
        psm: psm
    });

    if( pullResponse.error ){
        pullResponse.clean();
        throw  pullResponse.error;
    }

    if( !pullResponse.revs?.length ) {
        pullResponse.clean();
        throw new Error(`No migrate commited! Use ${ chalk.bold("psm migrate commit")} first!`);
    }

    const revs = pullResponse.revs;

    for (let i = 0; i < revs.length; i++) {
        let next = revs[i];
        const migrator = driver.migrator({
            url: process.env[next.psm.psm.url],
            migrate: next.migrate,
            check: "",
            core: "",
        });
        next.message.forEach( console.log );

        if( next.pulled ) return;
        else if( i === 0 && !!next.backup ) {
            await migrator.restore( next.backup );
        }

        const result = await migrator.migrate();

        if( !result.success ) {
            console.error( result.error );
            result.messages.forEach( error => {
                console.error( error );
            });
            console.error( `${next.label} is migrated at ${next.date} - ${chalk.redBright.bold( "FAILED")}` );
            throw new Error( "Migrate error: Push migration failed!" );
        }
        console.log( `${next.label} is migrated at ${next.date} - ${chalk.greenBright.bold( "SUCCESS")}` );
    }

    pullResponse.clean();
}

export interface FetchOptions {
    home:string,
    driver:PSMDriver,
    psm:PSMConfigFile
}
export async function fetch(opts: FetchOptions) {
    const revisionsDir = Path.join(opts.home, "psm/revisions/schema");
    const revFiles = fs.readdirSync(revisionsDir).filter(n => n.endsWith(".tar.gz"));

    const revs: Array<{
        temp:string
        psm: PSMConfigFile;
        migrate: string;
        backup?: string;
        pulled: boolean;
        label: string;
        message: string[];
        date: Date | null;
    }> = [];

    for (const file of revFiles) {
        const tempDir = fs.mkdtempSync(Path.join(os.tmpdir(), "psm-"));

        await tar.x({
            file: Path.join(revisionsDir, file),
            cwd: tempDir,
        });

        const extractedDir = fs.readdirSync(tempDir)[0];
        const fullPath = Path.join(tempDir, extractedDir);

        const psmPath = Path.join(fullPath, "psm.yml");
        const migrationPath = Path.join(fullPath, "migration.sql");
        const backup = Path.join(fullPath, "backup.sql");

        if (!fs.existsSync(psmPath) || !fs.existsSync(migrationPath)) {
            fs.rmSync(tempDir, { recursive: true, force: true });
            throw new Error(`Invalid migration archive: ${file}`);
        }

        const psm = yaml.parse(fs.readFileSync(psmPath, "utf-8")) as PSMConfigFile;
        const migrate = fs.readFileSync(migrationPath, "utf-8");

        const rev:typeof revs[number] = {
            psm,
            migrate,
            pulled: false,
            label: "",
            message: [],
            date: null,
            backup: backup,
            temp: tempDir
        }

        if( fs.existsSync( backup) ) rev.backup = backup;
        revs.push(rev);
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
            clean(){}
        };
    }

    // marcar revisões já migradas
    const migrated = await opts.driver.migrated({
        url: process.env[opts.psm.psm.url],
        sys: opts.psm.psm.sys,
    });

    if (!migrated.success) {
        console.error(migrated.error);
        migrated.messages.forEach(console.error);
        return { revs, error: new Error("Load Migrated failed!") };
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
        clean(){
            revs.forEach( value => {
                fs.rmSync(value.temp, { recursive: true, force: true });
            })
        }
    };
}