// filename: src/tools/custom.ts


import * as fs from "node:fs";
import * as Path from "node:path";
import chalk from "chalk";
import {CustomResources, CustomScript, PSMMigrator} from "../driver";
import {psmLockup} from "./common";
import { gitAddPath, sanitizeLabel} from "../utils/fs";
import {PSMConfigFile} from "../configs";
import * as tar from "tar";

export interface CustomOptions {
    save?:boolean
    schema?:string
    label?:string
    groups?:string[]
    driver?:string
}

export type ExecuteCustomOptions = {
    home:string,
    nextRev:string,
    migrator:PSMMigrator
}

export async function execute(opts:CustomOptions ) {
    require('dotenv').config();
    const moment = require('moment');


    const { psm, psm_sql, driver, home } = await psmLockup({ schema: opts.schema, driver: opts.driver });

    let label = "";
    if( !!opts.label ) label = ` - ${sanitizeLabel( opts.label )}`;
    const instante = moment().format( 'YYYYMMDDHHmmss' );
    const nextRev = Path.join( home, `psm/revisions/schema/${instante}${label}`);

    if( !opts.groups || !opts.groups?.length ) opts.groups = [
        "functions", "triggers", "views"
    ]
    const migrator = driver.migrator({
        url: process.env[ psm.psm.url ],
        migrate: "",
        check: "",
        core: ""
    });

    const custom = CreateCustom({
        home: home,
        nextRev: nextRev,
        migrator: migrator
    },... opts.groups );



    await custom.execute();
    if( !opts.save ) return;


    let preview:PSMConfigFile;

    psm.migration = {
        revision: `${ moment().format( 'YYYYMMDDHHmmss' ) } - ${psm.psm.migration}`,
        instante: moment().format( 'YYYYMMDDHHmmss' ),
        preview: preview?.migration.revision,
        label: opts.label
    }
    fs.mkdirSync( nextRev, { recursive: true });
    custom.createFiles();

    const archiveName = Path.join(home || process.cwd(), `psm/revisions/schema/${psm.migration.instante}${label}.tar.gz`);

    await tar.c(
        {
            gzip: {
                level: 9
            },
            file: archiveName,
            cwd: Path.dirname(nextRev)
        },
        [Path.basename(nextRev)]
    );

    console.log(chalk.green(`✔ Execute compactada: ${archiveName}`));

    fs.rmSync(nextRev, { recursive: true, force: true });

    gitAddPath(home || process.cwd(), archiveName );
}


export function CreateCustom(opts:ExecuteCustomOptions, ...groups:string[] ):CustomResources{
   const compiledScripts: Record<string, CustomScript[]> = {};

   for (const group of groups) {
       const scripts = collectScripts(group, opts.home );
       if (scripts)  compiledScripts[group] = scripts;
   }
    return {
        get resources(){
            return compiledScripts
        },
        async execute(): Promise<void> {
            for (const [group, sqlText] of Object.entries(compiledScripts)) {
                const result = await opts.migrator.execute(sqlText);
                if (result?.error || result?.success === false) {
                    console.error(result?.error || `Erro ao executar ${group}`);
                    throw new Error(`Erro ao executar ${group}`);
                }
                result.messages.forEach( value => {
                    console.log( value)
                });

            }
        },
        createFiles(){
            // 💾 Escrever scripts compilados dentro do nextRev
            fs.mkdirSync(opts.nextRev, { recursive: true });
            for (const [group, sqlText] of Object.entries(compiledScripts)) {
                fs.writeFileSync(Path.join( opts.nextRev, `${group}.sql`), opts.migrator.executeRaw(sqlText));
            }
        }
    }
}


/**
 * Lê todos os scripts SQL dentro de uma pasta (recursivamente),
 * concatena em um único texto e executa via migrator.execut().
 * Retorna o SQL compilado para ser salvo dentro do tar.
 */
function collectScripts(
    group: string,
    home: string
): CustomScript[] | null {
    const baseDir = Path.join(home, `psm/${group}`);
    if (!fs.existsSync(baseDir)) return null;

    const allFiles: CustomScript[] = [];

    function walk(dir: string) {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = Path.join(dir, entry.name);
            if (entry.isDirectory()) walk(fullPath);
            else if (entry.isFile() && entry.name.endsWith(".sql")) {
                const raw = fs.readFileSync(fullPath, "utf8").trim();
                allFiles.push({
                    group:group,
                    filename:fullPath,
                    raw: raw
                });
            }
        }
    }

    walk(baseDir);
    if (allFiles.length === 0) return null;

    console.log(chalk.cyan(`▶ Collected ${group} (${allFiles.length} scripts)`));

    return allFiles;
}