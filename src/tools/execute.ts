// filename: src/tools/custom.ts


import * as fs from "node:fs";
import * as Path from "node:path";
import chalk from "chalk";
import {PSMMigrator} from "../driver";
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


export function CreateCustom(opts:ExecuteCustomOptions, ...groups:string[] ){
   const compiledScripts: Record<string, string> = {};
    return {
       async execute(){
           for (const group of groups) {
               const sql = await processCustomScripts(group, opts.home, opts.migrator);
               if (sql) {
                   compiledScripts[`${group}.sql`] = sql;
               }
           }
       },

        createFiles(){
            // 💾 Escrever scripts compilados dentro do nextRev
            for (const [filename, sqlText] of Object.entries(compiledScripts)) {
                fs.writeFileSync(Path.join( opts.nextRev, filename), sqlText);
            }
        }
    }
}


/**
 * Lê todos os scripts SQL dentro de uma pasta (recursivamente),
 * concatena em um único texto e executa via migrator.execut().
 * Retorna o SQL compilado para ser salvo dentro do tar.
 */
async function processCustomScripts(
    group: string,
    home: string,
    migrator: PSMMigrator
): Promise<string | null> {
    const baseDir = Path.join(home, `psm/${group}`);
    if (!fs.existsSync(baseDir)) return null;

    const allFiles: string[] = [];

    function walk(dir: string) {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = Path.join(dir, entry.name);
            if (entry.isDirectory()) walk(fullPath);
            else if (entry.isFile() && entry.name.endsWith(".sql")) {
                allFiles.push(fullPath);
            }
        }
    }

    walk(baseDir);
    if (allFiles.length === 0) return null;

    const contents = allFiles.map((f) => fs.readFileSync(f, "utf8").trim());
    const sqlText = contents
        .map((c) => (c.endsWith(";") ? c : `${c};`))
        .join("\n\n");

    console.log(chalk.cyan(`▶ Executando ${group} (${allFiles.length} scripts)`));

    const result = await migrator.execute(sqlText);
    if (result?.error || result?.success === false) {
        console.error(result?.error || `Erro ao executar ${group}`);
        throw new Error(`Erro ao executar ${group}`);
    }
    result.messages.forEach( value => {
        console.log( value)
    });
    return sqlText;
}