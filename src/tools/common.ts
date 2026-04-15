import * as fs from "node:fs";
import * as Path from "node:path";
import * as yaml from "yaml";
import {PSMConfigFile} from "../configs";
import {PSMDriver} from "../driver";
import * as dotenv from "dotenv";


const prisma = [
    `${process.cwd()}/schema.prisma`,
    `${process.cwd()}/prisma/schema.prisma`,
];


export interface CommonSchema {
    schema:string
    driver?:string
}
export function loadEnv(home?: string) {
    dotenv.config();
    if (home) dotenv.config({ path: Path.join(home, ".env") });
}

export function resolveDatabaseUrl(value?: string, fallback?: string) {
    const candidate = value?.trim() || fallback?.trim();
    if (!candidate) return undefined;
    if (candidate.includes("://")) return candidate;
    if (process.env[candidate]) return process.env[candidate];
    if (fallback && process.env[fallback]) return process.env[fallback];
    return fallback?.includes("://") ? fallback : undefined;
}

export async function psmLockup( opts:CommonSchema ){
    let schema = opts.schema;
    if( !schema ) {
        schema = prisma.find( value => {
            return fs.existsSync( value );
        })
    }
    if( !schema ) {
        throw new Error ( "Migrate error: schema.prisma file not found!"  )
    }

    console.log(`PSM migrate base do schema ${schema}`);

    const home = Path.dirname( schema );
    const psm_yml = Path.join( home, "psm.yml" );
    const psm_sql = Path.join( home, "psm.sql");
    if( !fs.existsSync( psm_yml ) ) {
        throw new Error( "Migrate error: psm.yml file not found!" );
    }
    if( !fs.existsSync( psm_sql ) ) {
        throw new Error( "Migrate error: psm.sql file not found!" );
    }

    console.log(`PSM migrate using ${psm_yml}`);
    const psm = yaml.parse( fs.readFileSync( psm_yml ).toString() ) as PSMConfigFile;
    loadEnv(home);

    const driver = await import( opts?.driver ?? psm.psm.driver ) as PSMDriver;

    if(!!psm.migration && !psm.migration.instante && psm.migration["instate"]){
        psm.migration.instante = psm.migration["instate"];
    }

    return { schema, psm_yml, psm_sql, psm, driver, home }
}
