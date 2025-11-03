import * as fs from "node:fs";
import * as Path from "node:path";
import * as yaml from "yaml";
import {PSMConfigFile} from "../configs";
import {PSMDriver} from "../driver";


const prisma = [
    `${process.cwd()}/schema.prisma`,
    `${process.cwd()}/prisma/schema.prisma`,
];


export interface CommonSchema {
    schema:string
    driver?:string
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

    const driver = await import( opts?.driver ?? psm.psm.driver ) as PSMDriver;

    if(!!psm.migration && !psm.migration.instante && psm.migration["instate"]){
        psm.migration.instante = psm.migration["instate"];
    }

    return { schema, psm_yml, psm_sql, psm, driver, home }
}