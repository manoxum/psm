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
        throw  pullResponse.error;
    }

    if( !pullResponse.revs?.length ) {
        throw new Error(`No migrate commited! Use ${ chalk.bold("psm migrate commit")} first!`);
    }

    const revs = pullResponse.revs;

    for (let i = 0; i < revs.length; i++) {
        let next = revs[i];
        next.message.forEach( console.log );
        if( next.pulled ) return;

        const migrator = driver.migrator({
            url: process.env[next.psm.psm.url],
            migrate: next.migrate,
            check: "",
            core: "",
        });

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
}

export interface FetchOptions {
    home:string,
    driver:PSMDriver,
    psm:PSMConfigFile
}
export async function fetch(opts:FetchOptions){
    const revisions = Path.join( opts.home, `psm/revisions/schema`);
    let revs = fs.readdirSync( revisions )
        .filter( n => fs.statSync( Path.join( revisions, n)).isDirectory() )
        .map( n => {
            let psm = yaml.parse( fs.readFileSync( Path.join( revisions, n, "psm.yml") ).toString() ) as PSMConfigFile

            let migrate = fs.readFileSync( Path.join( revisions, n, "migration.sql") ).toString()
            return {
                psm,
                migrate,
                pulled:false,
                label:"",
                message:[]as string[],
                date: null as Date,
            }
        });

    if( !revs.length ){
        return {
            revs: revs,

        };
    }

    const missedPreviews = revs.filter( (n, i) => {
        return (!!n.psm.migration.preview && !revs.find( p => p.psm.migration.revision === n.psm.migration.preview))
            || ( i > 0 && !n.psm.migration.preview)
    });

    if( !!missedPreviews.length){
        const missed = missedPreviews.map(n=> `${n.psm.psm.migration} - ${n.psm.migration.label} at ${n.psm.migration.instante}`  );
        return {
            revs: revs,
            error: new Error(`MISSING PREVIEW migration for...\n ${missed.join(", ")}`),
        };
    }


    let migrated = await opts.driver.migrated({
        url: process.env[opts.psm.psm.url],
        sys: opts.psm.psm.sys
    });

    if( !migrated.success ) {
        console.error( migrated.error );
        migrated.messages.forEach( error => {
            console.error( error );
        });
        return {
            error: new Error( "Load Migrated failed!" ),
            hint: migrated.error,
            revs: revs
        };
    }

    for (let i = 0; i < revs.length; i++) {
        let next = revs[i];
        let pl = !!next.psm.migration.label?.length ? ` - ${next.psm.migration.label} `: " "
        let label = `RevNo ${ chalk.bold(next.psm.migration.revision)}${pl}commited at ${next.psm.migration.instante}`;
        const mig  = migrated.migrated.find( value => value.sid === next.psm.psm.migration );
        next.label = label;
        if( !!mig ) {
            next.date = mig.date;
            next.message.push(`${label} is migrated at ${mig.date}`);
            next.pulled = true;
        }
    }
    return {
        revs
    };
}