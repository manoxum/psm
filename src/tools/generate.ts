import * as fs from 'fs'
import * as Path from "node:path";
import * as yaml from "yaml";
import {generatorHandler, GeneratorManifest, GeneratorOptions} from "@prisma/generator-helper";
import {extractModels} from "../extractor/index";
import {customRandom, random} from "nanoid";
import {
    DriverConfigs,
    PSMMigrationFallbackRules,
    PSMProjectMigrationFile,
    ModelOptions,
    PSMDriver,
    PSMField,
    PSMMigrationResult,
    PSMModel,
    PSMParserOptions
} from "../driver";
import {parsePsmDoc} from "../docs";
import {PSMConfigFile} from "../configs";

function write( sql:string, dirname:string, file:string){
    fs.writeFileSync( Path.join( dirname, file ), sql );
}

function mergeFallbackRules(
    base: PSMMigrationFallbackRules = {},
    next: PSMMigrationFallbackRules = {}
): PSMMigrationFallbackRules {
    const models = { ...(base.models || {}) };

    Object.entries(next.models || {}).forEach(([model, fields]) => {
        models[model] = {
            ...(models[model] || {}),
            ...fields,
        };
    });

    return { models };
}

function compileFallbacks(file?: PSMProjectMigrationFile): PSMMigrationFallbackRules | undefined {
    const migrations = file?.migrations || [];
    if (!migrations.length) return undefined;

    return migrations.reduce<PSMMigrationFallbackRules>((rules, migration) => {
        return mergeFallbackRules(rules, migration.rules?.etl?.fallback || {});
    }, {});
}

function loadProjectMigration(home: string): { file?: string; rules?: PSMProjectMigrationFile; fallbacks?: PSMMigrationFallbackRules } {
    const candidates = [
        "psm.migration.yml",
        "psm.migration.yaml",
        "psm.migration.json",
    ];

    for (const filename of candidates) {
        const fullpath = Path.join(home, filename);
        if (!fs.existsSync(fullpath)) continue;

        const rules = yaml.parse(fs.readFileSync(fullpath, "utf-8")) as PSMProjectMigrationFile;

        return {
            file: filename,
            rules,
            fallbacks: compileFallbacks(rules),
        };
    }

    return {};
}

async function loadDriver(driverId: string): Promise<PSMDriver> {
    try {
        return await import(driverId) as PSMDriver;
    } catch (error: any) {
        if (driverId.startsWith("@prisma-psm/")) {
            const pkg = driverId.split("/")[1];
            const localDriver = Path.resolve(__dirname, "../../../", `psm-${pkg}`, "src", "index.js");

            if (fs.existsSync(localDriver)) {
                return await import(localDriver) as PSMDriver;
            }
        }

        throw error;
    }
}


export function generate(){
    generatorHandler({
        onManifest(): GeneratorManifest {
            return {
                version: '1.0.0',
                defaultOutput: './psm',
                prettyName: 'PSM - Prisma Safe Migration',
            }
        },
        async onGenerate(options: GeneratorOptions) {
            try {
                const output = options.generator.output?.value || "./psm";
                const home = Path.dirname( options.schemaPath );
                const definition = Path.join( output, "definitions");
                const revisions = Path.join( output, "revisions");
                const next = Path.join( output, "next");
                const migrationConfig = loadProjectMigration(home);

                fs.mkdirSync( definition, { recursive: true });
                fs.mkdirSync( revisions, { recursive: true });
                fs.mkdirSync( next, { recursive: true });
                const { datamodels, models} = extractModels( options, definition );


                let rand = customRandom( "abcdefghijklmnopqrstuvwxyz0123456789",8, random);
                const migration = rand();
                const configs:DriverConfigs = options.generator.config as any;
                const url = process.env[configs.url] as string;



                const driver = await loadDriver(configs.driver);


                let usePrepare= ( model:ModelOptions )=>{
                    return Promise.resolve( driver.prepare( model ) )
                }

                for (let i = 0; i < models.length; i++) {
                    const model = models[i] as any as ModelOptions;
                    model.model = model.name;
                    model.name = model.dbName||model.model;
                    model.temp = `temp_${i}_${model.name}`;


                    if( !!model.documentation ){
                        model.psm = parsePsmDoc<PSMModel>( model.documentation );
                    }
                    model.fields.forEach( field => {
                        if( !!field.documentation ) field.psm = parsePsmDoc<PSMField>( field.documentation );
                    });
                    await usePrepare(model);
                }

                const opts: PSMParserOptions = {
                    models: models as any,
                    indexes: datamodels.indexes,
                    migration: migration,
                    shadow: `psm_shadow_${rand()}`,
                    backup: "backup",
                    sys: configs.sys||"sys",
                    fallbacks: migrationConfig.fallbacks,
                };


                const generator = driver.generator(opts);



                const check = generator.check();
                const migrate = generator.migrate();
                const core = generator.core();

                write( check, next, "migration.next.check.sql" );


                let test:PSMMigrationResult|undefined;

                if( !!url ) {
                    const migrator =  driver.migrator({
                        migrate: migrate,
                        check: check,
                        core: core,
                        url: url as string
                    });

                    await migrator.core();

                    test = await migrator.test();
                }

                if( !configs.url ){
                    write( migrate, next, "migration.next.sql" );
                } else if( !!test && test.success ) {
                    write( migrate, next, "migration.next.sql" );
                } else {
                    console.error( `TEST OF MIGRATION FAILED! Check file migration.next.check.sql` );
                    if( fs.existsSync( Path.join( next, "migration.next.sql" ) ) ) fs.unlinkSync( Path.join( next, "migration.next.sql" ) );

                    if( !!test?.messages?.length ){
                        console.log("TESTE MESSAGES>>>>>>>>>>");
                        test.messages.forEach(value => {
                            console.log( value)
                        });
                    }

                    if( !!test?.error ){
                        console.log("TESTE ERROR>>>>>>>>>>");
                        console.error( test?.error );
                    }
                }

                const psm:PSMConfigFile = {
                    psm: {
                        migration: migration,
                        driver: options.generator.config.driver as string,
                        url: configs.url,
                        output: output,
                        schema: options.schemaPath,
                        sys: configs.sys||"sys",
                    },
                    sidecars: {
                        migration: migrationConfig.file,
                    },
                    migrationFile: migrationConfig.rules,
                    fallbacks: migrationConfig.fallbacks,
                    test: {
                        check: !!test? "checked": "skipped",
                        success: test?.success,
                        messages: test?.messages
                    }
                }
                fs.writeFileSync( Path.join( home, "psm.sql" ),  core )
                fs.writeFileSync( Path.join( home, "psm.yml" ), yaml.stringify(psm, null, {
                    version: "next",
                }))
            } catch (e){
                console.error( "console.error", e );
                throw e
            }
        },
    })
}
