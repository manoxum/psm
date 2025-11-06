export interface PSMField {
    comment?:string
    restore?: {
        expression?:string
    }
}

export interface UniqueOptions {
    name:string,
    fields:string[]
    indexes:[]
}

export interface IndexOptions {
    model:string,
    type:"id"|"unique"|"normal"
    name?:string
    dbName?:string
    isDefinedOnField?:boolean
    algorithm:string
    fields: ({ name:string })[]
}
export interface FieldOption {
    "name": string
    documentation?: string
    psm?: PSMField
    dbName?: string
    "kind": "scalar"|"object"
    "isList": boolean,
    "isRequired": boolean,
    "isUnique": boolean,
    "isId": boolean,
    "isReadOnly": boolean,
    "hasDefaultValue": boolean,
    "type": string,
    "isGenerated": boolean,
    "isUpdatedAt": boolean,
    relationName?:string
    relationFromFields?:string[]
    relationToFields?:string[]
    "default"?: {
        "name": string,
        "args": []
    },
    "nativeType"?: [
        string,
        [
            "6"
        ]
    ],
}

export interface PSMParserOptions {
    models:ModelOptions[]
    indexes:IndexOptions[]
    backup:string
    sys:string
    migration:string
    shadow:string
}

export interface PSMModel {
    view?:boolean
    query?:{
        [p:string]:string
    }
    backup?:{
        skip?:boolean
        clean?:boolean,
        rev?:{
            version?:string
            apply?:"ALWAYS"|"WHEN"|"WHEN_EXCEPTION",
            from?: "query"|"query:linked"|"relation"|"model"
            expression?:string
            exists?:string
            when?:string
        }
    }
    comment?:string
}

export interface ModelOptions {
    name:string,
    model:string,
    index:number
    documentation?:string,
    psm?:PSMModel,
    dbName?:string,
    schema:string,
    temp:string,
    fields:FieldOption[]
    primaryKey:FieldOption[]
    uniqueFields:string[][]
    uniqueIndexes:UniqueOptions[]
    isGenerated:FieldOption[]
    indexes:IndexOptions[]
}

export interface MigrationOptions {
    sql:string
    url:string,
    label:string
}


export interface PSMMigrationOptions {
    check:string,
    core:string,
    url:string,
    migrate:string
}

export interface Migrated {
    sid:string,
    date: Date
}

export interface PSMMigratedOptions {
    url:string,
    sys:string
}

export interface PSMMigrationResult {
    success?:boolean
    messages?:string[]
    error?:any
}


export interface PSMGenerator {
    migrate():string,
    check():string,
    core():string,
}

export interface PSMMigrated {
    messages:string[]
    error?:any
    success?:boolean
    migrated?:Migrated[]
}

export interface PSMExecute {
    messages:string[]
    error?:any
    success?:boolean
}

export interface PSMDumpResponse {
    error?:Error,
    output?:string
}

export type CustomScript = {
    group:string,
    filename:string,
    raw:string
}
export interface CustomResources {
    get resources():Record<string, CustomScript[]>,
    execute():Promise<void>
    createFiles():void
}

export interface PSMMigrator {
    core():Promise<PSMMigrationResult>,
    test():Promise<PSMMigrationResult>,
    migrate( custom?:CustomResources ):Promise<PSMMigrationResult>,
    dump():Promise<PSMDumpResponse>,
    execute(str:CustomScript[]):Promise<PSMExecute>
    executeRaw(str:CustomScript[]):string
}

export interface QueryBuilderResult {
    sql:string,
    template:string[],
    values:any
    push( ...builders:QueryBuilderResult[] ):void
}

export type ScriptLine = {
    line:number,
    filename: string,
    error:Error,
    column:number,
    func:string
}

export type RevisionProps<Props extends {[p in keyof Props]:Props[p]}> = Props
export type RevisionWhen<Props> = boolean|( (props:RevisionProps<Props>)=>boolean)|( (props:RevisionProps<Props>)=>Promise<boolean>);

export interface RegistryOptions <Props>{
    identifier?: string;
    connection?: string|"default"|"standard"|"superuser";
    unique?: boolean;
    force?: string;
    flags?: string[];
    level?: "critical" | "normal" | "optional";
    line?:ScriptLine,
    when?:RevisionWhen<Props>
}

export interface SimpleFile {
    filename: string;
}

export type PatchOptions<Props> = RegistryOptions<Props> & {
    module?: NodeModule | SimpleFile
}
// Define o tipo de ouvinte de revisão que inclui tratamento de atualização
export type RevisionListener = {
    onRegister?(error: Error, onRelease: () => void): void;
}

type SQLTemplate = ( sqlTemplate:TemplateStringsArray, ...values:any[] ) => QueryBuilderResult & {
    sql:SQLTemplate
    joins( ...builders:QueryBuilderResult[] ):void
}

export interface SqlPatch <Props>{
    opts:RegistryOptions<Props> & PatchOptions<Props>
    str: string | QueryBuilderResult;
    values: any[] | RevisionListener;
    listener?: RevisionListener;
    line:ScriptLine
}


export interface PSMDriver {
    migrated:( opts:PSMMigratedOptions )=>Promise<PSMMigrated>,
    generator:( opts:PSMParserOptions )=>PSMGenerator,
    migrator:( opts:PSMMigrationOptions ) =>PSMMigrator,
    prepare: ( model:ModelOptions )=>Promise<any>|void,
    // sql:SQLTemplate,
    // joins( ...builders:QueryBuilderResult[] ):void
    // patch <Props>(opts:PatchOptions<Props>, sql:QueryBuilderResult, listener?: RevisionListener):SqlPatch<Props>
    // patchSQL <Props>(opts?:PatchOptions<Props>, listener?: RevisionListener):SQLTemplate
}

export interface DriverConfigs {
    driver:string,
    url:string,
    sys:string,
}

