import { PSMMigrationFallbackRules, PSMProjectMigrationFile } from "./driver";

export interface PSMConfigFile {
    psm: {
        migration: string
        driver: string
        url: string
        sys: string
        output: string
        schema: string
    },
    sidecars?: {
        migration?: string
    },
    migrationFile?: PSMProjectMigrationFile,
    fallbacks?: PSMMigrationFallbackRules,
    test: {
        check: "checked"|"skipped",
        success: boolean,
        messages: string[]
    },
    migration?: {
        revision: string,
        instante: string,
        preview: string,
        label: string,
    }
}
