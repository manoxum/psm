import * as fs from "node:fs";
import * as Path from "node:path";
import * as yaml from "yaml";
import { sanitizeLabel, gitAddPath } from "../utils/fs";
import { PSMProjectMigrationFile, PSMProjectMigrationRevision, PSMMigrationRevisionRuleSet } from "../driver";

const schemaCandidates = [
    `${process.cwd()}/schema.prisma`,
    `${process.cwd()}/prisma/schema.prisma`,
];

export interface ProjectMigrationOptions {
    schema?: string;
    revision?: string;
    description?: string;
    once?: boolean;
}

export interface RenameColumnOptions extends ProjectMigrationOptions {
    model: string;
    from: string;
    to: string;
    references?: "preserve" | "drop";
}

export interface TransformColumnOptions extends ProjectMigrationOptions {
    model: string;
    column: string;
    to: string;
    from?: string;
    using?: string;
}

export interface MoveColumnOptions extends ProjectMigrationOptions {
    model: string;
    column: string;
    after?: string;
    first?: boolean;
}

export interface RlsPolicyOptions extends ProjectMigrationOptions {
    schema_name?: string;
    table: string;
    name: string;
    command?: "ALL" | "SELECT" | "INSERT" | "UPDATE" | "DELETE";
    to?: string[];
    using?: string;
    check?: string;
}

function resolveSchemaPath(schema?: string) {
    if (schema && fs.existsSync(schema)) return schema;

    const found = schemaCandidates.find((candidate) => fs.existsSync(candidate));
    if (!found) {
        throw new Error("Migrate error: schema.prisma file not found!");
    }

    return found;
}

function getMigrationFilePath(home: string) {
    return Path.join(home, "psm.migration.yml");
}

function loadProjectMigrationFile(filename: string): PSMProjectMigrationFile {
    if (!fs.existsSync(filename)) {
        return { migrations: [] };
    }

    return yaml.parse(fs.readFileSync(filename, "utf-8")) as PSMProjectMigrationFile;
}

function buildRevisionName(description?: string, revision?: string) {
    if (revision?.trim()) return revision.trim();

    const stamp = new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
    const label = sanitizeLabel(description || "migration-rule")
        .toLowerCase()
        .replace(/\s+/g, "-");

    return `${stamp}-${label || "migration-rule"}`;
}

function appendRevision(
    opts: ProjectMigrationOptions,
    defaultDescription: string,
    buildRules: () => PSMMigrationRevisionRuleSet,
) {
    const schema = resolveSchemaPath(opts.schema);
    const home = Path.dirname(schema);
    const filename = getMigrationFilePath(home);
    const migrationFile = loadProjectMigrationFile(filename);
    const revision = buildRevisionName(opts.description || defaultDescription, opts.revision);

    if (!migrationFile.migrations) migrationFile.migrations = [];
    if (migrationFile.migrations.some((item) => item.revision === revision)) {
        throw new Error(`Migration rule '${revision}' already exists in ${filename}`);
    }

    const next: PSMProjectMigrationRevision = {
        revision,
        description: opts.description || defaultDescription,
        once: opts.once ?? true,
        rules: buildRules(),
    };

    migrationFile.migrations.push(next);
    fs.writeFileSync(filename, yaml.stringify(migrationFile, null, { version: "1.2" }));
    gitAddPath(home, filename);

    return { schema, home, filename, revision };
}

export function addRenameColumnRule(opts: RenameColumnOptions) {
    return appendRevision(
        opts,
        `Rename column ${opts.model}.${opts.from} -> ${opts.to}`,
        () => ({
            rename: {
                columns: [
                    {
                        model: opts.model,
                        from: opts.from,
                        to: opts.to,
                        references: opts.references || "preserve",
                    },
                ],
            },
        }),
    );
}

export function addTransformColumnRule(opts: TransformColumnOptions) {
    return appendRevision(
        opts,
        `Transform column ${opts.model}.${opts.column} -> ${opts.to}`,
        () => ({
            transform: {
                columns: [
                    {
                        model: opts.model,
                        column: opts.column,
                        from: opts.from,
                        to: opts.to,
                        using: opts.using,
                    },
                ],
            },
        }),
    );
}

export function addMoveColumnRule(opts: MoveColumnOptions) {
    if (opts.first && opts.after) {
        throw new Error("Move column error: use either --first or --after, not both.");
    }

    return appendRevision(
        opts,
        `Move column ${opts.model}.${opts.column}`,
        () => ({
            move: {
                columns: [
                    {
                        model: opts.model,
                        column: opts.column,
                        after: opts.after,
                        first: opts.first,
                    },
                ],
            },
        }),
    );
}

export function addRlsPolicyRule(opts: RlsPolicyOptions) {
    return appendRevision(
        opts,
        `Define RLS policy ${opts.name} on ${opts.table}`,
        () => ({
            rls: {
                policies: [
                    {
                        schema: opts.schema_name || "public",
                        table: opts.table,
                        name: opts.name,
                        command: opts.command || "ALL",
                        to: opts.to,
                        using: opts.using,
                        check: opts.check,
                    },
                ],
            },
        }),
    );
}
