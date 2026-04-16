"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.addRenameColumnRule = addRenameColumnRule;
exports.addTransformColumnRule = addTransformColumnRule;
exports.addMoveColumnRule = addMoveColumnRule;
exports.addRlsPolicyRule = addRlsPolicyRule;
const fs = __importStar(require("node:fs"));
const Path = __importStar(require("node:path"));
const yaml = __importStar(require("yaml"));
const fs_1 = require("../utils/fs");
const schemaCandidates = [
    `${process.cwd()}/schema.prisma`,
    `${process.cwd()}/prisma/schema.prisma`,
];
function resolveSchemaPath(schema) {
    if (schema && fs.existsSync(schema))
        return schema;
    const found = schemaCandidates.find((candidate) => fs.existsSync(candidate));
    if (!found) {
        throw new Error("Migrate error: schema.prisma file not found!");
    }
    return found;
}
function getMigrationFilePath(home) {
    return Path.join(home, "psm.migration.yml");
}
function loadProjectMigrationFile(filename) {
    if (!fs.existsSync(filename)) {
        return { migrations: [] };
    }
    return yaml.parse(fs.readFileSync(filename, "utf-8"));
}
function buildRevisionName(description, revision) {
    if (revision === null || revision === void 0 ? void 0 : revision.trim())
        return revision.trim();
    const stamp = new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
    const label = (0, fs_1.sanitizeLabel)(description || "migration-rule")
        .toLowerCase()
        .replace(/\s+/g, "-");
    return `${stamp}-${label || "migration-rule"}`;
}
function appendRevision(opts, defaultDescription, buildRules) {
    var _a;
    const schema = resolveSchemaPath(opts.schema);
    const home = Path.dirname(schema);
    const filename = getMigrationFilePath(home);
    const migrationFile = loadProjectMigrationFile(filename);
    const revision = buildRevisionName(opts.description || defaultDescription, opts.revision);
    if (!migrationFile.migrations)
        migrationFile.migrations = [];
    if (migrationFile.migrations.some((item) => item.revision === revision)) {
        throw new Error(`Migration rule '${revision}' already exists in ${filename}`);
    }
    const next = {
        revision,
        description: opts.description || defaultDescription,
        once: (_a = opts.once) !== null && _a !== void 0 ? _a : true,
        rules: buildRules(),
    };
    migrationFile.migrations.push(next);
    fs.writeFileSync(filename, yaml.stringify(migrationFile, null, { version: "1.2" }));
    (0, fs_1.gitAddPath)(home, filename);
    return { schema, home, filename, revision };
}
function addRenameColumnRule(opts) {
    return appendRevision(opts, `Rename column ${opts.model}.${opts.from} -> ${opts.to}`, () => ({
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
    }));
}
function addTransformColumnRule(opts) {
    return appendRevision(opts, `Transform column ${opts.model}.${opts.column} -> ${opts.to}`, () => ({
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
    }));
}
function addMoveColumnRule(opts) {
    if (opts.first && opts.after) {
        throw new Error("Move column error: use either --first or --after, not both.");
    }
    return appendRevision(opts, `Move column ${opts.model}.${opts.column}`, () => ({
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
    }));
}
function addRlsPolicyRule(opts) {
    return appendRevision(opts, `Define RLS policy ${opts.name} on ${opts.table}`, () => ({
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
    }));
}
//# sourceMappingURL=project-migration.js.map