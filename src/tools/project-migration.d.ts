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
export declare function addRenameColumnRule(opts: RenameColumnOptions): {
    schema: string;
    home: string;
    filename: string;
    revision: string;
};
export declare function addTransformColumnRule(opts: TransformColumnOptions): {
    schema: string;
    home: string;
    filename: string;
    revision: string;
};
export declare function addMoveColumnRule(opts: MoveColumnOptions): {
    schema: string;
    home: string;
    filename: string;
    revision: string;
};
export declare function addRlsPolicyRule(opts: RlsPolicyOptions): {
    schema: string;
    home: string;
    filename: string;
    revision: string;
};
//# sourceMappingURL=project-migration.d.ts.map