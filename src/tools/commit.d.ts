export interface MigrateOptions {
    schema?: string;
    generate?: string;
    label?: string;
    "generate-command": string;
}
export declare function commit(opts: MigrateOptions): Promise<void>;
//# sourceMappingURL=commit.d.ts.map