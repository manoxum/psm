export interface BackupOptions {
    schema?: string;
    add?: boolean;
    label?: string;
    level?: number;
}
export declare function backup(opts: BackupOptions): Promise<void>;
//# sourceMappingURL=backup.d.ts.map