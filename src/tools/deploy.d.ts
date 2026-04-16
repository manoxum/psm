import { PSMConfigFile } from "../configs";
import { PSMDriver } from "../driver";
export interface DeployOptions {
    schema?: string;
    generate?: string;
    label?: string;
    "generate-command": string;
}
type RevisionEntry = {
    archive: string;
    temp: string;
    extractedDir: string;
    psm: PSMConfigFile;
    migrate: string;
    pulled: boolean;
    label: string;
    message: string[];
    date: Date | null;
    backup?: string;
};
export declare function deploy(opts: DeployOptions): Promise<void>;
export interface FetchOptions {
    home: string;
    driver: PSMDriver;
    psm: PSMConfigFile;
}
export declare function fetch(opts: FetchOptions): Promise<{
    revs: RevisionEntry[];
    error: Error;
    clean(): void;
} | {
    revs: RevisionEntry[];
    clean(): void;
    error?: undefined;
}>;
export {};
//# sourceMappingURL=deploy.d.ts.map