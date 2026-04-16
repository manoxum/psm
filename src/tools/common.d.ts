import { PSMConfigFile } from "../configs";
import { PSMDriver } from "../driver";
export interface CommonSchema {
    schema: string;
    driver?: string;
}
export declare function loadEnv(home?: string): void;
export declare function resolveDatabaseUrl(value?: string, fallback?: string): string;
export declare function psmLockup(opts: CommonSchema): Promise<{
    schema: string;
    psm_yml: string;
    psm_sql: string;
    psm: PSMConfigFile;
    driver: PSMDriver;
    home: string;
}>;
//# sourceMappingURL=common.d.ts.map