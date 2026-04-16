import { CustomResources, PSMMigrator } from "../driver";
export interface CustomOptions {
    save?: boolean;
    schema?: string;
    label?: string;
    groups?: string[];
    driver?: string;
}
export type ExecuteCustomOptions = {
    home: string;
    nextRev: string;
    migrator: PSMMigrator;
};
export declare function execute(opts: CustomOptions): Promise<void>;
export declare function CreateCustom(opts: ExecuteCustomOptions, ...groups: string[]): CustomResources;
//# sourceMappingURL=execute.d.ts.map