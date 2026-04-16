import { GeneratorOptions } from '@prisma/generator-helper';
export declare function extractModels(options: GeneratorOptions, dirname: string): {
    models: readonly import("@prisma/dmmf/dist/util").ReadonlyDeep<import("@prisma/dmmf/dist/util").ReadonlyDeep<import("@prisma/dmmf/dist/util").ReadonlyDeep<{
        name: string;
        dbName: string | null;
        schema: string | null;
        fields: import("@prisma/dmmf").Field[];
        uniqueFields: string[][];
        uniqueIndexes: import("@prisma/dmmf").uniqueIndex[];
        documentation?: string;
        primaryKey: import("@prisma/dmmf").PrimaryKey | null;
        isGenerated?: boolean;
    }>>>[];
    enums: readonly import("@prisma/dmmf/dist/util").ReadonlyDeep<import("@prisma/dmmf/dist/util").ReadonlyDeep<import("@prisma/dmmf/dist/util").ReadonlyDeep<{
        name: string;
        values: import("@prisma/dmmf").EnumValue[];
        dbName?: string | null;
        documentation?: string;
    }>>>[];
    datamodels: any;
    schema: import("@prisma/dmmf/dist/util").ReadonlyDeep<import("@prisma/dmmf/dist/util").ReadonlyDeep<{
        rootQueryType?: string;
        rootMutationType?: string;
        inputObjectTypes: {
            model?: import("@prisma/dmmf").InputType[];
            prisma: import("@prisma/dmmf").InputType[];
        };
        outputObjectTypes: {
            model: import("@prisma/dmmf").OutputType[];
            prisma: import("@prisma/dmmf").OutputType[];
        };
        enumTypes: {
            model?: import("@prisma/dmmf").SchemaEnum[];
            prisma: import("@prisma/dmmf").SchemaEnum[];
        };
        fieldRefTypes: {
            prisma?: import("@prisma/dmmf").FieldRefType[];
        };
    }>>;
};
//# sourceMappingURL=index.d.ts.map