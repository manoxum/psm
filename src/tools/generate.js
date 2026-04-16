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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.generate = generate;
const fs = __importStar(require("fs"));
const Path = __importStar(require("node:path"));
const yaml = __importStar(require("yaml"));
const generator_helper_1 = require("@prisma/generator-helper");
const index_1 = require("../extractor/index");
const nanoid_1 = require("nanoid");
const docs_1 = require("../docs");
function write(sql, dirname, file) {
    fs.writeFileSync(Path.join(dirname, file), sql);
}
function mergeFallbackRules(base = {}, next = {}) {
    const models = Object.assign({}, (base.models || {}));
    Object.entries(next.models || {}).forEach(([model, fields]) => {
        models[model] = Object.assign(Object.assign({}, (models[model] || {})), fields);
    });
    return { models };
}
function compileFallbacks(file) {
    const migrations = (file === null || file === void 0 ? void 0 : file.migrations) || [];
    if (!migrations.length)
        return undefined;
    return migrations.reduce((rules, migration) => {
        var _a, _b;
        return mergeFallbackRules(rules, ((_b = (_a = migration.rules) === null || _a === void 0 ? void 0 : _a.etl) === null || _b === void 0 ? void 0 : _b.fallback) || {});
    }, {});
}
function loadProjectMigration(home) {
    const candidates = [
        "psm.migration.yml",
        "psm.migration.yaml",
        "psm.migration.json",
    ];
    for (const filename of candidates) {
        const fullpath = Path.join(home, filename);
        if (!fs.existsSync(fullpath))
            continue;
        const rules = yaml.parse(fs.readFileSync(fullpath, "utf-8"));
        return {
            file: filename,
            rules,
            fallbacks: compileFallbacks(rules),
        };
    }
    return {};
}
function loadDriver(driverId) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            return yield Promise.resolve(`${driverId}`).then(s => __importStar(require(s)));
        }
        catch (error) {
            if (driverId.startsWith("@prisma-psm/")) {
                const pkg = driverId.split("/")[1];
                const localDriver = Path.resolve(__dirname, "../../../", `psm-${pkg}`, "src", "index.js");
                if (fs.existsSync(localDriver)) {
                    return yield Promise.resolve(`${localDriver}`).then(s => __importStar(require(s)));
                }
            }
            throw error;
        }
    });
}
function generate() {
    (0, generator_helper_1.generatorHandler)({
        onManifest() {
            return {
                version: '1.0.0',
                defaultOutput: './psm',
                prettyName: 'PSM - Prisma Safe Migration',
            };
        },
        onGenerate(options) {
            return __awaiter(this, void 0, void 0, function* () {
                var _a, _b;
                try {
                    const output = ((_a = options.generator.output) === null || _a === void 0 ? void 0 : _a.value) || "./psm";
                    const home = Path.dirname(options.schemaPath);
                    const definition = Path.join(output, "definitions");
                    const revisions = Path.join(output, "revisions");
                    const next = Path.join(output, "next");
                    const migrationConfig = loadProjectMigration(home);
                    fs.mkdirSync(definition, { recursive: true });
                    fs.mkdirSync(revisions, { recursive: true });
                    fs.mkdirSync(next, { recursive: true });
                    const { datamodels, models } = (0, index_1.extractModels)(options, definition);
                    let rand = (0, nanoid_1.customRandom)("abcdefghijklmnopqrstuvwxyz0123456789", 8, nanoid_1.random);
                    const migration = rand();
                    const configs = options.generator.config;
                    const url = process.env[configs.url];
                    const driver = yield loadDriver(configs.driver);
                    let usePrepare = (model) => {
                        return Promise.resolve(driver.prepare(model));
                    };
                    for (let i = 0; i < models.length; i++) {
                        const model = models[i];
                        model.model = model.name;
                        model.name = model.dbName || model.model;
                        model.temp = `temp_${i}_${model.name}`;
                        if (!!model.documentation) {
                            model.psm = (0, docs_1.parsePsmDoc)(model.documentation);
                        }
                        model.fields.forEach(field => {
                            if (!!field.documentation)
                                field.psm = (0, docs_1.parsePsmDoc)(field.documentation);
                        });
                        yield usePrepare(model);
                    }
                    const opts = {
                        models: models,
                        indexes: datamodels.indexes,
                        migration: migration,
                        shadow: `psm_shadow_${rand()}`,
                        backup: "backup",
                        sys: configs.sys || "sys",
                        fallbacks: migrationConfig.fallbacks,
                    };
                    const generator = driver.generator(opts);
                    const check = generator.check();
                    const migrate = generator.migrate();
                    const core = generator.core();
                    write(check, next, "migration.next.check.sql");
                    let test;
                    if (!!url) {
                        const migrator = driver.migrator({
                            migrate: migrate,
                            check: check,
                            core: core,
                            url: url
                        });
                        yield migrator.core();
                        test = yield migrator.test();
                    }
                    if (!configs.url) {
                        write(migrate, next, "migration.next.sql");
                    }
                    else if (!!test && test.success) {
                        write(migrate, next, "migration.next.sql");
                    }
                    else {
                        console.error(`TEST OF MIGRATION FAILED! Check file migration.next.check.sql`);
                        if (fs.existsSync(Path.join(next, "migration.next.sql")))
                            fs.unlinkSync(Path.join(next, "migration.next.sql"));
                        if (!!((_b = test === null || test === void 0 ? void 0 : test.messages) === null || _b === void 0 ? void 0 : _b.length)) {
                            console.log("TESTE MESSAGES>>>>>>>>>>");
                            test.messages.forEach(value => {
                                console.log(value);
                            });
                        }
                        if (!!(test === null || test === void 0 ? void 0 : test.error)) {
                            console.log("TESTE ERROR>>>>>>>>>>");
                            console.error(test === null || test === void 0 ? void 0 : test.error);
                        }
                    }
                    const psm = {
                        psm: {
                            migration: migration,
                            driver: options.generator.config.driver,
                            url: configs.url,
                            output: output,
                            schema: options.schemaPath,
                            sys: configs.sys || "sys",
                        },
                        sidecars: {
                            migration: migrationConfig.file,
                        },
                        migrationFile: migrationConfig.rules,
                        fallbacks: migrationConfig.fallbacks,
                        test: {
                            check: !!test ? "checked" : "skipped",
                            success: test === null || test === void 0 ? void 0 : test.success,
                            messages: test === null || test === void 0 ? void 0 : test.messages
                        }
                    };
                    fs.writeFileSync(Path.join(home, "psm.sql"), core);
                    fs.writeFileSync(Path.join(home, "psm.yml"), yaml.stringify(psm, null, {
                        version: "next",
                    }));
                }
                catch (e) {
                    console.error("console.error", e);
                    throw e;
                }
            });
        },
    });
}
//# sourceMappingURL=generate.js.map