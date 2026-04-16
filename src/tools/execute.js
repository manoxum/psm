"use strict";
// filename: src/tools/custom.ts
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.execute = execute;
exports.CreateCustom = CreateCustom;
const fs = __importStar(require("node:fs"));
const Path = __importStar(require("node:path"));
const chalk_1 = __importDefault(require("chalk"));
const common_1 = require("./common");
const fs_1 = require("../utils/fs");
const tar = __importStar(require("tar"));
function execute(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const moment = require('moment');
        const { psm, psm_sql, driver, home } = yield (0, common_1.psmLockup)({ schema: opts.schema, driver: opts.driver });
        let label = "";
        if (!!opts.label)
            label = ` - ${(0, fs_1.sanitizeLabel)(opts.label)}`;
        const instante = moment().format('YYYYMMDDHHmmss');
        const nextRev = Path.join(home, `psm/revisions/schema/${instante}${label}`);
        if (!opts.groups || !((_a = opts.groups) === null || _a === void 0 ? void 0 : _a.length))
            opts.groups = [
                "functions", "triggers", "views"
            ];
        const migrator = driver.migrator({
            url: (0, common_1.resolveDatabaseUrl)(psm.psm.url),
            migrate: "",
            check: "",
            core: ""
        });
        const custom = CreateCustom({
            home: home,
            nextRev: nextRev,
            migrator: migrator
        }, ...opts.groups);
        yield custom.execute();
        if (!opts.save)
            return;
        let preview;
        psm.migration = {
            revision: `${moment().format('YYYYMMDDHHmmss')} - ${psm.psm.migration}`,
            instante: moment().format('YYYYMMDDHHmmss'),
            preview: preview === null || preview === void 0 ? void 0 : preview.migration.revision,
            label: opts.label
        };
        fs.mkdirSync(nextRev, { recursive: true });
        custom.createFiles();
        const archiveName = Path.join(home || process.cwd(), `psm/revisions/schema/${psm.migration.instante}${label}.tar.gz`);
        yield tar.c({
            gzip: {
                level: 9
            },
            file: archiveName,
            cwd: Path.dirname(nextRev)
        }, [Path.basename(nextRev)]);
        console.log(chalk_1.default.green(`✔ Execute compactada: ${archiveName}`));
        fs.rmSync(nextRev, { recursive: true, force: true });
        (0, fs_1.gitAddPath)(home || process.cwd(), archiveName);
    });
}
function CreateCustom(opts, ...groups) {
    const compiledScripts = {};
    for (const group of groups) {
        const scripts = collectScripts(group, opts.home);
        if (scripts)
            compiledScripts[group] = scripts;
    }
    return {
        get resources() {
            return compiledScripts;
        },
        execute() {
            return __awaiter(this, void 0, void 0, function* () {
                for (const [group, sqlText] of Object.entries(compiledScripts)) {
                    const result = yield opts.migrator.execute(sqlText);
                    if ((result === null || result === void 0 ? void 0 : result.error) || (result === null || result === void 0 ? void 0 : result.success) === false) {
                        console.error((result === null || result === void 0 ? void 0 : result.error) || `Erro ao executar ${group}`);
                        throw new Error(`Erro ao executar ${group}`);
                    }
                    result.messages.forEach(value => {
                        console.log(value);
                    });
                }
            });
        },
        createFiles() {
            // 💾 Escrever scripts compilados dentro do nextRev
            fs.mkdirSync(opts.nextRev, { recursive: true });
            for (const [group, sqlText] of Object.entries(compiledScripts)) {
                fs.writeFileSync(Path.join(opts.nextRev, `${group}.sql`), opts.migrator.executeRaw(sqlText));
            }
        }
    };
}
/**
 * Lê todos os scripts SQL dentro de uma pasta (recursivamente),
 * concatena em um único texto e executa via migrator.execut().
 * Retorna o SQL compilado para ser salvo dentro do tar.
 */
function collectScripts(group, home) {
    const baseDir = Path.join(home, `psm/${group}`);
    if (!fs.existsSync(baseDir))
        return null;
    const allFiles = [];
    function walk(dir) {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = Path.join(dir, entry.name);
            if (entry.isDirectory())
                walk(fullPath);
            else if (entry.isFile() && entry.name.endsWith(".sql")) {
                const raw = fs.readFileSync(fullPath, "utf8").trim();
                allFiles.push({
                    group: group,
                    filename: fullPath,
                    raw: raw
                });
            }
        }
    }
    walk(baseDir);
    if (allFiles.length === 0)
        return null;
    console.log(chalk_1.default.cyan(`▶ Collected ${group} (${allFiles.length} scripts)`));
    return allFiles;
}
//# sourceMappingURL=execute.js.map