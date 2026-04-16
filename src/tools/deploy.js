"use strict";
// filename: src/tools/deploy.ts
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
exports.deploy = deploy;
exports.fetch = fetch;
const tar = __importStar(require("tar"));
const os = __importStar(require("node:os"));
const fs = __importStar(require("node:fs"));
const yaml = __importStar(require("yaml"));
const Path = __importStar(require("node:path"));
const chalk_1 = __importDefault(require("chalk"));
const common_1 = require("./common");
const TAG = "PSM DEPLOY >";
function extractRevisionFiles(archive, tempDir, files) {
    return __awaiter(this, void 0, void 0, function* () {
        const normalized = new Set(files);
        yield tar.x({
            file: archive,
            cwd: tempDir,
            filter: (pathname) => normalized.has(Path.basename(pathname)),
        });
    });
}
function resolveExtractedDir(tempDir) {
    const extractedDir = fs.readdirSync(tempDir)[0];
    if (!extractedDir) {
        throw new Error(`Invalid migration archive: ${tempDir}`);
    }
    return Path.join(tempDir, extractedDir);
}
function loadRevisionArchive(archive) {
    return __awaiter(this, void 0, void 0, function* () {
        const tempDir = fs.mkdtempSync(Path.join(os.tmpdir(), "psm-"));
        yield extractRevisionFiles(archive, tempDir, ["psm.yml", "migration.sql"]);
        const fullPath = resolveExtractedDir(tempDir);
        const psmPath = Path.join(fullPath, "psm.yml");
        const migrationPath = Path.join(fullPath, "migration.sql");
        if (!fs.existsSync(psmPath) || !fs.existsSync(migrationPath)) {
            fs.rmSync(tempDir, { recursive: true, force: true });
            throw new Error(`Invalid migration archive: ${Path.basename(archive)}`);
        }
        return {
            archive,
            temp: tempDir,
            extractedDir: fullPath,
            psm: yaml.parse(fs.readFileSync(psmPath, "utf-8")),
            migrate: fs.readFileSync(migrationPath, "utf-8"),
            pulled: false,
            label: "",
            message: [],
            date: null,
        };
    });
}
function ensureBackupExtracted(revision) {
    return __awaiter(this, void 0, void 0, function* () {
        if (revision.backup)
            return revision.backup;
        yield extractRevisionFiles(revision.archive, revision.temp, ["backup.sql"]);
        const backup = Path.join(revision.extractedDir, "backup.sql");
        if (fs.existsSync(backup)) {
            revision.backup = backup;
        }
        return revision.backup;
    });
}
function deploy(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c;
        const { psm, psm_sql, driver, home } = yield (0, common_1.psmLockup)({ schema: opts.schema });
        const currentUrl = (0, common_1.resolveDatabaseUrl)(psm.psm.url);
        let migrator = driver.migrator({
            url: currentUrl,
            migrate: "",
            check: "",
            core: fs.readFileSync(psm_sql).toString(),
        });
        let result = yield migrator.core();
        if (!result.success) {
            console.error(result.error);
            result.messages.forEach(error => {
                console.error(error);
            });
            throw new Error("Migrate error: Core failed!");
        }
        const pullResponse = yield fetch({
            home: home,
            driver: driver,
            psm: psm,
        });
        if (pullResponse.error) {
            pullResponse.clean();
            throw pullResponse.error;
        }
        if (!((_a = pullResponse.revs) === null || _a === void 0 ? void 0 : _a.length)) {
            pullResponse.clean();
            throw new Error(`No migrate commited! Use ${chalk_1.default.bold("psm migrate commit")} first!`);
        }
        let revs = pullResponse.revs;
        // Ordena as revisões por instante (cronologicamente)
        revs.sort((a, b) => a.psm.migration.instante.localeCompare(b.psm.migration.instante));
        let restored = false;
        for (let i = 0; i < revs.length; i++) {
            let next = revs[i];
            const nextUrl = (0, common_1.resolveDatabaseUrl)((_c = (_b = next.psm) === null || _b === void 0 ? void 0 : _b.psm) === null || _c === void 0 ? void 0 : _c.url, psm.psm.url);
            const migrator = driver.migrator({
                url: nextUrl,
                migrate: next.migrate,
                check: "",
                core: "",
            });
            next.message.forEach(console.log);
            // CORREÇÃO: se já foi migrada, apenas continua para a próxima
            if (next.pulled)
                continue;
            // Restaura backup apenas na primeira revisão não aplicada (se houver)
            if (!restored) {
                const backup = yield ensureBackupExtracted(next);
                if (backup) {
                    yield migrator.restore(backup);
                    restored = true;
                }
            }
            const result = yield migrator.migrate();
            if (!result.success) {
                console.error(result.error);
                result.messages.forEach(error => {
                    console.error(error);
                });
                console.error(`${next.label} is migrated at ${next.date} - ${chalk_1.default.redBright.bold("FAILED")}`);
                throw new Error("Migrate error: Push migration failed!");
            }
            console.log(`${next.label} is migrated at ${next.date} - ${chalk_1.default.greenBright.bold("SUCCESS")}`);
        }
        pullResponse.clean();
    });
}
function fetch(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const revisionsDir = Path.join(opts.home, "psm/revisions/schema");
        fs.mkdirSync(revisionsDir, { recursive: true });
        const revFiles = fs.readdirSync(revisionsDir).filter(n => n.endsWith(".tar.gz"));
        const revs = [];
        for (const file of revFiles) {
            revs.push(yield loadRevisionArchive(Path.join(revisionsDir, file)));
        }
        // verificar previews faltantes
        const missedPreviews = revs.filter((n, i) => {
            return (!!n.psm.migration.preview && !revs.find(p => p.psm.migration.revision === n.psm.migration.preview))
                || (i > 0 && !n.psm.migration.preview);
        });
        if (missedPreviews.length) {
            const missed = missedPreviews.map(n => `${n.psm.psm.migration} - ${n.psm.migration.label} at ${n.psm.migration.instante}`);
            return {
                revs,
                error: new Error(`MISSING PREVIEW migration for...\n${missed.join(", ")}`),
                clean() {
                    revs.forEach(value => {
                        fs.rmSync(value.temp, { recursive: true, force: true });
                    });
                },
            };
        }
        // marcar revisões já migradas
        const migrated = yield opts.driver.migrated({
            url: (0, common_1.resolveDatabaseUrl)(opts.psm.psm.url),
            sys: opts.psm.psm.sys,
            sids: revs.map((value) => value.psm.psm.migration),
        });
        if (!migrated.success) {
            console.error(migrated.error);
            migrated.messages.forEach(console.error);
            return {
                revs,
                error: new Error("Load Migrated failed!"),
                clean() {
                    revs.forEach(value => {
                        fs.rmSync(value.temp, { recursive: true, force: true });
                    });
                },
            };
        }
        for (const next of revs) {
            const pl = ((_a = next.psm.migration.label) === null || _a === void 0 ? void 0 : _a.length) ? ` - ${next.psm.migration.label} ` : " ";
            next.label = `RevNo ${next.psm.migration.revision}${pl}commited at ${next.psm.migration.instante}`;
            const mig = migrated.migrated.find(v => v.sid === next.psm.psm.migration);
            if (mig) {
                next.date = mig.date;
                next.message.push(`${next.label} is migrated at ${mig.date}`);
                next.pulled = true;
            }
        }
        return {
            revs,
            clean() {
                revs.forEach(value => {
                    fs.rmSync(value.temp, { recursive: true, force: true });
                });
            },
        };
    });
}
//# sourceMappingURL=deploy.js.map