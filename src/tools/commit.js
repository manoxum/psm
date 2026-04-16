"use strict";
// filename: src/tools/commit.ts
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
exports.commit = commit;
const fs = __importStar(require("node:fs"));
const Path = __importStar(require("node:path"));
const os = __importStar(require("node:os")); // <-- adicionado para diretório temporário
const yaml = __importStar(require("yaml"));
const tar = __importStar(require("tar"));
const chalk_1 = __importDefault(require("chalk"));
const cp = __importStar(require("node:child_process"));
const deploy_1 = require("./deploy");
const common_1 = require("./common");
const fs_1 = require("../utils/fs");
const execute_1 = require("./execute");
function readRevisionConfig(archivePath) {
    return __awaiter(this, void 0, void 0, function* () {
        const tempDir = fs.mkdtempSync(Path.join(os.tmpdir(), "psm-preview-"));
        try {
            yield tar.x({
                file: archivePath,
                cwd: tempDir,
                filter: (pathname) => pathname.endsWith("/psm.yml"),
            });
            const extractedItems = fs.readdirSync(tempDir);
            if (!extractedItems.length)
                return undefined;
            const psmYamlPath = Path.join(tempDir, extractedItems[0], "psm.yml");
            if (!fs.existsSync(psmYamlPath))
                return undefined;
            return yaml.parse(fs.readFileSync(psmYamlPath, "utf-8"));
        }
        finally {
            fs.rmSync(tempDir, { recursive: true, force: true });
        }
    });
}
function commit(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const moment = require('moment');
        if (opts.generate) {
            let command = opts["generate-command"];
            if (!command)
                command = "prisma generate";
            cp.spawnSync("npx", [...command.split(" ")], {
                cwd: process.cwd(),
            });
        }
        const { psm, psm_sql, driver, home } = yield (0, common_1.psmLockup)({ schema: opts.schema });
        const next = Path.join(psm.psm.output, "next/migration.next.sql");
        const check = Path.join(psm.psm.output, "next/migration.next.check.sql");
        // -------------------- Leitura do preview corrigida --------------------
        let preview;
        const revisionsDir = Path.join(home, "psm/revisions/schema");
        const lastFile = (0, fs_1.getLatestFolder)(revisionsDir);
        if (lastFile) {
            const tarPath = Path.join(revisionsDir, lastFile);
            preview = yield readRevisionConfig(tarPath);
        }
        // ---------------------------------------------------------------------
        psm.migration = {
            revision: `${moment().format('YYYYMMDDHHmmss')} - ${psm.psm.migration}`,
            instante: moment().format('YYYYMMDDHHmmss'),
            preview: (_a = preview === null || preview === void 0 ? void 0 : preview.migration) === null || _a === void 0 ? void 0 : _a.revision,
            label: opts.label,
        };
        let label = "";
        if (!!opts.label)
            label = ` - ${(0, fs_1.sanitizeLabel)(opts.label)}`;
        const nextRev = Path.join(home, `psm/revisions/schema/${psm.migration.instante}${label}`);
        if (!fs.existsSync(check)) {
            throw new Error("Migrate error: next/migration.next.check.sql file not found!");
        }
        if (!fs.existsSync(next)) {
            throw new Error("Migrate error: next/migration.next.sql file not found!");
        }
        const migrator = driver.migrator({
            url: (0, common_1.resolveDatabaseUrl)(psm.psm.url),
            migrate: fs.readFileSync(next).toString(),
            check: fs.readFileSync(check).toString(),
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
        const fetchResponse = yield (0, deploy_1.fetch)({
            psm: psm,
            driver: driver,
            home: home,
        });
        try {
            if (fetchResponse.error) {
                throw fetchResponse.error;
            }
            const noPulled = fetchResponse.revs.filter(n => !n.pulled);
            if (noPulled.length) {
                throw new Error(`Commit not pulled already exists! Please run ${chalk_1.default.bold("psm deploy")} first!`);
            }
        }
        finally {
            fetchResponse.clean();
        }
        result = yield migrator.test();
        if (!result.success) {
            console.error(result.error);
            result.messages.forEach(error => {
                console.error(error);
            });
            throw new Error("Migrate error: Check shadow failed!");
        }
        const dump = yield migrator.dump();
        if (dump.error) {
            throw dump.error;
        }
        fs.mkdirSync(nextRev, { recursive: true });
        const custom = (0, execute_1.CreateCustom)({
            home: home,
            nextRev: nextRev,
            migrator: migrator,
        }, "functions", "triggers", "views");
        result = yield migrator.migrate(custom).catch(reason => {
            return { error: reason, success: false };
        });
        if (!result.success) {
            console.error(result.error);
            result.messages.forEach(error => {
                console.error(error);
            });
            if (fs.existsSync(nextRev)) {
                fs.rmdirSync(nextRev);
            }
            throw new Error(`Migrate error: Commit migration failed! ${home}`);
        }
        custom.createFiles();
        fs.writeFileSync(Path.join(nextRev, "migration.sql"), migrator.migrateRaw(custom));
        fs.writeFileSync(Path.join(nextRev, "psm.yml"), yaml.stringify(psm));
        if (dump.file) {
            fs.copyFileSync(dump.file, Path.join(nextRev, "backup.sql"));
            fs.rmSync(Path.dirname(dump.file), { recursive: true, force: true });
        }
        else {
            fs.writeFileSync(Path.join(nextRev, "backup.sql"), dump.output || "");
        }
        fs.unlinkSync(check);
        const archiveName = Path.join(home || process.cwd(), `psm/revisions/schema/${psm.migration.instante}${label}.tar.gz`);
        yield tar.c({
            gzip: {
                level: 9,
            },
            file: archiveName,
            cwd: Path.dirname(nextRev),
        }, [Path.basename(nextRev)]);
        console.log(chalk_1.default.green(`✔ Migration compactada: ${archiveName}`));
        fs.rmSync(nextRev, { recursive: true, force: true });
        fs.unlinkSync(next);
        (0, fs_1.gitAddPath)(home || process.cwd(), archiveName);
    });
}
//# sourceMappingURL=commit.js.map