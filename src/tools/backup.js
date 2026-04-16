"use strict";
// filename: src/tools/backup.ts
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
exports.backup = backup;
const fs = __importStar(require("node:fs"));
const Path = __importStar(require("node:path"));
const os = __importStar(require("node:os"));
const tar = __importStar(require("tar"));
const chalk_1 = __importDefault(require("chalk"));
const common_1 = require("./common");
const fs_1 = require("../utils/fs");
function backup(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const { psm, psm_sql, driver, home, schema } = yield (0, common_1.psmLockup)({ schema: opts.schema });
        const migrator = driver.migrator({
            url: (0, common_1.resolveDatabaseUrl)(psm.psm.url),
            migrate: '', // Não necessário para backup
            check: '',
            core: fs.readFileSync(psm_sql).toString(),
        });
        // Executa dump
        const dump = yield migrator.dump();
        if (dump.error)
            throw dump.error;
        const moment = require('moment');
        const now = moment();
        const instant = now.format('YYYYMMDDHHmmss');
        const label = opts.label ? ` - ${(0, fs_1.sanitizeLabel)(opts.label)}` : '';
        const compressionLevel = (_a = opts.level) !== null && _a !== void 0 ? _a : 9; // nível de compressão padrão 9
        // Caminho temporário do arquivo
        const tmpDir = fs.mkdtempSync(Path.join(os.tmpdir(), `psm-backup-${instant}-`));
        fs.mkdirSync(tmpDir, { recursive: true });
        const backupFile = Path.join(tmpDir, 'backup.sql');
        if (dump.file) {
            fs.copyFileSync(dump.file, backupFile);
            fs.rmSync(Path.dirname(dump.file), { recursive: true, force: true });
        }
        else {
            fs.writeFileSync(backupFile, dump.output || "");
        }
        fs.writeFileSync(Path.join(tmpDir, 'README'), `
        Label   : ${opts.label}
        Level   : ${compressionLevel}
        Git Add : ${opts.add}
        Instante: ${now.toISOString()}
        Driver  : ${psm.psm.driver}
        Scheme  : ${schema}
    `.split("\n").map(value => value.trim()).filter(value => !!value).join("\n"));
        // Arquivo final .tar.gz
        const archiveName = Path.join(home, `psm/backup/${instant}${label}.tar.gz`);
        // Compacta **somente o arquivo backup.sql** (sem nível extra)
        yield tar.c({
            gzip: { level: compressionLevel },
            file: archiveName,
            cwd: tmpDir
        }, ['backup.sql', "README"]);
        console.log(chalk_1.default.green(`✔ Backup gerado: ${archiveName}`));
        // Remove pasta temporária
        fs.rmSync(tmpDir, { recursive: true, force: true });
        // Adiciona o .tar.gz ao git
        if (opts.add)
            (0, fs_1.gitAddPath)(home || process.cwd(), archiveName);
    });
}
//# sourceMappingURL=backup.js.map