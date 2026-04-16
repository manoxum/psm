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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.sanitizeLabel = sanitizeLabel;
exports.getLatestFolder = getLatestFolder;
exports.isGitRepo = isGitRepo;
exports.gitAddPath = gitAddPath;
const fs = __importStar(require("node:fs"));
const chalk_1 = __importDefault(require("chalk"));
const cp = __importStar(require("node:child_process"));
const Path = __importStar(require("node:path"));
function sanitizeLabel(label) {
    return label
        .replace(/[<>:"/\\|?*\x00-\x1F]/g, '') // remove caracteres inválidos no Windows
        .replace(/[\u{0080}-\u{FFFF}]/gu, '') // remove caracteres não ASCII (opcional)
        .trim()
        .replace(/\s+/g, ' '); // normaliza espaços
}
function getLatestFolder(basePath) {
    if (!fs.existsSync(basePath))
        return null;
    const dirs = fs.readdirSync(basePath, { withFileTypes: true })
        .filter(d => d.isFile())
        .map(d => d.name)
        .filter(name => /^\d{14}(\.tar\.gz)?( - .+)?$/.test(name))
        .sort((a, b) => b.localeCompare(a));
    return dirs[0] || null;
}
function isGitRepo(cwd) {
    const res = cp.spawnSync("git", ["rev-parse", "--is-inside-work-tree"], {
        cwd,
        encoding: "utf-8",
    });
    return res.status === 0 && res.stdout.trim() === "true";
}
function gitAddPath(cwd, targetPath) {
    var _a;
    try {
        if (!isGitRepo(cwd)) {
            console.log(chalk_1.default.gray("Repositório git não detectado; ignorando 'git add'."));
            return;
        }
        const relPath = Path.relative(cwd, targetPath);
        const addTarget = relPath.startsWith("..") ? targetPath : relPath;
        const addRes = cp.spawnSync("git", ["add", addTarget], {
            cwd,
            stdio: "inherit",
        });
        if (addRes.status !== 0) {
            console.warn(chalk_1.default.yellow(`Aviso: 'git add' falhou para ${addTarget}.`));
        }
        else {
            console.log(chalk_1.default.green(`✔ Adicionado ao git: ${addTarget}`));
        }
    }
    catch (err) {
        console.warn(chalk_1.default.yellow("Aviso: não foi possível adicionar ao git."), (_a = err === null || err === void 0 ? void 0 : err.message) !== null && _a !== void 0 ? _a : err);
    }
}
//# sourceMappingURL=fs.js.map