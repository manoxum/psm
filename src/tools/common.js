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
exports.loadEnv = loadEnv;
exports.resolveDatabaseUrl = resolveDatabaseUrl;
exports.psmLockup = psmLockup;
const fs = __importStar(require("node:fs"));
const Path = __importStar(require("node:path"));
const yaml = __importStar(require("yaml"));
const dotenv = __importStar(require("dotenv"));
const prisma = [
    `${process.cwd()}/schema.prisma`,
    `${process.cwd()}/prisma/schema.prisma`,
];
function loadEnv(home) {
    dotenv.config();
    if (home)
        dotenv.config({ path: Path.join(home, ".env") });
}
function resolveDatabaseUrl(value, fallback) {
    const candidate = (value === null || value === void 0 ? void 0 : value.trim()) || (fallback === null || fallback === void 0 ? void 0 : fallback.trim());
    if (!candidate)
        return undefined;
    if (candidate.includes("://"))
        return candidate;
    if (process.env[candidate])
        return process.env[candidate];
    if (fallback && process.env[fallback])
        return process.env[fallback];
    return (fallback === null || fallback === void 0 ? void 0 : fallback.includes("://")) ? fallback : undefined;
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
function psmLockup(opts) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        let schema = opts.schema;
        if (!schema) {
            schema = prisma.find(value => {
                return fs.existsSync(value);
            });
        }
        if (!schema) {
            throw new Error("Migrate error: schema.prisma file not found!");
        }
        console.log(`PSM migrate base do schema ${schema}`);
        const home = Path.dirname(schema);
        const psm_yml = Path.join(home, "psm.yml");
        const psm_sql = Path.join(home, "psm.sql");
        if (!fs.existsSync(psm_yml)) {
            throw new Error("Migrate error: psm.yml file not found!");
        }
        if (!fs.existsSync(psm_sql)) {
            throw new Error("Migrate error: psm.sql file not found!");
        }
        console.log(`PSM migrate using ${psm_yml}`);
        const psm = yaml.parse(fs.readFileSync(psm_yml).toString());
        loadEnv(home);
        const driver = yield loadDriver((_a = opts === null || opts === void 0 ? void 0 : opts.driver) !== null && _a !== void 0 ? _a : psm.psm.driver);
        if (!!psm.migration && !psm.migration.instante && psm.migration["instate"]) {
            psm.migration.instante = psm.migration["instate"];
        }
        return { schema, psm_yml, psm_sql, psm, driver, home };
    });
}
//# sourceMappingURL=common.js.map