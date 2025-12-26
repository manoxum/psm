import * as fs from "node:fs"
import chalk from "chalk";
import * as cp from "node:child_process";
import * as Path from "node:path";

export function sanitizeLabel( label:string ) {
    return label
        .replace(/[<>:"/\\|?*\x00-\x1F]/g, '') // remove caracteres inválidos no Windows
        .replace(/[\u{0080}-\u{FFFF}]/gu, '')  // remove caracteres não ASCII (opcional)
        .trim()
        .replace(/\s+/g, ' ');                 // normaliza espaços
}

export function getLatestFolder(basePath:string) {
    if (!fs.existsSync(basePath)) return null;
    const dirs = fs.readdirSync(basePath, { withFileTypes: true })
        .filter(d => d.isFile())
        .map(d => d.name)
        .filter(name => /^\d{14}(\.tar\.gz)?( - .+)?$/.test(name))
        .sort((a, b) => b.localeCompare(a));
    return dirs[0] || null;
}


export function isGitRepo(cwd: string) {
    const res = cp.spawnSync("git", ["rev-parse", "--is-inside-work-tree"], {
        cwd,
        encoding: "utf-8",
    });
    return res.status === 0 && res.stdout.trim() === "true";
}

export function gitAddPath(cwd: string, targetPath: string) {
    try {
        if (!isGitRepo(cwd)) {
            console.log(chalk.gray("Repositório git não detectado; ignorando 'git add'."));
            return;
        }

        const relPath = Path.relative(cwd, targetPath);
        const addTarget = relPath.startsWith("..") ? targetPath : relPath;

        const addRes = cp.spawnSync("git", ["add", addTarget], {
            cwd,
            stdio: "inherit",
        });

        if (addRes.status !== 0) {
            console.warn(chalk.yellow(`Aviso: 'git add' falhou para ${addTarget}.`));
        } else {
            console.log(chalk.green(`✔ Adicionado ao git: ${addTarget}`));
        }
    } catch (err: any) {
        console.warn(
            chalk.yellow("Aviso: não foi possível adicionar ao git."),
            err?.message ?? err,
        );
    }
}