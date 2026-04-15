// filename: src/tools/backup.ts

import * as fs from "node:fs";
import * as Path from "node:path";
import * as os from "node:os";
import * as tar from "tar";
import chalk from "chalk";

import {psmLockup, resolveDatabaseUrl} from "./common";
import {gitAddPath, sanitizeLabel} from "../utils/fs";

export interface BackupOptions {
    schema?: string;
    add?: boolean;
    label?: string;
    level?: number; // novo parâmetro de compressão
}

export async function backup(opts: BackupOptions) {
    const { psm, psm_sql, driver, home, schema } = await psmLockup({ schema: opts.schema });

    const migrator = driver.migrator({
        url: resolveDatabaseUrl(psm.psm.url),
        migrate: '',  // Não necessário para backup
        check: '',
        core: fs.readFileSync(psm_sql).toString(),
    });

    // Executa dump
    const dump = await migrator.dump();
    if (dump.error) throw dump.error;

    const moment = require('moment');
    const now = moment();
    const instant = now.format('YYYYMMDDHHmmss');
    const label = opts.label ? ` - ${sanitizeLabel(opts.label)}` : '';

    const compressionLevel = opts.level ?? 9; // nível de compressão padrão 9

    // Caminho temporário do arquivo
    const tmpDir = fs.mkdtempSync(Path.join(os.tmpdir(), `psm-backup-${instant}-`));
    fs.mkdirSync(tmpDir, { recursive: true });
    const backupFile = Path.join(tmpDir, 'backup.sql');
    if (dump.file) {
        fs.copyFileSync(dump.file, backupFile);
        fs.rmSync(Path.dirname(dump.file), { recursive: true, force: true });
    } else {
        fs.writeFileSync(backupFile, dump.output || "");
    }
    fs.writeFileSync(Path.join(tmpDir, 'README'), `
        Label   : ${opts.label}
        Level   : ${compressionLevel}
        Git Add : ${ opts.add }
        Instante: ${  now.toISOString() }
        Driver  : ${  psm.psm.driver }
        Scheme  : ${  schema }
    `.split("\n").map( value => value.trim()).filter( value => !!value).join("\n"));

    // Arquivo final .tar.gz
    const archiveName = Path.join(home, `psm/backup/${instant}${label}.tar.gz`);

    // Compacta **somente o arquivo backup.sql** (sem nível extra)
    await tar.c(
        {
            gzip: { level: compressionLevel },
            file: archiveName,
            cwd: tmpDir
        },
        ['backup.sql', "README"]
    );

    console.log(chalk.green(`✔ Backup gerado: ${archiveName}`));

    // Remove pasta temporária
    fs.rmSync(tmpDir, { recursive: true, force: true });

    // Adiciona o .tar.gz ao git
    if( opts.add )gitAddPath(home || process.cwd(), archiveName);
}
