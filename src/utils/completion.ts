// filename: src/utils/completion.ts

import { PSMMigrator } from "../driver";

// Palavras‑chave SQL padrão (ANSI) – podem ser sobrescritas pelo driver
export const DEFAULT_KEYWORDS = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT",
    "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT",
    "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX", "FUNCTION", "TRIGGER",
    "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "SAVEPOINT",
    "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IN", "EXISTS", "BETWEEN", "LIKE",
    "IS", "AS", "ON", "USING", "CASE", "WHEN", "THEN", "ELSE", "END",
    "PRIMARY KEY", "FOREIGN KEY", "REFERENCES", "CONSTRAINT", "UNIQUE", "CHECK",
    "DEFAULT", "AUTO_INCREMENT", "SERIAL", "BIGINT", "INT", "VARCHAR", "TEXT", "DATE", "TIMESTAMP"
];

/**
 * Interface para o resultado de uma consulta de tabelas.
 */
interface TableRow {
    table_name: string;
}

/**
 * Atualiza o cache de tabelas consultando o information_schema.
 * @param migrator Instância do migrator para executar a consulta.
 * @returns Lista de nomes de tabelas.
 */
export async function fetchTableNames(migrator: PSMMigrator): Promise<string[]> {
    try {
        // Tenta obter lista de tabelas do schema atual (funciona em PostgreSQL, MySQL, etc.)
        const tableQuery = `
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'mysql')
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        `;
        const result = await migrator.execute([{
            raw: tableQuery,
            group: "completion",
            filename: "completion"
        }]);
        if (result.success && result.rows) {
            return result.rows.map((row: TableRow) => row.table_name);
        }
    } catch (e) {
        // Ignora falhas (pode não ter permissão ou não ser suportado)
    }
    return [];
}

/**
 * Função de completar para usar no readline.
 * @param line Linha atual digitada.
 * @param keywords Lista de palavras‑chave.
 * @param tables Lista de nomes de tabelas.
 * @returns Tupla [sugestões, parte a completar].
 */
export function completerFunction(
    line: string,
    keywords: string[],
    tables: string[]
): [string[], string] {
    const words = line.split(/\s+/);
    const lastWord = words.pop() || '';
    const hits: string[] = [];

    // Sugere palavras‑chave (case‑insensitive)
    const upperLast = lastWord.toUpperCase();
    for (const kw of keywords) {
        if (kw.startsWith(upperLast)) {
            hits.push(kw);
        }
    }

    // Sugere nomes de tabelas (case‑insensitive)
    const lowerLast = lastWord.toLowerCase();
    for (const tbl of tables) {
        if (tbl.toLowerCase().startsWith(lowerLast)) {
            hits.push(tbl);
        }
    }

    // Retorna as correspondências ou um subconjunto de palavras‑chave se não houver
    return [hits.length ? hits : keywords.slice(0, 10), lastWord];
}