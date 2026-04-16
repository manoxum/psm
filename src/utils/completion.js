"use strict";
// filename: src/utils/completion.ts
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
exports.DEFAULT_KEYWORDS = void 0;
exports.fetchTableNames = fetchTableNames;
exports.completerFunction = completerFunction;
// Palavras‑chave SQL padrão (ANSI) – podem ser sobrescritas pelo driver
exports.DEFAULT_KEYWORDS = [
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
 * Atualiza o cache de tabelas consultando o information_schema.
 * @param migrator Instância do migrator para executar a consulta.
 * @returns Lista de nomes de tabelas.
 */
function fetchTableNames(migrator) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            // Tenta obter lista de tabelas do schema atual (funciona em PostgreSQL, MySQL, etc.)
            const tableQuery = `
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'mysql')
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        `;
            const result = yield migrator.execute([{
                    raw: tableQuery,
                    group: "completion",
                    filename: "completion"
                }]);
            if (result.success && result.rows) {
                return result.rows.map((row) => row.table_name);
            }
        }
        catch (e) {
            // Ignora falhas (pode não ter permissão ou não ser suportado)
        }
        return [];
    });
}
/**
 * Função de completar para usar no readline.
 * @param line Linha atual digitada.
 * @param keywords Lista de palavras‑chave.
 * @param tables Lista de nomes de tabelas.
 * @returns Tupla [sugestões, parte a completar].
 */
function completerFunction(line, keywords, tables) {
    const words = line.split(/\s+/);
    const lastWord = words.pop() || '';
    const hits = [];
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
//# sourceMappingURL=completion.js.map