import { PSMMigrator } from "../driver";
export declare const DEFAULT_KEYWORDS: string[];
/**
 * Atualiza o cache de tabelas consultando o information_schema.
 * @param migrator Instância do migrator para executar a consulta.
 * @returns Lista de nomes de tabelas.
 */
export declare function fetchTableNames(migrator: PSMMigrator): Promise<string[]>;
/**
 * Função de completar para usar no readline.
 * @param line Linha atual digitada.
 * @param keywords Lista de palavras‑chave.
 * @param tables Lista de nomes de tabelas.
 * @returns Tupla [sugestões, parte a completar].
 */
export declare function completerFunction(line: string, keywords: string[], tables: string[]): [string[], string];
//# sourceMappingURL=completion.d.ts.map