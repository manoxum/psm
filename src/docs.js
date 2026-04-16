"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.parsePsmDoc = parsePsmDoc;
//filename: src/libs/migrate/psm.ts
const json5_1 = __importDefault(require("json5"));
function parsePsmDoc(doc) {
    const result = {};
    const lines = doc.split('\n');
    let i = 0;
    while (i < lines.length) {
        let line = lines[i].trim();
        // Heredoc multilinha
        const heredocMatch = line.match(/^@psm\.([\w.]+)\s*=\s*<<<(\w+)$/);
        if (heredocMatch) {
            const [, key, delimiter] = heredocMatch;
            i++;
            const contentLines = [];
            while (i < lines.length && lines[i].trim() !== delimiter) {
                contentLines.push(lines[i]);
                i++;
            }
            i++; // pula delimitador
            const content = contentLines.join('\n');
            setNestedValue(result, key, content);
            continue;
        }
        // Flag booleana: @psm.flag
        const flagMatch = line.match(/^@psm\.([\w.]+)\s*$/);
        if (flagMatch) {
            const [, key] = flagMatch;
            setNestedValue(result, key, true);
            i++;
            continue;
        }
        // Atribuição com índice: @psm.key[idx] = value
        const indexMatch = line.match(/^@psm\.([\w.]+)\[(\d+)]\s*=\s*(.+)$/);
        if (indexMatch) {
            const [, key, indexStr, rawValue] = indexMatch;
            const index = parseInt(indexStr, 10);
            let value = tryParseObject(rawValue.trim());
            let arr = getNestedValue(result, key) || [];
            if (!Array.isArray(arr))
                arr = [arr];
            arr[index] = value;
            setNestedValue(result, key, arr);
            i++;
            continue;
        }
        // Atribuição normal: @psm.key = value ou @psm.key += value
        const lineMatch = line.match(/^@psm\.([\w.]+)\s*(\+?=)\s*(.+)$/);
        if (lineMatch) {
            const [, key, operator, rawValue] = lineMatch;
            let value = tryParseObject(rawValue.trim());
            // Tenta coerção de tipos simples
            if (typeof value === 'string') {
                if (value === 'true')
                    value = true;
                else if (value === 'false')
                    value = false;
                else if (!isNaN(Number(value)))
                    value = Number(value);
            }
            const current = getNestedValue(result, key);
            if (operator === '=') {
                if (typeof current === 'string' && typeof value === 'string') {
                    setNestedValue(result, key, current + '\n' + value);
                }
                else {
                    setNestedValue(result, key, value);
                }
            }
            else if (operator === '+=') {
                if (!current) {
                    setNestedValue(result, key, [value]);
                }
                else if (Array.isArray(current)) {
                    current.push(value);
                    setNestedValue(result, key, current);
                }
                else {
                    setNestedValue(result, key, [current, value]);
                }
            }
        }
        i++;
    }
    return result;
}
function tryParseObject(value) {
    value = value.trim();
    if ((value.startsWith('{') && value.endsWith('}')) ||
        (value.startsWith('[') && value.endsWith(']'))) {
        try {
            return json5_1.default.parse(value);
        }
        catch (_a) {
            return value;
        }
    }
    return value;
}
// Utilitários para manipulação de objetos aninhados
function setNestedValue(obj, path, value) {
    const keys = path.split('.');
    let current = obj;
    keys.forEach((key, idx) => {
        if (idx === keys.length - 1) {
            current[key] = value;
        }
        else {
            if (!(key in current))
                current[key] = {};
            current = current[key];
        }
    });
}
function getNestedValue(obj, path) {
    const keys = path.split('.');
    let current = obj;
    for (const key of keys) {
        if (current == null || !(key in current))
            return undefined;
        current = current[key];
    }
    return current;
}
//# sourceMappingURL=docs.js.map