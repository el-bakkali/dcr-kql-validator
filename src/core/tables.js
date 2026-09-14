/**
 * Log Analytics table reference, generated from the Azure Monitor docs by
 * scripts/generate-tables.mjs.
 *
 * The index is small and always loaded. Column schemas are 600 KB, so they are
 * fetched only when a rule actually names a standard table.
 */
import indexData from "../data/tables-index.json";

const DOC_BASE =
  "https://github.com/MicrosoftDocs/azure-monitor-docs/blob/main/articles/azure-monitor/reference/tables";

// [name, transform, api, basic, aux]
const byLowerName = new Map(indexData.tables.map((row) => [row[0].toLowerCase(), row]));

let columnsPromise = null;

/**
 * Resolve a stream name to a table name.
 *
 *   Microsoft-Event         -> Event
 *   Microsoft-Table-Event   -> Event   (workspace transformation DCRs)
 *   Custom-MyTable_CL       -> null    (not a standard table)
 */
export function streamToTable(stream) {
  if (typeof stream !== "string") return null;
  if (stream.startsWith("Microsoft-Table-")) return stream.slice("Microsoft-Table-".length) || null;
  if (stream.startsWith("Microsoft-")) return stream.slice("Microsoft-".length) || null;
  return null;
}

/** Look a table up case-insensitively. Returns null when there is no such table. */
export function findTable(name) {
  if (typeof name !== "string" || !name) return null;
  const row = byLowerName.get(name.toLowerCase());
  if (!row) return null;
  return { name: row[0], transform: row[1] === 1, api: row[2] === 1, basic: row[3] === 1, aux: row[4] === 1 };
}

/** Column schema for a table, or null when we have none. */
export async function getTableColumns(name) {
  const table = findTable(name);
  if (!table) return null;

  columnsPromise ??= import("../data/tables-columns.json").then((m) => m.default ?? m);
  const data = await columnsPromise;

  const columns = data.tables[table.name];
  if (!columns) return null;
  return columns.map(([column, type]) => ({ name: column, type }));
}

/** Names of every table that can be a transformation destination, sorted. */
export function listTransformableTables() {
  return indexData.tables.filter((row) => row[1] === 1).map((row) => row[0]);
}

/** Link to the upstream reference page a table's schema was generated from. */
export function tableDocUrl(name) {
  const table = findTable(name);
  if (!table) return null;
  // Page filenames are lowercase even though the table names are not.
  return `${DOC_BASE}/${table.name.toLowerCase()}.md`;
}

export const tablesGenerated = indexData.generated;
