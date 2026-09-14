/**
 * Regenerate the Log Analytics table reference from the Azure Monitor docs.
 *
 *   npm run generate:tables
 *
 * Both upstream sources are marked "automatically generated", so their shape is
 * stable enough to parse.
 *
 *   tables-features.md   every table plus its Basic / Auxiliary / DCR / API
 *                        support, and the authoritative spelling of each name
 *   tables/<Name>.md     the column schema for one table
 *
 * Writes two files. The index covers every table so we can tell "no such table"
 * apart from "that table can't be transformed". Columns are only collected for
 * transformation-capable tables, since nothing else can be a destination.
 *
 *   src/data/tables-index.json    name and feature flags
 *   src/data/tables-columns.json  column names and types
 */
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = resolve(root, "src/data");

const REFERENCE = "https://raw.githubusercontent.com/MicrosoftDocs/azure-monitor-docs/main/articles/azure-monitor/reference/";
const SOURCE = "https://github.com/MicrosoftDocs/azure-monitor-docs/tree/main/articles/azure-monitor/reference";

const DCR_ICON = "collection-icon.svg";
const BASIC_ICON = "basic-table.svg";
const AUX_ICON = "auxiliary-table.svg";
const CONCURRENCY = 24;

async function getText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.text();
}

/**
 * Parse the feature matrix. Rows are `[Name](./tables/Name.md)|basic|aux|dcr|api`
 * and start with the link, not a pipe. Support is signalled by an icon image.
 */
function parseFeatureMatrix(markdown) {
  const tables = [];
  for (const line of markdown.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("[")) continue;

    const name = trimmed.match(/^\[([^\]]+)\]\(\.\/tables\//)?.[1];
    if (!name) continue;

    const cells = trimmed.slice(trimmed.indexOf(")") + 1).split("|");
    const [basic = "", aux = "", dcr = "", api = ""] = cells.slice(1);

    tables.push({
      name,
      basic: basic.includes(BASIC_ICON),
      aux: aux.includes(AUX_ICON),
      transform: dcr.includes(DCR_ICON),
      api: api.trim().length > 0,
    });
  }
  return tables;
}

/** Pull the `## Columns` table out of a per-table page. */
function parseColumns(markdown) {
  const section = markdown.split(/^##\s+Columns\s*$/m)[1];
  if (!section) return [];

  const columns = [];
  for (const line of section.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("|")) {
      if (columns.length) break;
      continue;
    }
    const cells = trimmed.split("|").slice(1, -1).map((c) => c.trim());
    if (cells.length < 2) continue;

    const [column, type] = cells;
    if (!column || /^-+$/.test(column)) continue;
    if (column.toLowerCase() === "column") continue;

    columns.push([column, type.toLowerCase()]);
  }
  return columns;
}

async function mapWithLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return results;
}

const all = parseFeatureMatrix(await getText(`${REFERENCE}tables-features.md`));
if (!all.length) throw new Error("Feature matrix parsed to nothing; the upstream format probably changed");

const transformable = all.filter((t) => t.transform);
console.log(`Tables in the feature matrix: ${all.length}`);
console.log(`Support DCR transformations:  ${transformable.length}`);
console.log("Fetching column schemas...");

let done = 0;
let missing = 0;
const withColumns = await mapWithLimit(transformable, CONCURRENCY, async (table) => {
  try {
    // The matrix links use PascalCase but the files on disk are lowercase.
    const columns = parseColumns(await getText(`${REFERENCE}tables/${table.name.toLowerCase()}.md`));
    if (++done % 250 === 0) console.log(`  ${done}/${transformable.length}`);
    return { ...table, columns };
  } catch {
    missing++;
    return { ...table, columns: [] };
  }
});

all.sort((a, b) => a.name.localeCompare(b.name));

const index = {
  generated: new Date().toISOString().slice(0, 10),
  source: SOURCE,
  // [name, transform, api, basic, aux] with flags as 0/1
  tables: all.map((t) => [t.name, t.transform ? 1 : 0, t.api ? 1 : 0, t.basic ? 1 : 0, t.aux ? 1 : 0]),
};

const columns = {
  generated: index.generated,
  source: SOURCE,
  // name -> [[column, type], ...]
  tables: Object.fromEntries(
    withColumns.filter((t) => t.columns.length).map((t) => [t.name, t.columns]),
  ),
};

await mkdir(outDir, { recursive: true });
await writeFile(resolve(outDir, "tables-index.json"), JSON.stringify(index));
await writeFile(resolve(outDir, "tables-columns.json"), JSON.stringify(columns));

const kb = (value) => (Buffer.byteLength(JSON.stringify(value)) / 1024).toFixed(1);
const columnCount = Object.values(columns.tables).reduce((n, c) => n + c.length, 0);

console.log(`\nPages that failed to fetch:   ${missing}`);
console.log(`Tables with columns:          ${Object.keys(columns.tables).length}`);
console.log(`Columns captured:             ${columnCount}`);
console.log(`tables-index.json             ${kb(index)} KB`);
console.log(`tables-columns.json           ${kb(columns)} KB`);
