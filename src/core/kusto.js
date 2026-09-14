/**
 * Wrapper around @kusto/language-service-next, the JavaScript build of
 * microsoft/Kusto-Query-Language.
 *
 * The package ships two plain scripts that attach themselves to globalThis
 * rather than exporting anything, and Bridge.NET mangles every constructor name
 * to "ctor", so type identity has to come from Bridge.getTypeName.
 */

let cached = null;

/**
 * Load the language service once. Both scripts must run, in this order: the
 * first publishes Bridge, the second reads it and publishes Kusto.
 *
 * These are classic scripts that need top-level `this` to be the global object.
 * The kusto-classic-scripts plugin in vite.config.js arranges that for the
 * browser; Node's CommonJS interop already satisfies it.
 */
export async function loadKusto() {
  if (cached) return cached;

  if (!globalThis.Kusto?.Language) {
    await import("@kusto/language-service-next/bridge.min.js");
    await import("@kusto/language-service-next/Kusto.Language.Bridge.min.js");
  }

  const K = globalThis.Kusto?.Language;
  if (!K) throw new Error("Kusto language service failed to load");

  cached = { K, S: K.Symbols, SE: K.Syntax.SyntaxElement, Bridge: globalThis.Bridge };
  return cached;
}

/** Map a streamDeclarations column type onto a Kusto scalar type. */
function scalarType(S, type) {
  const t = String(type || "").toLowerCase();
  const map = {
    string: S.ScalarTypes.String,
    int: S.ScalarTypes.Int,
    long: S.ScalarTypes.Long,
    real: S.ScalarTypes.Real,
    double: S.ScalarTypes.Real,
    boolean: S.ScalarTypes.Bool,
    bool: S.ScalarTypes.Bool,
    dynamic: S.ScalarTypes.Dynamic,
    datetime: S.ScalarTypes.DateTime,
    timespan: S.ScalarTypes.TimeSpan,
    guid: S.ScalarTypes.Guid,
  };
  return map[t] ?? S.ScalarTypes.Dynamic;
}

/**
 * Build a GlobalState whose current database contains a table named `source`.
 *
 * `columns` is the streamDeclarations column array. With no columns the table is
 * declared open so that unknown column references are tolerated rather than
 * reported as errors. Without a schema we cannot know what is legitimate.
 */
export async function buildGlobals(columns) {
  const { K, S } = await loadKusto();

  const hasSchema = Array.isArray(columns) && columns.length > 0;
  const cols = hasSchema
    ? columns
        .filter((c) => c && c.name)
        .map((c) => new S.ColumnSymbol.ctor(String(c.name), scalarType(S, c.type)))
    : [];

  let source = new S.TableSymbol.$ctor4("source", cols);
  if (!hasSchema && typeof source.WithIsOpen === "function") {
    source = source.WithIsOpen(true);
  }

  const db = new S.DatabaseSymbol.$ctor1("DcrTransform", [source]);
  const cluster = new S.ClusterSymbol.$ctor1("dcr", [db]);

  return { globals: K.GlobalState.Default.WithCluster(cluster).WithDatabase(db), hasSchema };
}

/**
 * Parse and semantically analyse a transformation query.
 *
 * Returns parser diagnostics, the operator nodes found, the function calls made
 * and the resulting output columns. This layer knows nothing about Azure Monitor
 * restrictions. The parser accepts `summarize` quite happily.
 */
export async function analyzeQuery(text, columns = null) {
  const { K, SE, Bridge } = await loadKusto();
  const { globals, hasSchema } = await buildGlobals(columns);

  const code = K.KustoCode.ParseAndAnalyze(text, globals);

  const diagnostics = [];
  const list = code.GetDiagnostics();
  for (let i = 0; i < list.Count; i++) {
    const d = list.getItem(i);
    diagnostics.push({
      message: d.Message,
      start: d.Start,
      length: d.Length,
      severity: String(d.Severity || "Error").toLowerCase(),
    });
  }

  const operators = [];
  const functions = [];
  const nodes = [];

  SE.WalkNodes(code.Syntax, (n) => {
    const typeName = Bridge.getTypeName(n);
    const short = typeName.replace(/^.*\.Syntax\./, "");
    nodes.push(short);

    if (/Operator$/.test(short)) {
      const start = n.TextStart ?? 0;
      const length = n.Width ?? 0;
      const entry = { node: short, start, length, text: text.slice(start, start + length) };
      if (short === "ParseOperator") {
        entry.columnCount = countNameDeclarations(n, Bridge);
        entry.kind = parseKind(entry.text);
      }
      operators.push(entry);
    }

    if (short === "FunctionCallExpression") {
      const name = safeText(n.Name ?? n);
      if (name) functions.push({ name: name.trim(), start: n.TextStart ?? 0, length: n.Width ?? 0 });
    }
  });

  const outputColumns = [];
  const rt = code.ResultType;
  if (rt?.Columns) {
    for (let i = 0; i < rt.Columns.Count; i++) {
      const c = rt.Columns.getItem(i);
      outputColumns.push({ name: c.Name, type: c.Type?.Name ?? String(c.Type) });
    }
  }

  return { diagnostics, operators, functions, nodes, outputColumns, hasSchema, code };
}

function safeText(node) {
  try {
    return node?.ToString?.() ?? "";
  } catch {
    return "";
  }
}

/** Count the columns a parse statement declares, which the docs cap at 10. */
function countNameDeclarations(node, Bridge) {
  let count = 0;
  const visit = (n) => {
    if (!n) return;
    if (Bridge.getTypeName(n).endsWith(".NameDeclaration")) count++;
    const kids = n.ChildCount ?? 0;
    for (let i = 0; i < kids; i++) visit(n.GetChild(i));
  };
  visit(node);
  return count;
}

/** `parse kind=regex` behaves differently in transformations than in Log Analytics. */
function parseKind(text) {
  const m = /\bkind\s*=\s*([A-Za-z]+)/.exec(text || "");
  return m ? m[1].toLowerCase() : "simple";
}
