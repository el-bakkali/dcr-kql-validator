import { ValidationResult } from "./result.js";
import { analyzeQuery } from "./kusto.js";
import {
  OPERATOR_NODE_TO_KEYWORD,
  BLOCKED_OPERATOR_REASONS,
  ALLOWED_OPERATORS,
  ALLOWED_FUNCTIONS,
  FUNCTION_CORRECTIONS,
  MAX_PARSE_COLUMNS,
  MAX_GEO_LOCATION_CALLS,
  VALID_QUERY_ROOTS,
} from "../rules/kql-rules.js";

const MAX_INPUT = 5 * 1024 * 1024;

/**
 * Validate a transformation query.
 *
 * Two layers: the Kusto parser decides whether the query is well-formed and
 * type-correct, then these rules decide whether Azure Monitor will actually
 * accept it. The parser alone is not enough. It reports nothing at all for
 * `summarize`, which a transformation cannot run.
 *
 * @param {string} query
 * @param {Array<{name:string,type:string}>|null} columns streamDeclarations schema, when known
 */
export async function validateKql(query, columns = null) {
  const result = new ValidationResult();
  const text = (query ?? "").trim();

  if (!text) {
    return result.addError("KQL001", "Query is empty", "Enter a transformation query starting with 'source'.");
  }
  if (text.length > MAX_INPUT) {
    return result.addError("KQL002", "Query exceeds the maximum size of 5 MB", "Reduce the size of the query.");
  }

  let analysis;
  try {
    analysis = await analyzeQuery(text, columns);
  } catch (err) {
    return result.addError("KQL003", `Could not parse the query: ${err.message}`, "Check for unbalanced quotes or brackets.");
  }

  reportParserDiagnostics(analysis, result);
  checkQueryRoot(text, result);
  checkOperators(analysis, result);
  checkFunctions(analysis, text, result);
  checkParseStatements(analysis, result);
  checkTimeGenerated(analysis, text, result);
  checkGeoLocation(analysis, result);

  if (result.valid) {
    result.addInfo("KQL000", "The transformation query is valid.");
    if (analysis.hasSchema && analysis.outputColumns.length) {
      const cols = analysis.outputColumns.map((c) => `${c.name}:${c.type}`).join(", ");
      result.addInfo("KQL004", `Output columns: ${cols}`);
    }
  }

  result.analysis = analysis;
  return result;
}

/** Syntax and semantic findings straight from the language service. */
function reportParserDiagnostics(analysis, result) {
  for (const d of analysis.diagnostics) {
    const span = { start: d.start, length: d.length };
    if (/does not refer to any known column/i.test(d.message)) {
      result.addError(
        "KQL101",
        d.message,
        "Check the column name against the streamDeclarations schema for this stream. Column names are case-sensitive.",
        span
      );
    } else if (/not defined for the operand types/i.test(d.message)) {
      result.addError("KQL102", d.message, "Convert one side with a conversion function such as tostring() or toint().", span);
    } else {
      result.addError("KQL100", d.message, null, span);
    }
  }
}

/** Only `source`, `print`, `let` and `datatable` may begin a transformation. */
function checkQueryRoot(text, result) {
  const firstWord = /^\s*([A-Za-z_][\w-]*)/.exec(text)?.[1]?.toLowerCase();
  if (!firstWord) return;

  if (!VALID_QUERY_ROOTS.includes(firstWord)) {
    result.addError(
      "KQL010",
      `A transformation must start with 'source', not '${firstWord}'`,
      "`source` is the virtual table representing the incoming stream. If you copied this from Log Analytics, replace the table name with 'source'.",
      { start: 0, length: firstWord.length }
    );
  }
}

/** Reject tabular operators that cannot run per-record. */
function checkOperators(analysis, result) {
  const seen = new Set();

  for (const op of analysis.operators) {
    const keyword = OPERATOR_NODE_TO_KEYWORD[op.node];
    if (!keyword || ALLOWED_OPERATORS.includes(keyword) || seen.has(keyword)) continue;

    const reason = BLOCKED_OPERATOR_REASONS[keyword];
    seen.add(keyword);
    result.addError(
      "KQL020",
      `The '${keyword}' operator is not supported in transformations`,
      reason ?? "Only extend, project, print, where, parse, project-away, project-rename, datatable and columnifexists are supported.",
      { start: op.start, length: op.length }
    );
  }
}

/** Enforce the scalar function allowlist and catch the documented renames. */
function checkFunctions(analysis, text, result) {
  const seen = new Set();

  for (const fn of analysis.functions) {
    const name = fn.name.toLowerCase();
    if (seen.has(name)) continue;
    seen.add(name);

    if (FUNCTION_CORRECTIONS[name]) {
      result.addError("KQL030", `'${name}' cannot be used in transformations`, FUNCTION_CORRECTIONS[name], {
        start: fn.start,
        length: fn.length,
      });
      continue;
    }

    if (!ALLOWED_FUNCTIONS.has(name)) {
      result.addWarning(
        "KQL031",
        `'${name}' is not in the documented list of supported transformation functions`,
        "Transformations support a restricted subset of KQL. Verify this function against the supported features reference before deploying.",
        { start: fn.start, length: fn.length }
      );
    }
  }

  // Some blocked names are keywords to the parser rather than function calls, so
  // they never appear as a call node. `materialize(x)` parses as a syntax error.
  for (const [name, advice] of Object.entries(FUNCTION_CORRECTIONS)) {
    if (seen.has(name)) continue;
    const m = new RegExp(`(?:^|[^\\w.])(${name})\\s*\\(`, "i").exec(text);
    if (!m) continue;
    seen.add(name);
    result.addError("KQL030", `'${name}' cannot be used in transformations`, advice, {
      start: m.index + m[0].length - m[1].length - 1,
      length: name.length,
    });
  }

  // dynamic() literals are parsed as a literal expression, not a function call.
  const dyn = /\bdynamic\s*\(/i.exec(text);
  if (dyn && !seen.has("dynamic")) {
    result.addWarning(
      "KQL032",
      "Use parse_json() rather than dynamic() literals in transformations",
      "The documentation shows parse_json('{...}') as the supported form for dynamic literals.",
      { start: dyn.index, length: 7 }
    );
  }
}

/** The parse operator is capped at 10 columns, and regex parsing differs here. */
function checkParseStatements(analysis, result) {
  for (const op of analysis.operators) {
    if (op.node !== "ParseOperator") continue;

    if (op.columnCount > MAX_PARSE_COLUMNS) {
      result.addError(
        "KQL050",
        `A parse statement declares ${op.columnCount} columns, exceeding the limit of ${MAX_PARSE_COLUMNS}`,
        "Split the parse into multiple statements, each extracting at most 10 columns.",
        { start: op.start, length: op.length }
      );
    }

    if (op.kind === "regex") {
      result.addWarning(
        "KQL051",
        "'parse kind=regex' must match the entire input string in a transformation",
        "Unlike Log Analytics and Sentinel, a partial match populates no fields here. Add a trailing matcher after the last column, for example @'\\sby' *, so the pattern extends to the end of the line.",
        { start: op.start, length: op.length }
      );
    }
  }
}

/** Every transformation must emit a TimeGenerated column of type datetime. */
function checkTimeGenerated(analysis, text, result) {
  if (analysis.hasSchema && analysis.outputColumns.length) {
    const tg = analysis.outputColumns.find((c) => c.name === "TimeGenerated");
    if (!tg) {
      result.addError(
        "KQL040",
        "The output does not include a 'TimeGenerated' column",
        "Every transformation must output a valid timestamp in a datetime column named TimeGenerated. Add: extend TimeGenerated = now()"
      );
      return;
    }
    if (!/datetime/i.test(tg.type)) {
      result.addError(
        "KQL041",
        `'TimeGenerated' is typed ${tg.type} rather than datetime`,
        "Convert it with todatetime(), for example: project TimeGenerated = todatetime(myTimeColumn)"
      );
    }
    return;
  }

  // Without a schema the output columns cannot be resolved, so fall back to a textual check.
  if (!text.includes("TimeGenerated")) {
    result.addWarning(
      "KQL042",
      "The query may be missing a 'TimeGenerated' column",
      "Every transformation must output a datetime column named TimeGenerated. Supply the stream schema to check this precisely."
    );
  }
}

/** geo_location reaches an external service and adds ingestion latency. */
function checkGeoLocation(analysis, result) {
  const calls = analysis.functions.filter((f) => f.name.toLowerCase() === "geo_location");
  if (calls.length > MAX_GEO_LOCATION_CALLS) {
    result.addWarning(
      "KQL060",
      `geo_location is called ${calls.length} times`,
        "This function calls an external IP geolocation service. The documentation advises using it sparingly, no more than a few times per transformation."
    );
  }
}
