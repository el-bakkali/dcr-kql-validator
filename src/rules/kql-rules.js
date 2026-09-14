/**
 * Azure Monitor transformation KQL rules.
 *
 * Source of truth: articles/azure-monitor/data-collection/data-collection-transformations-kql.md
 * in MicrosoftDocs/azure-monitor-docs (ms.date 05/15/2026). Every entry below appears
 * verbatim in that document. Do not add anything that isn't listed there.
 */

/** Tabular operators permitted in a transformation. */
export const ALLOWED_OPERATORS = [
  "extend",
  "project",
  "print",
  "where",
  "parse",
  "project-away",
  "project-rename",
  "datatable",
  "columnifexists",
];

/**
 * Kusto AST node type names (from Bridge.getTypeName) mapped to the operator
 * keyword they represent. Node names do not match keywords: `where` parses to
 * `FilterOperator`, so this mapping must be explicit.
 */
export const OPERATOR_NODE_TO_KEYWORD = {
  FilterOperator: "where",
  ExtendOperator: "extend",
  ProjectOperator: "project",
  ProjectAwayOperator: "project-away",
  ProjectRenameOperator: "project-rename",
  ProjectKeepOperator: "project-keep",
  ProjectReorderOperator: "project-reorder",
  ParseOperator: "parse",
  ParseWhereOperator: "parse-where",
  PrintOperator: "print",
  SummarizeOperator: "summarize",
  JoinOperator: "join",
  UnionOperator: "union",
  SortOperator: "sort",
  TopOperator: "top",
  TopHittersOperator: "top-hitters",
  TopNestedOperator: "top-nested",
  TakeOperator: "take",
  CountOperator: "count",
  DistinctOperator: "distinct",
  MvExpandOperator: "mv-expand",
  MvApplyOperator: "mv-apply",
  RenderOperator: "render",
  EvaluateOperator: "evaluate",
  LookupOperator: "lookup",
  MakeSeriesOperator: "make-series",
  InvokeOperator: "invoke",
  FindOperator: "find",
  SearchOperator: "search",
  ForkOperator: "fork",
  FacetOperator: "facet",
  SampleOperator: "sample",
  SampleDistinctOperator: "sample-distinct",
  ConsumeOperator: "consume",
  GetSchemaOperator: "getschema",
  SerializeOperator: "serialize",
  RangeOperator: "range",
  ReduceByOperator: "reduce",
  ScanOperator: "scan",
  PartitionOperator: "partition",
  AsOperator: "as",
  ExecuteAndCacheOperator: "execute-and-cache",
};

/** Why a given operator cannot work in a transformation. */
export const BLOCKED_OPERATOR_REASONS = {
  summarize:
    "Transformations process each record individually and cannot aggregate across records. Use 'extend' with conditional logic instead.",
  join: "Transformations cannot correlate data across records or tables. Each record is processed independently.",
  union: "Transformations cannot combine multiple data streams. Use separate data flows in the DCR instead.",
  sort: "Transformations cannot reorder records. Each record is processed independently.",
  top: "Transformations cannot select top N records. Each record is processed independently.",
  "top-hitters": "top-hitters aggregates across records and is not supported in transformations.",
  "top-nested": "top-nested aggregates across records and is not supported in transformations.",
  take: "Transformations cannot limit the number of records. Use 'where' to filter instead.",
  count: "Transformations cannot count records across the stream. Each record is processed individually.",
  distinct: "Transformations cannot deduplicate records. Each record is processed independently.",
  "mv-expand": "mv-expand can emit multiple rows per input record and is not supported in transformations.",
  "mv-apply": "mv-apply is not supported in transformations.",
  render: "Transformations cannot render visualizations. They only filter and modify data.",
  evaluate: "The evaluate operator is not supported in transformations.",
  lookup: "Transformations cannot perform lookups against other tables.",
  "make-series": "make-series aggregates across records and is not supported in transformations.",
  invoke: "The invoke operator is not supported in transformations.",
  find: "The find operator is not supported in transformations.",
  search: "The search operator is not supported in transformations. Use 'where' with string operators instead.",
  fork: "The fork operator is not supported in transformations.",
  facet: "The facet operator is not supported in transformations.",
  sample: "The sample operator is not supported in transformations.",
  "sample-distinct": "sample-distinct is not supported in transformations.",
  consume: "The consume operator is not supported in transformations.",
  getschema: "The getschema operator is not supported in transformations.",
  serialize: "The serialize operator is not supported in transformations.",
  range: "The range operator is not supported in transformations.",
  reduce: "The reduce operator is not supported in transformations.",
  scan: "The scan operator works across records and is not supported in transformations.",
  partition: "The partition operator is not supported in transformations.",
  "project-keep": "project-keep is not listed as a supported transformation operator. Use 'project' instead.",
  "project-reorder":
    "project-reorder is not listed as a supported transformation operator. Column order is defined by the destination table.",
  "parse-where": "parse-where is not listed as a supported transformation operator. Use 'parse' with a 'where' clause.",
  as: "The as operator is not supported in transformations.",
  "execute-and-cache": "execute_and_cache is not supported in transformations.",
};

/**
 * Scalar functions permitted in a transformation, grouped as the documentation
 * groups them. `parse_cef_dictionary` and `geo_location` exist only inside
 * transformations and are unavailable in ordinary log queries.
 */
export const ALLOWED_FUNCTIONS = new Set([
  // Bitwise
  "binary_and", "binary_or", "binary_not", "binary_shift_left", "binary_shift_right", "binary_xor",
  // Conversion
  "tobool", "todatetime", "todouble", "toreal", "toguid", "toint", "tolong", "tostring", "totimespan",
  // DateTime and TimeSpan
  "ago", "datetime_add", "datetime_diff", "datetime_part", "dayofmonth", "dayofweek", "dayofyear",
  "endofday", "endofmonth", "endofweek", "endofyear", "getmonth", "monthofyear", "getyear", "hourofday",
  "make_datetime", "make_timespan", "now", "startofday", "startofmonth", "startofweek", "startofyear",
  "weekofyear",
  // Dynamic and array
  "array_concat", "array_length", "pack", "pack_array", "parse_json", "parse_xml", "zip",
  // Mathematical
  "abs", "bin", "floor", "ceiling", "exp", "exp10", "exp2", "isfinite", "isinf", "isnan",
  "log", "log10", "log2", "pow", "round", "sign",
  // Conditional
  "case", "iif", "iff", "max_of", "min_of",
  // String
  "base64_encodestring", "base64_decodestring", "countof", "extract", "extract_all", "indexof",
  "isempty", "isnotempty", "replace", "split", "strcat", "strcat_delim", "strlen", "substring",
  "tolower", "toupper", "hash_sha256",
  // Type
  "gettype", "isnotnull", "isnull",
  // Transformation-only
  "parse_cef_dictionary", "geo_location",
  // Permitted as an operator rather than a function
  "columnifexists",
]);

/**
 * Names that are valid Kusto elsewhere but renamed or unavailable in transformations.
 * These are the mistakes that pass review and then fail silently at ingestion.
 */
export const FUNCTION_CORRECTIONS = {
  column_ifexists: "Use 'columnifexists' (no underscore) in transformations.",
  base64_encode_tostring: "Use 'base64_encodestring' in transformations.",
  base64_decode_tostring: "Use 'base64_decodestring' in transformations.",
  replace_string: "Use 'replace' in transformations.",
  replace_regex: "replace_regex is not supported in transformations. Use 'replace' or 'extract'.",
  todynamic: "Use 'parse_json' instead of 'todynamic' in transformations.",
  materialize: "materialize is not supported in transformations.",
  bag_unpack: "bag_unpack is not supported in transformations.",
  trim: "trim is not listed as a supported transformation function. Use 'replace' or 'extract'.",
  trim_start: "trim_start is not listed as a supported transformation function.",
  trim_end: "trim_end is not listed as a supported transformation function.",
  reverse: "reverse is not listed as a supported transformation function.",
  strrep: "strrep is not listed as a supported transformation function.",
  hash: "hash is not supported in transformations. Use 'hash_sha256'.",
  hash_md5: "hash_md5 is not supported in transformations. Use 'hash_sha256'.",
  hash_sha1: "hash_sha1 is not supported in transformations. Use 'hash_sha256'.",
  format_datetime: "format_datetime is not listed as a supported transformation function.",
  format_timespan: "format_timespan is not listed as a supported transformation function.",
  coalesce: "coalesce is not listed as a supported transformation function. Use 'iif' with 'isnull'.",
  array_slice: "array_slice is not listed as a supported transformation function.",
  array_sort_asc: "array_sort_asc is not listed as a supported transformation function.",
  set_union: "set_union is not listed as a supported transformation function.",
  todecimal: "todecimal is not supported in transformations. Use 'toreal' or 'todouble'.",
};

/** The parse operator is capped at this many columns per statement for performance. */
export const MAX_PARSE_COLUMNS = 10;

/** geo_location calls an external service; the docs say use it sparingly. */
export const MAX_GEO_LOCATION_CALLS = 3;

/** Only these may begin a transformation query. */
export const VALID_QUERY_ROOTS = ["source", "print", "let", "datatable"];
