use crate::models::ValidationResult;

// ── Allowed tabular operators in transformations ──
const ALLOWED_TABULAR_OPERATORS: &[&str] = &[
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

// ── Blocked tabular operators (with user-friendly messages) ──
const BLOCKED_OPERATORS: &[(&str, &str)] = &[
    ("summarize", "Transformations process each record individually and cannot aggregate multiple records. Use 'extend' with conditional logic instead."),
    ("join", "Transformations cannot correlate data across multiple records or tables. Process each record independently."),
    ("union", "Transformations cannot combine multiple data streams. Use separate data flows in the DCR instead."),
    ("sort", "Transformations cannot reorder records. Each record is processed independently."),
    ("order", "Transformations cannot reorder records ('order by' is an alias for 'sort'). Each record is processed independently."),
    ("top", "Transformations cannot select top N records. Each record is processed independently."),
    ("limit", "Transformations cannot limit the number of records. Use 'where' to filter instead."),
    ("take", "Transformations cannot limit the number of records ('take' is an alias for 'limit'). Use 'where' to filter instead."),
    ("count", "Transformations cannot count records across the stream. Each record is processed individually."),
    ("distinct", "Transformations cannot deduplicate records. Each record is processed independently."),
    ("mv-expand", "mv-expand is not supported in transformations."),
    ("mv-apply", "mv-apply is not supported in transformations."),
    ("render", "Transformations cannot render visualizations. They only filter/modify data."),
    ("evaluate", "The evaluate operator is not supported in transformations."),
    ("lookup", "Transformations cannot perform lookups against other tables."),
    ("make-series", "make-series is not supported in transformations. Each record is processed individually."),
    ("invoke", "The invoke operator is not supported in transformations."),
    ("externaldata", "externaldata is not supported in transformations (no external data access)."),
    ("find", "The find operator is not supported in transformations."),
    ("search", "The search operator is not supported in transformations. Use 'where' with string operators instead."),
    ("fork", "The fork operator is not supported in transformations."),
    ("facet", "The facet operator is not supported in transformations."),
    ("sample", "The sample operator is not supported in transformations."),
    ("sample-distinct", "sample-distinct is not supported in transformations."),
    ("consume", "The consume operator is not supported in transformations."),
    ("getschema", "The getschema operator is not supported in transformations."),
    ("serialize", "The serialize operator is not supported in transformations."),
    ("range", "The range operator is not supported in transformations."),
];

// ── Allowed scalar functions ──
const ALLOWED_FUNCTIONS: &[&str] = &[
    // Bitwise
    "binary_and", "binary_or", "binary_not", "binary_xor",
    "binary_shift_left", "binary_shift_right",
    // Conversion
    "tobool", "todatetime", "todouble", "toreal", "toguid",
    "toint", "tolong", "tostring", "totimespan",
    // DateTime & TimeSpan
    "ago", "datetime_add", "datetime_diff", "datetime_part",
    "dayofmonth", "dayofweek", "dayofyear",
    "endofday", "endofmonth", "endofweek", "endofyear",
    "getmonth", "getyear", "hourofday",
    "make_datetime", "make_timespan", "now",
    "startofday", "startofmonth", "startofweek", "startofyear",
    "weekofyear",
    // Dynamic & Array
    "array_concat", "array_length", "pack_array", "pack",
    "parse_json", "parse_xml", "zip",
    // Math
    "abs", "bin", "floor", "ceiling", "exp", "exp10", "exp2",
    "isfinite", "isinf", "isnan",
    "log", "log10", "log2", "pow", "round", "sign",
    // Conditional
    "case", "iif", "iff", "max_of", "min_of",
    // String
    "base64_encodestring", "base64_decodestring",
    "countof", "extract", "extract_all",
    "indexof", "isempty", "isnotempty",
    "replace_string", "reverse", "split",
    "strcat", "strcat_delim", "strlen", "substring",
    "tolower", "toupper", "trim", "trim_start", "trim_end",
    "hash_sha256",
    // Type
    "gettype", "isnotnull", "isnull",
    // Special (transformation-only)
    "parse_cef_dictionary", "geo_location",
    // Operators used as functions
    "todynamic",
    // Let-related
    "materialize",
];

// ── Common mistakes: wrong function names ──
const FUNCTION_ALIASES: &[(&str, &str)] = &[
    ("column_ifexists", "Use 'columnifexists' (no underscore) in transformations"),
    ("base64_encode_tostring", "Use 'base64_encodestring' in transformations"),
    ("base64_decode_tostring", "Use 'base64_decodestring' in transformations"),
    ("dynamic", "Use 'parse_json()' instead of 'dynamic()' literals in transformations"),
];

// ── Token types ──
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    StringLiteral,
    NumberLiteral,
    Pipe,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Semicolon,
    Dot,
    Operator(String),
    Newline,
}

// ── Tokenizer ──
fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Skip whitespace (except newlines)
        if ch == ' ' || ch == '\t' || ch == '\r' {
            i += 1;
            continue;
        }

        // Newlines
        if ch == '\n' {
            tokens.push(Token::Newline);
            i += 1;
            continue;
        }

        // Escaped newlines in DCR JSON (\n as two chars)
        if ch == '\\' && i + 1 < len && chars[i + 1] == 'n' {
            tokens.push(Token::Newline);
            i += 2;
            continue;
        }

        // Line comments
        if ch == '/' && i + 1 < len && chars[i + 1] == '/' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // String literals (single-quoted)
        if ch == '\'' {
            i += 1;
            while i < len && chars[i] != '\'' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if i < len {
                i += 1; // closing quote
            }
            tokens.push(Token::StringLiteral);
            continue;
        }

        // String literals (double-quoted)
        if ch == '"' {
            i += 1;
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if i < len {
                i += 1;
            }
            tokens.push(Token::StringLiteral);
            continue;
        }

        // Verbatim string @'...' or @"..."
        if ch == '@' && i + 1 < len && (chars[i + 1] == '\'' || chars[i + 1] == '"') {
            let quote = chars[i + 1];
            i += 2;
            while i < len && chars[i] != quote {
                i += 1;
            }
            if i < len {
                i += 1;
            }
            tokens.push(Token::StringLiteral);
            continue;
        }

        // Pipe
        if ch == '|' {
            tokens.push(Token::Pipe);
            i += 1;
            continue;
        }

        // Brackets
        if ch == '(' { tokens.push(Token::LParen); i += 1; continue; }
        if ch == ')' { tokens.push(Token::RParen); i += 1; continue; }
        if ch == '[' { tokens.push(Token::LBracket); i += 1; continue; }
        if ch == ']' { tokens.push(Token::RBracket); i += 1; continue; }
        if ch == ',' { tokens.push(Token::Comma); i += 1; continue; }
        if ch == ';' { tokens.push(Token::Semicolon); i += 1; continue; }
        if ch == '.' { tokens.push(Token::Dot); i += 1; continue; }

        // Multi-char operators
        if ch == '=' && i + 1 < len && chars[i + 1] == '=' {
            tokens.push(Token::Operator("==".to_string()));
            i += 2;
            continue;
        }
        if ch == '=' && i + 1 < len && chars[i + 1] == '~' {
            tokens.push(Token::Operator("=~".to_string()));
            i += 2;
            continue;
        }
        if ch == '!' && i + 1 < len && chars[i + 1] == '=' {
            tokens.push(Token::Operator("!=".to_string()));
            i += 2;
            continue;
        }
        if ch == '!' && i + 1 < len && chars[i + 1] == '~' {
            tokens.push(Token::Operator("!~".to_string()));
            i += 2;
            continue;
        }
        if ch == '<' && i + 1 < len && chars[i + 1] == '=' {
            tokens.push(Token::Operator("<=".to_string()));
            i += 2;
            continue;
        }
        if ch == '>' && i + 1 < len && chars[i + 1] == '=' {
            tokens.push(Token::Operator(">=".to_string()));
            i += 2;
            continue;
        }

        // Single-char operators
        if ch == '=' || ch == '<' || ch == '>' || ch == '+' || ch == '-'
            || ch == '*' || ch == '/' || ch == '%' || ch == '!' || ch == '~'
        {
            tokens.push(Token::Operator(ch.to_string()));
            i += 1;
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() {
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == 'e' || chars[i] == 'E') {
                i += 1;
            }
            // Timespan suffixes (d, h, m, s, ms, us, tick)
            if i < len && chars[i].is_ascii_alphabetic() {
                while i < len && chars[i].is_ascii_alphabetic() {
                    i += 1;
                }
            }
            tokens.push(Token::NumberLiteral);
            continue;
        }

        // Identifiers and keywords
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            tokens.push(Token::Identifier(word));
            continue;
        }

        // Curly braces (for dynamic objects) — skip
        if ch == '{' || ch == '}' {
            i += 1;
            continue;
        }

        // Unknown characters — skip
        i += 1;
    }

    tokens
}

// ── Helper: get pipe-separated stages ──
fn get_pipe_stages(tokens: &[Token]) -> Vec<Vec<Token>> {
    let mut stages: Vec<Vec<Token>> = vec![vec![]];

    // First, separate by semicolons to handle let statements
    // Then separate by pipes within each statement
    let mut current_statement_tokens: Vec<Token> = Vec::new();
    let mut all_statements: Vec<Vec<Token>> = Vec::new();

    for token in tokens {
        if *token == Token::Semicolon {
            if !current_statement_tokens.is_empty() {
                all_statements.push(current_statement_tokens.clone());
                current_statement_tokens.clear();
            }
        } else {
            current_statement_tokens.push(token.clone());
        }
    }
    if !current_statement_tokens.is_empty() {
        all_statements.push(current_statement_tokens);
    }

    // For the main query (last statement, or the one starting with source/print)
    // split by pipes
    stages.clear();
    for statement in &all_statements {
        let mut current: Vec<Token> = Vec::new();
        for token in statement {
            if *token == Token::Pipe {
                if !current.is_empty() {
                    stages.push(current.clone());
                    current.clear();
                }
            } else if *token != Token::Newline {
                current.push(token.clone());
            }
        }
        if !current.is_empty() {
            stages.push(current);
        }
    }

    stages
}

// ── Helper: get the operator name at the start of a stage ──
fn get_stage_operator(stage: &[Token]) -> Option<String> {
    // Handle compound operators like project-away, project-rename
    if stage.len() >= 3 {
        if let Token::Identifier(ref first) = stage[0] {
            if let Token::Operator(ref op) = stage[1] {
                if op == "-" {
                    if let Token::Identifier(ref second) = stage[2] {
                        let compound = format!("{}-{}", first.to_lowercase(), second.to_lowercase());
                        if compound == "project-away" || compound == "project-rename"
                            || compound == "mv-expand" || compound == "mv-apply"
                            || compound == "make-series" || compound == "sample-distinct"
                        {
                            return Some(compound);
                        }
                    }
                }
            }
        }
    }

    // Simple operator
    if let Some(Token::Identifier(ref name)) = stage.first() {
        return Some(name.to_lowercase());
    }

    None
}

// ── Find function calls: identifier followed by '(' ──
fn find_function_calls(tokens: &[Token]) -> Vec<String> {
    let mut functions = Vec::new();
    for i in 0..tokens.len().saturating_sub(1) {
        if let Token::Identifier(ref name) = tokens[i] {
            if tokens[i + 1] == Token::LParen {
                functions.push(name.clone());
            }
        }
    }
    functions
}

// ── Main validation entry point ──
pub fn validate(query: &str) -> ValidationResult {
    let mut result = ValidationResult::new();
    let trimmed = query.trim();

    if trimmed.is_empty() {
        result.add_error("KQL001", "Query is empty", Some("Enter a KQL transformation query starting with 'source'"));
        return result;
    }

    let tokens = tokenize(trimmed);
    let non_newline_tokens: Vec<&Token> = tokens.iter().filter(|t| **t != Token::Newline).collect();

    if non_newline_tokens.is_empty() {
        result.add_error("KQL001", "Query is empty", Some("Enter a KQL transformation query starting with 'source'"));
        return result;
    }

    // Check 1: Query must start with 'source', 'print', or 'let'
    check_query_start(&non_newline_tokens, &mut result);

    // Check 2: Validate pipe stages have allowed operators
    let stages = get_pipe_stages(&tokens);
    check_stage_operators(&stages, &mut result);

    // Check 3: Validate function calls
    check_function_calls(&tokens, &mut result);

    // Check 4: Check for TimeGenerated in output
    check_time_generated(&tokens, trimmed, &mut result);

    // Check 5: Check parse column limit
    check_parse_column_limit(&stages, &mut result);

    // Check 6: Check for common mistakes
    check_common_mistakes(&tokens, &mut result);

    // Check 7: Scan all identifiers for blocked operators (catches usage inside let statements)
    check_blocked_anywhere(&tokens, &mut result);

    // Check 8: Check that the main query body uses 'source', not a table name
    check_main_query_uses_source(&stages, &mut result);

    // Add success info if valid
    if result.valid {
        result.add_info("KQL000", "KQL transformation query is valid");
    }

    result
}

fn check_query_start(tokens: &[&Token], result: &mut ValidationResult) {
    if let Some(Token::Identifier(ref name)) = tokens.first() {
        let lower = name.to_lowercase();
        if lower != "source" && lower != "print" && lower != "let" {
            result.add_error(
                "KQL002",
                &format!("Query must start with 'source', not '{}'", name),
                Some("All transformation queries must begin with 'source' which represents the input data stream, or 'print' for constant output, or 'let' for variable declarations"),
            );
        }
    }
}

fn check_stage_operators(stages: &[Vec<Token>], result: &mut ValidationResult) {
    for (i, stage) in stages.iter().enumerate() {
        if stage.is_empty() {
            continue;
        }

        let op_name = match get_stage_operator(stage) {
            Some(name) => name,
            None => continue,
        };

        // Skip the first stage (source/print/let)
        if i == 0 {
            let lower = op_name.to_lowercase();
            if lower == "source" || lower == "print" || lower == "let" {
                continue;
            }
        }

        // Check for let statements
        if op_name == "let" {
            continue;
        }

        // Check against blocked operators
        for (blocked, message) in BLOCKED_OPERATORS {
            if op_name == *blocked {
                result.add_error(
                    "KQL003",
                    &format!("Unsupported operator '{}' in transformation", blocked),
                    Some(message),
                );
                return; // Don't duplicate errors for same operator
            }
        }

        // Check if it's a known allowed operator
        if i > 0 || (op_name != "source" && op_name != "print" && op_name != "let") {
            let is_allowed = ALLOWED_TABULAR_OPERATORS.iter().any(|&allowed| op_name == allowed);
            let is_blocked = BLOCKED_OPERATORS.iter().any(|(blocked, _)| op_name == *blocked);

            if !is_allowed && !is_blocked && op_name != "source" && op_name != "print" && op_name != "let" {
                result.add_warning(
                    "KQL004",
                    &format!("Operator '{}' may not be supported in transformations", op_name),
                    Some(&format!("Supported operators: {}", ALLOWED_TABULAR_OPERATORS.join(", "))),
                );
            }
        }
    }
}

fn check_function_calls(tokens: &[Token], result: &mut ValidationResult) {
    let functions = find_function_calls(tokens);

    for func_name in &functions {
        let lower = func_name.to_lowercase();

        // Skip tabular operators that use parentheses (datatable, etc.)
        if ALLOWED_TABULAR_OPERATORS.contains(&lower.as_str()) {
            continue;
        }
        if lower == "source" || lower == "print" {
            continue;
        }

        // Check for common wrong function names
        for (wrong_name, suggestion) in FUNCTION_ALIASES {
            if lower == *wrong_name {
                result.add_error(
                    "KQL005",
                    &format!("Function '{}' is not valid in transformations", func_name),
                    Some(suggestion),
                );
                return;
            }
        }

        // Check against allowed function list
        let is_allowed = ALLOWED_FUNCTIONS.iter().any(|&allowed| lower == allowed);
        if !is_allowed {
            // Don't flag identifiers that might be column-access or user-defined let functions
            // Only flag if it looks like a well-known KQL function
            if is_known_unsupported_function(&lower) {
                result.add_error(
                    "KQL006",
                    &format!("Function '{}' is not supported in transformations", func_name),
                    Some("See the Azure Monitor documentation for the list of supported scalar functions in transformations"),
                );
            } else {
                result.add_warning(
                    "KQL007",
                    &format!("Function '{}' may not be supported in transformations", func_name),
                    Some("Verify this function is in the supported functions list for Azure Monitor transformations"),
                );
            }
        }
    }
}

fn is_known_unsupported_function(name: &str) -> bool {
    const KNOWN_UNSUPPORTED: &[&str] = &[
        "arg_max", "arg_min", "avg", "avgif", "buildschema",
        "count", "countif", "dcount", "dcountif",
        "make_bag", "make_list", "make_set",
        "max", "maxif", "min", "minif",
        "percentile", "percentiles", "stdev", "stdevif",
        "sum", "sumif", "variance", "varianceif",
        "any", "take_any", "take_anyif",
        "hll", "hll_merge", "tdigest", "tdigest_merge",
        "prev", "next", "row_number", "row_cumsum", "row_rank_dense",
        "bag_merge", "bag_pack", "bag_remove_keys",
        "format_datetime", "format_timespan",
        "url_encode", "url_decode",
        "base64_encode_tostring", "base64_decode_tostring",
        "column_ifexists",
    ];
    KNOWN_UNSUPPORTED.contains(&name)
}

fn check_time_generated(tokens: &[Token], raw_query: &str, result: &mut ValidationResult) {
    // Check if the query references TimeGenerated
    let has_time_generated = tokens.iter().any(|t| {
        if let Token::Identifier(ref name) = t {
            name == "TimeGenerated"
        } else {
            false
        }
    });

    // Check if the query has a project statement (which defines output columns)
    let has_project = tokens.iter().any(|t| {
        if let Token::Identifier(ref name) = t {
            name.to_lowercase() == "project"
        } else {
            false
        }
    });

    // Also check for TimeGenerated in string form (case-sensitive)
    let has_tg_in_raw = raw_query.contains("TimeGenerated");

    if has_project && !has_time_generated && !has_tg_in_raw {
        result.add_warning(
            "KQL008",
            "Output may be missing 'TimeGenerated' column",
            Some("The output of every transformation must contain a valid timestamp in a column called 'TimeGenerated' of type 'datetime'. Add it with: extend TimeGenerated = now() or project TimeGenerated = todatetime(your_time_column)"),
        );
    }

    if !has_project && !has_time_generated {
        result.add_info(
            "KQL009",
            "Tip: Ensure your output includes a 'TimeGenerated' column of type datetime",
        );
    }
}

fn check_parse_column_limit(stages: &[Vec<Token>], result: &mut ValidationResult) {
    for stage in stages {
        let op = match get_stage_operator(stage) {
            Some(name) => name,
            None => continue,
        };

        if op != "parse" {
            continue;
        }

        // Count identifiers that appear after 'parse' — heuristic for column count
        // In 'parse ... with (col1:type, col2:type, ...)', count identifiers after 'with'
        let mut column_count = 0;
        let mut after_with = false;
        let mut in_parens = 0;

        for token in stage.iter().skip(1) {
            match token {
                Token::Identifier(ref name) if name.to_lowercase() == "with" => {
                    after_with = true;
                }
                Token::LParen if after_with => {
                    in_parens += 1;
                }
                Token::RParen if after_with && in_parens > 0 => {
                    in_parens -= 1;
                }
                Token::Identifier(ref name) if after_with => {
                    // Skip type names
                    let lower = name.to_lowercase();
                    if lower != "string" && lower != "int" && lower != "long"
                        && lower != "real" && lower != "double" && lower != "datetime"
                        && lower != "bool" && lower != "guid" && lower != "dynamic"
                        && lower != "timespan"
                    {
                        column_count += 1;
                    }
                }
                _ => {}
            }
        }

        // Also count by star patterns: parse ... * "..." * "..."
        if !after_with {
            column_count = 0;
            let mut found_star_patterns = false;
            for token in stage.iter().skip(1) {
                if let Token::Identifier(_) = token {
                    // Count identifiers between string literals (parse pattern columns)
                    column_count += 1;
                    found_star_patterns = true;
                }
            }
            if !found_star_patterns {
                continue;
            }
            // Rough heuristic — parse identifies could be non-column identifiers
            // Only warn if clearly over limit
            if column_count <= 10 {
                continue;
            }
        }

        if column_count > 10 {
            result.add_warning(
                "KQL010",
                &format!("Parse statement may have {} columns (limit is 10 per statement)", column_count),
                Some("The parse command in a transformation is limited to 10 columns per statement. Split it into multiple parse statements."),
            );
        }
    }
}

fn check_common_mistakes(tokens: &[Token], result: &mut ValidationResult) {
    // Check for 'order by' (two-word operator)
    for i in 0..tokens.len().saturating_sub(1) {
        if let Token::Identifier(ref name) = tokens[i] {
            if name.to_lowercase() == "order" {
                if let Token::Identifier(ref next) = tokens[i + 1] {
                    if next.to_lowercase() == "by" {
                        result.add_error(
                            "KQL003",
                            "Unsupported operator 'order by' in transformation",
                            Some("Transformations cannot reorder records. Each record is processed independently."),
                        );
                    }
                }
            }
        }
    }

    // Check for 'has_any' or 'has_all' which are not in the supported list
    for i in 0..tokens.len().saturating_sub(1) {
        if let Token::Identifier(ref name) = tokens[i] {
            let lower = name.to_lowercase();
            if (lower == "has_any" || lower == "has_all") && tokens[i + 1] == Token::LParen {
                result.add_warning(
                    "KQL011",
                    &format!("'{}' may not be supported in transformations", name),
                    Some("Use 'has' operator with multiple conditions joined by 'or' instead"),
                );
            }
        }
    }

    // Check for toscalar (not supported)
    for token in tokens {
        if let Token::Identifier(ref name) = token {
            if name.to_lowercase() == "toscalar" {
                result.add_error(
                    "KQL012",
                    "'toscalar' is not supported in transformations",
                    Some("Transformations process each record individually and cannot use scalar subqueries"),
                );
            }
        }
    }
}

/// Scan all tokens for blocked operators used anywhere (including inside let statements)
fn check_blocked_anywhere(tokens: &[Token], result: &mut ValidationResult) {
    // These operators are never valid anywhere in a transformation, even inside let
    const ALWAYS_BLOCKED: &[(&str, &str)] = &[
        ("externaldata", "'externaldata' is not supported in transformations. Transformations cannot access external data sources or URLs."),
    ];

    for token in tokens {
        if let Token::Identifier(ref name) = token {
            let lower = name.to_lowercase();
            for (blocked, message) in ALWAYS_BLOCKED {
                if lower == *blocked {
                    result.add_error("KQL013", message, None);
                    return;
                }
            }
        }
    }
}

/// Check that the main query body (the last semicolon-separated statement that isn't a let)
/// uses 'source' or 'print' as its data source, not a table name.
fn check_main_query_uses_source(stages: &[Vec<Token>], result: &mut ValidationResult) {
    // Walk stages to find the first non-let stage — that's the main query entry point
    for stage in stages {
        if stage.is_empty() {
            continue;
        }
        if let Some(Token::Identifier(ref name)) = stage.first() {
            let lower = name.to_lowercase();
            // Skip let statements
            if lower == "let" {
                continue;
            }
            // source and print are valid
            if lower == "source" || lower == "print" {
                return;
            }
            // Allowed tabular operators after a pipe are fine (extend, where, project, etc.)
            if ALLOWED_TABULAR_OPERATORS.contains(&lower.as_str()) {
                continue;
            }
            // Anything else as a standalone stage start (not after a pipe operator) is likely a table name
            // Check if this looks like a table name (starts with uppercase, or known pattern)
            if lower != "source" && lower != "print" && lower != "let" {
                // Only flag if this is a stage that starts a new tabular expression
                // (i.e., not preceded by a pipe from source)
                // Heuristic: if the identifier is not an allowed operator, it's likely a table name
                let is_operator = ALLOWED_TABULAR_OPERATORS.contains(&lower.as_str())
                    || BLOCKED_OPERATORS.iter().any(|(b, _)| lower == *b);
                if !is_operator {
                    result.add_error(
                        "KQL014",
                        &format!("'{}' appears to be a table name. Transformations must use 'source' as the data source, not a table name", name),
                        Some("Replace the table name with 'source'. All transformation queries must reference 'source' which represents the incoming data stream."),
                    );
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_simple_query() {
        let result = validate("source");
        assert!(result.valid);
    }

    #[test]
    fn test_valid_filter_query() {
        let result = validate("source | where severity == \"Critical\"");
        assert!(result.valid);
    }

    #[test]
    fn test_valid_complex_query() {
        let result = validate(
            "source | where severity == \"Critical\" | extend Properties = parse_json(properties) | project TimeGenerated = todatetime(time), Category = category"
        );
        assert!(result.valid);
    }

    #[test]
    fn test_blocked_summarize() {
        let result = validate("source | summarize count() by severity");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL003"));
    }

    #[test]
    fn test_blocked_join() {
        let result = validate("source | join other on key");
        assert!(!result.valid);
    }

    #[test]
    fn test_empty_query() {
        let result = validate("");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL001"));
    }

    #[test]
    fn test_wrong_start() {
        let result = validate("Syslog | where severity == 'error'");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL002"));
    }

    #[test]
    fn test_wrong_function_name() {
        let result = validate("source | extend x = column_ifexists('col', '')");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL005"));
    }

    #[test]
    fn test_externaldata_in_let() {
        let result = validate("let data=externaldata(col:string)[h'https://example.com/file.txt']\n| parse RawData with col:string;\nsource\n| where col has 'test'");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL013"));
    }

    #[test]
    fn test_table_name_instead_of_source() {
        let result = validate("CloudAppEvents\n| where TimeGenerated > ago(1h)");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL014" || e.code == "KQL002"));
    }

    #[test]
    fn test_table_name_after_let() {
        let result = validate("let x = 1;\nSyslog\n| where severity == 'error'");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "KQL014"));
    }
}
