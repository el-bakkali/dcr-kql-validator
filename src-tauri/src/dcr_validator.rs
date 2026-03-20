use crate::kql_validator;
use crate::models::ValidationResult;
use serde_json::Value;

const VALID_COLUMN_TYPES: &[&str] = &[
    "string", "int", "long", "real", "double", "datetime",
    "bool", "boolean", "dynamic", "guid", "timespan",
];

const KNOWN_MICROSOFT_STREAMS: &[&str] = &[
    "Microsoft-Event",
    "Microsoft-Syslog",
    "Microsoft-Perf",
    "Microsoft-InsightsMetrics",
    "Microsoft-WindowsEvent",
    "Microsoft-SecurityEvent",
    "Microsoft-CommonSecurityLog",
];

const VALID_LOG_FILE_FORMATS: &[&str] = &["text", "json"];

pub fn validate(json_str: &str) -> ValidationResult {
    let mut result = ValidationResult::new();
    let trimmed = json_str.trim();

    if trimmed.is_empty() {
        result.add_error("DCR001", "DCR JSON is empty", Some("Provide a valid DCR JSON definition"));
        return result;
    }

    // Step 1: Parse JSON
    let json: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(e) => {
            result.add_error(
                "DCR002",
                &format!("Invalid JSON: {}", e),
                Some("Fix the JSON syntax error before proceeding"),
            );
            return result;
        }
    };

    let obj = match json.as_object() {
        Some(o) => o,
        None => {
            result.add_error("DCR003", "DCR must be a JSON object", None);
            return result;
        }
    };

    // Step 2: Check top-level fields
    check_top_level(obj, &mut result);

    // Step 3: Check kind-specific rules
    if let Some(kind) = obj.get("kind").and_then(|v| v.as_str()) {
        check_kind_rules(kind, obj, &mut result);
    }

    // Step 4: Validate properties
    if let Some(properties) = obj.get("properties").and_then(|v| v.as_object()) {
        check_properties(properties, &mut result);
    }

    // Add success info
    if result.valid {
        result.add_info("DCR000", "DCR JSON structure is valid");
    }

    result
}

fn check_top_level(obj: &serde_json::Map<String, Value>, result: &mut ValidationResult) {
    // Location
    if !obj.contains_key("location") {
        result.add_warning(
            "DCR010",
            "Missing 'location' field",
            Some("DCR should specify an Azure region (e.g., 'eastus', 'westeurope')"),
        );
    }

    // Properties
    if !obj.contains_key("properties") {
        result.add_error(
            "DCR011",
            "Missing 'properties' object",
            Some("The DCR must have a 'properties' object containing dataSources, destinations, and dataFlows"),
        );
    } else if !obj["properties"].is_object() {
        result.add_error(
            "DCR012",
            "'properties' must be a JSON object",
            None,
        );
    }
}

fn check_kind_rules(kind: &str, obj: &serde_json::Map<String, Value>, result: &mut ValidationResult) {
    match kind {
        "WorkspaceTransforms" => {
            // dataSources must be empty
            if let Some(properties) = obj.get("properties").and_then(|v| v.as_object()) {
                if let Some(ds) = properties.get("dataSources") {
                    if let Some(ds_obj) = ds.as_object() {
                        if !ds_obj.is_empty() {
                            result.add_error(
                                "DCR020",
                                "WorkspaceTransforms DCR must have empty 'dataSources'",
                                Some("For workspace transformation DCRs, the dataSources section must be empty ({})"),
                            );
                        }
                    }
                }

                // Must have exactly one Log Analytics destination
                if let Some(destinations) = properties.get("destinations").and_then(|v| v.as_object()) {
                    if let Some(la) = destinations.get("logAnalytics").and_then(|v| v.as_array()) {
                        if la.len() != 1 {
                            result.add_error(
                                "DCR021",
                                &format!("WorkspaceTransforms DCR must have exactly 1 Log Analytics destination, found {}", la.len()),
                                Some("Workspace transformation DCRs must include one and only one Log Analytics workspace destination"),
                            );
                        }
                    } else {
                        result.add_error(
                            "DCR022",
                            "WorkspaceTransforms DCR is missing 'logAnalytics' destination",
                            Some("Add a logAnalytics destination with the workspace resource ID"),
                        );
                    }
                }

                // DataFlows streams must use Microsoft-Table- prefix
                if let Some(data_flows) = properties.get("dataFlows").and_then(|v| v.as_array()) {
                    for (i, flow) in data_flows.iter().enumerate() {
                        if let Some(streams) = flow.get("streams").and_then(|v| v.as_array()) {
                            for stream in streams {
                                if let Some(s) = stream.as_str() {
                                    if !s.starts_with("Microsoft-Table-") {
                                        result.add_warning(
                                            "DCR023",
                                            &format!("DataFlow {} stream '{}' should use 'Microsoft-Table-<TableName>' format for workspace transforms", i + 1, s),
                                            Some("Workspace transformation streams should be formatted as 'Microsoft-Table-<TableName>'"),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "Direct" => {
            if let Some(properties) = obj.get("properties").and_then(|v| v.as_object()) {
                if !properties.contains_key("dataCollectionEndpointId")
                    && !obj.contains_key("dataCollectionEndpointId")
                {
                    result.add_warning(
                        "DCR024",
                        "Direct DCR should have 'dataCollectionEndpointId'",
                        Some("DCRs with kind 'Direct' (for Logs Ingestion API) need a data collection endpoint ID. This is often auto-generated."),
                    );
                }
            }
        }
        _ => {
            // Other kinds are valid (e.g., no kind specified is fine for AMA DCRs)
        }
    }
}

fn check_properties(properties: &serde_json::Map<String, Value>, result: &mut ValidationResult) {
    // Collect destination names for cross-referencing
    let mut destination_names: Vec<String> = Vec::new();
    // Collect declared stream names for cross-referencing
    let mut declared_streams: Vec<String> = Vec::new();
    // Track if DCR uses logFiles data source
    let mut has_log_files = false;

    // Check destinations
    if let Some(destinations) = properties.get("destinations") {
        if let Some(dest_obj) = destinations.as_object() {
            // Log Analytics destinations
            if let Some(la) = dest_obj.get("logAnalytics").and_then(|v| v.as_array()) {
                for (i, dest) in la.iter().enumerate() {
                    check_log_analytics_destination(dest, i, result, &mut destination_names);
                }
            }

            // Azure Monitor Metrics
            if let Some(metrics) = dest_obj.get("azureMonitorMetrics").and_then(|v| v.as_object()) {
                if let Some(name) = metrics.get("name").and_then(|v| v.as_str()) {
                    destination_names.push(name.to_string());
                }
            }
        } else {
            result.add_error("DCR030", "'destinations' must be a JSON object", None);
        }
    } else {
        result.add_error(
            "DCR031",
            "Missing 'destinations' in properties",
            Some("The DCR must specify at least one destination"),
        );
    }

    // Check stream declarations
    if let Some(stream_decls) = properties.get("streamDeclarations") {
        if let Some(decls_obj) = stream_decls.as_object() {
            for (stream_name, stream_def) in decls_obj {
                declared_streams.push(stream_name.clone());
                check_stream_declaration(stream_name, stream_def, result);
            }
        }
    }

    // Check dataSources.logFiles
    if let Some(data_sources) = properties.get("dataSources").and_then(|v| v.as_object()) {
        if let Some(log_files) = data_sources.get("logFiles").and_then(|v| v.as_array()) {
            has_log_files = true;
            for (i, lf) in log_files.iter().enumerate() {
                check_log_file_source(lf, i, &declared_streams, result);
            }
        }
    }

    // Enhancement 3: dataCollectionEndpointId required for logFiles-based DCRs
    if has_log_files && !properties.contains_key("dataCollectionEndpointId") {
        result.add_error(
            "DCR090",
            "Missing 'dataCollectionEndpointId' for logFiles-based DCR",
            Some("DCRs that collect from log files require a dataCollectionEndpointId in the properties section"),
        );
    }

    // Check dataFlows
    if let Some(data_flows) = properties.get("dataFlows") {
        if let Some(flows) = data_flows.as_array() {
            if flows.is_empty() {
                result.add_error(
                    "DCR040",
                    "'dataFlows' array is empty",
                    Some("The DCR must have at least one data flow"),
                );
            }
            for (i, flow) in flows.iter().enumerate() {
                check_data_flow(flow, i, &destination_names, &declared_streams, result);
            }
        } else {
            result.add_error("DCR041", "'dataFlows' must be a JSON array", None);
        }
    } else {
        result.add_error(
            "DCR042",
            "Missing 'dataFlows' in properties",
            Some("The DCR must specify at least one data flow pairing streams with destinations"),
        );
    }
}

fn check_log_analytics_destination(
    dest: &Value,
    index: usize,
    result: &mut ValidationResult,
    names: &mut Vec<String>,
) {
    let dest_obj = match dest.as_object() {
        Some(o) => o,
        None => {
            result.add_error(
                "DCR032",
                &format!("logAnalytics destination {} must be a JSON object", index + 1),
                None,
            );
            return;
        }
    };

    if let Some(name) = dest_obj.get("name").and_then(|v| v.as_str()) {
        names.push(name.to_string());
    } else {
        result.add_error(
            "DCR033",
            &format!("logAnalytics destination {} is missing 'name'", index + 1),
            Some("Each destination must have a unique 'name' to be referenced in dataFlows"),
        );
    }

    if !dest_obj.contains_key("workspaceResourceId") {
        result.add_error(
            "DCR034",
            &format!("logAnalytics destination {} is missing 'workspaceResourceId'", index + 1),
            Some("Provide the full Azure resource ID of the Log Analytics workspace"),
        );
    } else if let Some(id) = dest_obj.get("workspaceResourceId").and_then(|v| v.as_str()) {
        if !id.contains("/providers/") {
            result.add_warning(
                "DCR035",
                &format!("logAnalytics destination {} workspaceResourceId doesn't look like a valid Azure resource ID", index + 1),
                Some("Format: /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<name>"),
            );
        }
    }
}

fn check_stream_declaration(stream_name: &str, stream_def: &Value, result: &mut ValidationResult) {
    // Custom streams must start with Custom-
    if !stream_name.starts_with("Custom-") && !stream_name.starts_with("Microsoft-") {
        result.add_warning(
            "DCR050",
            &format!("Stream '{}' should start with 'Custom-' for custom streams", stream_name),
            Some("Custom stream declarations must be prefixed with 'Custom-'"),
        );
    }

    // Check columns
    if let Some(columns) = stream_def.get("columns").and_then(|v| v.as_array()) {
        for (i, col) in columns.iter().enumerate() {
            if let Some(col_obj) = col.as_object() {
                if !col_obj.contains_key("name") {
                    result.add_error(
                        "DCR051",
                        &format!("Column {} in stream '{}' is missing 'name'", i + 1, stream_name),
                        None,
                    );
                }

                if let Some(col_type) = col_obj.get("type").and_then(|v| v.as_str()) {
                    if !VALID_COLUMN_TYPES.contains(&col_type.to_lowercase().as_str()) {
                        result.add_error(
                            "DCR052",
                            &format!("Column {} in stream '{}' has invalid type '{}' ", i + 1, stream_name, col_type),
                            Some(&format!("Valid types: {}", VALID_COLUMN_TYPES.join(", "))),
                        );
                    }
                } else {
                    result.add_error(
                        "DCR053",
                        &format!("Column {} in stream '{}' is missing 'type'", i + 1, stream_name),
                        Some(&format!("Valid types: {}", VALID_COLUMN_TYPES.join(", "))),
                    );
                }
            }
        }
    } else {
        result.add_error(
            "DCR054",
            &format!("Stream '{}' must have a 'columns' array", stream_name),
            None,
        );
    }
}

fn check_log_file_source(
    lf: &Value,
    index: usize,
    declared_streams: &[String],
    result: &mut ValidationResult,
) {
    let lf_obj = match lf.as_object() {
        Some(o) => o,
        None => {
            result.add_error(
                "DCR100",
                &format!("logFiles entry {} must be a JSON object", index + 1),
                None,
            );
            return;
        }
    };

    let lf_label = format!("logFiles entry {}", index + 1);

    // Check required 'name' field
    if !lf_obj.contains_key("name") {
        result.add_error(
            "DCR101",
            &format!("{} is missing 'name'", lf_label),
            Some("Each logFiles data source must have a unique 'name'"),
        );
    }

    // Check required 'streams' field and cross-reference with streamDeclarations
    if let Some(streams) = lf_obj.get("streams").and_then(|v| v.as_array()) {
        if streams.is_empty() {
            result.add_error(
                "DCR102",
                &format!("{} has empty 'streams' array", lf_label),
                Some("Specify at least one stream that references a stream declared in streamDeclarations"),
            );
        }
        for stream in streams {
            if let Some(s) = stream.as_str() {
                if s.starts_with("Custom-") && !declared_streams.contains(&s.to_string()) {
                    result.add_error(
                        "DCR103",
                        &format!("{} references stream '{}' which is not declared in streamDeclarations", lf_label, s),
                        Some("Custom streams used in logFiles must be defined in the streamDeclarations section with their column schema"),
                    );
                }
            }
        }
    } else {
        result.add_error(
            "DCR104",
            &format!("{} is missing 'streams' array", lf_label),
            Some("logFiles data source must specify which streams to collect"),
        );
    }

    // Check required 'filePatterns' field
    if let Some(patterns) = lf_obj.get("filePatterns").and_then(|v| v.as_array()) {
        if patterns.is_empty() {
            result.add_error(
                "DCR105",
                &format!("{} has empty 'filePatterns' array", lf_label),
                Some("Specify at least one file pattern (e.g., 'C:\\logs\\*.txt')"),
            );
        }
    } else {
        result.add_error(
            "DCR106",
            &format!("{} is missing 'filePatterns' array", lf_label),
            Some("Specify file path patterns for the log files to collect (e.g., 'C:\\logs\\*.txt')"),
        );
    }

    // Enhancement 4: Validate 'format' field
    if let Some(format) = lf_obj.get("format").and_then(|v| v.as_str()) {
        if !VALID_LOG_FILE_FORMATS.contains(&format) {
            result.add_error(
                "DCR107",
                &format!("{} has invalid format '{}'", lf_label, format),
                Some("Valid formats are 'text' or 'json'"),
            );
        }

        // Enhancement 5: For text format, check recordStartTimestampFormat
        if format == "text" {
            let has_timestamp_format = lf_obj
                .get("settings")
                .and_then(|v| v.get("text"))
                .and_then(|v| v.get("recordStartTimestampFormat"))
                .is_some();

            if !has_timestamp_format {
                result.add_warning(
                    "DCR108",
                    &format!("{} with format 'text' should include settings.text.recordStartTimestampFormat", lf_label),
                    Some("Text log files typically need a recordStartTimestampFormat (e.g., 'ISO 8601') in settings.text"),
                );
            }
        }
    } else {
        result.add_error(
            "DCR109",
            &format!("{} is missing 'format' field", lf_label),
            Some("Specify the log file format: 'text' or 'json'"),
        );
    }
}

fn check_data_flow(
    flow: &Value,
    index: usize,
    destination_names: &[String],
    declared_streams: &[String],
    result: &mut ValidationResult,
) {
    let flow_obj = match flow.as_object() {
        Some(o) => o,
        None => {
            result.add_error(
                "DCR060",
                &format!("DataFlow {} must be a JSON object", index + 1),
                None,
            );
            return;
        }
    };

    let flow_label = format!("DataFlow {}", index + 1);

    // Check streams and cross-reference with declared streams
    if let Some(streams) = flow_obj.get("streams").and_then(|v| v.as_array()) {
        if streams.is_empty() {
            result.add_error(
                "DCR061",
                &format!("{} has empty 'streams' array", flow_label),
                Some("Specify at least one stream"),
            );
        }

        // Enhancement 2: Stream cross-referencing
        for stream in streams {
            if let Some(s) = stream.as_str() {
                if s.starts_with("Custom-") {
                    // Custom streams must be declared in streamDeclarations
                    if !declared_streams.contains(&s.to_string()) {
                        result.add_error(
                            "DCR066",
                            &format!("{} references custom stream '{}' which is not declared in streamDeclarations", flow_label, s),
                            Some("Custom streams must be defined in the streamDeclarations section with their column schema"),
                        );
                    }
                } else if s.starts_with("Microsoft-") {
                    // Microsoft streams don't need declaration but should be a known stream
                    // (Microsoft-Table-* for workspace transforms are also valid)
                    if !KNOWN_MICROSOFT_STREAMS.contains(&s) && !s.starts_with("Microsoft-Table-") {
                        result.add_warning(
                            "DCR067",
                            &format!("{} references Microsoft stream '{}' which is not a commonly known stream", flow_label, s),
                            Some("Verify the stream name is correct. Common streams: Microsoft-Event, Microsoft-Syslog, Microsoft-Perf, Microsoft-InsightsMetrics, Microsoft-WindowsEvent, Microsoft-SecurityEvent, Microsoft-CommonSecurityLog"),
                        );
                    }
                }
            }
        }
    } else {
        result.add_error(
            "DCR062",
            &format!("{} is missing 'streams' array", flow_label),
            None,
        );
    }

    // Check destinations
    if let Some(destinations) = flow_obj.get("destinations").and_then(|v| v.as_array()) {
        if destinations.is_empty() {
            result.add_error(
                "DCR063",
                &format!("{} has empty 'destinations' array", flow_label),
                Some("Specify at least one destination"),
            );
        }

        // Cross-reference destination names
        for dest in destinations {
            if let Some(dest_name) = dest.as_str() {
                if !destination_names.contains(&dest_name.to_string()) {
                    result.add_error(
                        "DCR064",
                        &format!("{} references destination '{}' which is not defined", flow_label, dest_name),
                        Some("Make sure the destination name matches a destination defined in the 'destinations' section"),
                    );
                }
            }
        }
    } else {
        result.add_error(
            "DCR065",
            &format!("{} is missing 'destinations' array", flow_label),
            None,
        );
    }

    // Check transformKql
    if let Some(transform) = flow_obj.get("transformKql").and_then(|v| v.as_str()) {
        let trimmed = transform.trim();
        if !trimmed.is_empty() && trimmed != "source" {
            let kql_result = kql_validator::validate(trimmed);
            if !kql_result.valid {
                for err in &kql_result.errors {
                    result.add_error(
                        &format!("DCR070-{}", err.code),
                        &format!("{} transformKql: {}", flow_label, err.message),
                        err.suggestion.as_deref(),
                    );
                }
            }
            for warn in &kql_result.warnings {
                result.add_warning(
                    &format!("DCR071-{}", warn.code),
                    &format!("{} transformKql: {}", flow_label, warn.message),
                    warn.suggestion.as_deref(),
                );
            }
        }
    }

    // Check outputStream
    if let Some(output_stream) = flow_obj.get("outputStream").and_then(|v| v.as_str()) {
        // Custom tables must end with _CL
        if output_stream.starts_with("Custom-") && !output_stream.ends_with("_CL") {
            result.add_error(
                "DCR080",
                &format!("{} outputStream '{}' must end with '_CL' for custom tables", flow_label, output_stream),
                Some("Custom log table names must end with '_CL' suffix"),
            );
        }

        // Microsoft tables should start with Microsoft-
        if !output_stream.starts_with("Custom-") && !output_stream.starts_with("Microsoft-") {
            result.add_warning(
                "DCR081",
                &format!("{} outputStream '{}' should start with 'Custom-' or 'Microsoft-'", flow_label, output_stream),
                Some("Output streams should be prefixed with 'Custom-' for custom tables or 'Microsoft-' for built-in tables"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_json() {
        let result = validate("");
        assert!(!result.valid);
    }

    #[test]
    fn test_invalid_json() {
        let result = validate("{invalid}");
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "DCR002"));
    }

    #[test]
    fn test_valid_basic_dcr() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataSources": {},
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "myWorkspace"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Microsoft-Syslog"],
                    "destinations": ["myWorkspace"],
                    "transformKql": "source",
                    "outputStream": "Microsoft-Syslog"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(result.valid);
    }

    #[test]
    fn test_valid_custom_text_log_dcr() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataCollectionEndpointId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Insights/dataCollectionEndpoints/my-dce",
                "streamDeclarations": {
                    "Custom-MyLogFile": {
                        "columns": [
                            { "name": "TimeGenerated", "type": "datetime" },
                            { "name": "RawData", "type": "string" },
                            { "name": "FilePath", "type": "string" },
                            { "name": "Computer", "type": "string" }
                        ]
                    }
                },
                "dataSources": {
                    "logFiles": [{
                        "streams": ["Custom-MyLogFile"],
                        "filePatterns": ["C:\\logs\\*.txt"],
                        "format": "text",
                        "settings": {
                            "text": {
                                "recordStartTimestampFormat": "ISO 8601"
                            }
                        },
                        "name": "myTextLog"
                    }]
                },
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "MyDest"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Custom-MyLogFile"],
                    "destinations": ["MyDest"],
                    "transformKql": "source",
                    "outputStream": "Custom-MyTable_CL"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(result.valid);
    }

    #[test]
    fn test_logfiles_missing_endpoint() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "streamDeclarations": {
                    "Custom-MyLog": {
                        "columns": [
                            { "name": "TimeGenerated", "type": "datetime" },
                            { "name": "RawData", "type": "string" }
                        ]
                    }
                },
                "dataSources": {
                    "logFiles": [{
                        "streams": ["Custom-MyLog"],
                        "filePatterns": ["C:\\logs\\*.txt"],
                        "format": "text",
                        "settings": { "text": { "recordStartTimestampFormat": "ISO 8601" } },
                        "name": "myLog"
                    }]
                },
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "ws"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Custom-MyLog"],
                    "destinations": ["ws"],
                    "transformKql": "source",
                    "outputStream": "Custom-MyTable_CL"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "DCR090"));
    }

    #[test]
    fn test_stream_cross_reference_missing() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataSources": {},
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "ws"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Custom-UndeclaredStream"],
                    "destinations": ["ws"],
                    "transformKql": "source",
                    "outputStream": "Custom-MyTable_CL"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "DCR066"));
    }

    #[test]
    fn test_logfiles_invalid_format() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataCollectionEndpointId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Insights/dataCollectionEndpoints/my-dce",
                "streamDeclarations": {
                    "Custom-MyLog": {
                        "columns": [
                            { "name": "TimeGenerated", "type": "datetime" },
                            { "name": "RawData", "type": "string" }
                        ]
                    }
                },
                "dataSources": {
                    "logFiles": [{
                        "streams": ["Custom-MyLog"],
                        "filePatterns": ["C:\\logs\\*.csv"],
                        "format": "csv",
                        "name": "myLog"
                    }]
                },
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "ws"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Custom-MyLog"],
                    "destinations": ["ws"],
                    "transformKql": "source",
                    "outputStream": "Custom-MyTable_CL"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "DCR107"));
    }

    #[test]
    fn test_workspace_transforms_dcr() {
        let dcr = r#"{
            "kind": "WorkspaceTransforms",
            "location": "eastus",
            "properties": {
                "dataSources": {},
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "clv2ws1"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Microsoft-Table-LAQueryLogs"],
                    "destinations": ["clv2ws1"],
                    "transformKql": "source | where QueryText !contains 'LAQueryLogs'"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(result.valid);
    }

    #[test]
    fn test_missing_destination() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataSources": {},
                "dataFlows": [{
                    "streams": ["Microsoft-Syslog"],
                    "destinations": ["missingDest"],
                    "transformKql": "source"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(!result.valid);
    }

    #[test]
    fn test_invalid_kql_in_dcr() {
        let dcr = r#"{
            "location": "eastus",
            "properties": {
                "dataSources": {},
                "destinations": {
                    "logAnalytics": [{
                        "workspaceResourceId": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
                        "name": "ws"
                    }]
                },
                "dataFlows": [{
                    "streams": ["Microsoft-Syslog"],
                    "destinations": ["ws"],
                    "transformKql": "source | summarize count() by severity"
                }]
            }
        }"#;
        let result = validate(dcr);
        assert!(!result.valid);
    }
}
