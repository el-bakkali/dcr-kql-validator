mod dcr_validator;
mod kql_validator;
mod models;

use models::ValidationResult;

/// Maximum input size: 5MB. Prevents memory exhaustion from oversized input.
const MAX_INPUT_SIZE: usize = 5 * 1024 * 1024;

#[tauri::command]
fn validate_kql(query: String) -> ValidationResult {
    if query.len() > MAX_INPUT_SIZE {
        let mut r = ValidationResult::new();
        r.add_error("KQL000", "Input exceeds maximum size (5MB)", Some("Reduce the query size"));
        return r;
    }
    kql_validator::validate(&query)
}

#[tauri::command]
fn validate_dcr(json: String) -> ValidationResult {
    if json.len() > MAX_INPUT_SIZE {
        let mut r = ValidationResult::new();
        r.add_error("DCR000", "Input exceeds maximum size (5MB)", Some("Reduce the JSON size"));
        return r;
    }
    dcr_validator::validate(&json)
}

pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![validate_kql, validate_dcr])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
