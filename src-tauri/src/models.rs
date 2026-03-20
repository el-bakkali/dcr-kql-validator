use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<ValidationMessage>,
    pub warnings: Vec<ValidationMessage>,
    pub info: Vec<ValidationMessage>,
}

#[derive(Serialize, Clone, Debug)]
pub struct ValidationMessage {
    pub code: String,
    pub message: String,
    pub severity: String,
    pub suggestion: Option<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            valid: true,
            errors: vec![],
            warnings: vec![],
            info: vec![],
        }
    }

    pub fn add_error(&mut self, code: &str, message: &str, suggestion: Option<&str>) {
        self.valid = false;
        self.errors.push(ValidationMessage {
            code: code.to_string(),
            message: message.to_string(),
            severity: "error".to_string(),
            suggestion: suggestion.map(|s| s.to_string()),
        });
    }

    pub fn add_warning(&mut self, code: &str, message: &str, suggestion: Option<&str>) {
        self.warnings.push(ValidationMessage {
            code: code.to_string(),
            message: message.to_string(),
            severity: "warning".to_string(),
            suggestion: suggestion.map(|s| s.to_string()),
        });
    }

    pub fn add_info(&mut self, code: &str, message: &str) {
        self.info.push(ValidationMessage {
            code: code.to_string(),
            message: message.to_string(),
            severity: "info".to_string(),
            suggestion: None,
        });
    }
}
