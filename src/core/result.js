/** Severity-bucketed findings, shared by both validators. */
export class ValidationResult {
  constructor() {
    this.valid = true;
    this.errors = [];
    this.warnings = [];
    this.info = [];
  }

  addError(code, message, suggestion = null, span = null) {
    this.valid = false;
    this.errors.push({ code, message, severity: "error", suggestion, span });
    return this;
  }

  addWarning(code, message, suggestion = null, span = null) {
    this.warnings.push({ code, message, severity: "warning", suggestion, span });
    return this;
  }

  addInfo(code, message, suggestion = null, span = null) {
    this.info.push({ code, message, severity: "info", suggestion, span });
    return this;
  }

  /** Merge another result in, prefixing codes and messages for context. */
  merge(other, codePrefix, messagePrefix) {
    for (const e of other.errors) {
      this.addError(`${codePrefix}-${e.code}`, `${messagePrefix}: ${e.message}`, e.suggestion, e.span);
    }
    for (const w of other.warnings) {
      this.addWarning(`${codePrefix}-${w.code}`, `${messagePrefix}: ${w.message}`, w.suggestion, w.span);
    }
    return this;
  }

  get all() {
    return [...this.errors, ...this.warnings, ...this.info];
  }

  get counts() {
    return { errors: this.errors.length, warnings: this.warnings.length, info: this.info.length };
  }
}
