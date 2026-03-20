# DCR & KQL Transformation Validator

Offline desktop tool for validating Azure Monitor **Data Collection Rules (DCR)** and **KQL transformation queries** before deployment.

Built with Rust + Tauri for maximum security and minimal footprint. No data leaves your machine.

## Features

### KQL Transformation Validator
- Validates queries start with `source` / `print` / `let`
- Detects blocked operators (`summarize`, `join`, `union`, `sort`, `top`, etc.) with clear explanations
- Validates ~90 allowed scalar functions against the Azure Monitor allowlist
- Checks for `TimeGenerated` column in output
- Enforces `parse` column limit (max 10 per statement)
- Catches common mistakes (`column_ifexists` vs `columnifexists`, `base64_encode_tostring` vs `base64_encodestring`)

### DCR JSON Validator
- Validates JSON structure and required fields
- Checks `kind`-specific rules (`WorkspaceTransforms`, `Direct`)
- Cross-references streams in `dataFlows` against `streamDeclarations`
- Cross-references destination names
- Validates `logFiles` data sources (`filePatterns`, `format`, `recordStartTimestampFormat`)
- Validates `dataCollectionEndpointId` requirement for custom log DCRs
- Validates column types in stream declarations
- Validates `outputStream` naming (`Custom-*_CL` suffix)
- Embeds KQL validation for `transformKql` fields

## Security

- **Offline** — zero network calls, no telemetry, no data exfiltration
- **Memory safe** — pure Rust backend, zero `unsafe` blocks
- **Minimal permissions** — Tauri `core:default` only, no file/shell/network access
- **Hardened CSP** — `default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; form-action 'none'`
- **Input bounded** — 5MB max input size enforced at IPC layer
- **Audited** — against OWASP ASVS, NIST CSF, and CIS Controls

## Install

Download the latest signed `.msi` installer from [Releases](../../releases).

## Build from Source

### Prerequisites
- [Rust](https://rustup.rs/) (stable)
- [Node.js](https://nodejs.org/) (v18+)
- [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) (C++ workload)

### Steps
```bash
git clone https://github.com/YOUR_USERNAME/dcr-kql-validator.git
cd dcr-kql-validator
npm install
npm run tauri -- build
```

Release artifacts will be in `src-tauri/target/release/bundle/msi/`.

## Development

```bash
npm install
npm run tauri -- dev
```

## Stack
- **Backend:** Rust + Tauri 2
- **Frontend:** Vite + Vanilla JavaScript
- **Binary size:** ~8 MB exe, ~3 MB MSI
- **Dependencies:** 3 direct Rust crates (tauri, serde, serde_json)

## License

[MIT](LICENSE)
