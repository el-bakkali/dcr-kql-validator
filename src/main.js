import { invoke } from "@tauri-apps/api/core";

// ── Tab switching ──
document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById(`tab-${tab.dataset.tab}`).classList.add("active");
  });
});

// ── KQL Validator ──
document.getElementById("kql-validate").addEventListener("click", async () => {
  const query = document.getElementById("kql-input").value;
  const btn = document.getElementById("kql-validate");
  btn.classList.add("loading");
  btn.textContent = "Validating...";
  try {
    const result = await invoke("validate_kql", { query });
    renderResults("kql-results", result);
  } catch (err) {
    renderError("kql-results", err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = "Validate";
  }
});

document.getElementById("kql-clear").addEventListener("click", () => {
  document.getElementById("kql-input").value = "";
  document.getElementById("kql-results").innerHTML = "";
});

// ── DCR Validator ──
document.getElementById("dcr-validate").addEventListener("click", async () => {
  const json = document.getElementById("dcr-input").value;
  const btn = document.getElementById("dcr-validate");
  btn.classList.add("loading");
  btn.textContent = "Validating...";
  try {
    const result = await invoke("validate_dcr", { json });
    renderResults("dcr-results", result);
  } catch (err) {
    renderError("dcr-results", err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = "Validate";
  }
});

document.getElementById("dcr-clear").addEventListener("click", () => {
  document.getElementById("dcr-input").value = "";
  document.getElementById("dcr-results").innerHTML = "";
});

// ── Render results ──
function renderResults(containerId, result) {
  const container = document.getElementById(containerId);
  let html = "";

  // Header
  const errorCount = result.errors.length;
  const warnCount = result.warnings.length;
  const infoCount = result.info.length;

  if (result.valid) {
    html += `<div class="result-header valid">&#10003; Validation Passed`;
    if (warnCount > 0) html += ` <span class="badge badge-warning">${warnCount} warning${warnCount > 1 ? "s" : ""}</span>`;
    if (infoCount > 0) html += ` <span class="badge badge-info">${infoCount} info</span>`;
    html += `</div>`;
  } else {
    html += `<div class="result-header invalid">&#10007; Validation Failed`;
    html += ` <span class="badge badge-error">${errorCount} error${errorCount > 1 ? "s" : ""}</span>`;
    if (warnCount > 0) html += ` <span class="badge badge-warning">${warnCount} warning${warnCount > 1 ? "s" : ""}</span>`;
    html += `</div>`;
  }

  // Errors
  for (const err of result.errors) {
    html += renderItem("error", "E", err);
  }

  // Warnings
  for (const warn of result.warnings) {
    html += renderItem("warning", "W", warn);
  }

  // Info
  for (const info of result.info) {
    html += renderItem("info", "i", info);
  }

  container.innerHTML = html;
}

function renderItem(severity, icon, item) {
  let html = `<div class="result-item">`;
  html += `<div class="result-icon ${severity}">${icon}</div>`;
  html += `<div class="result-body">`;
  html += `<div class="result-code">${escapeHtml(item.code)}</div>`;
  html += `<div class="result-message">${escapeHtml(item.message)}</div>`;
  if (item.suggestion) {
    html += `<div class="result-suggestion">${escapeHtml(item.suggestion)}</div>`;
  }
  html += `</div></div>`;
  return html;
}

function renderError(containerId, err) {
  const container = document.getElementById(containerId);
  container.innerHTML = `<div class="result-header invalid">&#10007; Internal Error</div>
    <div class="result-item">
      <div class="result-icon error">E</div>
      <div class="result-body">
        <div class="result-message">${escapeHtml(String(err))}</div>
      </div>
    </div>`;
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// ── Sample data ──
const KQL_SAMPLES = {
  "valid-filter": `source
| where severity == "Critical"
| project TimeGenerated, Message, severity`,

  "valid-transform": `source
| extend Properties = parse_json(properties)
| extend Level = toint(Properties.Level)
| extend DeviceId = tostring(Properties.DeviceID)
| project TimeGenerated, Message, Level, DeviceId`,

  "valid-project": `source
| where EventLevelName in ('Error', 'Critical', 'Warning')
| project-away ParameterXml`,

  "invalid-summarize": `source
| summarize count() by severity
| project TimeGenerated, severity, count_`,

  "invalid-join": `source
| join kind=inner (OtherTable) on CommonKey
| project TimeGenerated, Message`,

  "invalid-start": `Syslog
| where SeverityLevel != "info"
| project TimeGenerated, Message`,

  "invalid-function": `source
| extend col = column_ifexists('MyCol', '')
| project TimeGenerated, col`,
};

const DCR_SAMPLES = {
  "valid-syslog": JSON.stringify({
    location: "eastus",
    properties: {
      dataSources: {
        syslog: [{
          name: "syslogBase",
          streams: ["Microsoft-Syslog"],
          facilityNames: ["daemon", "syslog"],
          logLevels: ["Warning", "Error", "Critical", "Alert", "Emergency"]
        }]
      },
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
          name: "centralWorkspace"
        }]
      },
      dataFlows: [{
        streams: ["Microsoft-Syslog"],
        destinations: ["centralWorkspace"],
        transformKql: "source | where SeverityLevel != 'info'",
        outputStream: "Microsoft-Syslog"
      }]
    }
  }, null, 2),

  "valid-workspace": JSON.stringify({
    kind: "WorkspaceTransforms",
    location: "eastus",
    properties: {
      dataSources: {},
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
          name: "clv2ws1"
        }]
      },
      dataFlows: [
        {
          streams: ["Microsoft-Table-LAQueryLogs"],
          destinations: ["clv2ws1"],
          transformKql: "source | where QueryText !contains 'LAQueryLogs'"
        },
        {
          streams: ["Microsoft-Table-Event"],
          destinations: ["clv2ws1"],
          transformKql: "source | where EventLevelName in ('Error', 'Critical', 'Warning') | project-away ParameterXml"
        }
      ]
    }
  }, null, 2),

  "valid-custom": JSON.stringify({
    location: "eastus",
    kind: "Direct",
    properties: {
      streamDeclarations: {
        "Custom-MyTable": {
          columns: [
            { name: "Time", type: "datetime" },
            { name: "Computer", type: "string" },
            { name: "AdditionalContext", type: "string" }
          ]
        }
      },
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
          name: "LogAnalyticsDest"
        }]
      },
      dataFlows: [{
        streams: ["Custom-MyTable"],
        destinations: ["LogAnalyticsDest"],
        transformKql: "source | extend jsonContext = parse_json(AdditionalContext) | project TimeGenerated = Time, Computer, AdditionalContext = jsonContext, ExtendedColumn = tostring(jsonContext.CounterName)",
        outputStream: "Custom-MyTable_CL"
      }]
    }
  }, null, 2),

  "invalid-missing": JSON.stringify({
    location: "eastus",
    properties: {
      dataSources: {},
      dataFlows: [{
        streams: ["Microsoft-Syslog"],
        destinations: ["missingDest"],
        transformKql: "source"
      }]
    }
  }, null, 2),

  "invalid-kql": JSON.stringify({
    location: "eastus",
    properties: {
      dataSources: {},
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
          name: "ws"
        }]
      },
      dataFlows: [{
        streams: ["Microsoft-Syslog"],
        destinations: ["ws"],
        transformKql: "source | summarize count() by SeverityLevel",
        outputStream: "Microsoft-Syslog"
      }]
    }
  }, null, 2),

  "valid-textlog": JSON.stringify({
    location: "eastus",
    properties: {
      dataCollectionEndpointId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.Insights/dataCollectionEndpoints/my-dce",
      streamDeclarations: {
        "Custom-MyLogFileFormat": {
          columns: [
            { name: "TimeGenerated", type: "datetime" },
            { name: "RawData", type: "string" },
            { name: "FilePath", type: "string" },
            { name: "Computer", type: "string" }
          ]
        }
      },
      dataSources: {
        logFiles: [{
          streams: ["Custom-MyLogFileFormat"],
          filePatterns: ["C:\\logs\\*.txt"],
          format: "text",
          settings: {
            text: {
              recordStartTimestampFormat: "ISO 8601"
            }
          },
          name: "myLogFileFormat-Windows"
        }]
      },
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
          name: "MyDestination"
        }]
      },
      dataFlows: [{
        streams: ["Custom-MyLogFileFormat"],
        destinations: ["MyDestination"],
        transformKql: "source | project d = split(RawData,\",\") | project TimeGenerated=todatetime(d[0]), Code=toint(d[1]), Severity=tostring(d[2]), Module=tostring(d[3]), Message=tostring(d[4])",
        outputStream: "Custom-MyTable_CL"
      }]
    }
  }, null, 2),

  "valid-jsonlog": JSON.stringify({
    location: "eastus",
    properties: {
      dataCollectionEndpointId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.Insights/dataCollectionEndpoints/my-dce",
      streamDeclarations: {
        "Custom-Json-stream": {
          columns: [
            { name: "TimeGenerated", type: "datetime" },
            { name: "FilePath", type: "string" },
            { name: "Code", type: "int" },
            { name: "Module", type: "string" },
            { name: "Message", type: "string" }
          ]
        }
      },
      dataSources: {
        logFiles: [{
          streams: ["Custom-Json-stream"],
          filePatterns: ["C:\\logs\\*.json"],
          format: "json",
          name: "MyJsonFile"
        }]
      },
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
          name: "MyDestination"
        }]
      },
      dataFlows: [{
        streams: ["Custom-Json-stream"],
        destinations: ["MyDestination"],
        transformKql: "source",
        outputStream: "Custom-MyTable_CL"
      }]
    }
  }, null, 2),

  "invalid-stream": JSON.stringify({
    location: "eastus",
    properties: {
      dataSources: {},
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
          name: "ws"
        }]
      },
      dataFlows: [{
        streams: ["Custom-UndeclaredStream"],
        destinations: ["ws"],
        transformKql: "source",
        outputStream: "Custom-MyTable_CL"
      }]
    }
  }, null, 2),

  "invalid-logfile": JSON.stringify({
    location: "eastus",
    properties: {
      streamDeclarations: {
        "Custom-MyLog": {
          columns: [
            { name: "TimeGenerated", type: "datetime" },
            { name: "RawData", type: "string" }
          ]
        }
      },
      dataSources: {
        logFiles: [{
          streams: ["Custom-MyLog"],
          filePatterns: ["C:\\logs\\*.txt"],
          format: "text",
          name: "myLog"
        }]
      },
      destinations: {
        logAnalytics: [{
          workspaceResourceId: "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
          name: "ws"
        }]
      },
      dataFlows: [{
        streams: ["Custom-MyLog"],
        destinations: ["ws"],
        transformKql: "source",
        outputStream: "Custom-MyTable_CL"
      }]
    }
  }, null, 2),
};

// ── Sample loaders ──
document.getElementById("kql-samples").addEventListener("change", (e) => {
  const key = e.target.value;
  if (key && KQL_SAMPLES[key]) {
    document.getElementById("kql-input").value = KQL_SAMPLES[key];
    document.getElementById("kql-results").innerHTML = "";
  }
  e.target.value = "";
});

document.getElementById("dcr-samples").addEventListener("change", (e) => {
  const key = e.target.value;
  if (key && DCR_SAMPLES[key]) {
    document.getElementById("dcr-input").value = DCR_SAMPLES[key];
    document.getElementById("dcr-results").innerHTML = "";
  }
  e.target.value = "";
});

// ── Keyboard shortcut: Ctrl+Enter to validate ──
document.getElementById("kql-input").addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    document.getElementById("kql-validate").click();
  }
});

document.getElementById("dcr-input").addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    document.getElementById("dcr-validate").click();
  }
});
