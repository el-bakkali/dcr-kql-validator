import "./styles.css";
import { validateKqlAsync, validateDcrAsync, warmUp } from "./app/validator-client.js";
import { renderResults, renderPlaceholder, renderError } from "./app/results.js";
import { createEditor } from "./app/editor.js";
import { KQL_SAMPLE, DCR_SAMPLE, SAMPLE_COLUMNS } from "./app/samples.js";

const editors = {};
globalThis.__editors = editors;

setupTabs();
setupBanner();
setupKql();
setupDcr();
warmUp();

function setupBanner() {
  const banner = document.getElementById("welcome-banner");
  if (!banner) return;

  if (localStorage.getItem("welcome-dismissed") === "1") {
    banner.hidden = true;
  }

  document.getElementById("welcome-dismiss")?.addEventListener("click", () => {
    banner.hidden = true;
    localStorage.setItem("welcome-dismissed", "1");
  });

  document.getElementById("banner-sample")?.addEventListener("click", () => {
    const active = document.querySelector(".tab.active")?.dataset.tab ?? "kql";
    document.getElementById(`${active}-sample`)?.click();
  });
}

function setupTabs() {
  const tabs = [...document.querySelectorAll(".tab")];
  const panels = [...document.querySelectorAll(".panel")];

  for (const tab of tabs) {
    tab.addEventListener("click", () => {
      for (const t of tabs) {
        const active = t === tab;
        t.classList.toggle("active", active);
        t.setAttribute("aria-selected", String(active));
      }
      for (const p of panels) p.classList.toggle("active", p.id === `panel-${tab.dataset.tab}`);
      editors[tab.dataset.tab]?.editor?.layout?.();
    });
  }
}

function setupKql() {
  const input = document.getElementById("kql-input");
  const host = document.getElementById("kql-monaco");
  const results = document.getElementById("kql-results");
  const status = document.getElementById("kql-status");
  const schemaSelect = document.getElementById("kql-schema-source");
  const schemaPanel = document.getElementById("kql-schema-panel");
  const schemaInput = document.getElementById("kql-schema");
  const tableField = document.getElementById("kql-table-field");
  const tableInput = document.getElementById("kql-table");
  const tableList = document.getElementById("kql-table-list");
  const sourceHint = document.getElementById("kql-source-hint");

  // The table reference is only worth downloading if the user asks for it.
  let tablesModule = null;
  const loadTables = async () => (tablesModule ??= await import("./core/tables.js"));

  renderPlaceholder(results, "Enter a transformation query and select Validate.");

  schemaSelect.addEventListener("change", async () => {
    const mode = schemaSelect.value;
    schemaPanel.hidden = mode !== "custom";
    tableField.hidden = mode !== "table";

    if (mode === "table" && !tableList.childElementCount) {
      const { listTransformableTables } = await loadTables();
      tableList.append(
        ...listTransformableTables().map((name) => {
          const option = document.createElement("option");
          option.value = name;
          return option;
        }),
      );
    }
    applySchema();
  });

  schemaInput.addEventListener("change", applySchema);
  tableInput.addEventListener("change", applySchema);

  createEditor({ host, textarea: input, language: "kusto" }).then((e) => {
    if (e) {
      editors.kql = e;
      applySchema();
    }
  });

  document.getElementById("kql-sample").addEventListener("click", () => {
    setValue("kql", input, KQL_SAMPLE);
    schemaSelect.value = "custom";
    schemaPanel.hidden = false;
    tableField.hidden = true;
    schemaInput.value = JSON.stringify(SAMPLE_COLUMNS, null, 2);
    applySchema();
  });

  document.getElementById("kql-clear").addEventListener("click", () => {
    setValue("kql", input, "");
    editors.kql?.setMarkers([]);
    renderPlaceholder(results, "Enter a transformation query and select Validate.");
    status.textContent = "";
  });

  document.getElementById("kql-validate").addEventListener("click", async () => {
    const query = getValue("kql", input);
    const columns = await readColumns();

    status.textContent = "Validating\u2026";
    try {
      const result = await validateKqlAsync(query, columns);
      renderResults(results, result, {
        context: columns ? null : "No stream schema supplied, so column names and types were not checked.",
      });
      editors.kql?.setMarkers([...result.errors, ...result.warnings]);
      status.textContent = "";
    } catch (err) {
      renderError(results, err.message);
      status.textContent = "";
    }
  });

  async function readColumns() {
    if (schemaSelect.value === "custom") {
      const raw = schemaInput.value.trim();
      if (!raw) return null;
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
        if (Array.isArray(parsed.columns)) return parsed.columns;
        return null;
      } catch {
        return null;
      }
    }

    if (schemaSelect.value === "table") {
      const name = tableInput.value.trim();
      if (!name) return null;
      const { getTableColumns } = await loadTables();
      return await getTableColumns(name);
    }

    return null;
  }

  async function applySchema() {
    const columns = await readColumns();

    if (schemaSelect.value === "table") {
      const name = tableInput.value.trim();
      tableInput.classList.toggle("is-invalid", Boolean(name) && !columns);

      if (columns) {
        const { findTable, tableDocUrl } = await loadTables();
        const table = findTable(name);
        sourceHint.replaceChildren(
          document.createTextNode(`source is the ${table.name} schema, ${columns.length} columns`),
        );

        const url = tableDocUrl(name);
        if (url) {
          const link = document.createElement("a");
          link.className = "hint-link";
          link.href = url;
          link.target = "_blank";
          link.rel = "noopener";
          link.textContent = "Schema reference";
          sourceHint.append(document.createTextNode(" \u00b7 "), link);
        }
      } else {
        sourceHint.textContent = "Transformation query starting with source";
      }
    } else {
      tableInput.classList.remove("is-invalid");
      sourceHint.innerHTML = "Transformation query starting with <code>source</code>";
    }

    editors.kql?.setSchema(columns ?? []);
  }
}

function setupDcr() {
  const input = document.getElementById("dcr-input");
  const host = document.getElementById("dcr-monaco");
  const results = document.getElementById("dcr-results");
  const status = document.getElementById("dcr-status");

  renderPlaceholder(results, "Paste a data collection rule and select Validate.");

  createEditor({ host, textarea: input, language: "json" }).then((e) => {
    if (e) editors.dcr = e;
  });

  document.getElementById("dcr-sample").addEventListener("click", () => setValue("dcr", input, DCR_SAMPLE));

  document.getElementById("dcr-clear").addEventListener("click", () => {
    setValue("dcr", input, "");
    renderPlaceholder(results, "Paste a data collection rule and select Validate.");
    status.textContent = "";
  });

  document.getElementById("dcr-validate").addEventListener("click", async () => {
    status.textContent = "Validating\u2026";
    try {
      const result = await validateDcrAsync(getValue("dcr", input));
      renderResults(results, result);
      status.textContent = "";
    } catch (err) {
      renderError(results, err.message);
      status.textContent = "";
    }
  });
}

function getValue(key, textarea) {
  return editors[key]?.getValue() ?? textarea.value;
}

function setValue(key, textarea, value) {
  textarea.value = value;
  editors[key]?.setValue(value);
}
