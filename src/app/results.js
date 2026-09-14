/** Render validation findings into the results panel. */

const SEVERITY_LABEL = { error: "Error", warning: "Warning", info: "Info" };

export function renderResults(container, result, { context } = {}) {
  if (!result) {
    container.innerHTML = "";
    return;
  }

  const { errors, warnings, info } = result;
  const parts = [];

  parts.push(renderHeader(result));

  if (context) {
    parts.push(`<p class="context">${escapeHtml(context)}</p>`);
  }

  for (const group of [errors, warnings, info]) {
    for (const m of group) parts.push(renderMessage(m));
  }

  container.innerHTML = parts.join("");
}

function renderHeader(result) {
  const { errors, warnings, info } = result;
  const badges = [];

  if (errors.length) badges.push(badge("error", errors.length, "error"));
  if (warnings.length) badges.push(badge("warning", warnings.length, "warning"));
  if (info.length && !errors.length) badges.push(badge("info", info.length, "note"));

  const ok = result.valid;
  return `
    <div class="verdict ${ok ? "ok" : "bad"}">
      <span class="verdict-icon" aria-hidden="true">${ok ? "&#10003;" : "&#10007;"}</span>
      <span class="verdict-text">${ok ? "Validation passed" : "Validation failed"}</span>
      <span class="verdict-badges">${badges.join("")}</span>
    </div>`;
}

function badge(kind, count, noun) {
  const label = count === 1 ? noun : `${noun}s`;
  return `<span class="badge badge-${kind}">${count} ${label}</span>`;
}

function renderMessage(m) {
  const suggestion = m.suggestion ? `<p class="suggestion">${escapeHtml(m.suggestion)}</p>` : "";
  const line = m.span?.line ? `<span class="msg-line">line ${m.span.line}</span>` : "";

  return `
    <div class="msg msg-${m.severity}">
      <div class="msg-head">
        <span class="msg-severity">${SEVERITY_LABEL[m.severity] ?? m.severity}</span>
        <code class="msg-code">${escapeHtml(m.code)}</code>
        ${line}
      </div>
      <p class="msg-text">${escapeHtml(m.message)}</p>
      ${suggestion}
    </div>`;
}

export function renderPlaceholder(container, text) {
  container.innerHTML = `<p class="placeholder">${escapeHtml(text)}</p>`;
}

export function renderError(container, message) {
  container.innerHTML = `<div class="msg msg-error"><p class="msg-text">${escapeHtml(message)}</p></div>`;
}

/** Escape before interpolation. Validation messages echo user input back. */
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[c]);
}
