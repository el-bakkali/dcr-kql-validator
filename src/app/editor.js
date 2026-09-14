/**
 * Lazy Monaco loader.
 *
 * Monaco and the Kusto language service together are the largest thing the app
 * downloads, so nothing here is imported until an editor is actually needed. A
 * plain textarea backs every editor and stays in place if Monaco fails to load
 * or the device is too small to use it. The app must remain usable either way.
 */

let monacoPromise = null;

// Monaco needs room for a gutter, scrollbar and a readable line. Below this the
// plain textarea is genuinely the better editor, on a phone or a narrow window.
const MIN_WIDTH = 700;

function shouldUseMonaco() {
  if (typeof window === "undefined") return false;
  return window.innerWidth >= MIN_WIDTH;
}

async function loadMonaco() {
  if (monacoPromise) return monacoPromise;

  monacoPromise = (async () => {
    const monaco = await import("monaco-editor/esm/vs/editor/editor.api.js");

    // Monaco resolves its workers through this hook rather than bundler imports.
    // Classic workers, so the Kusto scripts can be loaded with importScripts.
    const [{ default: KustoWorker }, { default: JsonWorker }, { default: EditorWorker }] = await Promise.all([
      import("./monaco-kusto.worker.js?worker"),
      import("./monaco-json.worker.js?worker"),
      import("./monaco-editor.worker.js?worker"),
    ]);

    self.MonacoEnvironment = {
      getWorker(_moduleId, label) {
        if (label === "kusto") return new KustoWorker();
        if (label === "json") return new JsonWorker();
        return new EditorWorker();
      },
    };

    // editor.api carries no languages, so both have to be registered.
    const [, { jsonDefaults }] = await Promise.all([
      import("@kusto/monaco-kusto/release/esm/monaco.contribution.js"),
      import("monaco-editor/esm/vs/language/json/monaco.contribution.js"),
    ]);

    // A DCR is strict JSON, and we validate the semantics ourselves.
    jsonDefaults.setDiagnosticsOptions({
      validate: true,
      allowComments: false,
      schemas: [],
      enableSchemaRequest: false,
    });

    monaco.editor.defineTheme("dcr-light", {
      base: "vs",
      inherit: true,
      rules: [],
      colors: {
        "editor.background": "#ffffff",
        "editorGutter.background": "#ffffff",
        "editorLineNumber.foreground": "#a0a8b4",
      },
    });

    return monaco;
  })();

  return monacoPromise;
}

/**
 * Attach a Monaco editor to `host`, mirroring its value into `textarea`.
 * Resolves to null when Monaco is unavailable, leaving the textarea in charge.
 */
export async function createEditor({ host, textarea, language, onChange }) {
  if (!shouldUseMonaco()) return null;

  let monaco;
  try {
    monaco = await loadMonaco();
  } catch (err) {
    console.warn("Monaco unavailable, continuing with the plain editor:", err);
    return null;
  }

  const model = monaco.editor.createModel(textarea.value, language);
  const editor = monaco.editor.create(host, {
    model,
    theme: "dcr-light",
    automaticLayout: true,
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    fontSize: 13,
    lineNumbersMinChars: 3,
    padding: { top: 12, bottom: 12 },
    renderLineHighlight: "line",
    tabSize: 2,
    fixedOverflowWidgets: true,
  });

  model.onDidChangeContent(() => {
    textarea.value = model.getValue();
    onChange?.(model.getValue());
  });

  host.hidden = false;
  textarea.hidden = true;

  return {
    monaco,
    editor,
    model,
    getValue: () => model.getValue(),
    setValue: (v) => model.setValue(v),

    /** Paint validation findings as squiggles on the offending tokens. */
    setMarkers(messages) {
      const markers = messages
        .filter((m) => m.span && Number.isFinite(m.span.start))
        .map((m) => {
          const start = model.getPositionAt(m.span.start);
          const end = model.getPositionAt(m.span.start + Math.max(m.span.length, 1));
          return {
            startLineNumber: start.lineNumber,
            startColumn: start.column,
            endLineNumber: end.lineNumber,
            endColumn: end.column,
            message: m.suggestion ? `${m.message}\n\n${m.suggestion}` : m.message,
            severity:
              m.severity === "error"
                ? monaco.MarkerSeverity.Error
                : m.severity === "warning"
                  ? monaco.MarkerSeverity.Warning
                  : monaco.MarkerSeverity.Info,
            code: m.code,
          };
        });
      monaco.editor.setModelMarkers(model, "dcr-validator", markers);
    },

    /** Point the language service at the stream schema so completions match it. */
    async setSchema(columns) {
      try {
        const kusto = monaco.languages.kusto;
        if (!kusto?.getKustoWorker) return;
        const workerAccessor = await kusto.getKustoWorker();
        const client = await workerAccessor(model.uri);
        await client.setSchema(buildSchema(columns));
      } catch (err) {
        console.warn("Could not apply the stream schema to the editor:", err);
      }
    },
  };
}

/**
 * Present the stream's columns as a table called `source`, which is the name a
 * transformation uses for its input.
 *
 * monaco-kusto caches the compiled database symbol by name and only rebuilds it
 * when majorVersion increases, so every update has to claim a newer version or
 * the first (empty) schema sticks and no column ever resolves.
 */
let schemaVersion = 0;

function buildSchema(columns) {
  // Stream declarations use the DCR spelling; the language service wants CSL.
  const CSL_TYPE = {
    string: "string",
    int: "int",
    long: "long",
    real: "real",
    double: "real",
    boolean: "bool",
    bool: "bool",
    dynamic: "dynamic",
    datetime: "datetime",
  };

  const cols = (columns ?? [])
    .filter((c) => c?.name)
    .map((c) => ({
      name: String(c.name),
      type: CSL_TYPE[String(c.type ?? "string").toLowerCase()] ?? "dynamic",
    }));

  const database = {
    name: "DcrTransform",
    tables: [{ name: "source", columns: cols, docstring: "The incoming data stream" }],
    functions: [],
    graphs: [],
    entityGroups: [],
    majorVersion: ++schemaVersion,
    minorVersion: 0,
  };

  return {
    clusterType: "Engine",
    cluster: { connectionString: "https://dcr.transform", databases: [database] },
    database,
  };
}
