import { defineConfig } from "vite";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Resolve from this file, not the cwd, so CI and local builds agree.
const root = dirname(fileURLToPath(import.meta.url));

// monaco-kusto imports monaco with extensionless specifiers, which don't resolve
// against monaco-editor's exports map. Map them onto the real files.
const monacoApi = resolve(root, "node_modules/monaco-editor/esm/vs/editor/editor.api.js");
const monacoWorker = resolve(root, "node_modules/monaco-editor/esm/vs/editor/editor.worker.js");

// The Kusto scripts are classic scripts, not modules. See the plugin below.

/**
 * The Kusto language service ships classic scripts. Each ends with
 *
 *   (function (n) { ... n.Bridge = d; })(this)
 *
 * which needs top-level `this` to be the global object. In an ES module `this`
 * is undefined, so nothing is ever published and the next script fails with
 * "Bridge is not defined". A Bridge.NET CommonJS branch also reads the bare
 * identifier `global`, which browsers don't have.
 *
 * Wrapping the body in a function invoked with globalThis fixes both, for every
 * consumer at once, including monaco-kusto's own imports.
 */
function kustoClassicScripts() {
  const pattern = /[/\\]@kusto[/\\]language-service(?:-next)?[/\\][^/\\]+\.js$/;

  return {
    name: "kusto-classic-scripts",
    enforce: "pre",
    transform(code, id) {
      if (!pattern.test(id.split("?")[0])) return null;

      return {
        code:
          `if (typeof globalThis.global === "undefined") globalThis.global = globalThis;\n` +
          `(function () {\n${code}\n}).call(globalThis);\n`,
        map: null,
      };
    },
  };
}

export default defineConfig({
  // Served from https://<user>.github.io/dcr-kql-validator/
  base: "/dcr-kql-validator/",

  plugins: [kustoClassicScripts()],

  optimizeDeps: {
    // Only the classic scripts are excluded, so the transform above can see
    // them. monaco-kusto must stay pre-bundled: it depends on CommonJS packages
    // such as xregexp that need converting to ESM.
    exclude: ["@kusto/language-service", "@kusto/language-service-next"],
  },

  resolve: {
    alias: [
      { find: "monaco-editor/esm/vs/editor/editor.api", replacement: monacoApi },
      { find: "monaco-editor/esm/vs/editor/editor.worker", replacement: monacoWorker },
    ],
  },

  worker: {
    format: "es",
  },

  build: {
    target: "es2022",
    sourcemap: false,
    chunkSizeWarningLimit: 4096,
  },

  server: {
    port: 1420,
  },
});
