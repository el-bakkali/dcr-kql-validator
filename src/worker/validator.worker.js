/**
 * Validation runs here so the 600 KB language service never loads on the main
 * thread. Requests are correlated by id because validation is asynchronous and
 * the user can type faster than a parse completes.
 */
import { validateKql } from "../core/kql.js";
import { validateDcr } from "../core/dcr.js";

self.addEventListener("message", async (event) => {
  const { id, kind, payload, columns } = event.data ?? {};
  if (!id) return;

  try {
    const result = kind === "dcr" ? await validateDcr(payload) : await validateKql(payload, columns ?? null);

    self.postMessage({
      id,
      ok: true,
      result: {
        valid: result.valid,
        errors: result.errors,
        warnings: result.warnings,
        info: result.info,
        outputColumns: result.analysis?.outputColumns ?? null,
      },
    });
  } catch (err) {
    self.postMessage({ id, ok: false, error: err?.message ?? String(err) });
  }
});
