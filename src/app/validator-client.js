/** Promise-based RPC over the validation worker. */
import ValidatorWorker from "../worker/validator.worker.js?worker";

let worker = null;
let nextId = 1;
const pending = new Map();

function ensureWorker() {
  if (worker) return worker;

  worker = new ValidatorWorker();

  worker.addEventListener("message", (event) => {
    const { id, ok, result, error } = event.data ?? {};
    const entry = pending.get(id);
    if (!entry) return;
    pending.delete(id);
    ok ? entry.resolve(result) : entry.reject(new Error(error));
  });

  worker.addEventListener("error", (event) => {
    for (const { reject } of pending.values()) reject(new Error(event.message || "Validation worker failed"));
    pending.clear();
  });

  return worker;
}

function call(kind, payload, columns) {
  const id = nextId++;
  ensureWorker().postMessage({ id, kind, payload, columns });
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

export const validateKqlAsync = (query, columns = null) => call("kql", query, columns);
export const validateDcrAsync = (jsonText) => call("dcr", jsonText);

/** Start loading the worker ahead of the first click. */
export const warmUp = () => ensureWorker();
