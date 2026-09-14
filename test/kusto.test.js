import { describe, it, expect } from "vitest";
import { analyzeQuery, loadKusto } from "../src/core/kusto.js";

const COLUMNS = [
  { name: "TimeGenerated", type: "datetime" },
  { name: "Message", type: "string" },
  { name: "AdditionalContext", type: "dynamic" },
];

describe("kusto language service", () => {
  it("loads", async () => {
    const { K } = await loadKusto();
    expect(K.KustoCode).toBeTruthy();
  });

  it("accepts a valid schema-aware query", async () => {
    const r = await analyzeQuery("source | where Message has 'error' | project TimeGenerated, Message", COLUMNS);
    expect(r.diagnostics).toEqual([]);
  });

  it("reports unknown columns with a span", async () => {
    const r = await analyzeQuery("source | where NotAColumn == 1", COLUMNS);
    expect(r.diagnostics.length).toBeGreaterThan(0);
    expect(r.diagnostics[0].message).toMatch(/does not refer to any known column/i);
    expect(r.diagnostics[0].length).toBe("NotAColumn".length);
  });

  it("reports type errors", async () => {
    const r = await analyzeQuery("source | where Message > 5", COLUMNS);
    expect(r.diagnostics.some((d) => /not defined for the operand types/i.test(d.message))).toBe(true);
  });

  it("tolerates unknown columns when no schema is supplied", async () => {
    const r = await analyzeQuery("source | where Anything == 1", null);
    expect(r.hasSchema).toBe(false);
    expect(r.diagnostics).toEqual([]);
  });

  it("identifies operators from the AST", async () => {
    const r = await analyzeQuery("source | where Message == 'x' | summarize c = count() by Message", COLUMNS);
    const nodes = r.operators.map((o) => o.node);
    expect(nodes).toContain("FilterOperator");
    expect(nodes).toContain("SummarizeOperator");
  });

  it("extracts output columns", async () => {
    const r = await analyzeQuery("source | project TimeGenerated, Msg = Message", COLUMNS);
    expect(r.outputColumns.map((c) => c.name)).toEqual(["TimeGenerated", "Msg"]);
  });
});
