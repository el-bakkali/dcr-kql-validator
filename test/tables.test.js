import { describe, it, expect } from "vitest";
import { validateDcr } from "../src/core/dcr.js";
import { findTable, getTableColumns, streamToTable } from "../src/core/tables.js";

const codes = (r) => r.all.map((m) => m.code);
const errorCodes = (r) => r.errors.map((m) => m.code);

/** An AMA DCR shaped like the ones the portal produces. */
function amaDcr({ stream = "Microsoft-Perf", outputStream = "Microsoft-Perf", transformKql = "source" } = {}) {
  return JSON.stringify({
    kind: "Linux",
    location: "uksouth",
    properties: {
      dataSources: {
        performanceCounters: [
          {
            streams: [stream],
            samplingFrequencyInSeconds: 60,
            counterSpecifiers: ["Processor(*)\\% Processor Time"],
            name: "perfCounterDataSource60",
          },
        ],
      },
      destinations: {
        logAnalytics: [
          {
            workspaceResourceId:
              "/subscriptions/7f548071-b7cb-4177-baa6-bdeb494ab01c/resourceGroups/AMA-RG/providers/Microsoft.OperationalInsights/workspaces/law-ama",
            workspaceId: "7d764aea-ffd6-463d-89de-a81d62cb09cc",
            name: "la-793377090",
          },
        ],
      },
      dataFlows: [{ streams: [stream], destinations: ["la-793377090"], transformKql, outputStream }],
    },
  });
}

describe("table reference", () => {
  it("maps streams to table names", () => {
    expect(streamToTable("Microsoft-Perf")).toBe("Perf");
    expect(streamToTable("Microsoft-Table-Event")).toBe("Event");
    expect(streamToTable("Custom-MyTable_CL")).toBeNull();
  });

  it("knows which tables support transformations", () => {
    expect(findTable("Perf").transform).toBe(true);
    expect(findTable("Event").transform).toBe(true);
    // Documented as unsupported in the feature matrix.
    expect(findTable("Heartbeat").transform).toBe(false);
  });

  it("looks tables up case-insensitively and returns the documented spelling", () => {
    expect(findTable("commonsecuritylog").name).toBe("CommonSecurityLog");
  });

  it("returns null for tables that do not exist", () => {
    expect(findTable("Eventt")).toBeNull();
    expect(findTable("")).toBeNull();
  });

  it("loads column schemas on demand", async () => {
    const columns = await getTableColumns("Perf");
    const names = columns.map((c) => c.name);
    expect(names).toContain("TimeGenerated");
    expect(names).toContain("CounterValue");
    expect(columns.find((c) => c.name === "CounterValue").type).toBe("real");
  });
});

describe("outputStream is checked against real tables", () => {
  it("accepts a real table", async () => {
    const r = await validateDcr(amaDcr());
    expect(errorCodes(r)).not.toContain("DCR082");
  });

  it("rejects a misspelled table", async () => {
    const r = await validateDcr(amaDcr({ outputStream: "Microsoft-Perff" }));
    expect(errorCodes(r)).toContain("DCR082");
  });

  it("rejects transforming into a table that does not support transformations", async () => {
    const r = await validateDcr(
      amaDcr({ stream: "Microsoft-Heartbeat", outputStream: "Microsoft-Heartbeat", transformKql: "source | where Computer != ''" })
    );
    expect(errorCodes(r)).toContain("DCR083");
  });
});

describe("standard streams are checked without a stream declaration", () => {
  it("reports which schema it used", async () => {
    const r = await validateDcr(amaDcr({ transformKql: "source | where CounterValue > 0" }));
    expect(codes(r)).toContain("DCR084");
  });

  it("catches a column that the table does not have", async () => {
    const r = await validateDcr(amaDcr({ transformKql: "source | where NotAColumn > 0" }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => /NotAColumn/.test(e.message))).toBe(true);
  });

  it("accepts columns the table does have", async () => {
    const r = await validateDcr(amaDcr({ transformKql: "source | where Computer != '' and CounterValue > 0" }));
    expect(r.errors.some((e) => /does not refer to any known column/.test(e.message))).toBe(false);
  });
});

describe("output schema is compared with the destination table", () => {
  it("warns about columns the destination does not define", async () => {
    const r = await validateDcr(
      amaDcr({ transformKql: "source | extend Unexpected = 'x' | project TimeGenerated, Computer, Unexpected" })
    );
    expect(codes(r)).toContain("DCR085");
  });

  it("errors when a column type does not match", async () => {
    // CounterValue is real in the Perf table.
    const r = await validateDcr(
      amaDcr({ transformKql: "source | project TimeGenerated, Computer, CounterValue = 'not a number'" })
    );
    expect(errorCodes(r)).toContain("DCR086");
  });

  it("stays quiet when the output matches", async () => {
    const r = await validateDcr(
      amaDcr({ transformKql: "source | project TimeGenerated, Computer, CounterName, CounterValue" })
    );
    expect(codes(r)).not.toContain("DCR085");
    expect(codes(r)).not.toContain("DCR086");
  });
});
