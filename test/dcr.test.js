import { describe, it, expect } from "vitest";
import { validateDcr } from "../src/core/dcr.js";

const codes = (r) => r.all.map((m) => m.code);
const errorCodes = (r) => r.errors.map((m) => m.code);

/** Verbatim from data-collection-transformations-create.md. */
const WORKSPACE_TRANSFORM_DCR = {
  kind: "WorkspaceTransforms",
  location: "eastus",
  properties: {
    dataSources: {},
    destinations: {
      logAnalytics: [
        {
          workspaceResourceId:
            "/subscriptions/71b36fb6-4fe4-4664-9a7b-245dc62f2930/resourcegroups/my-resource-group/providers/microsoft.operationalinsights/workspaces/my-workspace",
          name: "MyWorkspace",
        },
      ],
    },
    dataFlows: [
      {
        streams: ["Microsoft-Table-LAQueryLogs"],
        destinations: ["MyWorkspace"],
        transformKql: "source | where QueryText !contains 'LAQueryLogs'",
      },
      {
        streams: ["Microsoft-Table-Event"],
        destinations: ["MyWorkspace"],
        transformKql: "source | project-away ParameterXml",
      },
    ],
  },
};

const DIRECT_DCR = {
  kind: "Direct",
  location: "eastus",
  properties: {
    dataCollectionEndpointId: "/subscriptions/x/resourceGroups/rg/providers/Microsoft.Insights/dataCollectionEndpoints/dce",
    streamDeclarations: {
      "Custom-MyTable": {
        columns: [
          { name: "TimeGenerated", type: "datetime" },
          { name: "Message", type: "string" },
          { name: "AdditionalContext", type: "dynamic" },
        ],
      },
    },
    destinations: {
      logAnalytics: [
        {
          workspaceResourceId:
            "/subscriptions/x/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws",
          name: "MyWorkspace",
        },
      ],
    },
    dataFlows: [
      {
        streams: ["Custom-MyTable"],
        destinations: ["MyWorkspace"],
        transformKql: "source | project TimeGenerated, Message",
        outputStream: "Custom-MyTable_CL",
      },
    ],
  },
};

const json = (o) => JSON.stringify(o);
const clone = (o) => JSON.parse(JSON.stringify(o));

describe("known-good samples from the documentation", () => {
  it("accepts the workspace transformation DCR", async () => {
    const r = await validateDcr(json(WORKSPACE_TRANSFORM_DCR));
    expect(r.errors).toEqual([]);
    expect(r.valid).toBe(true);
  });

  it("accepts a Direct ingestion DCR", async () => {
    const r = await validateDcr(json(DIRECT_DCR));
    expect(r.errors).toEqual([]);
  });
});

describe("input handling", () => {
  it("rejects empty input", async () => {
    expect(errorCodes(await validateDcr(""))).toContain("DCR001");
  });

  it("reports malformed JSON", async () => {
    expect(errorCodes(await validateDcr("{ not json"))).toContain("DCR003");
  });

  it("rejects a JSON array", async () => {
    expect(errorCodes(await validateDcr("[]"))).toContain("DCR004");
  });

  it("unwraps an ARM template resource", async () => {
    const template = {
      $schema: "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
      resources: [{ type: "Microsoft.Insights/dataCollectionRules", ...WORKSPACE_TRANSFORM_DCR }],
    };
    const r = await validateDcr(json(template));
    expect(codes(r)).toContain("DCR005");
    expect(r.errors).toEqual([]);
  });
});

describe("WorkspaceTransforms rules", () => {
  it("requires dataSources to be empty", async () => {
    const d = clone(WORKSPACE_TRANSFORM_DCR);
    d.properties.dataSources = { syslog: [{ name: "s", streams: ["Microsoft-Syslog"] }] };
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR020");
  });

  it("requires exactly one Log Analytics destination", async () => {
    const d = clone(WORKSPACE_TRANSFORM_DCR);
    d.properties.destinations.logAnalytics.push({ name: "Second", workspaceResourceId: "/providers/x" });
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR021");
  });
});

describe("stream declarations", () => {
  it("rejects the guid type, which is unavailable here", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.streamDeclarations["Custom-MyTable"].columns.push({ name: "Id", type: "guid" });
    const r = await validateDcr(json(d));
    const e = r.errors.find((x) => x.code === "DCR058");
    expect(e).toBeTruthy();
    expect(e.suggestion).toContain("string");
  });

  it("rejects an unknown type", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.streamDeclarations["Custom-MyTable"].columns.push({ name: "X", type: "float" });
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR052");
  });

  it("requires the Custom- prefix", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.streamDeclarations["MyTable"] = d.properties.streamDeclarations["Custom-MyTable"];
    delete d.properties.streamDeclarations["Custom-MyTable"];
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR050");
  });

  it("flags duplicate column names", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.streamDeclarations["Custom-MyTable"].columns.push({ name: "Message", type: "string" });
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR057");
  });
});

describe("cross references", () => {
  it("catches an undefined destination", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.dataFlows[0].destinations = ["Nope"];
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR064");
  });

  it("catches an undeclared custom stream", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.dataFlows[0].streams = ["Custom-Missing"];
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR066");
  });

  it("requires the _CL suffix on custom output streams", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.dataFlows[0].outputStream = "Custom-MyTable";
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR080");
  });
});

describe("embedded transformation queries", () => {
  it("reports a blocked operator inside transformKql", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.dataFlows[0].transformKql = "source | summarize c = count() by Message";
    const r = await validateDcr(json(d));
    expect(errorCodes(r).some((c) => c.includes("KQL020"))).toBe(true);
  });

  it("validates transformKql against the declared stream schema", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.dataFlows[0].transformKql = "source | project TimeGenerated, NotDeclared";
    const r = await validateDcr(json(d));
    expect(errorCodes(r).some((c) => c.includes("KQL101"))).toBe(true);
  });
});

describe("multi-stage transformations", () => {
  const multi = {
    kind: "Linux",
    location: "eastus",
    properties: {
      transformations: [
        {
          name: "syslog_filter",
          headerProcessor: { processor: "header.Syslog", configuration: {} },
          processors: [{ processor: "filter.Basic", configuration: { any: [] } }],
        },
      ],
      dataSources: {
        syslog: [{ name: "sys", streams: ["Microsoft-Syslog"], transform: "syslog_filter" }],
      },
      destinations: {
        logAnalytics: [{ name: "ws", workspaceResourceId: "/subscriptions/x/providers/y" }],
      },
      dataFlows: [{ streams: ["Microsoft-Syslog"], destinations: ["ws"] }],
    },
  };

  it("accepts a valid multi-stage DCR", async () => {
    const r = await validateDcr(json(multi));
    expect(r.errors).toEqual([]);
    expect(codes(r)).toContain("DCR121");
  });

  it("rejects transformKql and transform together", async () => {
    const d = clone(multi);
    d.properties.dataFlows[0].transform = "syslog_filter";
    d.properties.dataFlows[0].transformKql = "source | where true";
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR110");
  });

  it("catches a transform reference that does not exist", async () => {
    const d = clone(multi);
    d.properties.dataFlows[0].transform = "missing_transform";
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR111");
  });

  it("treats processor names as case-sensitive", async () => {
    const d = clone(multi);
    d.properties.transformations[0].processors[0].processor = "filter.basic";
    const r = await validateDcr(json(d));
    const e = r.errors.find((x) => x.code === "DCR128");
    expect(e).toBeTruthy();
    expect(e.suggestion).toContain("filter.Basic");
  });

  it("requires a header processor first", async () => {
    const d = clone(multi);
    delete d.properties.transformations[0].headerProcessor;
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR125");
  });
});

describe("destinations", () => {
  it("accepts Azure Data Explorer", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.destinations.azureDataExplorer = [
      { name: "adx", resourceId: "/subscriptions/x", databaseName: "db", ingestionUri: "https://ingest.example" },
    ];
    const r = await validateDcr(json(d));
    expect(r.errors.filter((e) => e.code === "DCR036")).toEqual([]);
  });

  it("reports missing required destination fields", async () => {
    const d = clone(DIRECT_DCR);
    d.properties.destinations.microsoftFabric = [{ name: "fab" }];
    expect(errorCodes(await validateDcr(json(d)))).toContain("DCR033");
  });
});
