import { describe, it, expect } from "vitest";
import { validateKql } from "../src/core/kql.js";

const COLUMNS = [
  { name: "TimeGenerated", type: "datetime" },
  { name: "Message", type: "string" },
  { name: "SeverityLevel", type: "string" },
  { name: "AdditionalContext", type: "dynamic" },
];

const codes = (r) => r.all.map((m) => m.code);

describe("valid transformations", () => {
  it("accepts the documented basic example", async () => {
    const r = await validateKql(
      "source | where SeverityLevel != 'info' | project TimeGenerated, Message",
      COLUMNS
    );
    expect(r.errors).toEqual([]);
    expect(r.valid).toBe(true);
  });

  it("accepts the dynamic-column example from the docs", async () => {
    const r = await validateKql(
      `source
       | extend parsedAdditionalContext = parse_json(AdditionalContext)
       | extend Level = toint(parsedAdditionalContext.Level)
       | extend DeviceId = tostring(parsedAdditionalContext.DeviceID)
       | project TimeGenerated, Message, Level, DeviceId`,
      COLUMNS
    );
    expect(r.errors).toEqual([]);
  });

  it("accepts a print-rooted query", async () => {
    const r = await validateKql("print x = 2 + 2, y = 5 | extend z = exp2(x) + exp2(y)");
    expect(r.errors.filter((e) => e.code === "KQL010")).toEqual([]);
  });
});

describe("blocked operators", () => {
  it.each([
    ["summarize", "source | summarize c = count() by Message"],
    ["join", "source | join kind=inner (source) on Message"],
    ["sort", "source | sort by TimeGenerated"],
    ["take", "source | take 10"],
    ["distinct", "source | distinct Message"],
    ["mv-expand", "source | mv-expand AdditionalContext"],
    ["union", "source | union source"],
    ["evaluate", "source | evaluate bag_unpack(AdditionalContext)"],
    ["render", "source | render table"],
  ])("rejects %s", async (keyword, query) => {
    const r = await validateKql(query, COLUMNS);
    const blocked = r.errors.find((e) => e.code === "KQL020");
    expect(blocked, `expected ${keyword} to be blocked`).toBeTruthy();
    expect(blocked.message).toContain(keyword);
  });

  it("does not flag a column that merely shares a name with an operator", async () => {
    const r = await validateKql("source | where Message has 'join' | project TimeGenerated, Message", COLUMNS);
    expect(r.errors.filter((e) => e.code === "KQL020")).toEqual([]);
  });
});

describe("function allowlist", () => {
  it("accepts replace(), which the docs mandate", async () => {
    const r = await validateKql(
      "source | extend M = replace(';', ' ', Message) | project TimeGenerated, M",
      COLUMNS
    );
    expect(r.errors).toEqual([]);
  });

  it("rejects replace_string(), the pre-rename name", async () => {
    const r = await validateKql("source | extend M = replace_string(Message, ';', ' ')", COLUMNS);
    const e = r.errors.find((x) => x.code === "KQL030");
    expect(e).toBeTruthy();
    expect(e.suggestion).toContain("replace");
  });

  it.each([
    ["column_ifexists", "source | project columnX = column_ifexists('a', '')"],
    ["base64_encode_tostring", "source | extend B = base64_encode_tostring(Message)"],
    ["base64_decode_tostring", "source | extend B = base64_decode_tostring(Message)"],
    ["todynamic", "source | extend D = todynamic(Message)"],
    ["materialize", "source | extend M = materialize(Message)"],
    ["hash_md5", "source | extend H = hash_md5(Message)"],
  ])("rejects %s", async (name, query) => {
    const r = await validateKql(query, COLUMNS);
    expect(r.errors.some((e) => e.code === "KQL030")).toBe(true);
  });

  it("accepts the transformation-only functions", async () => {
    const r = await validateKql(
      "source | extend G = geo_location('1.0.0.5'), C = parse_cef_dictionary(Message) | project TimeGenerated, G, C",
      COLUMNS
    );
    expect(r.errors.filter((e) => e.code === "KQL030")).toEqual([]);
  });

  it("accepts hash_sha256", async () => {
    const r = await validateKql("source | extend H = hash_sha256(Message) | project TimeGenerated, H", COLUMNS);
    expect(r.errors).toEqual([]);
  });
});

describe("query root", () => {
  it("rejects a Log Analytics table name", async () => {
    const r = await validateKql("Syslog | where SeverityLevel != 'info'");
    const e = r.errors.find((x) => x.code === "KQL010");
    expect(e).toBeTruthy();
    expect(e.suggestion).toContain("source");
  });
});

describe("TimeGenerated", () => {
  it("errors when the projection drops it", async () => {
    const r = await validateKql("source | project Message", COLUMNS);
    expect(codes(r)).toContain("KQL040");
  });

  it("errors when it is not a datetime", async () => {
    const r = await validateKql("source | project TimeGenerated = Message", COLUMNS);
    expect(codes(r)).toContain("KQL041");
  });

  it("accepts a converted timestamp", async () => {
    const r = await validateKql("source | project TimeGenerated = todatetime(Message)", COLUMNS);
    expect(r.errors).toEqual([]);
  });
});

describe("parse operator", () => {
  it("rejects more than 10 columns in one statement", async () => {
    const cols = Array.from({ length: 12 }, (_, i) => `@',' c${i}: string`).join(" ");
    const r = await validateKql(`source | parse Message with ${cols}`, COLUMNS);
    expect(codes(r)).toContain("KQL050");
  });

  it("allows 10 columns", async () => {
    const cols = Array.from({ length: 10 }, (_, i) => `@',' c${i}: string`).join(" ");
    const r = await validateKql(`source | parse Message with ${cols}`, COLUMNS);
    expect(codes(r)).not.toContain("KQL050");
  });

  it("warns that kind=regex must match the whole line", async () => {
    const r = await validateKql("source | parse kind=regex Message with @'^a' x: string", COLUMNS);
    expect(codes(r)).toContain("KQL051");
  });
});

describe("schema awareness", () => {
  it("catches a misspelled column", async () => {
    const r = await validateKql("source | project TimeGenerated, Mesage", COLUMNS);
    expect(codes(r)).toContain("KQL101");
  });

  it("catches a type mismatch", async () => {
    const r = await validateKql("source | where Message > 5", COLUMNS);
    expect(codes(r)).toContain("KQL102");
  });

  it("stays quiet about unknown columns when no schema is given", async () => {
    const r = await validateKql("source | where Whatever == 1 | extend TimeGenerated = now()");
    expect(codes(r)).not.toContain("KQL101");
  });
});

describe("input handling", () => {
  it("rejects empty input", async () => {
    expect(codes(await validateKql(""))).toContain("KQL001");
  });

  it("reports a syntax error", async () => {
    const r = await validateKql("source | where Message ==", COLUMNS);
    expect(r.valid).toBe(false);
  });
});
