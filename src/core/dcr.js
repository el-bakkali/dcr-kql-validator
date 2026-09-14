import { ValidationResult } from "./result.js";
import { validateKql } from "./kql.js";
import { findTable, getTableColumns, streamToTable } from "./tables.js";
import {
  DCR_KINDS,
  STREAM_COLUMN_TYPES,
  TOLERATED_COLUMN_TYPES,
  REJECTED_COLUMN_TYPES,
  DESTINATION_TYPES,
  DATA_SOURCE_TYPES,
  LOG_FILE_FORMATS,
  PROCESSORS,
  MULTISTAGE_MIN_API_VERSION,
} from "../rules/dcr-rules.js";

const MAX_INPUT = 5 * 1024 * 1024;

/**
 * Validate a data collection rule definition, including any embedded
 * transformation queries.
 */
export async function validateDcr(jsonText) {
  const result = new ValidationResult();
  const text = (jsonText ?? "").trim();

  if (!text) {
    return result.addError("DCR001", "The DCR definition is empty", "Paste a DCR JSON definition.");
  }
  if (text.length > MAX_INPUT) {
    return result.addError("DCR002", "Input exceeds the maximum size of 5 MB");
  }

  let doc;
  try {
    doc = JSON.parse(text);
  } catch (err) {
    return result.addError("DCR003", `Invalid JSON: ${err.message}`, "Fix the JSON syntax before validating.");
  }

  if (doc === null || typeof doc !== "object" || Array.isArray(doc)) {
    return result.addError("DCR004", "The DCR must be a JSON object");
  }

  // Accept either a bare resource or an ARM template resource wrapper.
  const root = unwrapArmResource(doc, result);
  const props = root.properties && typeof root.properties === "object" ? root.properties : null;

  checkTopLevel(root, result);
  if (!props) return finish(result);

  const kind = typeof root.kind === "string" ? root.kind : null;
  const ctx = collectContext(props);

  checkKind(kind, root, props, ctx, result);
  checkDestinations(props, ctx, result);
  checkStreamDeclarations(props, result);
  checkDataSources(props, ctx, result);
  checkTransformations(props, ctx, result);
  await checkDataFlows(props, ctx, result);

  return finish(result);
}

function finish(result) {
  if (result.valid) result.addInfo("DCR000", "The DCR structure is valid.");
  return result;
}

/** ARM templates wrap the rule in resources[]; validate the inner resource. */
function unwrapArmResource(doc, result) {
  if (Array.isArray(doc.resources) && doc.resources.length) {
    const dcr = doc.resources.find((r) => typeof r?.type === "string" && /dataCollectionRules/i.test(r.type));
    if (dcr) {
      result.addInfo("DCR005", "Validating the dataCollectionRules resource inside the ARM template.");
      return dcr;
    }
  }
  return doc;
}

/** Gather names and streams that later cross-reference checks depend on. */
function collectContext(props) {
  const ctx = {
    destinationNames: new Set(),
    declaredStreams: new Set(),
    transformationNames: new Set(),
    streamColumns: new Map(),
    hasLogFiles: false,
    hasDce: false,
  };

  const dest = props.destinations;
  if (dest && typeof dest === "object") {
    for (const [type, spec] of Object.entries(DESTINATION_TYPES)) {
      const entry = dest[type];
      if (!entry) continue;
      const items = spec.isArray ? (Array.isArray(entry) ? entry : []) : [entry];
      for (const d of items) {
        if (d && typeof d.name === "string") ctx.destinationNames.add(d.name);
      }
    }
  }

  const decls = props.streamDeclarations;
  if (decls && typeof decls === "object") {
    for (const [name, def] of Object.entries(decls)) {
      ctx.declaredStreams.add(name);
      if (Array.isArray(def?.columns)) ctx.streamColumns.set(name, def.columns);
    }
  }

  if (Array.isArray(props.transformations)) {
    for (const t of props.transformations) {
      if (t && typeof t.name === "string") ctx.transformationNames.add(t.name);
    }
  }

  ctx.hasLogFiles = Array.isArray(props.dataSources?.logFiles) && props.dataSources.logFiles.length > 0;
  ctx.hasDce = typeof props.dataCollectionEndpointId === "string";

  return ctx;
}

function checkTopLevel(root, result) {
  if (!("location" in root)) {
    result.addWarning("DCR010", "Missing 'location'", "A DCR must specify an Azure region, for example 'eastus'.");
  }
  if (!("properties" in root)) {
    result.addError("DCR011", "Missing 'properties'", "The DCR must have a 'properties' object.");
  } else if (typeof root.properties !== "object" || root.properties === null || Array.isArray(root.properties)) {
    result.addError("DCR012", "'properties' must be a JSON object");
  }

  if (typeof root.kind === "string" && !(root.kind in DCR_KINDS)) {
    result.addWarning(
      "DCR013",
      `'${root.kind}' is not a recognised DCR kind`,
      `Known kinds: ${Object.keys(DCR_KINDS).join(", ")}.`
    );
  }
}

function checkKind(kind, root, props, ctx, result) {
  if (kind === "WorkspaceTransforms") {
    const ds = props.dataSources;
    if (ds && typeof ds === "object" && Object.keys(ds).length > 0) {
      result.addError(
        "DCR020",
        "A WorkspaceTransforms DCR must have an empty 'dataSources' section",
        "Set \"dataSources\": {}. A workspace transformation DCR has no input stream."
      );
    }

    const la = props.destinations?.logAnalytics;
    if (!Array.isArray(la)) {
      result.addError(
        "DCR022",
        "A WorkspaceTransforms DCR needs a 'logAnalytics' destination",
        "Add exactly one Log Analytics workspace destination. This is the workspace the transformation applies to."
      );
    } else if (la.length !== 1) {
      result.addError(
        "DCR021",
        `A WorkspaceTransforms DCR must have exactly one Log Analytics destination, found ${la.length}`,
        "The workspace transformation DCR applies to a single workspace."
      );
    }

    for (const [i, flow] of asArray(props.dataFlows).entries()) {
      for (const s of asArray(flow?.streams)) {
        if (typeof s === "string" && !s.startsWith("Microsoft-Table-")) {
          result.addWarning(
            "DCR023",
            `Data flow ${i + 1} uses stream '${s}'`,
            "Workspace transformation streams are named 'Microsoft-Table-<TableName>'."
          );
        }
      }
    }
  }

  if (kind === "Direct" && !ctx.hasDce && typeof root.dataCollectionEndpointId !== "string") {
    result.addInfo(
      "DCR024",
      "No 'dataCollectionEndpointId' is present. For DCRs created after March 2024 the ingestion endpoint is generated automatically and exposed under 'endpoints'."
    );
  }
}

function checkDestinations(props, ctx, result) {
  const dest = props.destinations;

  if (dest === undefined) {
    result.addError("DCR031", "Missing 'destinations'", "The DCR must define at least one destination.");
    return;
  }
  if (typeof dest !== "object" || dest === null || Array.isArray(dest)) {
    result.addError("DCR030", "'destinations' must be a JSON object");
    return;
  }

  for (const [type, value] of Object.entries(dest)) {
    const spec = DESTINATION_TYPES[type];
    if (!spec) {
      result.addWarning(
        "DCR036",
        `'${type}' is not a recognised destination type`,
        `Known destinations: ${Object.keys(DESTINATION_TYPES).join(", ")}.`
      );
      continue;
    }

    const items = spec.isArray ? (Array.isArray(value) ? value : null) : [value];
    if (items === null) {
      result.addError("DCR037", `'destinations.${type}' must be an array`);
      continue;
    }

    items.forEach((item, i) => {
      const label = `${type} destination ${i + 1}`;
      if (!item || typeof item !== "object") {
        result.addError("DCR032", `${label} must be a JSON object`);
        return;
      }
      for (const field of spec.required) {
        if (!(field in item)) {
          result.addError(
            "DCR033",
            `${label} is missing '${field}'`,
            field === "name" ? "Each destination needs a unique name so data flows can reference it." : null
          );
        }
      }
      const id = item.workspaceResourceId;
      if (typeof id === "string" && !/\/providers\//i.test(id)) {
        result.addWarning(
          "DCR035",
          `${label} has a workspaceResourceId that doesn't look like an Azure resource ID`,
          "Expected /subscriptions/<id>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<name>."
        );
      }
    });
  }

  if (ctx.destinationNames.size === 0) {
    result.addError("DCR038", "No usable destination was found", "Define at least one destination with a 'name'.");
  }
}

function checkStreamDeclarations(props, result) {
  const decls = props.streamDeclarations;
  if (decls === undefined) return;

  if (typeof decls !== "object" || decls === null || Array.isArray(decls)) {
    result.addError("DCR055", "'streamDeclarations' must be a JSON object keyed by stream name");
    return;
  }

  for (const [name, def] of Object.entries(decls)) {
    if (!name.startsWith("Custom-")) {
      result.addError(
        "DCR050",
        `Stream declaration '${name}' must begin with 'Custom-'`,
        "Every declared stream name starts with 'Custom-'."
      );
    }

    const columns = def?.columns;
    if (!Array.isArray(columns)) {
      result.addError("DCR054", `Stream '${name}' must have a 'columns' array`);
      continue;
    }

    const seen = new Set();
    let hasTimeGenerated = false;

    columns.forEach((col, i) => {
      const label = `Column ${i + 1} in stream '${name}'`;
      if (!col || typeof col !== "object") {
        result.addError("DCR056", `${label} must be a JSON object`);
        return;
      }

      if (typeof col.name !== "string" || !col.name) {
        result.addError("DCR051", `${label} is missing 'name'`);
      } else {
        if (seen.has(col.name)) result.addError("DCR057", `${label} duplicates the column name '${col.name}'`);
        seen.add(col.name);
        if (col.name === "TimeGenerated") hasTimeGenerated = true;
      }

      if (typeof col.type !== "string" || !col.type) {
        result.addError("DCR053", `${label} is missing 'type'`, `Valid types: ${STREAM_COLUMN_TYPES.join(", ")}.`);
        return;
      }

      const type = col.type.toLowerCase();
      if (REJECTED_COLUMN_TYPES[type]) {
        result.addError("DCR058", `${label} uses the type '${col.type}'`, REJECTED_COLUMN_TYPES[type]);
      } else if (TOLERATED_COLUMN_TYPES.includes(type)) {
        result.addWarning(
          "DCR059",
          `${label} uses '${col.type}', which is not in the documented type list`,
          `Documented types: ${STREAM_COLUMN_TYPES.join(", ")}.`
        );
      } else if (!STREAM_COLUMN_TYPES.includes(type)) {
        result.addError("DCR052", `${label} has the invalid type '${col.type}'`, `Valid types: ${STREAM_COLUMN_TYPES.join(", ")}.`);
      }
    });

    if (!hasTimeGenerated) {
      result.addWarning(
        "DCR060",
        `Stream '${name}' does not declare a 'TimeGenerated' column`,
        "Either declare TimeGenerated in the stream or add it in the transformation, since the destination table requires it."
      );
    }
  }
}

function checkDataSources(props, ctx, result) {
  const ds = props.dataSources;
  if (ds === undefined) return;

  if (typeof ds !== "object" || ds === null || Array.isArray(ds)) {
    result.addError("DCR070", "'dataSources' must be a JSON object");
    return;
  }

  for (const [type, value] of Object.entries(ds)) {
    const spec = DATA_SOURCE_TYPES[type];
    if (!spec) {
      result.addWarning(
        "DCR071",
        `'${type}' is not a recognised data source type`,
        `Known types: ${Object.keys(DATA_SOURCE_TYPES).join(", ")}.`
      );
      continue;
    }

    asArray(value).forEach((item, i) => {
      const label = `${type} entry ${i + 1}`;
      if (!item || typeof item !== "object") {
        result.addError("DCR072", `${label} must be a JSON object`);
        return;
      }

      for (const field of spec.required) {
        if (!(field in item)) result.addError("DCR073", `${label} is missing '${field}'`);
      }

      for (const s of asArray(item.streams)) {
        if (typeof s === "string" && s.startsWith("Custom-") && !ctx.declaredStreams.has(s)) {
          result.addError(
            "DCR074",
            `${label} references undeclared stream '${s}'`,
            "Custom streams must be defined in 'streamDeclarations' with their column schema."
          );
        }
      }

      if (typeof item.transform === "string" && !ctx.transformationNames.has(item.transform)) {
        result.addError(
          "DCR075",
          `${label} references transformation '${item.transform}', which is not defined`,
          "The 'transform' value must match a 'name' in the 'transformations' array exactly."
        );
      }

      if (type === "logFiles") checkLogFile(item, label, result);
    });
  }

  if (ctx.hasLogFiles && !ctx.hasDce) {
    result.addWarning(
      "DCR090",
      "A logFiles data source is present but no 'dataCollectionEndpointId' is set",
      "Custom text and JSON log collection normally requires a data collection endpoint."
    );
  }
}

function checkLogFile(item, label, result) {
  if (Array.isArray(item.filePatterns) && item.filePatterns.length === 0) {
    result.addError("DCR105", `${label} has an empty 'filePatterns' array`, "Specify at least one file pattern.");
  }

  if (typeof item.format === "string") {
    if (!LOG_FILE_FORMATS.includes(item.format)) {
      result.addError("DCR107", `${label} has the invalid format '${item.format}'`, "Valid formats are 'text' and 'json'.");
    }
    if (item.format === "text" && item.settings?.text?.recordStartTimestampFormat === undefined) {
      result.addWarning(
        "DCR108",
        `${label} uses format 'text' without settings.text.recordStartTimestampFormat`,
        "Text logs normally need a record start timestamp format, for example 'ISO 8601'."
      );
    }
  }
}

/** Multi-stage transformations: the preview `transformations` section. */
function checkTransformations(props, ctx, result) {
  const transformations = props.transformations;
  if (transformations === undefined) return;

  if (!Array.isArray(transformations)) {
    result.addError("DCR120", "'transformations' must be an array");
    return;
  }

  result.addInfo(
    "DCR121",
    `Multi-stage transformations are in preview and require API version ${MULTISTAGE_MIN_API_VERSION} or later.`
  );

  const names = new Set();

  transformations.forEach((t, i) => {
    const label = `Transformation ${i + 1}`;
    if (!t || typeof t !== "object") {
      result.addError("DCR122", `${label} must be a JSON object`);
      return;
    }

    if (typeof t.name !== "string" || !t.name) {
      result.addError("DCR123", `${label} is missing 'name'`);
    } else {
      if (names.has(t.name)) result.addError("DCR124", `${label} duplicates the transformation name '${t.name}'`);
      names.add(t.name);
    }

    const header = t.headerProcessor;
    if (!header || typeof header !== "object") {
      result.addError(
        "DCR125",
        `${label} is missing 'headerProcessor'`,
        "Every transformation starts with a header processor that schematises the raw data."
      );
    } else {
      checkProcessor(header, `${label} header processor`, true, result);
    }

    asArray(t.processors).forEach((p, j) => {
      checkProcessor(p, `${label} processor ${j + 1}`, false, result);
    });
  });
}

function checkProcessor(p, label, isHeader, result) {
  if (!p || typeof p !== "object") {
    result.addError("DCR126", `${label} must be a JSON object`);
    return;
  }

  const name = p.processor;
  if (typeof name !== "string" || !name) {
    result.addError("DCR127", `${label} is missing 'processor'`);
    return;
  }

  const spec = PROCESSORS[name];
  if (!spec) {
    const match = Object.keys(PROCESSORS).find((k) => k.toLowerCase() === name.toLowerCase());
    result.addError(
      "DCR128",
      `${label} uses the unknown processor '${name}'`,
      match
          ? `Processor names are case-sensitive. Use '${match}'.`
        : `Known processors: ${Object.keys(PROCESSORS).join(", ")}.`
    );
    return;
  }

  const isHeaderProcessor = spec.family === "header";
  if (isHeader && !isHeaderProcessor) {
    result.addError("DCR129", `${label} must be a header processor, but '${name}' is a ${spec.family} processor`);
  }
  if (!isHeader && isHeaderProcessor) {
    result.addError("DCR130", `${label} uses header processor '${name}' outside the header position`, "A header processor must come first.");
  }
  if (p.configuration === undefined) {
    result.addWarning("DCR131", `${label} has no 'configuration' object`);
  }
}

async function checkDataFlows(props, ctx, result) {
  const flows = props.dataFlows;

  if (flows === undefined) {
    result.addError("DCR042", "Missing 'dataFlows'", "The DCR must pair at least one stream with a destination.");
    return;
  }
  if (!Array.isArray(flows)) {
    result.addError("DCR041", "'dataFlows' must be a JSON array");
    return;
  }
  if (flows.length === 0) {
    result.addError("DCR040", "'dataFlows' is empty", "Add at least one data flow.");
    return;
  }

  for (const [i, flow] of flows.entries()) {
    const label = `Data flow ${i + 1}`;

    if (!flow || typeof flow !== "object") {
      result.addError("DCR060", `${label} must be a JSON object`);
      continue;
    }

    const streams = asArray(flow.streams);
    if (!Array.isArray(flow.streams)) {
      result.addError("DCR062", `${label} is missing a 'streams' array`);
    } else if (streams.length === 0) {
      result.addError("DCR061", `${label} has an empty 'streams' array`);
    }

    for (const s of streams) {
      if (typeof s !== "string") continue;
      if (s.startsWith("Custom-")) {
        if (!ctx.declaredStreams.has(s)) {
          result.addError(
            "DCR066",
            `${label} references undeclared custom stream '${s}'`,
            "Custom streams must be defined in 'streamDeclarations'."
          );
        }
      } else if (s.startsWith("Microsoft-")) {
        const table = findTable(streamToTable(s));
        if (!table) {
          result.addWarning(
            "DCR067",
            `${label} references '${s}', which does not match any documented table`,
            "Check the spelling against the Azure Monitor table reference."
          );
        } else if (!table.transform) {
          result.addWarning(
            "DCR069",
            `${label} uses stream '${s}', and the ${table.name} table does not support DCR transformations`,
            "Only tables marked for DCR workspace transformation support can be transformed."
          );
        }
      } else {
        result.addError("DCR068", `${label} stream '${s}' must start with 'Microsoft-' or 'Custom-'`);
      }
    }

    const destinations = asArray(flow.destinations);
    if (!Array.isArray(flow.destinations)) {
      result.addError("DCR065", `${label} is missing a 'destinations' array`);
    } else if (destinations.length === 0) {
      result.addError("DCR063", `${label} has an empty 'destinations' array`);
    }
    for (const d of destinations) {
      if (typeof d === "string" && !ctx.destinationNames.has(d)) {
        result.addError(
          "DCR064",
          `${label} references destination '${d}', which is not defined`,
          "The name must match an entry in the 'destinations' section."
        );
      }
    }

    const hasKql = typeof flow.transformKql === "string" && flow.transformKql.trim() !== "";
    const hasTransform = typeof flow.transform === "string" && flow.transform !== "";

    if (hasKql && hasTransform) {
      result.addError(
        "DCR110",
        `${label} sets both 'transformKql' and 'transform'`,
        "These properties are mutually exclusive per data flow. Use one or the other."
      );
    }

    if (hasTransform && !ctx.transformationNames.has(flow.transform)) {
      result.addError(
        "DCR111",
        `${label} references transformation '${flow.transform}', which is not defined`,
        "The value must match a 'name' in the 'transformations' array exactly."
      );
    }

    if ((hasKql || hasTransform) && streams.length > 1) {
      result.addWarning(
        "DCR112",
        `${label} applies a transformation to ${streams.length} streams`,
        "A data flow that includes a transformation should use a single stream, since the query must match one input schema."
      );
    }

    if (typeof flow.outputStream === "string") {
      const out = flow.outputStream;
      if (out.startsWith("Custom-")) {
        if (!out.endsWith("_CL")) {
          result.addError(
            "DCR080",
            `${label} outputStream '${out}' must end with '_CL'`,
            "Custom table names carry the '_CL' suffix."
          );
        }
      } else if (!out.startsWith("Microsoft-")) {
        result.addWarning(
          "DCR081",
          `${label} outputStream '${out}' should start with 'Microsoft-' or 'Custom-'`
        );
      } else {
        const target = findTable(streamToTable(out));
        if (!target) {
          result.addError(
            "DCR082",
            `${label} outputStream '${out}' does not match any documented table`,
            "Check the spelling against the Azure Monitor table reference."
          );
        } else if ((hasKql || hasTransform) && !target.transform) {
          result.addError(
            "DCR083",
            `${label} transforms into ${target.name}, which does not support DCR transformations`,
            "Send the data to a custom table instead, or remove the transformation."
          );
        }
      }
    }

    if (hasKql) {
      let columns = streams.length === 1 ? ctx.streamColumns.get(streams[0]) ?? null : null;

      // With no stream declaration, fall back to the documented schema so the
      // query still gets column and type checking.
      let inferredFrom = null;
      if (!columns && streams.length === 1) {
        const standard = findTable(streamToTable(streams[0]));
        if (standard) {
          columns = await getTableColumns(standard.name);
          if (columns) inferredFrom = standard.name;
        }
      }

      const kql = await validateKql(flow.transformKql, columns);
      result.merge(kql, "DCR070", `${label} transformKql`);

      if (inferredFrom) {
        result.addInfo("DCR084", `${label} was checked against the documented ${inferredFrom} schema.`);
      }

      await checkOutputSchema(flow, streams, kql, label, result);
    }
  }
}

/**
 * Compare the transformation output against the destination table.
 *
 * Schema mismatch is the most common transformation failure, so this is worth
 * reporting even though extra columns are accepted at ingestion.
 */
async function checkOutputSchema(flow, streams, kql, label, result) {
  const produced = kql.analysis?.outputColumns;
  if (!produced?.length) return;

  const destinationStream =
    typeof flow.outputStream === "string" ? flow.outputStream : streams.length === 1 ? streams[0] : null;

  const target = findTable(streamToTable(destinationStream));
  if (!target) return;

  const targetColumns = await getTableColumns(target.name);
  if (!targetColumns?.length) return;

  const byName = new Map(targetColumns.map((c) => [c.name.toLowerCase(), c]));

  const unknown = produced.filter((c) => !byName.has(c.name.toLowerCase()));
  if (unknown.length) {
    result.addWarning(
      "DCR085",
      `${label} produces ${unknown.length === 1 ? "a column" : "columns"} that ${target.name} does not define: ${unknown.map((c) => c.name).join(", ")}`,
      "Extra columns are accepted but still billed. Remove them unless the table has been extended with matching custom columns."
    );
  }

  const mistyped = [];
  for (const column of produced) {
    const match = byName.get(column.name.toLowerCase());
    if (match && !typesAgree(column.type, match.type)) {
      mistyped.push(`${column.name} is ${column.type}, ${target.name} expects ${match.type}`);
    }
  }
  if (mistyped.length) {
    result.addError(
      "DCR086",
      `${label} output does not match the ${target.name} schema: ${mistyped.join("; ")}`,
      "Convert the column, for example with tostring() or todatetime()."
    );
  }
}

/** Kusto and the table reference spell some types differently. */
function typesAgree(produced, expected) {
  const canonical = (t) => {
    const v = String(t || "").toLowerCase();
    if (v === "boolean") return "bool";
    if (v === "double") return "real";
    if (v === "guid") return "string";
    if (v === "timespan") return "timespan";
    return v;
  };
  const a = canonical(produced);
  const b = canonical(expected);
  if (a === b) return true;
  // Dynamic carries anything, and an unresolved column type tells us nothing.
  return a === "dynamic" || b === "dynamic" || a === "" || b === "";
}

function asArray(v) {
  return Array.isArray(v) ? v : v === undefined || v === null ? [] : [v];
}
