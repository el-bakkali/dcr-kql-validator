/**
 * Data collection rule structural rules.
 *
 * Source of truth: data-collection-rule-structure.md and
 * data-collection-transformations-create.md in MicrosoftDocs/azure-monitor-docs
 * (ms.date 05/15/2026).
 */

/** DCR kinds and the collection scenario each covers. */
export const DCR_KINDS = {
  Direct: "Direct ingestion using the Logs ingestion API.",
  AgentSettings: "Configure Azure Monitor agent parameters.",
  Linux: "Collect events and performance data from Linux machines.",
  PlatformTelemetry: "Export platform metrics.",
  Windows: "Collect events and performance data from Windows machines.",
  WorkspaceTransforms: "Workspace transformation DCR. Has no input stream.",
  All: "Cross-platform agent DCR.",
};

/**
 * Column types valid in streamDeclarations.
 *
 * `guid` is deliberately absent: the docs state it isn't available here and that
 * GUID values must be declared as `string`.
 */
export const STREAM_COLUMN_TYPES = ["string", "int", "long", "real", "boolean", "dynamic", "datetime"];

/** Types that are real Kusto types but undocumented for streamDeclarations. */
export const TOLERATED_COLUMN_TYPES = ["bool", "double", "timespan"];

/** Explicitly rejected column types, with the documented replacement. */
export const REJECTED_COLUMN_TYPES = {
  guid: "The 'guid' type isn't available in stream declarations. Declare the column as 'string'. The Tables API stores and queries GUIDs as strings.",
  decimal: "The 'decimal' type isn't available in stream declarations. Use 'real'.",
};

/** Destination sections and the parameters each requires. */
export const DESTINATION_TYPES = {
  logAnalytics: { required: ["name", "workspaceResourceId"], isArray: true },
  azureMonitorMetrics: { required: ["name"], isArray: false },
  azureDataExplorer: { required: ["name", "resourceId", "databaseName", "ingestionUri"], isArray: true },
  microsoftFabric: { required: ["name", "tenantId", "databaseName", "ingestionUri"], isArray: true },
  eventHubs: { required: ["name", "eventHubResourceId"], isArray: true },
  eventHubsDirect: { required: ["name", "eventHubResourceId"], isArray: true },
  storageBlobsDirect: { required: ["name", "storageAccountResourceId", "containerName"], isArray: true },
  storageTablesDirect: { required: ["name", "storageAccountResourceId", "tableName"], isArray: true },
  storageAccounts: { required: ["name", "storageAccountResourceId", "containerName"], isArray: true },
};

/** Data source types and the streams each produces. */
export const DATA_SOURCE_TYPES = {
  eventHub: { streams: null, required: ["name", "streams"] },
  iisLogs: { streams: ["Microsoft-W3CIISLog"], required: ["name", "streams"] },
  logFiles: { streams: null, required: ["name", "streams", "filePatterns", "format"] },
  performanceCounters: {
    streams: ["Microsoft-Perf", "Microsoft-InsightsMetrics"],
    required: ["name", "streams", "counterSpecifiers"],
  },
  prometheusForwarder: { streams: ["Microsoft-PrometheusMetrics"], required: ["name", "streams"] },
  syslog: { streams: ["Microsoft-Syslog", "Microsoft-CommonSecurityLog"], required: ["name", "streams"] },
  windowsEventLogs: { streams: ["Microsoft-Event", "Microsoft-SecurityEvent"], required: ["name", "streams"] },
  windowsFirewallLogs: { streams: ["Microsoft-WindowsFirewall"], required: ["name", "streams"] },
  extension: { streams: null, required: ["name", "streams", "extensionName"] },
  platformTelemetry: { streams: null, required: ["name", "streams"] },
  dataImports: { streams: null, required: ["name"] },
};

/** Streams commonly produced by agent data sources. */
export const KNOWN_MICROSOFT_STREAMS = [
  "Microsoft-Event",
  "Microsoft-SecurityEvent",
  "Microsoft-WindowsEvent",
  "Microsoft-Syslog",
  "Microsoft-CommonSecurityLog",
  "Microsoft-Perf",
  "Microsoft-InsightsMetrics",
  "Microsoft-W3CIISLog",
  "Microsoft-PrometheusMetrics",
  "Microsoft-WindowsFirewall",
  "Microsoft-ServiceMap",
  "Microsoft-ProtectionStatus",
  "Microsoft-Heartbeat",
  "Microsoft-ContainerLog",
  "Microsoft-ContainerLogV2",
  "Microsoft-ContainerInventory",
  "Microsoft-KubeEvents",
  "Microsoft-KubePodInventory",
  "Microsoft-KubeNodeInventory",
  "Microsoft-KubeServices",
  "Microsoft-KubePVInventory",
  "Microsoft-InsightsMetrics-Cluster",
  "Microsoft-Table-LAQueryLogs",
];

export const LOG_FILE_FORMATS = ["text", "json"];

/** Multi-stage transformation processors, with the stage each may run in. */
export const PROCESSORS = {
  "header.Syslog": { family: "header", client: true, ingestion: false },
  "header.WindowsEvents": { family: "header", client: true, ingestion: false },
  "header.WindowsPerformanceCounters": { family: "header", client: true, ingestion: false },
  "header.LinuxPerformanceCounters": { family: "header", client: true, ingestion: false },
  "header.TextLog": { family: "header", client: true, ingestion: false },
  "header.IISLog": { family: "header", client: true, ingestion: false },
  "header.WindowsFirewallLog": { family: "header", client: true, ingestion: false },
  "header.StandardStream": { family: "header", client: false, ingestion: true },
  "header.CustomStream": { family: "header", client: false, ingestion: true },
  "filter.Basic": { family: "filter", client: true, ingestion: true },
  "map.Rename": { family: "map", client: true, ingestion: true },
  "map.Drop": { family: "map", client: true, ingestion: true },
  "parse.JsonPath": { family: "parse", client: true, ingestion: true },
  "parse.XmlPath": { family: "parse", client: true, ingestion: true },
  "parse.CEFAttribute": { family: "parse", client: true, ingestion: true },
  "aggregate.Basic": { family: "aggregate", client: true, ingestion: true },
  "enrich.DNSLookup": { family: "enrich", client: true, ingestion: true },
  "transform.KQL": { family: "transform", client: false, ingestion: true },
};

/** Multi-stage transformations require at least this API version. */
export const MULTISTAGE_MIN_API_VERSION = "2025-05-11";
