/** Samples drawn from the Azure Monitor documentation. */

export const SAMPLE_COLUMNS = [
  { name: "TimeGenerated", type: "datetime" },
  { name: "Message", type: "string" },
  { name: "SeverityLevel", type: "string" },
  { name: "AdditionalContext", type: "dynamic" },
];

export const KQL_SAMPLE = `source
| where SeverityLevel != 'info'
| extend Context = parse_json(AdditionalContext)
| extend DeviceId = tostring(Context.DeviceID)
| project
    TimeGenerated,
    Message,
    SeverityLevel,
    DeviceId`;

export const DCR_SAMPLE = JSON.stringify(
  {
    kind: "Direct",
    location: "eastus",
    properties: {
      streamDeclarations: {
        "Custom-MyApp": {
          columns: [
            { name: "TimeGenerated", type: "datetime" },
            { name: "Message", type: "string" },
            { name: "SeverityLevel", type: "string" },
            { name: "AdditionalContext", type: "dynamic" },
          ],
        },
      },
      destinations: {
        logAnalytics: [
          {
            workspaceResourceId:
              "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/my-rg/providers/Microsoft.OperationalInsights/workspaces/my-workspace",
            name: "MyWorkspace",
          },
        ],
      },
      dataFlows: [
        {
          streams: ["Custom-MyApp"],
          destinations: ["MyWorkspace"],
          transformKql: "source | where SeverityLevel != 'info' | project TimeGenerated, Message, SeverityLevel",
          outputStream: "Custom-MyApp_CL",
        },
      ],
    },
  },
  null,
  2
);
