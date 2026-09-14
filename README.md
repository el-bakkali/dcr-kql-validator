# DCR & KQL Validator

Check Azure Monitor data collection rules and transformation queries before you deploy them.

**[Open the app](https://el-bakkali.github.io/dcr-kql-validator/)**

A broken transformation rarely announces itself. The DCR deploys, the agent reports
healthy, and the data quietly stops arriving. You find out days later when a query
returns nothing. This catches those before they ship.

Everything runs in your browser. Nothing is uploaded, and there is no backend.

## What it checks

### Transformation queries

Transformations only support a subset of KQL, and a query that runs happily in Log
Analytics can still fail once it is embedded in a DCR. The validator parses your query
with the real Kusto engine and then applies the Azure Monitor rules on top.

- Blocked operators such as `summarize`, `join` and `sort`, with the reason each one
  cannot work on a per-record transformation
- Functions outside the documented allowlist, including the ones that are easy to get
  wrong: `replace` rather than `replace_string`, `columnifexists` rather than
  `column_ifexists`, `base64_encodestring` rather than `base64_encode_tostring`
- Column names and types, when a schema is supplied
- A `TimeGenerated` column of type `datetime` in the output
- The 10-column limit on a single `parse` statement
- `parse kind=regex` patterns that do not match the whole input, which silently populate
  nothing in a transformation even though they work in Log Analytics
- Repeated `geo_location` calls, which reach an external service and add latency

### Data collection rules

- Structure and required fields for every `kind`, including `Linux`, `Windows`,
  `Direct`, `AgentSettings`, `PlatformTelemetry` and `WorkspaceTransforms`
- Streams in `dataFlows` cross-referenced against `streamDeclarations`, and destination
  names against the `destinations` section
- Column types in stream declarations, including `guid`, which the Tables API accepts but
  stream declarations do not
- `outputStream` checked against the real table list, so a typo is caught rather than
  deployed
- Tables that do not support DCR transformations
- Multi-stage transformations: processor names, `transform` references, and the rule that
  `transform` and `transformKql` are mutually exclusive on a data flow
- Embedded `transformKql` queries, validated in full

### Output schema matching

This is the one that actually bites. Your query parses, deploys, and runs, then drops
every record because it produced a `string` where the table expects a `real`. The
validator works out the output columns of your query and compares them against the
destination table, so the mismatch shows up now rather than in a support case.

## Schemas

Give the validator a schema and it checks column names and types, and offers completions
that match. Without one you still get syntax and operator checks, just nothing that
depends on knowing your columns.

| Source | Use it for |
|:---|:---|
| Standard Azure Monitor table | The 732 tables that support transformations |
| `streamDeclarations` columns | Custom streams and Logs Ingestion API rules |
| None | Syntax and operator checks only |

Table schemas come from the
[Azure Monitor table reference](https://github.com/MicrosoftDocs/azure-monitor-docs/tree/main/articles/azure-monitor/reference/tables),
and the app links to the reference page for whichever table you pick. Only
transformation-capable tables are listed, since the rest cannot be a destination anyway.

To refresh them after the docs change:

```bash
npm run generate:tables
```

## Editor

The query editor is Monaco running the Kusto language service, the same pairing behind
Azure Data Explorer. Errors are underlined where they occur and completions come from
whichever schema you selected. It loads only when you need it, and a plain text area
takes over if it fails or the screen is too small to use it.

## Development

```bash
npm install
npm run dev
npm test
```

## How it works

Validation happens in two layers, because neither one is enough on its own.

The first is [`@kusto/language-service-next`](https://www.npmjs.com/package/@kusto/language-service-next),
the JavaScript build of [microsoft/Kusto-Query-Language](https://github.com/microsoft/Kusto-Query-Language).
It handles syntax, name resolution and type checking, and it runs in a web worker so the
interface never blocks.

The second is this project's own rules, and it exists because of something that surprised
me while building this. Hand `summarize` to the Kusto engine and it reports nothing at
all. The query is perfectly valid KQL. It just happens to be illegal in a transformation,
and the parser has no idea Azure Monitor exists. Same story for `replace_string`.

So the Azure Monitor rules are applied on top, against the parse tree rather than the raw
text. Scanning for keywords is tempting and wrong: it flags a column named `join`, or the
word `sort` inside a string literal. Walking the tree does not.

Rules follow the Azure Monitor documentation:

- [Supported KQL features in transformations](https://learn.microsoft.com/azure/azure-monitor/data-collection/data-collection-transformations-kql)
- [Structure of a data collection rule](https://learn.microsoft.com/azure/azure-monitor/data-collection/data-collection-rule-structure)
- [Create a transformation](https://learn.microsoft.com/azure/azure-monitor/data-collection/data-collection-transformations-create)

## License

[MIT](LICENSE)
