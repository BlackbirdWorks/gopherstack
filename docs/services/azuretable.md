# Azure Table Storage

In-memory Azure Table Storage with an Azurite-compatible wire protocol (REST, JSON and OData), a full `$filter` grammar and EDM type fidelity.

## Quick start

```bash
curl -X POST "http://localhost:10002/devstoreaccount1/Tables" \
    -H "Content-Type: application/json" --data '{"TableName":"my-table"}'

curl -X POST "http://localhost:10002/devstoreaccount1/my-table" \
    -H "Content-Type: application/json" \
    --data '{"PartitionKey":"p1","RowKey":"r1","Name":"example"}'

curl "http://localhost:10002/devstoreaccount1/my-table()?\$filter=PartitionKey%20eq%20%27p1%27"
```

Go SDK:

```go
cred, _ := aztables.NewSharedKeyCredential("devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
svc, _ := aztables.NewServiceClientWithSharedKey(
    "http://localhost:10002/devstoreaccount1", cred, nil)
table := svc.NewClient("my-table")
_, _ = table.CreateTable(ctx, nil)
body, _ := json.Marshal(aztables.EDMEntity{
    Entity:     aztables.Entity{PartitionKey: "p1", RowKey: "r1"},
    Properties: map[string]any{"Name": "example"},
})
_, _ = table.AddEntity(ctx, body, nil)
```

The account is path-style. The built-in account is Azurite's `devstoreaccount1` with its published development key.

## Supported operations

| Area | Operations |
|------|------------|
| Account | `ListTables`, `GetServiceProperties` |
| Tables | `CreateTable`, `DeleteTable`, `GetTable`, `SetTableACL`, `GetTableACL` |
| Entities | `InsertEntity`, `GetEntity` (`$select`), `QueryEntities` (`$filter`, `$top`, `$select`), `ReplaceEntity`, `MergeEntity`, `DeleteEntity` |
| Batch | `POST /<account>/$batch`: one `multipart/mixed` changeset, applied atomically to one partition |

- `ReplaceEntity` without `If-Match` is an upsert. `MergeEntity` works through `PATCH` or `X-Http-Method: MERGE`. `DeleteEntity` requires `If-Match`.
- `ListTables` and `QueryEntities` page with continuation headers.
- The `Accept` header's `odata=` level (`nometadata`, `minimalmetadata`, `fullmetadata`) is honoured.

## Configuration

The table service runs on its own TCP port. Startup fails if the port is taken.

| Flag | Env | Default |
|------|-----|---------|
| `--azure-table-port` | `AZURE_TABLE_PORT` | `10002` |

## Differences from Azure

> [!WARNING]
> Signatures are never verified. The `Authorization` header is parsed, and any credentials, or none, are accepted.

- SAS tokens are not parsed. Stored access policies are saved but not enforced.
- `$select` always returns `PartitionKey`, `RowKey` and `Timestamp`, even when not listed.
- `ListTables` ignores `$filter`.
- Set Table Service Properties and Get Table Service Stats are not routed. `GetServiceProperties` returns an empty body.

See also the [service parity audit](../../services/azuretable/PARITY.md) and [AZURE.md](../../AZURE.md).
