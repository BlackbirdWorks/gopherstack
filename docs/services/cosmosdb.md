# Azure Cosmos DB

In-memory Cosmos DB Core (SQL) API with a SQL query engine, plus the Table API on the same port.

## Quick start

```bash
curl "http://localhost:8081/"

curl -X POST "http://localhost:8081/dbs" \
    -H "Content-Type: application/json" --data '{"id":"my-db"}'

curl -X POST "http://localhost:8081/dbs/my-db/colls" \
    -H "Content-Type: application/json" \
    --data '{"id":"my-container","partitionKey":{"paths":["/pk"],"kind":"Hash"}}'

curl -X POST "http://localhost:8081/dbs/my-db/colls/my-container/docs" \
    -H "Content-Type: application/json" \
    --data '{"id":"doc1","pk":"partition1","name":"example"}'
```

Go SDK:

```go
cred, _ := azcosmos.NewKeyCredential(
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
client, _ := azcosmos.NewClientWithKey("http://localhost:8081", cred, nil)
_, _ = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "my-db"}, nil)
db, _ := client.NewDatabase("my-db")
_, _ = db.CreateContainer(ctx, azcosmos.ContainerProperties{
    ID: "my-container",
    PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
}, nil)
```

The default master key is the real Cosmos DB Local Emulator's published key, shown above.

## Supported operations

| Area | Operations |
|------|------------|
| Account | `GetDatabaseAccount` |
| Databases | `CreateDatabase`, `GetDatabase`, `ListDatabases`, `QueryDatabases`, `DeleteDatabase` |
| Containers | `CreateContainer`, `GetContainer`, `ListContainers`, `QueryContainers`, `ReplaceContainer`, `DeleteContainer` |
| Documents | `CreateDocument` (upsert with `x-ms-documentdb-is-upsert: true`), `GetDocument`, `ListDocuments`, `QueryDocuments`, `ReplaceDocument`, `DeleteDocument`, `PatchDocument`, `ExecuteBatch` |
| Change feed, partitions | `ReadChangeFeed`, `ReadPKRanges` |
| Offers | `ListOffers`, `QueryOffers`, `GetOffer`, `ReplaceOffer` |
| Conflicts | `ListConflicts`, `GetConflict`, `DeleteConflict`, `QueryConflicts` |
| Users and permissions | users and permissions CRUD, resource tokens |
| Stored procedures, triggers, UDFs | Create, Get, Replace, Delete, List and Query for each, plus `ExecuteSproc` (JavaScript) |
| Table API | `CreateTable`, `DeleteTable`, `ListTables`, `InsertEntity`, `GetEntity`, `QueryEntities`, `ReplaceEntity`, `MergeEntity`, `DeleteEntity`, and `POST /$batch` (one `multipart/mixed` changeset on one partition, atomic) |

- `GetDocument` and `DeleteDocument` need `x-ms-documentdb-partitionkey`. `ReplaceDocument` is an upsert without `If-Match` and returns `412` on a mismatch.
- Containers take up to three partition-key paths (`kind: MultiHash`). Point operations need every key component. A query's partition key may be a leading prefix.
- A container `defaultTtl` (`-1` means on, no default expiry) and per-item `ttl` expire documents. Expired documents are invisible at once and removed by a background sweeper.
- Lists and queries page with `x-ms-max-item-count` and `x-ms-continuation`.

## SQL support

`SELECT [DISTINCT] [TOP n] (* | VALUE expr | expr [AS name], ...)`, `FROM c [JOIN x IN c.arr]...`, `WHERE`, `GROUP BY`, multi-key `ORDER BY`, `OFFSET n LIMIT n`, `@parameters`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `EXISTS`, `ARRAY` and scalar subqueries, `IN`, `BETWEEN`, `LIKE`, `?:`, `??`, and string, math, type-check, array, date and spatial functions. Full list: [parity audit](../../services/cosmosdb/PARITY.md).

## Configuration

Cosmos DB runs on its own TCP port, as the real emulator does. Startup fails if the port is taken.

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--cosmosdb-port` | `COSMOSDB_PORT` | `8081` | Listener port |
| `--cosmosdb-master-key` | `COSMOSDB_MASTER_KEY` | emulator key | Key checked when auth validation is on |
| `--cosmosdb-validate-auth` | `COSMOSDB_VALIDATE_AUTH` | `false` | Verify master-key signatures on every request |
| `--cosmosdb-enforce-throughput` | `COSMOSDB_ENFORCE_THROUGHPUT` | `false` | Throttle with `429` once a container's provisioned RU/s is spent |
| `--cosmosdb-burst-capacity` | `COSMOSDB_BURST_CAPACITY` | `false` | Let a partition spend up to 300 s of idle RU/s at up to 3000 RU/s. Needs enforce-throughput |
| `--cosmosdb-serverless` | `COSMOSDB_SERVERLESS` | `false` | Reject throughput provisioning and cap containers at 5000 RU/s. Needs enforce-throughput to throttle |
| `--cosmosdb-resource-token-max-seconds` | `COSMOSDB_RESOURCE_TOKEN_MAX_SECONDS` | `86400` | Resource-token lifetime, 600 to 86400 |
| none | `COSMOSDB_TLS=true` | off | Serve HTTPS with an in-memory self-signed certificate |
| none | `COSMOSDB_TLS_HOSTS` | none | Extra certificate hosts (the certificate already covers `localhost`, `127.0.0.1`, `::1`) |

With `COSMOSDB_TLS=true`, point the SDK at `https://localhost:8081` and skip certificate verification.

With validation on, every request must carry a correctly signed `Authorization: type=master&ver=1.0&sig=...` header and an `x-ms-date` (or `Date`) within 15 minutes of server time, or it gets `401`. Table API requests may use SharedKey or SharedKeyLite with the same key.

Throughput offers (`/offers`, the `x-ms-offer-throughput` and `x-ms-cosmos-offer-autopilot-settings` headers) are always served. An overdrawn budget returns `429` with `x-ms-retry-after-ms` and `x-ms-substatus: 3200`.

## Differences from Azure

> [!WARNING]
> Authentication is off by default: any `Authorization` header, or none, is accepted. Use `--cosmosdb-validate-auth` to check signatures.

- Queries the indexing policy cannot serve are rejected unless the request sends `x-ms-documentdb-query-enable-scan`.
- `x-ms-request-charge` is an approximation from payload size and operation. Nothing is throttled unless `--cosmosdb-enforce-throughput` is set.
