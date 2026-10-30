# CosmosDB — Azure Cosmos DB (Core/SQL API)

Gopherstack provides an in-memory Azure Cosmos DB Core/SQL API implementation with databases, containers (TTL, hierarchical partition keys), documents, continuation tokens, request-unit accounting, and a Cosmos SQL engine (joins, subqueries, aggregates, `GROUP BY`, built-in functions) with three-valued-logic evaluation. The Table API is served on the same port.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `GetDatabaseAccount` | Database-account root resource (required by every real Cosmos SDK before any data-plane call) |
| `CreateDatabase` | Create a new database |
| `GetDatabase` | Get a database by id |
| `ListDatabases` | List databases (`x-ms-max-item-count` / `x-ms-continuation` paging) |
| `QueryDatabases` | SQL query over databases (what the SDK query pager sends) |
| `DeleteDatabase` | Delete a database (cascades to its containers/documents) |
| `CreateContainer` | Create a container (1-3 partition-key paths, `defaultTtl`, `indexingPolicy`) |
| `ReplaceContainer` | Update `defaultTtl` / `indexingPolicy` (partition key is immutable) |
| `GetContainer` | Get a container by id |
| `ListContainers` | List containers in a database (paged) |
| `QueryContainers` | SQL query over containers |
| `DeleteContainer` | Delete a container (cascades to its documents) |
| `CreateDocument` | Create a document (also serves as Upsert with `x-ms-documentdb-is-upsert: true`) |
| `QueryDocuments` | Run a Cosmos SQL query (cross-partition, or scoped by `x-ms-documentdb-partitionkey`; paged) |
| `ListDocuments` | Read-feed list of documents (paged; TTL-expired documents hidden) |
| `GetDocument` | Get a document (requires `x-ms-documentdb-partitionkey`) |
| `ReplaceDocument` | Full-body replace (upsert if no `If-Match`; `412` on `If-Match` mismatch) |
| `DeleteDocument` | Delete a document (requires `x-ms-documentdb-partitionkey`) |

## Configuration

Cosmos DB runs on its own dedicated, synchronously-bound TCP port, mirroring the real Cosmos DB Local Emulator's own published default rather than Azurite's port convention (Cosmos is a separate tool, not part of the Azurite family).

| Setting | Default | Flag | Env var |
|---|---|---|---|
| Port | `8081` (real Cosmos DB Local Emulator's default) | `--cosmosdb-port` | `COSMOSDB_PORT` |
| Master key | real emulator's well-known fixed key (below) | `--cosmosdb-master-key` | `COSMOSDB_MASTER_KEY` |
| Validate auth | `false` | `--cosmosdb-validate-auth` | `COSMOSDB_VALIDATE_AUTH` |
| Enforce throughput | `false` | `--cosmosdb-enforce-throughput` | `COSMOSDB_ENFORCE_THROUGHPUT` |
| Serverless | `false` | `--cosmosdb-serverless` | `COSMOSDB_SERVERLESS` |
| Resource-token max lifetime | `86400` (600..86400; `18000` is the 2020 Create Permission page cap) | `--cosmosdb-resource-token-max-seconds` | `COSMOSDB_RESOURCE_TOKEN_MAX_SECONDS` |
| TLS (self-signed) | `false` | (env only) | `COSMOSDB_TLS=true` |
| Extra TLS hosts | none | (env only) | `COSMOSDB_TLS_HOSTS=name1,10.0.0.5` |

Startup fails fast if the port is unavailable — there is no fallback pool. With `COSMOSDB_TLS=true` the listener serves HTTPS with an in-memory self-signed certificate (valid for `localhost`, `127.0.0.1`, `::1` and `COSMOSDB_TLS_HOSTS`); point the SDK at `https://localhost:8081` and skip certificate verification. The default is plain HTTP.

## Authentication

Gopherstack accepts the real Cosmos DB Local Emulator's published well-known master key by default:

```text
C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
```

Cosmos DB has an **opt-in** `--cosmosdb-validate-auth` flag. With it off (the default), any Authorization header, or none, is accepted. With it on, **every** request (account root, databases, containers, documents and the Table API) must carry a correctly signed master-key `Authorization: type=master&ver=1.0&sig=...` header and an `x-ms-date` (or `Date`) within 15 minutes of server time, otherwise the response is `401 Unauthorized`. Table API requests may alternatively be SharedKey/SharedKeyLite signed with the same key. The dashboard's Cosmos DB page keeps working with validation on: its in-process proxy signs forwarded requests server-side, so the key never reaches the browser.

Throughput offers (`/offers`, the `x-ms-offer-throughput` / `x-ms-cosmos-offer-autopilot-settings` create headers, azcosmos `ReadThroughput`/`ReplaceThroughput`, ARM `throughputSettings`) are always served. With **opt-in** `--cosmosdb-enforce-throughput`, a container's (or its database's shared) provisioned RU/s is enforced from the request charges: an overdrawn budget answers `429 TooManyRequests` with `x-ms-retry-after-ms` and `x-ms-substatus: 3200`. `--cosmosdb-serverless` rejects throughput provisioning and caps containers at 5000 RU/s.

## Request charge

`x-ms-request-charge` is computed from payload size and operation: point read 1 RU + 0.09 per KB beyond the first; create/upsert 5.5 + 0.73 per KB; replace twice a create; delete 5.5; query 2.3 + 0.005 per scanned item + 0.1 per KB returned; metadata operations 1. It is an approximation; nothing is throttled.

## SQL support

`SELECT [DISTINCT] [TOP n] (* | VALUE expr | expr [AS name], ...)`, `FROM c [JOIN x IN c.arr]...`, `WHERE`, `GROUP BY`, multi-key `ORDER BY`, `OFFSET n LIMIT n`, `@parameters`, `COUNT/SUM/AVG/MIN/MAX`, `EXISTS(...)`/`ARRAY(...)`/scalar subqueries, `IN`/`BETWEEN`/`LIKE`/`?:`/`??`, and the common string, math, type-check, array and date functions. See [PARITY.md](../../services/cosmosdb/PARITY.md) for the full list.

## TTL and partition keys

A container `defaultTtl` (`-1` = on without default expiry) and per-item `ttl` expire documents: expired documents are invisible to reads and queries at once and physically removed by a background sweeper. Containers accept up to three hierarchical partition-key paths (`kind: MultiHash`); point operations then need every key component, and a query's `x-ms-documentdb-partitionkey` may be a leading prefix.

## Go SDK Example

```go
import "github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

cred, _ := azcosmos.NewKeyCredential(
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")

client, _ := azcosmos.NewClientWithKey("http://localhost:8081", cred, &azcosmos.ClientOptions{
    EnableContentResponseOnWrite: true,
})

ctx := context.Background()
_, _ = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "my-db"}, nil)
db, _ := client.NewDatabase("my-db")
_, _ = db.CreateContainer(ctx, azcosmos.ContainerProperties{
    ID: "my-container",
    PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
}, nil)
```

## curl Examples

```bash
# Database account root (required before any data-plane call)
curl "http://localhost:8081/"

# Create a database
curl -X POST "http://localhost:8081/dbs" \
    -H "Content-Type: application/json" \
    --data '{"id":"my-db"}'

# Create a container
curl -X POST "http://localhost:8081/dbs/my-db/colls" \
    -H "Content-Type: application/json" \
    --data '{"id":"my-container","partitionKey":{"paths":["/pk"],"kind":"Hash"}}'

# Create a document
curl -X POST "http://localhost:8081/dbs/my-db/colls/my-container/docs" \
    -H "Content-Type: application/json" \
    --data '{"id":"doc1","pk":"partition1","name":"example"}'
```

## Known Limitations

- No provisioned throughput, autoscale or `429` throttling; the offers APIs are not served.
- `indexingPolicy` is stored and echoed but never enforced; queries always scan the container.
- One fixed region; no failover, multi-write or consistency-level behavior beyond a static session token.
- A query is re-executed for each continuation page, so results can shift if data changes between pages.
- No user-defined functions, spatial functions, PATCH, transactional batch, change feed, stored procedures or triggers.
- Database/container ETags are static (derived from their RID, never versioned).
- Table API `$batch` is not implemented.

## More

- [Service README](../../services/cosmosdb/README.md)
- [Full parity audit](../../services/cosmosdb/PARITY.md)
- [AZURE.md](../../AZURE.md) — overall Azure support plan and milestones
- [All services](../../README.md#services)
