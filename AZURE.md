# Azure Support Implementation Plan for gopherstack

Repo cloned and inspected at `/private/tmp/claude-503/-Users-jacob-hochstetler-Code/926630d2-46b5-4853-a931-b4ea837b9658/scratchpad/gopherstack` (module `github.com/blackbirdworks/gopherstack`, GitHub `jh125486/gopherstack`). Findings below are grounded in that source (paths cited); Azure/Azurite mechanics are standard public documentation.

## 1. How gopherstack is built today (relevant facts)

- **Per-service package layout** (`services/<name>/`): `provider.go` (implements `service.Provider`: `Name()`, `Init(ctx *service.AppContext) (service.Registerable, error)`), `handler.go` (implements `service.Registerable`: `Name()`, `Handler() echo.HandlerFunc`, `RouteMatcher() service.Matcher`, `MatchPriority() int`, `ExtractOperation`/`ExtractResource` for metrics), `store.go` (`InMemoryBackend`), `interfaces.go` (`StorageBackend` interface for testability), `persistence.go` (versioned JSON snapshot/restore), `settings.go`, `janitor.go` (TTL sweeper), `errors.go`, `README.md` + `PARITY.md`. Examples inspected: `services/s3/*`, `services/sqs/*`, `services/dynamodb/*`.
- **Central plumbing** (`pkgs/service/service.go`, `router.go`, `priorities.go`): one Echo HTTP server, one port. Every service registers a `Registerable`; a `Router` sorts all registered matchers by `MatchPriority()` (100=exact header, 95=partial header, 90=form POST, 85=versioned path, 80=standard form, 75=target-prefixed, 50=path UI, 0=catch-all) and evaluates them per request, first match wins. `AppContext{Config, JanitorCtx, Logger, PortAlloc, JanitorTimeout}` is passed to every `Provider.Init`. Optional interfaces a handler can also implement: `DashboardProvider`, `ChaosProvider`, `BackgroundWorker`, `Shutdowner`, `Resettable`, `Purgeable`, `FISActionProvider`.
- **Registration**: new services are added to a flat `[]service.Provider` literal in `cli.go` (`getCoreServiceProviders`/`getRemainingServiceProviders`).
- **Auth**: `services/s3/sigv4.go` shows the house style — SigV4/SigV2 `Authorization` headers are *structurally* parsed and validated, but cryptographic signature verification is opt-in (`WithPresignValidation`/`PresignSecret`); with no secret configured, any credentials are accepted. This is functionally identical to Azurite's fixed-dev-key model, which derisks the whole Azure effort.
- **Persistence**: versioned snapshot structs (`backendSnapshot{Version int, ...}`) restored on boot, guarded against shape drift.
- **Docs/parity machinery**: each service ships a `PARITY.md` (per-op table: `wire`/`errors`/`state`/`persist` status, known gaps, deferred items, audited against a pinned SDK module version) that `cmd/gendocs` renders into the service `README.md` and the root `README.md` services table/badges.
- **Testing conventions**: table-driven unit tests colocated with source, `export_test.go` for whitebox access, `leak_test.go`/`leak_main_test.go` (goroutine-leak checks), `bench_test.go`. `test/integration/<service>_test.go` uses the real upstream AWS SDK (`aws-sdk-go-v2`) against a running instance (via testcontainers), doing full lifecycle flows (e.g. `TestIntegration_SQS_QueueLifecycle`: create → list → get-attrs → delete → verify-gone). No non-Go-SDK wire tests currently exist in the repo.

## 2. Azure wire-protocol minimums (Azurite-equivalent)

**Azurite's dev-auth model** (the shape to copy): a fixed well-known account, `devstoreaccount1`, with a fixed published key (`Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`), reachable via `UseDevelopmentStorage=true` or an explicit connection string (`AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1`). SDKs sign requests with SharedKey (HMAC-SHA256 over a canonicalized string, key is the base64-decoded account key) into `Authorization: SharedKey devstoreaccount1:<sig>`. Critically, **Azurite runs Blob/Queue/Table on three separate ports (10000/10001/10002)** rather than multiplexing one port — because the path shape (`/<account>/<resource>`) is otherwise ambiguous across the three services.

**Blob** (REST+XML, `x-ms-version` header): `GET /<account>?comp=list` (list containers); `PUT`/`DELETE /<account>/<container>?restype=container` (create/delete container); `PUT /<account>/<container>/<blob>` with `x-ms-blob-type` (put blob), `GET`/`HEAD` (get blob/properties, Range support), `DELETE` (delete blob), `GET /<account>/<container>?restype=container&comp=list` (list blobs). Large-object upload uses Put Block + Put Block List — deferrable to a later pass.

**Queue** (REST+XML): `GET /<account>?comp=list`; `PUT`/`DELETE /<account>/<queue>`; message ops under `/<account>/<queue>/messages`: `POST` (put), `GET` (get, with `numofmessages`/`visibilitytimeout`), `GET ?peekonly=true` (peek), `DELETE /messages/<id>?popreceipt=` (delete), `PUT` (update visibility), `DELETE /messages` (clear).

**Table** (REST+JSON/OData, `Accept: application/json;odata=nometadata`): `POST /<account>/Tables` (create), `DELETE /<account>/Tables('<name>')`, `GET /<account>/Tables` (list); entities: `POST /<account>/<table>` (insert), `PUT`/`MERGE` to `/<account>/<table>(PartitionKey='..',RowKey='..')` (replace/merge, `If-Match` for optimistic concurrency), `DELETE` same path, `GET /<account>/<table>()?$filter=...` (query). Batch (`POST /<account>/$batch`, multipart/mixed changesets) is real-world-important but complex — good candidate for a later milestone. **Important reuse fact: Cosmos DB's Table API is the same wire protocol as Azure Table Storage** (same OData entity model, different auth/endpoint), so Table Storage work is directly reusable for a future Cosmos Table API surface.

**Cosmos DB (Core/SQL API)** is REST+JSON over HTTPS (not just the gRPC/TCP "direct mode" some SDKs also support — the Gateway/REST mode is what unmodified SDKs fall back to and is what's feasible to emulate). Resource hierarchy: `/dbs` → `/dbs/<db>/colls` → `/dbs/<db>/colls/<coll>/docs`. Auth header: `Authorization: type=master&ver=1.0&sig=<base64 HMAC-SHA256>` (URL-encoded), signed with the base64 master key over verb+resourceType+resourceId+date; plus `x-ms-date` and `x-ms-version` headers, and `x-ms-documentdb-partitionkey` on point ops. **The real Cosmos DB Local Emulator uses a fixed, publicly documented well-known master key** (`C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==`) at a fixed endpoint (`https://localhost:8081` for the real emulator) — exactly the same "one fixed dev secret" pattern as Azurite, reinforcing that gopherstack's existing permissive-by-default auth model is the right target here too. Queries execute via `POST .../docs` with `x-ms-documentdb-isquery: True` and a `{"query": "...", "parameters": [...]}` body — needs a small SQL-like query engine, structurally similar to work gopherstack has already done (S3 Select has a full SQL tokenizer/parser/executor in `services/s3/select_sql_*.go`; DynamoDB has its own expression engine in `services/dynamodb/expr/`). Real SDKs also expect `x-ms-request-charge`, `x-ms-session-token`, and `etag` on responses — MVP can return static/fake values rather than real RU accounting.

**Prior art**: an existing open-source lightweight Cosmos DB emulator, **cosmium** (github.com/pikami/cosmium), is worth reviewing directly when scoping the Cosmos milestone (M4) — it has already solved the "how minimal can this REST surface be and still satisfy real SDKs" problem for Cosmos specifically, and is a useful reference for the resource model, the SQL-query subset, and the fake RU/session-token response shape.

## 3. Proposed package layout

```
services/azureblob/    provider.go handler.go store.go interfaces.go persistence.go
                        settings.go janitor.go errors.go models.go
                        blob_ops.go container_ops.go conditional.go
                        README.md PARITY.md *_test.go export_test.go
services/azurequeue/   (same skeleton) queue_ops.go message_ops.go visibility.go
services/azuretable/   (same skeleton) table_ops.go entity_ops.go odata_filter.go
services/cosmosdb/     (same skeleton) database_ops.go container_ops.go document_ops.go
                        sql_query.go (mini SQL parser/executor; reuse patterns from
                        services/s3/select_sql_*.go and services/dynamodb/expr/;
                        cross-check minimal-surface scoping against cosmium)
pkgs/azureauth/        sharedkey.go (SharedKey/SharedKeyLite canonicalization + the
                        fixed devstoreaccount1 name/key constants) — shared across
                        blob/queue/table since, unlike AWS's per-service SigV4
                        variance, Azure Storage's SharedKey algorithm is identical
                        across all three; Cosmos's master-key HMAC scheme is
                        different enough to live in services/cosmosdb/ instead.
```

## 4. Routing/port strategy (the one real architectural decision)

gopherstack's flagship pattern is single-port, priority-matcher multiplexing — but that only works because AWS services are disambiguated by headers (`X-Amz-Target`) or distinctive path/form shapes. Azure Blob/Queue/Table share the *same* `/<account>/<resource>` path shape with no service-identifying header, so multiplexing them on one port risks exactly the collision the AWS router avoids by construction.

**Recommendation: give each Azure service its own port, mirroring Azurite's own 10000/10001/10002 convention (and Cosmos its own port, mirroring the real emulator's fixed 8081 default).** This is *more* wire-compatible, not less, since SDKs' default connection strings/emulator constants already assume separate ports. gopherstack already has the machinery for this — `AppContext.PortAlloc *portalloc.Allocator` is used elsewhere (e.g. EC2-docker SSH port ranges) for per-resource port allocation, so per-service dedicated listeners are an established pattern, not a new one. Each Azure service's `Provider.Init` stands up its own `echo.Echo` (or shares Echo's engine but binds a second listener), independent of the AWS `Router`.

## 5. Auth/connection-string strategy per service

- **Blob/Queue/Table**: default to the fixed Azurite account name/key pair (`devstoreaccount1` / the published emulator key) so `UseDevelopmentStorage=true` and unmodified Azurite-targeting SDK config work out of the box. `Authorization: SharedKey ...` headers are parsed structurally (account name extraction for routing/logging); cryptographic verification is opt-in via a `WithSharedKeyValidation` toggle — directly mirroring `services/s3`'s `PresignSecret`/`WithPresignValidation` opt-in pattern. Env var overrides (`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`) for anyone who wants a non-default identity.
- **Cosmos**: default to the real emulator's published fixed master key at a configurable local endpoint; `Authorization: type=master&ver=1.0&sig=...` parsed structurally, verification opt-in the same way.
- **TLS**: Cosmos SDKs often default to HTTPS even against local emulators, requiring an explicit "allow insecure/disable SSL verification" client flag pointed at plain HTTP — document that flag first (matches gopherstack's lightweight, no-extra-infra ethos); a self-signed cert on the Cosmos listener is a stretch goal, not MVP.

## 6. Implementation order and rationale

1. **Azure Blob Storage** — closest analog to S3 (gopherstack's most mature service); reuses S3's XML serialization, chunked-upload, conditional-header, checksum, and persistence-snapshot patterns almost directly. Lowest risk, highest day-to-day dev-workflow value, fastest to land.
2. **Azure Queue Storage** — also REST+XML (reuses Blob's XML plumbing), and its message lifecycle (visibility timeout, pop receipts) maps closely onto concepts gopherstack already solved for SQS.
3. **Azure Table Storage** — introduces the OData/JSON entity model and a `$filter` mini-grammar; partition-key/row-key semantics map onto DynamoDB's hash/range-key model, so the `services/dynamodb/expr` experience transfers directly to the `$filter` parser.
4. **Cosmos DB (Core/SQL API)** — most complex: master-key HMAC auth, partition-key routing, and a real SQL-like query subset (`SELECT * FROM c WHERE ...`), best tackled last so it can borrow the SQL-parsing muscle built for S3 Select and the entity/query plumbing built for Table Storage (Cosmos's own Table API is literally the Table Storage protocol, a natural stretch goal once both exist). Cross-check scope against cosmium's existing implementation before locking the op list.

## 7. Testing strategy (matches repo conventions)

- **Unit**: table-driven Go tests colocated per file, `t.Parallel()`, `t.Context()`, `export_test.go` for whitebox internals, `leak_test.go`/`leak_main_test.go` for janitor goroutine hygiene, `bench_test.go` for hot paths (blob PUT/GET, table query).
- **Integration** (`test/integration/azureblob_test.go`, `azurequeue_test.go`, `azuretable_test.go`, `cosmosdb_test.go`): use the real `azure-sdk-for-go` client against a running instance via testcontainers, exercising full CRUD lifecycles exactly like `TestIntegration_SQS_QueueLifecycle` (create → list → operate → delete → verify-gone).
- **Cross-SDK wire-compat smoke tests** (the one genuine gap versus existing conventions, since the repo's own suite is Go-SDK-only, but this task's hard requirement is multi-SDK compatibility): add minimal fixtures under the existing `test/e2e/` directory — a short Node script using `@azure/storage-blob`/`@azure/data-tables`/`@azure/cosmos` and a short Python script using `azure-storage-blob`/`azure-cosmos`, run in CI against a live gopherstack instance, proving JS/Python SDKs work unmodified (not just the Go SDK).
- **PARITY.md** seeded per service from day one in the established format (per-op `wire`/`errors`/`state`/`persist` status, known gaps, deferred items, audited against a pinned SDK module version per language — pick `azure-sdk-for-go` as the canonical pinned version for the audit), wired into `cmd/gendocs` so it flows into the service `README.md` and the root README's services table/badges like every existing service.

## 8. Milestones

- **M0** — `pkgs/azureauth` (SharedKey canonicalization + fixed devstoreaccount1 constants); `services/azureblob` skeleton wired to its own port via `PortAlloc`; Create/Delete/List Container, Put/Get/Delete Blob, List Blobs; seeded `PARITY.md`; unit tests + one Go integration test.
- **M1** — Blob completeness: properties/metadata, block-blob multipart (Put Block/Put Block List), conditional headers (`If-Match`/`If-None-Match`), error-mapping table (mirrors `services/sqs`'s `errorDetails` pattern).
- **M2** — `services/azurequeue`: full CRUD + message lifecycle (put/get/peek/delete/update/clear), visibility timeout.
- **M3** — `services/azuretable`: table CRUD, entity insert/get/query/update/merge/delete, `$filter` subset (eq/ne/lt/gt/and/or on partition/row key plus scalar properties), ETag-based optimistic concurrency.
- **M4** — `services/cosmosdb`: database/container CRUD (with partition-key-path declaration), document CRUD, SQL-subset query engine, fake RU/session-token/etag headers; scope the op list against cosmium (github.com/pikami/cosmium) as reference prior art; integration tests against `azure-sdk-for-go`, `azure-sdk-for-js`, and `azure-cosmos` (Python).
- **M5** — Docs/polish: root README services table + badges/icons, `docs/services/*.md` guides, a docker-compose example under `examples/`, and the `test/e2e` cross-SDK smoke suite covering all four services.

## Key files referenced

- `/private/tmp/.../gopherstack/pkgs/service/service.go`, `router.go`, `priorities.go` — routing/registration contracts
- `/private/tmp/.../gopherstack/services/s3/provider.go`, `sigv4.go`, `persistence.go` — provider pattern, auth-opt-in pattern, snapshot pattern
- `/private/tmp/.../gopherstack/services/sqs/handler.go`, `provider.go` — handler/dispatch pattern, error-table pattern
- `/private/tmp/.../gopherstack/services/dynamodb/expr/` — expression-parser precedent for Table Storage's `$filter`
- `/private/tmp/.../gopherstack/services/s3/select_sql_*.go` — SQL-parser precedent for Cosmos queries
- `/private/tmp/.../gopherstack/cli.go` (`getServiceProviders`) — service registration list
- `/private/tmp/.../gopherstack/test/integration/sqs_test.go` — integration test convention
- `/private/tmp/.../gopherstack/services/sqs/PARITY.md`, `README.md` — parity-doc format
