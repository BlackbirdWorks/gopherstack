---
service: cosmosdb
sdk_module: azure-sdk-for-go/sdk/data/azcosmos@v1.5.0
last_audit_commit: 19af2a18c
last_audit_date: 2026-10-20
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetDatabaseAccount: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "GET (and HEAD) / (or empty path) -- the database-account root resource. Every real Cosmos SDK, azcosmos included, issues this call before it will make a single data-plane call, to discover the account's readable/writable regional endpoints (azure-sdk-for-go/sdk/data/azcosmos's globalEndpointManager.GetAccountProperties); omitting this resource entirely made the service unreachable by any unmodified SDK until this was added -- an integration-test-only failure mode no unit test could catch, since unit tests call the handler directly rather than going through a real SDK's client-construction path. Returns writableLocations/readableLocations (one entry each, region name \"South Central US\" purely for verisimilitude), enableMultipleWriteLocations: false, and userConsistencyPolicy.defaultConsistencyLevel: \"Session\". databaseAccountEndpoint in each location is built from the REQUEST's own Host header, never from the server's configured port: under testcontainers (or any port-mapping/proxy setup) the container's configured port is frequently not the port a client actually connects through, so advertising the configured port instead of echoing the client's own Host would make the SDK \"discover\" a location pointing at a port nothing is listening on and redirect every subsequent request there -- see TestHandler_AccountRoot_EndpointEchoesRequestHost."}
  CreateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /dbs, body {\"id\":\"..\"}. 201 + database body (id/_rid/_self/_etag/_ts/_colls/_users). 409 Conflict on duplicate id."}
  GetDatabase: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs/{db}. 200 + database body; 404 NotFound if absent."}
  ListDatabases: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs -> {\"_rid\":\"\",\"Databases\":[...],\"_count\":N}, sorted by id. x-ms-max-item-count / x-ms-continuation page the feed (see families.continuation); a malformed token is 400."}
  DeleteDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /dbs/{db}. 204; cascades to delete every container/document beneath it. 404 NotFound if absent."}
  CreateContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /dbs/{db}/colls, body {id, partitionKey:{paths,kind,version}, defaultTtl, indexingPolicy}. One to three distinct \"/\"-prefixed paths; more than one needs kind MultiHash (hierarchical keys, see families.partition_keys); 400 otherwise. defaultTtl must be -1 or a positive integer. indexingPolicy is stored and echoed (default policy when absent), never enforced. 201 + container body; 409 on duplicate id; 404 if the database is missing."}
  GetContainer: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs/{db}/colls/{coll}. 200 + container body (including its partitionKey definition); 404 if absent."}
  ListContainers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs/{db}/colls -> {\"_rid\":\"\",\"DocumentCollections\":[...],\"_count\":N}, sorted by id; paged like ListDatabases."}
  DeleteContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /dbs/{db}/colls/{coll}. 204; cascades to delete every document in it. 404 if absent."}
  QueryDatabases: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /dbs with the query header (what the SDK's query-databases pager sends): the SQL engine runs over database resource bodies; paged like ListDatabases."}
  QueryContainers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /dbs/{db}/colls with the query header: the SQL engine runs over container resource bodies; 404 if the database is missing."}
  ReplaceContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /dbs/{db}/colls/{coll}. Updates defaultTtl (omitted disables TTL) and indexingPolicy; the partition key is immutable (400); id must match the path; 404 if absent."}
  CreateDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /dbs/{db}/colls/{coll}/docs; also Upsert when x-ms-documentdb-is-upsert: true. id from the body (non-empty string) or generated; partition key extracted per the container's paths (absent component -> null). Per-item ttl must be -1 or a positive integer (400 otherwise). 201 + body + ETag; 409 on duplicate (id, partition key) unless upsert; an expired document does not conflict."}
  QueryDocuments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /dbs/{db}/colls/{coll}/docs with x-ms-documentdb-isquery: True and/or Content-Type: application/query+json, body {\"query\":\"...\",\"parameters\":[...]}. Runs the SQL engine (families.sql_query). Cross-partition unless x-ms-documentdb-partitionkey carries a non-empty key, which scopes the query to that partition (a leading prefix on hierarchical containers). Paged via x-ms-max-item-count / x-ms-continuation; x-ms-request-charge follows the RU model. Parse errors are 400 BadRequest."}
  ListDocuments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs/{db}/colls/{coll}/docs (read feed). Response {\"_rid\":\"\",\"Documents\":[...],\"_count\":N}, sorted by (id, partition key); expired (TTL) documents are hidden; paged via x-ms-max-item-count / x-ms-continuation."}
  GetDocument: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /dbs/{db}/colls/{coll}/docs/{id}. Requires x-ms-documentdb-partitionkey: a JSON array of one to three scalars matching the container's path count exactly (400 otherwise, also for malformed, trailing-content or non-scalar values). 200 + document body + ETag; 404 if absent or expired."}
  ReplaceDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /dbs/{db}/colls/{coll}/docs/{id}. Requires x-ms-documentdb-partitionkey like GetDocument. If-Match absent -> unconditional replace (upsert semantics: creates if absent); If-Match: <etag> -> 412 PreconditionFailed on mismatch. Always full-body replace (drops fields not present in the new body); id/partition key are fixed by the path/header, not the body -- but if the replacement body itself declares a value at the container's partition-key path, it MUST agree with the header's partition key (400 BadRequest on a mismatch), so a document can never end up stored under one partition while its own body claims another."}
  DeleteDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /dbs/{db}/colls/{coll}/docs/{id}. Requires x-ms-documentdb-partitionkey. If-Match absent -> unconditional delete OF AN EXISTING DOCUMENT (still 404 if the document is already absent -- Delete, unlike Replace, has no upsert concept, so an earlier version's absent-plus-no-If-Match short-circuit that returned 204 for a document that never existed was a real bug); If-Match: <etag> -> 412 on mismatch. 404 if not found."}
  PatchDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /dbs/{db}/colls/{coll}/docs/{id} with {\"condition\":\"from c where ...\",\"operations\":[{op,path,from,value}]}. Operations add, set, replace, remove, incr, move on JSON-pointer paths (~0/~1 escapes; array index or - to append); at most 10 per request; applied in order to a copy, so one failing operation leaves the document unchanged (400). incr creates a missing object property and rejects non-numbers; replace/remove/move need an existing target. The id, the partition key and system properties cannot be patched (400). condition runs through the SQL engine against the document and a false result is 412; If-Match mismatch is 412; a missing document 404. Requires the partition key header like ReplaceDocument. 200 + the patched document + ETag. Pre/post triggers do not fire for PATCH."}
  ExecuteTransactionalBatch: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST .../docs with x-ms-cosmos-is-batch-request: true and the partition key header; body is the operations array ({operationType: Create|Upsert|Read|Replace|Delete|Patch, id, ifMatch, resourceBody}) exactly as azcosmos TransactionalBatch serializes it. Always atomic: all operations run under one write transaction in that partition and either all commit (200 + one result per operation: statusCode 201/200/204, eTag, requestCharge, resourceBody) or none do (207 Multi-Status: the failing operation carries its own status -- 404, 409, 412, 400 -- and every other result is 424). A document in a different partition than the header fails with 400. 1 to 100 operations (400 otherwise). Prefer: return=minimal drops write bodies, never read bodies. Verified with azcosmos ExecuteTransactionalBatch (batch_test.go)."}
  ReadChangeFeed: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET .../docs with A-IM: Incremental feed, latest-version mode. Every create/upsert/replace/patch stamps the document with a per-container monotonic sequence (persisted; _lsn in the feed); the feed returns live documents with a sequence above the continuation, in sequence order, with an ETag of the form \"<sequence>\" to pass back as If-None-Match. No If-None-Match starts from the beginning, * starts from now (304 + current ETag), If-Modified-Since starts from a time, x-ms-documentdb-partitionkey scopes to a partition (prefix on hierarchical keys), x-ms-max-item-count pages, x-ms-documentdb-partitionkeyrangeid must be 0 (400 otherwise). Nothing new is 304 with the unchanged ETag. Deletes (and TTL expiries) are not reported: all-versions-and-deletes mode does not exist. Verified with azcosmos ReadChangeFeed (continuation tokens, feed range, partition key, start time)."}
  ReadPartitionKeyRanges: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET .../colls/{coll}/pkranges: one physical range (id 0, minInclusive empty, maxExclusive FF) with a static ETag (If-None-Match -> 304). It is what azcosmos needs to resolve a change feed FeedRange; the emulator never splits."}
  StoredProcedures: {wire: ok, errors: ok, state: ok, persist: ok, note: "Create (POST /sprocs, 201/409), Get, List (StoredProcedures feed, paged), Query, Replace (PUT, id must match), Delete on /dbs/{db}/colls/{coll}/sprocs[/{id}]. The body must compile as a single JavaScript function (400 otherwise)."}
  Triggers: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRUD on /triggers[/{id}] as StoredProcedures, plus triggerType (Pre|Post) and triggerOperation (All|Create|Delete|Replace|Update), both validated and canonicalized."}
  UserDefinedFunctions: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRUD on /udfs[/{id}] as StoredProcedures; callable from SQL, see families.server_side_scripts."}
  ExecuteStoredProcedure: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /sprocs/{id} with the partition key header and a JSON array of parameters. 200 + the JSON body set with getResponse().setBody (empty body when never set). A thrown error is 400 with the JS message, a run over the time limit is 408; either way every write the script made is rolled back. x-ms-documentdb-script-enable-logging returns console.log output in x-ms-documentdb-script-log-results. Not served by azcosmos v1.5.0 (no sproc API), so tests use signed raw REST."}
families:
  server_side_scripts: {status: ok, note: "Stored procedures, triggers and UDFs run on the goja engine already used by AppSync (scripts.go, script_engine.go, script_env.go, script_exec.go, triggers.go, udf.go). Sandbox: a bare runtime with no require, process, fetch, timers or file/network access, a call-stack cap, and a time limit (5s, like Cosmos) enforced with vm.Interrupt. getContext() offers getCollection() (getSelfLink, createDocument, upsertDocument, readDocument, replaceDocument, deleteDocument with options.etag, queryDocuments with a SQL string or {query, parameters}), getResponse() and getRequest() (getBody/setBody), plus console.log. Every operation is confined to the request's partition (a document of another partition is a 400 passed to the callback) and runs in one transaction with the script: callbacks are invoked synchronously, an error passed to a callback is only thrown if the script throws it, and any thrown error, timeout or failed trigger rolls the whole run back (containerTx undo log; the change feed sequence is restored too). Triggers are named in x-ms-documentdb-pre-trigger-include / post-trigger-include on create, upsert, replace and delete (comma separated); an unknown trigger or one whose type/operation does not match is 400. A pre-trigger sees and may replace the request body, a post-trigger sees the stored document and may replace the response body, and both run in the write's transaction. UDFs are called as udf.name(args) in SQL (the parser only treats udf.x( as a call, so an alias named udf still works); an unknown or throwing UDF is a 400, a runaway one 408. Scripts hold the container write lock while they run (the whole backend, in fact), so a slow script stalls other requests until it finishes or times out."}
  change_feed: {status: partial, note: "Latest-version incremental feed only (ops.ReadChangeFeed). One partition key range, so a FeedRange covering it always resolves; no splits or merges. Continuations are per-container sequences, so a token stays valid across restarts but not across a container delete and recreate."}
  conflict_resolution: {status: ok, note: "conflictResolutionPolicy is accepted on container create and replace, persisted and echoed (LastWriterWins on /_ts when absent). The emulator is a single region with a single writer, so no conflict ever arises and the /conflicts feed does not exist."}
  auth: {status: ok, note: "Opt-in (--cosmosdb-validate-auth / COSMOSDB_VALIDATE_AUTH, default off: every request accepted, malformed headers only logged). When on, EVERY route (account root, databases, containers, documents, Table API) needs a valid Authorization: type=master&ver=1.0&sig=<base64 HMAC-SHA256> over verb/resourceType/resourceId/x-ms-date (masterkey.go, known-answer vector in masterkey_test.go). A missing header, a missing or unparsable x-ms-date/Date, a date more than 15 minutes from server time, and a bad signature all return 401 Unauthorized. Table API routes also accept SharedKey/SharedKeyLite signed with the same key (pkgs/azureauth). The dashboard proxy signs its forwarded requests server-side with the in-process key (Handler.SignRequest), so it keeps working with validation on and no key reaches the browser. Verified end to end with the azcosmos SDK (sdk_test.go)."}
  wire_protocol: {status: ok, note: "REST+JSON. x-ms-version (2020-07-15), x-ms-request-id, Date, x-ms-request-charge (RU model, families.request_charge), x-ms-session-token (static 0:0#0), x-ms-activity-id on every response; x-ms-item-count and x-ms-continuation on feeds and queries. Errors use {\"code\",\"message\"} with the matching status."}
  sql_query: {status: ok, note: "Lexer -> recursive-descent parser -> resolver -> executor (sql_tokenizer.go, sql_parser.go, sql_resolve.go, sql_exec.go, sql_eval.go, sql_funcs.go, sql_agg.go). SELECT [DISTINCT] [TOP n] (* | VALUE expr | expr [AS name], ...) FROM container [alias] (JOIN x IN expr | JOIN expr alias)* WHERE GROUP BY ORDER BY (multiple keys, ASC/DESC, any expression) OFFSET n LIMIT n, with @params anywhere a value fits (including TOP/OFFSET/LIMIT). Expressions: comparison, AND/OR/NOT, IN, BETWEEN, LIKE [ESCAPE], IS [NOT] NULL, arithmetic, ||, ??, ?:, property and [index] access, array and object literals, undefined. Subqueries: EXISTS(...), ARRAY(...), scalar (...), correlated, with their own FROM x IN c.arr. Aggregates COUNT/SUM/AVG/MIN/MAX (also VALUE COUNT(1)); an aggregate over an empty set yields COUNT 0 and an undefined SUM/AVG/MIN/MAX. Built-ins: string (CONCAT CONTAINS STARTSWITH ENDSWITH LOWER UPPER LENGTH SUBSTRING REPLACE TRIM LTRIM RTRIM LEFT RIGHT REVERSE REPLICATE INDEX_OF TOSTRING STRINGEQUALS REGEXMATCH STRINGTONUMBER STRINGTOBOOLEAN STRINGTONULL), math (ABS CEILING FLOOR ROUND TRUNC SQRT SQUARE POWER SIGN EXP LOG LOG10 PI), type checks (IS_DEFINED IS_NULL IS_NUMBER IS_STRING IS_BOOL IS_ARRAY IS_OBJECT IS_PRIMITIVE IS_FINITE_NUMBER IS_INTEGER), array (ARRAY_CONTAINS ARRAY_LENGTH ARRAY_SLICE ARRAY_CONCAT SETINTERSECT SETUNION), date (GetCurrentDateTime/Timestamp/Ticks and Static variants, DateTimeAdd, DateTimeDiff, DateTimePart, DateTimeToTimestamp, TimestampToDateTime). Three-valued logic: a missing field or unbound parameter is undefined and never flips under NOT; only a definite true keeps a row; IS NULL on undefined is false. Every identifier must resolve to a FROM/JOIN/subquery alias (unqualified or unknown names are 400). Numbers stay json.Number, int64-exact beyond 2^53. Recursion is bounded (maxQueryDepth=100). Unnamed projections are named $1, $2 as in Cosmos. Parse and runtime-argument errors are 400, never a panic."}
  continuation: {status: ok, note: "ListDatabases, ListContainers, ListDocuments, QueryDatabases, QueryContainers and QueryDocuments honor x-ms-max-item-count (absent or <= 0 means one unbounded page) and x-ms-continuation, via pkgs/page opaque tokens over the stable sort (id, then partition key; query results in result order). A query is re-executed per page and sliced, so a concurrent write between pages can shift results. A malformed token is 400."}
  ttl: {status: ok, note: "Container defaultTtl (-1 = on, no default expiry; absent = off; 0 and < -1 rejected) and per-item ttl (-1 never, positive seconds; a container with TTL off ignores item ttl). Expiry is _ts + ttl. Enforced lazily on every read, query, list, replace, delete and create-conflict check (an expired document reads as absent) and physically removed by a worker.Group janitor (TTLSweeper, every 10s, Handler.SweepInterval) started in StartWorker and stopped in Shutdown. PUT on the container updates defaultTtl."}
  request_charge: {status: partial, note: "x-ms-request-charge is computed per operation, not static: point read 1 + 0.09 RU per KB beyond the first; create/upsert 5.5 + 0.73 per KB beyond the first; replace 2x create; delete 5.5; read feed = read cost of the returned payload; query 2.3 + 0.005 per scanned item + 0.1 per KB returned; metadata operations 1. Sizes are the JSON payload length (ru.go). An approximation of documented costs for default indexing, not a metering of index lookups; no throughput limit or 429 exists."}
  partition_keys: {status: ok, note: "Single-path (Hash) and hierarchical (MultiHash, 2-3 paths) keys. A hierarchical document is keyed by the JSON array of its components (absent component -> null); point operations need every component (400 otherwise); the body's key must agree with the request's on replace (400). A query's x-ms-documentdb-partitionkey header scopes it to that partition, a leading prefix on hierarchical containers; an empty array means cross-partition."}
  tls: {status: ok, note: "Opt-in HTTPS via COSMOSDB_TLS=true (read in Provider.Init; COSMOSDB_TLS_HOSTS adds comma-separated names/IPs to the certificate): the listener serves an in-memory self-signed certificate from pkgs/devtls valid for localhost, 127.0.0.1 and ::1. Handler.TLSCertPEM returns the certificate for clients to trust. The account root advertises https:// endpoints when TLS is on. Default is plain HTTP."}
  edm_fidelity: {status: ok, note: "Document bodies are decoded (decodeJSONObject) and deep-copied (deepCopyBody/deepCopyJSONValue) via json.Decoder.UseNumber throughout the create/replace/get/query/snapshot paths, so a JSON number beyond float64's 53-bit mantissa (e.g. 9007199254740993) never silently loses precision -- covered end-to-end by TestInMemoryBackend_Int64PrecisionPreservedThroughStorage, TestBackend_SnapshotRestore_Int64PrecisionRoundTrip, and TestExecuteQuery_Int64PrecisionInComparison, at the math.MaxInt64/MinInt64 boundaries too."}
  etag_and_timestamp: {status: ok, note: "ETag format \"<hex-encoded UnixNano>\" derived from a document's Timestamp (models.go's etagFor) -- an opaque quoted token real SDKs never parse, only echo back via If-Match. Every mutation advances Timestamp by at least 100ns past its previous value even if the injected clock returns the same instant twice in a row (store.go's bumpTimestampFrom), guaranteeing a distinct ETag on every write -- covered by TestInMemoryBackend_ETagChangesOnEveryWrite."}
  aliasing: {status: ok, note: "Every document body that crosses the backend boundary (incoming request body, outgoing DocumentInfo.Body, snapshot round-trip) is deep-copied via deepCopyBody/deepCopyJSONValue -- a full marshal/unmarshal round trip through json.Decoder.UseNumber, not a shallow map copy -- so a caller mutating what it passed in or received back can never alias/corrupt stored state, closing the exact bug class AZURE.md's process rules flag from M2."}
  composite_keys: {status: ok, note: "Documents are keyed within a container by documentCompositeKey, a {PartitionKeyJSON, ID} struct (persisted via MarshalText as a JSON string array), not a delimiter-joined string -- closing the same NUL/delimiter-collision class services/azuretable's entityCompositeKey closes for (PartitionKey, RowKey)."}
  persistence: {status: ok, note: "Snapshot version 1, unchanged: new container fields (DefaultTTL, PartitionKeyPaths, IndexingPolicy) and the Table API state (tableApi, the odatatable snapshot) are optional additions that older snapshots simply lack. storedDocument.Body round-trips through json.RawMessage with UseNumber so numbers beyond 2^53 survive. Restore tolerates null nested maps, rejects null entries, and a snapshot without tableApi empties the Table API."}
  routing_isolation: {status: ok, note: "Runs on its own dedicated *http.Server, bound synchronously in StartWorker to a fixed port (default 8081 via --cosmosdb-port/COSMOSDB_PORT, the real Cosmos DB Local Emulator's own published default -- NOT any of Azurite's 10000/10001/10002; no fallback pool -- fails fast if unavailable). Unlike AzureBlob/AzureQueue/AzureTable, 8081 sits OUTSIDE --port-range-start/--port-range-end's own default range (10000-10100), exactly like services/iot's MQTT broker default (1883) -- but cli.go's reserveFixedServicePorts still reserves it in the shared PortAlloc pool, since a custom operator-configured range could include 8081. See cli_cosmosdb_port_reservation_test.go's table, which (unlike AzureTable's) covers both the outside-range default case and a custom in-range case."}
  observability: {status: ok, note: "StartWorker wraps its Echo handler with telemetry.WrapEchoHandler so ExtractOperation/ExtractResource feed Prometheus metrics. InMemoryBackend uses *lockmetrics.RWMutex instead of raw sync.RWMutex, matching repo convention."}
  table_api: {status: partial, note: "Table API CRUD and $filter on the same port, routed by path shape, delegating to pkgs/odatatable via an independent TableBackend; auth as families.auth (master key or SharedKey). Table state is included in Snapshot/Restore/Reset. $batch is not implemented (501); see services/azuretable/PARITY.md for the engine's own gaps."}
gaps: []
items_still_open:
  - Throughput: provisioned throughput, autoscale, serverless limits and 429 throttling do not exist; x-ms-request-charge is reported but never limited, and the offers/throughput APIs are not served
  - IndexingPolicy: stored and echoed on containers but never enforced; queries always scan the container, so index-dependent behaviors (composite-index-required ORDER BY, excluded paths, EnableScanInQuery) are not reproduced
  - MultiRegion: one fixed region (South Central US) with a single writable location; failover, multi-write, preferred regions, the conflicts feed and consistency levels other than a static session token are not modeled
  - RequestCharge: an approximation of documented costs, not a metering of index and storage work
  - Continuation: a query is re-executed for every page, so results can shift if the container changes between pages
  - SQL: spatial functions (ST_*), the remaining date and conversion built-ins, regex modifier x, and ORDER BY combined with GROUP BY are not implemented; unsupported functions are a 400 parse error
  - ItemOperations: the change feed has no all-versions-and-deletes mode (deletes and TTL expiries never appear) and a single partition key range; pre/post triggers do not fire for PATCH or batch operations; stored procedure callbacks run synchronously and hold the backend write lock; there is no server-side bulk operation (the SDKs implement bulk client-side over batches and point operations)
  - Containers: unique-key policy and the geospatial/vector/computed-property settings are not stored or enforced; database and container ETags are static
  - TableAPI: $batch (multipart changesets) is not implemented, and Table API responses carry no per-operation request charge or continuation beyond what pkgs/odatatable provides
structural_gaps: []
deferred: []
leaks: {status: clean, note: "The listener http.Server and the TTL sweeper worker.Group are both stopped by Shutdown (Group.Stop joins the sweeper goroutine); TestTTL_JanitorSweepsAndStops covers start and double shutdown."}
---

## Notes

### Why Cosmos DB gets its own port, and why that port is 8081 (not another Azurite-style number)
Every other Azure service in this repo (Blob 10000, Queue 10001, Table 10002)
mirrors Azurite's own multi-port convention, because Azurite emulates all
three. The real **Cosmos DB Local Emulator** is a separate tool with its own
separate published default port, **8081** -- not part of the Azurite family
at all. Picking 8081 (rather than continuing the 10003 pattern) matches
AZURE.md section 4's stated intent ("Cosmos its own port, mirroring the real
emulator's fixed 8081 default") and means `UseDevelopmentStorage`-style
zero-configuration client setups pointed at the real emulator's documented
endpoint (`https://localhost:8081`, with `COSMOSDB_TLS=true` -- see below) work
unmodified against gopherstack too.

One consequence: 8081 sits **outside** `--port-range-start`/
`--port-range-end`'s own default range (10000-10100), exactly like
`services/iot`'s MQTT broker default (1883) -- see AZURE.md section 4's
discussion of that precedent. `StartWorker` still binds it synchronously
with no fallback pool (fails fast on conflict), and `cli.go`'s
`reserveFixedServicePorts` still calls `alloc.Reserve(cli.CosmosDB.Port,
"cosmosdb")` even though that's a no-op against the shipped defaults: the
range is user-configurable, so an operator who narrows it to (say)
`8000-8100` needs PortAlloc to know 8081 is spoken for.
`cli_cosmosdb_port_reservation_test.go`'s table is therefore the inverse of
`cli_azuretable_port_reservation_test.go`'s: the *default*-port case is
`wantBlockedFromPool: false`, and a *separate* custom-range case
demonstrates the reservation actually taking effect.

### TLS is opt-in
SDKs default to HTTPS against local emulators. Set `COSMOSDB_TLS=true` (and
optionally `COSMOSDB_TLS_HOSTS=name1,10.0.0.5`) to serve the listener over TLS
with an in-memory self-signed certificate from `pkgs/devtls` (valid for
`localhost`, `127.0.0.1`, `::1` plus the extra hosts). The certificate is new
on every start; clients must skip verification or trust
`Handler.TLSCertPEM()`. The flag is an environment variable read in
`Provider.Init` rather than a CLI flag. Default stays plain HTTP.

### Master-key auth: why it isn't in pkgs/azureauth
AZURE.md section 3 states this explicitly: Cosmos's master-key HMAC scheme
is different enough from Blob/Queue/Table's shared SharedKey scheme
(different header shape -- a single URL-encoded `type=...&ver=...&sig=...`
string, not `Scheme account:sig` -- and a different, simpler
canonicalization with no header-block canonicalization step) that sharing a
package wasn't worth it. `masterkey.go` mirrors `pkgs/azureauth`'s internal
shape (a parse function, a canonicalization function, a sign function, an
opt-in verify function) as unexported package-level code in
`services/cosmosdb` instead. The canonicalization is exactly:

```text
lowercase(verb) + "\n" + lowercase(resourceType) + "\n" + resourceId +
"\n" + lowercase(x-ms-date) + "\n" + lowercase(date-header) + "\n"
```

(`resourceId`'s casing is preserved -- it identifies an exact resource
path). `masterkey_test.go`'s `TestVerifyMasterKey_KnownAnswerVector` proves
the HMAC-SHA256 signing algorithm itself is correct against a hand-computed
vector (independently verified via Python's `hmac`/`hashlib`), even though
enforcement (`--cosmosdb-validate-auth`) is opt-in and off by default; when on it covers every route (see `families.auth`).

### SQL query grammar
```text
query      := SELECT [DISTINCT] [TOP operand] ( '*' | [VALUE] item (',' item)* )
              FROM source (JOIN source)* [WHERE expr] [GROUP BY expr (',' expr)*]
              [ORDER BY expr [ASC|DESC] (',' expr [ASC|DESC])*] [OFFSET operand LIMIT operand]
source     := ident [AS] [ident]                  -- top-level container, first source only
            | ident IN expr | expr [AS] ident     -- JOINs and subqueries
item       := expr [AS ident]
```
Expressions cover comparison, `AND`/`OR`/`NOT`, `IN`, `BETWEEN`, `LIKE`,
`IS [NOT] NULL`, arithmetic, `||`, `??`, `?:`, property and index access,
array/object literals, calls, and subqueries (`EXISTS`, `ARRAY`, scalar).
Every identifier must resolve to a `FROM`/`JOIN`/subquery alias; the resolver
binds each to an environment slot, so an unqualified field or unknown alias is
a parse error rather than silently resolved. Execution is a streaming pipeline
over source tuples (container rows x JOIN expansions): WHERE, then GROUP BY /
aggregates, projection, DISTINCT, ORDER BY, then OFFSET/LIMIT or TOP, with an
early exit when no ordering or grouping needs the full set. Recursion is
bounded (`maxQueryDepth = 100`) and a parse error is always `400`, never a
panic. `sql_engine_test.go` is the table of supported queries.

### Int64/float64 precision (the M2 bug class, guarded from day one)
Every JSON-number-bearing path in this package -- request body decode
(`decodeJSONObject`), backend storage/return (`deepCopyBody`/
`deepCopyJSONValue`), snapshot persistence (`storedDocumentWire`), and SQL
comparison (`compareJSONNumbers`) -- uses `json.Number` via
`json.Decoder.UseNumber`, never a bare `map[string]any` decode (which always
produces `float64` and silently rounds any integer magnitude beyond
`2^53`). `compareJSONNumbers` specifically tries `Int64()` on both operands
first and only falls back to `Float64()` when at least one isn't an exact
integer, so `WHERE c.big = 9007199254740993` distinguishes that value from
`9007199254740992` correctly. Covered end-to-end by
`TestInMemoryBackend_Int64PrecisionPreservedThroughStorage`,
`TestBackend_SnapshotRestore_Int64PrecisionRoundTrip`, and
`TestExecuteQuery_Int64PrecisionInComparison`, at `math.MaxInt64`/
`math.MinInt64` boundaries.

### Aliasing and composite keys (the other two M2 bug classes)
`deepCopyBody`/`deepCopyJSONValue` (a full marshal/unmarshal round trip, not
a shallow `maps.Copy`) guards every document body that crosses the backend
boundary, so a caller mutating a map it passed in or got back can never
corrupt stored state. `documentCompositeKey` is a `{PartitionKeyJSON, ID}`
struct (persisted via `MarshalText` as a JSON string array), never a
delimiter-joined string, closing the same NUL-byte collision class
`services/azuretable`'s `entityCompositeKey` closes.

### Table API (M6)
Cosmos DB's Table API (`table_api.go`/`table_api_ops.go`) is a second wire
surface on this same fixed port (8081), added in AZURE.md section 9's M6
milestone. It is **not** a reimplementation: it delegates entity CRUD,
`$filter` parsing, and `$filter` evaluation entirely to `pkgs/odatatable`,
the exact same shared engine `services/azuretable` imports (see that
package's doc comment for the extraction this milestone performed). This
service's own `families.table_api` entry above covers what's new here
(routing, its own independent `TableBackend` instance, auth reuse); for the
engine's own known gaps -- pagination, `$select`'s PartitionKey/RowKey/
Timestamp behavior, the Edm.Double/Int32 unannotated-number ambiguity, `$batch`
deferral, and so on -- see **`services/azuretable/PARITY.md`** directly
rather than duplicating that list here: every gap recorded there applies
here identically, since it's the same code.

Table API state is part of `Handler.Snapshot`/`Restore`/`Reset`: the
`odatatable` snapshot rides in the backend snapshot under `tableApi`.

## More

- [Full parity audit](PARITY.md)
- [services/azuretable/PARITY.md](../azuretable/PARITY.md) -- the shared `pkgs/odatatable` entity/$filter engine's own known gaps
- [All services](../../README.md#services)
