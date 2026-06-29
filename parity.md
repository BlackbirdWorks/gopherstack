# gopherstack Parity Audit — DynamoDB + popular services

**Goal: 100% real AWS emulation (match and exceed LocalStack's open tier), starting with
DynamoDB and the most-used LocalStack-core services.**

This document is the live punch-list for the **DynamoDB family** (`dynamodb`,
`dynamodbstreams`, `dax`) plus the **popular services** (S3, Lambda, SQS, SNS, IAM, STS,
KMS, Secrets Manager, SSM, CloudFormation, CloudWatch, CloudWatch Logs, EventBridge,
Kinesis, Firehose, API Gateway v1/v2, EC2, ECR, ECS, Route 53, Step Functions, ElastiCache,
OpenSearch). Each is audited against four axes: **AWS-emulation parity**, **performance**,
**resource leaks**, and **UI/console coverage**. Every bullet is a concrete, code-cited
*remaining* gap (`file:line`). When an item lands, delete it from here. Fixed items are not
repeated as gaps; a compact `_Recently closed_` line per service records them so the history
is auditable without re-reading git.

**How this scan was produced.** Each service's Go sources under `services/<name>/` (handlers,
`*_ops.go`, janitor, persistence, evaluators, the DAX `dataplane/`) were read directly against
the AWS SDK v2 surface, and the Svelte console under `ui/src/routes/<name>/` was compared to
the backend operation surface. The previous full-fleet audit's findings were used to seed each
service so this pass verifies *current* code (fixed vs. still-present) rather than restating
stale claims.

**Working principles (apply to every fix):**
- **No stubs that lie.** An advertised op must mutate/return real state or return the
  AWS-accurate error. A success envelope over a no-op is a parity bug.
- **AWS error shape.** Distinct conditions need distinct codes — don't collapse
  not-found, conflict, and validation into one; JSON errors need `__type` in the right
  service namespace.
- **Opaque, mutation-stable pagination.** Tokens must survive concurrent insert/delete.
- **Bounded memory, clean lifecycle.** Every map that grows with traffic needs a cap or
  sweep; every goroutine/ticker needs context-aware shutdown.
- **Parse once per request, not once per item.** Expression lexing/parsing belongs above
  the item loop.

---

## Status at a glance

DynamoDB is the most complete service in the repo: control plane, item CRUD, batch,
transactions (now atomic with rollback), PartiQL, streams, backups, PITR, S3 export/import,
and global-table replicas are all **real, not stubs**. Since the previous audit a large
batch of parity gaps closed (see *Recently closed*). What remains is a focused tail —
mostly **async-lifecycle fidelity** (export/import/DAX states run synchronously), a few
**unreachable error classes** (`TransactionConflictException`), **streams shard/iterator
realism**, **DAX cache/TTL emulation**, and **read-hot-path allocations**. The console is
broad but still missing transactions, restore, native S3 export/import, post-create
capacity editing, and DAX mutations.

| Area | Parity | Performance | Leaks | UI |
|---|---|---|---|---|
| dynamodb | ~95% — tail of async/validation gaps | hot-path re-parse on key conditions; per-item size calc | clean (bounded + swept) | missing txn/restore/native export/capacity-edit |
| dynamodbstreams | ~80% — shard/iterator realism gaps | O(buffer)/O(n²) per poll | trim-horizon lost on compaction; ring aliasing race | viewer present (proprietary endpoints), no native Streams API |
| dax | ~75% — no async states, no cache | global attr-mutex; describe deep-copies | bounded now; reboot not restore-safe | strictly read-only; region-pinned to us-east-1 |

---

## dynamodb — remaining gaps

### Parity
- **`TransactionConflictException` is unreachable.** `TransactWriteItems`/`ExecuteTransaction`
  emit `TransactionCanceledException`/`TransactionInProgressException` but never
  `TransactionConflictException` for a concurrent conflicting transaction — the error class
  defined in `errors.go` has no emission site (`transact_ops.go:112,238`; `extra_ops.go`
  transaction path). Real DynamoDB raises it inside `CancellationReasons`.
- **Export/Import are synchronous; `ImportTable` never shows `IN_PROGRESS`.**
  `ExportTableToPointInTime` emits `IN_PROGRESS` then finalizes in the same call
  (`handler.go:1237-1255`, `completeExportSync` `handler.go:1292-1336`); `ImportTable` only
  ever stores `COMPLETED`/`FAILED` (`extra_ops.go:1665-1670`), so `DescribeImport`/
  `ListImports` can never observe in-flight state. AWS dwells in `IN_PROGRESS`.
- **`exportDescriptionFields.ExportTime` is derived from `EndTime`, not the requested
  point-in-time** (`store.go:947`). AWS reflects the user-supplied `ExportTime`/PITR point.
- **`ListImports` ignores `TableArn`** (filters only by region, `extra_ops.go:1747-1771`),
  and **silently clamps `PageSize` below 25** (`extra_ops.go:1739` only overrides when
  `*PageSize < 25`), so a caller asking for 50 gets 25.
- **`UpdateTable` does not validate throughput against billing mode.** Setting
  `ProvisionedThroughput` on a `PAY_PER_REQUEST` table (or vice-versa) is accepted with no
  error (`applyUpdateTableThroughput` `table_ops.go:1100-1112`; no
  `validateProvisionedThroughput` call in the update path). Some multi-mutation combos
  (throughput + GSI + replica + SSE in one request) are also still accepted
  (`table_ops.go:872-917`) — only BillingMode+GSI-in-one-call is rejected
  (`table_ops.go:862-867`).
- **Replica creation copies no items.** `applyOneReplicaTableEntry` uses `cloneTableSchema`,
  which produces an empty `Items` slice (`table_ops.go:974`), so a newly added replica
  starts empty instead of mirroring the source region's data.
- **Logical-keyword bare attribute names still mis-tokenize.** Function keywords (`size`,
  `contains`, …) are demoted to identifiers when not followed by `(` (`expr/lexer.go:162-164`),
  but logical/operator keywords used as bare names — `In`, `Add`, `AND`, `OR`, `BETWEEN`,
  `SET`, `DELETE`, `NOT` — are always tokenized as keywords (`expr/lexer.go:258-281`). A bare
  attribute named `In`/`Add` (legal when aliased, but commonly written bare) still breaks.
- **`compareAttributeValues` falls back to `fmt.Sprintf` for M/L-typed key values**
  (`item_ops.go:276-277`), so pagination disambiguation on map/list-typed index attributes is
  order-unstable. Minor (uncommon for index keys) but not byte-accurate.

### Performance
- **Key-condition expressions are re-parsed per item.** `scanPage`/`collectQueryPage`
  pre-parse `FilterExpression` and projection once and reuse across items
  (`item_ops_scan.go:253,301`; `item_ops_query.go:488-494`), but the key-condition path
  `allExprPartsMatch` still calls `evaluateExpression(part, …)` — re-lexing/re-parsing every
  AND-part for every candidate item (`item_ops_query.go:531-544`, via `filterUsingIndices:316`
  and `filterCandidatesScan:380`). For a known-PK query over many sort-key items this is
  N×(parts) full parses.
- **No cross-request AST cache.** `ExpressionCache` only caches key schema
  (`partiql:ks:<table>`), never parsed condition/projection ASTs (`expression_cache.go`,
  `item_ops.go:92-111`); identical expressions re-parse on every request.
- **`CalculateItemSize` recomputed per item on every Scan/Query page** for 1 MB-page
  accounting (`item_ops_scan.go:290`, `item_ops_query.go:504`) with no cached/incremental
  size — same O(n) cost as DescribeTable below, on the read hot path.
- **`DescribeTable` recomputes `estimateTableSizeBytes` over all items every call**
  (`table_ops.go:630` under `table.mu.RLock`; `item_ops_crud.go:991-999`). Real DynamoDB
  serves `TableSizeBytes`/`ItemCount` from maintained counters (~6 h stale), not a live walk.
- **`ExpressionAttributeValues` parsed multiple times per request.**
  `models.FromSDKItem(input.ExpressionAttributeValues)` runs in `preParseQueryPKValue`
  (`item_ops_query.go:577`), again in `filterCandidatesForKeyCondition` (`:229`), and again in
  `processQueryResults` (`:431`).
- **Parallel-scan `applySegmentFilter` allocates per item** — a fresh `fnv.New32a()` and
  `fmt.Sprintf("%v", …)` for every item (`item_ops_scan.go:355-358`).
- **`exportTableToS3` buffers the entire gzipped table in memory** before one `PutObject`
  (`import_export_s3.go:307-334`) on top of a full deep copy under `t.mu.RLock`
  (`snapshotItemsByTableARN:355-377`); **`importFromS3` calls `db.PutItem` once per item**
  (`import_export_s3.go:110-118` → `putImportedItem:406-422`). Fine for small tables, O(n)
  memory/locking for large ones.
- **`CreateBackup` deep-copies all items under `table.mu.RLock`** (`backup_interface.go:38-71`)
  and does an O(n) duplicate-name scan over all backups per create (`backup_interface.go:113-128`).
- **Scan/Query metadata copy.** Scan unconditionally shallow-copies the full `Items` slice plus
  all key schema/GSI/LSI/attr-def metadata every call (`item_ops_scan.go:84-97`); GSI/LSI and
  unknown-PK queries copy the entire `Items` slice (`item_ops_query.go:149-150`). The offset
  snapshot fast-path only applies to known-PK primary-table queries (`item_ops_query.go:146-151`).

### Leaks
- **No outstanding leaks.** Backups (cap 10 000, `store.go:129`), exports (5 000), imports
  (5 000), `txnTokens`/`txnPending` (100 000 with half-eviction, `janitor.go:24-26,381-383`),
  `exprCache` and `iteratorStore` (LRU+TTL, swept) are all bounded and swept. The janitor now
  runs on `worker.NewGroup` tickers with **per-tick panic recovery** (`janitor.go:80-87`;
  `pkgs/worker/group.go:128-136`), so a panicking sweep no longer kills the loop, and table
  timers are stopped on shutdown (`runTableCleaner:183`). *Steady-state cost to note, not a
  leak:* PITR snapshots deep-copy all items each janitor pass into a ring of `maxPITRSnapshots`
  (`janitor.go:146-150`), and stream Old/New images are retained per table until the 24 h
  compaction threshold — both bounded but memory-heavy for large/high-write tables.

---

## dynamodbstreams — remaining gaps

### Parity
- **Records never set `StreamViewType` or `SizeBytes`.** `buildSDKRecord` sets only
  `SequenceNumber` + `ApproximateCreationDateTime` (`streams_ops.go:715-751`); the source
  `models.StreamRecord` has no `SizeBytes`/`StreamViewType` (`models/types.go:315-324`) and
  `appendStreamRecord` never computes them (`store.go:343-364`). The wire structs carry the
  fields (`streams_wire.go:68-69`) but they always serialize empty.
- **Unknown/empty `ShardIteratorType` silently treated as `TRIM_HORIZON`** instead of
  `ValidationException` (`resolveStartSeq` default branch, `streams_ops.go:416-425`).
- **Legacy plaintext iterator `tableName:seq:timestamp` still accepted unconditionally**
  (`resolveIterator` `streams_ops.go:538-560`) — forgeable and cross-table (any table name
  accepted; only the timestamp TTL is checked). No prod/env gate. It also parses the seq as a
  raw int (`:544`) where opaque tokens use 20-zero padding, so the two formats interpret
  sequence numbers inconsistently.
- **`AT`/`AFTER` sequence numbers are only range-checked vs `trimSeq`, not the shard's
  start/end** (`streams_ops.go:389-414`); the shard-boundary params are passed but unused in
  that branch.
- **Shard lifecycle is fabricated from ring rotation.** `splitActiveShard` fires on the
  1000-write ring wrap (`store.go:388-402,404-420`) rather than the AWS ~4 h shard close, and an
  empty stream renders a placeholder open shard with an empty `StartingSequenceNumber`
  (`buildSDKShards` `streams_ops.go:253-263`). (`CreationRequestDateTime` and `StreamLabel` are
  now correctly populated — `streams_ops.go:218-248`.)
- **`eventID` is `<table>-<seq>`, not an opaque hash** (`store.go:344`).
- **`LimitExceededException` is defined but never returned** by any streams op
  (no emission site in `streams_ops.go`).
- **TTL-expiry `REMOVE` records carry no `userIdentity`.** Real DynamoDB tags TTL deletes with
  `principalId="dynamodb.amazonaws.com", type="Service"`; TTL deletes flow through the same
  `appendStreamRecord` with no identity (`streams_ops.go:720-721`).
- *(Fixed: the error `__type` namespace is now rewritten to `dynamodbstreams` at the handler
  boundary — `dynamodbstreams/handler.go:229-233`.)*

### Performance
- **`GetRecords` re-parses every sequence number and linearly skips below `startSeq` per
  call** — O(buffer) every poll (`appendMatchingRecords` `streams_ops.go:1003-1019`).
- **`ListStreams` insertion sort is O(n²)** (`sortStreamListEntries` `streams_ops.go:624-630`),
  with a per-table `RLock` in `collectEnabledStreams` (`streams_ops.go:672-675`).
- **`DescribeStream`/`GetShardIterator` copy the full shard slice per call**
  (`streams_ops.go:173-174,337-338`).

### Leaks
- **24 h compaction loses the trim horizon.** `sweepStreamRecords` resets the ring
  (`StreamRecords = make(...)`, `StreamHead = 0`) but never advances `streamTrimSeq`
  (`janitor.go:644-647`), so post-compaction `GetRecords` mis-evaluates trimmed-data checks
  instead of returning `TrimmedDataAccessException`.
- **Data race: `streamRecordsInOrder` returns sub-slices aliasing the live ring backing
  array** (`store.go:452-465`); `GetRecords` reads them after `RUnlock` (`streams_ops.go:482-494`)
  while a concurrent `overwriteRingSlot` (`store.go:382`) writes the same array.
  `GetRecentEvents` (`streams_ops.go:638-646`) shares the root cause via a second call site.

---

## dax — remaining gaps

### Parity (control plane)
- **No lifecycle state machine — clusters are born `available`.** `CreateCluster` sets
  `StatusAvailable` immediately (`backend.go:416`, nodes `:357`); `Creating`/`Modifying`/`Failed`
  are defined (`models.go:9-13`) but never assigned.
- **`DeleteCluster` is synchronous and destructive.** It stamps `StatusDeleting` on the returned
  copy only, then immediately `delete(b.clusters,…)` (`backend.go:623-650`); a poll-until-deleted
  client gets `ResourceNotFound` instantly instead of observing the transient `deleting` status.
- **`Increase`/`DecreaseReplicationFactor` mutate instantly with no `MODIFYING` status** — nodes
  are appended/truncated synchronously while `cluster.Status` stays `available`
  (`backend.go:706-721,779-785`). Node-index reuse (`%s-%04d` at `backend.go:703`, numbering from
  `len(cluster.Nodes)`) can also produce duplicate NodeIDs after a decrease-then-increase.
- **Subnet validation is cosmetic.** `subnetEntriesFromIDs` accepts any string and assigns AZ
  `<region>a` (`backend.go:1651-1662`); `vpcIDFromSubnets` fabricates a VPC from string munging
  (`backend.go:1708-1724`). No existence/format validation.
- *(Fixed: AZ-indexing bug in `IncreaseReplicationFactor` `backend.go:699-701`;
  `UpdateParameterGroup` now marks dependent clusters `pending-reboot` with `NodeIDsToReboot`
  `backend.go:1127-1137`; `RebootNode` empty-NodeId now returns `InvalidParameterValue`
  `backend.go:799-801`.)*

### Parity (data plane)
- **DAX caching/TTL is not emulated — pure pass-through to the live DynamoDB backend**
  (`dataplane/server.go:76-88`, `ops.go:172`). `query-ttl-millis`/`record-ttl-millis` are stored
  and validated in param groups (`models.go:75-76,140-149`) but never consulted; every read hits
  DynamoDB fresh. This is the single biggest DAX-specific behavior AWS users depend on (stale
  reads within TTL) and it is absent.
- **`GetItem` ignores `ProjectionExpression`** — returns the full item (`ops.go:163-194`).
  (Query/Scan/TransactGet do project — `update_query_scan.go:290-292`, `transact.go:461`.)
- **Transact/Batch responses omit metadata** — `ConsumedCapacity` and `ItemCollectionMetrics`
  are never encoded (item-response writers `ops.go:347-406`); `UnprocessedItems`/`UnprocessedKeys`
  for batch ops have no encoding path.
- *(Fixed: `ConsistentRead` on `GetItem` `ops.go:60-66,175`; typed error mapping —
  `writeBackendError` now distinguishes ConditionalCheckFailed / ResourceNotFound /
  TransactionCanceled / TransactionConflict / ProvisionedThroughputExceeded /
  ItemCollectionSizeLimitExceeded `ops.go:409-441`, with ValidationException only as fallback.)*

### Performance
- **`attrListID` serializes all connections on one global mutex per non-key write**
  (`dataplane/control.go:257`); the sort/join now happen outside the lock
  (`idForAttrNames` `control.go:300-306`) so hold time is just a map op, but it is still a single
  global `attrMu` on every item-write response across all connections.
- **`Describe*` deep-copy every record under `RLock`** before paginating
  (`paginateClusters` `backend.go:527-528` under the DescribeClusters RLock `:540`; same for
  param/subnet groups).
- **`decodedExpression.nameRef` linear scan** (prior O(n²)) — unverified this pass after the
  expression code moved to `dataplane/projection.go`/`expression.go`; flag to re-check.

### Leaks
- **Reboot recovery is not restore-safe.** `RebootNode` spawns a goroutine that flips the node
  back to `available` after 1 s (`backend.go:838-855`), but a snapshot taken mid-reboot restores a
  node stuck in `StatusRebooting` with no goroutine rescheduled on `Restore`
  (`persistence.go:138`). In-memory-only recovery.
- *(Fixed/bounded: `attrToID`/`idToAttr` now capped at 65 536 with refuse-on-overflow
  `server.go:64-71`, `control.go:267-273`; the schema cache was removed — `schemaFor` fetches live
  via DescribeTable every call `server.go:440-449`, so drop/recreate no longer mis-encodes keys;
  rebooted nodes recover via the goroutine above.)*

---

## UI / console — remaining gaps

The DynamoDB console is broad: ListTables + DescribeTable, Create/Delete/Purge, a 12-tab table
detail (Overview, Query, Scan, Items, Indexes, Stream Events, PartiQL, Metrics, Backups, PITR,
Replicas, Tags), item CRUD with BatchWrite delete, PartiQL editor, backup create/list/delete,
PITR toggle, TTL config, streams enable + shard/lag/event viewer, replica add/remove, tags,
deletion protection, GSI create **and delete**. A lighter `table/[tableName]` route mirrors a
subset. DAX has a read-only Clusters / Parameter Groups / Subnet Groups page. Remaining gaps:

- **No transactions UI.** No `TransactWriteItems`/`TransactGetItems`/`ExecuteTransaction`/
  `BatchExecuteStatement` anywhere in `ui/src` (backend supports all — `extra_ops.go`,
  `transact_ops.go`).
- **No backup restore wiring.** The Restore button is still a no-op toast — `onclick={() =>
  toast.success('Restore not supported in local emulator')}` (`dynamodb/+page.svelte:1851`).
  Backend supports `RestoreTableFromBackup`/`RestoreTableToPointInTime`.
- **No native S3 Export/Import.** "Export" is a client-side blob download and "Import" a per-item
  `PutItem` loop (`+page.svelte:220-228,760-775`); no `ExportTableToPointInTime`/`ImportTable`/
  `ListExports`/`ListImports` SDK calls, despite full backend support.
- **No post-create capacity/billing-mode editing.** Billing mode/RCU/WCU are settable only at
  create (`+page.svelte:264-272`); detail tabs only display them. `UpdateTable` is wired for
  streams/deletion-protection/replicas/GSI but never for `BillingMode`/`ProvisionedThroughput`.
- **No GSI throughput Update.** GSI create + delete are wired (`+page.svelte:894-924`,
  `deleteGsi:879-892`) but `GlobalSecondaryIndexUpdates:[{Update}]` (per-index provisioned
  throughput) has no UI path.
- **No attribute-level `UpdateItem` and no `BatchGetItem` read.** Item edits go through a
  full-item `PutItem` (`saveEditItem:777-791`) — no `UpdateItem` with `UpdateExpression`/
  `ConditionExpression`; the only batch path is BatchWrite-for-delete, no multi-key get.
- **Query/Scan tabs don't paginate.** `executeQuery`/`executeScan` send one `Limit` request and
  never use `LastEvaluatedKey`/`ExclusiveStartKey` (`+page.svelte:340-386`); only the Items tab
  paginates. Same in the `[tableName]` route (`runQuery:127-163`).
- **DAX page is strictly read-only** — imports only the three `Describe*` commands
  (`dax/+page.svelte:5-11`); no Create/Update/Delete cluster, replication-factor controls, or
  param/subnet-group mutations, all of which the backend supports (`dax/handler.go:151-189`).
- **DAX console is pinned to `us-east-1`.** `getDAXClient` defaults to `clientConfig(region)`
  with `region = defaultRegion` and never calls `getStoredRegion()` (`aws-client.ts:719-721`,
  contrast `aws/client.ts:50-52`); the DAX page builds the client once at load with no region
  arg and registers no `gopherstack:region-change`/`storage` listener (`dax/+page.svelte:15`), so
  it ignores the region switcher.
- **No native DynamoDB Streams API in the console.** The Stream Events tab reads proprietary
  `/stream-info` + `/stream-events` endpoints (`+page.svelte:706-717`) and shows the amber
  "DynamoDB Streams polling backend is not configured" warning when unavailable
  (`+page.svelte:1619-1629`); there is no `GetShardIterator`/`GetRecords`/`DescribeStream`
  fallback, so records are unviewable when that backend is off.

---

## Recently closed (since previous audit — for history)

**dynamodb:** PITR snapshots now fire in the production janitor loop (restore reads a populated
ring); GSI/LSI `LastEvaluatedKey` now includes base-table PK and resumption disambiguates by
base key; PartiQL `NextToken`/`ORDER BY`/`DuplicateItemException`/error-codes implemented;
`contains()` does real SS/NS/BS/L membership; backup/restore preserves GSI/LSI/BillingMode/SSE/
StreamSpec and honors restore overrides; `DescribeContinuousBackups` populates Earliest/Latest;
`CreateTable` stores+surfaces `OnDemandThroughput` and `validateProvisionedThroughput` rejects
mismatched billing/throughput; `validateComplexValue` recurses into `L`; `ExecuteTransaction` is
atomic with snapshot rollback and honors `ReturnConsumedCapacity`; `UpdateTimeToLive` validates
empty/missing spec. **Performance/leaks:** `compareValues` dropped the per-compare `%T`
formatting; the orphaned GSI-status `AfterFunc` on delete is now stopped; the janitor survives a
panicking sweep. **dynamodbstreams:** `CreationRequestDateTime`/`StreamLabel` populated; error
`__type` rewritten to the `dynamodbstreams` namespace. **dax:** AZ-indexing bug fixed;
`UpdateParameterGroup` marks dependents pending-reboot; empty-NodeId returns the right error;
typed data-plane errors no longer collapse to `ValidationException`; `ConsistentRead` honored on
`GetItem`; `attrToID` map bounded; schema cache removed (no stale-after-recreate); rebooted nodes
recover. **UI:** GSI delete wired; stream shard/record viewer added; `UpdateTable` wired for
streams/deletion-protection/replicas.

---

# Popular services — remaining gaps

Same four axes, same code-cited rule. These services are largely complete (most prior-audit
gaps are now fixed — see each `_Recently closed_` line); what follows is the remaining tail to
reach full LocalStack parity and beyond. Highest-leverage themes across the fleet: a few
**lifecycle state machines** still resolve instantly (`elasticache`, `opensearch`, parts of
`ssm`), some **async failure/retry paths** drop instead of routing to DLQ/destinations
(`lambda`, `sns`, `firehose`), a handful of **non-opaque pagination tokens** remain, and the
**console still trails the backend** on advanced ops in several services.

## Storage & compute

### s3
- **Parity:** `GetObjectAttributes` ignores the `X-Amz-Object-Attributes` header and always returns
  the full set (`handler_stubs.go:230-281`).
- **Performance:** lifecycle transition sweeps hold `bucket.mu.RLock` across the whole O(n) object
  scan, blocking writers (`janitor.go:1009-1010,1083-1084`); `dispatchAccessLog` does a synchronous
  `GetBucketLogging` inline on every logged request before spawning (`access_log.go:37-56`).
- **Leaks:** notification dispatch and access-log dispatch use untracked fire-and-forget goroutines,
  not drained at shutdown (`object_ops.go:374,480,934,1031`; `multipart_ops.go:267`;
  `access_log.go:58`).
- **UI:** no console for S3 Select (`?select`), RequestPayment, Transfer Acceleration (`?accelerate`),
  RestoreObject (`?restore`), Object Lambda, or Directory Buckets.
- _Recently closed:_ ABAC/inventory/journal/directory-bucket no-ops now persist; default-multipart and
  noncurrent-version sweeps de-locked; replication goroutine cancels+waits on Shutdown; lifecycle/CORS/
  policy/versioning/encryption/ACL/object-lock/replication/tagging/batch-delete UI all added.

### lambda
- **Parity:** async failure path never delivers to DLQ or `DestinationConfig` (OnFailure/OnSuccess) —
  config is stored but no dispatch exists (`backend.go:2127-2130,507`; `handler.go:1903`). `LogType=Tail`
  unsupported — `X-Amz-Log-Type` never read, no base64 `X-Amz-Log-Result` header (`handler.go:1955-1973`).
  `X-Amz-Client-Context` ignored.
- **Performance:** `withInvocationChain` allocates a fresh slice per invocation on the hot path
  (`backend.go:116-122`; documented as intentional, minor).
- **Leaks:** none material remaining.
- **UI:** no console for reserved/provisioned concurrency, event-invoke-config/DLQ/destinations,
  code-signing, layer-version permissions, runtime-management, Function-URL update + invoke-via-URL, ESM
  update, recursion/scaling config, SnapStart.
- _Recently closed:_ real vnd.amazon.eventstream framing; true fire-and-forget async invoke; ESM health
  sweeps; `activeConcurrencies` zero-delete; `cleanupSem`/`logSem` Reset-safe.

### ec2
- **Parity:** ~389 ops still return bare `stubResponse{Return:true}` (CreateVpnConnection, all IPAM,
  TrafficMirror, LocalGateway, CapacityReservation, VerifiedAccess) — `handler_stubs.go:8-13,1622`.
  `DescribeInstances`/`DescribeImages`/`DescribeInstanceTypes` NextToken is base64-wrapped but still a raw
  decimal offset, decodable/forgeable (`handler.go:610-618,631`; `handler_ext.go:752`).
- **Performance:** `DeleteVpc` still does two full-map O(n) scans under the write lock — ENIs
  (`backend.go:1164`) and NAT gateways (`backend.go:1141`) — instead of per-VPC indexes.
- **Leaks:** none (terminated-instance/cancelled-spot janitor TTLs; lifecycle goroutine ctx-cancel).
- **UI:** console covers only core + VpcEndpoint; CreateFleet, transit gateways, IPAM, traffic mirror,
  VPN, verified access, reserved instances, dedicated hosts, capacity reservations, network insights have
  backend state but no UI.
- _Recently closed:_ lifecycle ctx-cancel; real CreateFleet; DescribeImages pagination; DeleteVpc
  secondary-index cascades; correct EC2 error XML shape; persistence covers new resource maps.

### ecr
- **Parity:** `DescribeImages` nextToken is the raw `imageDigest`, not opaque (`handler.go:988`);
  `GetAuthorizationToken` returns a constant `dummy-password` for every registry (`handler.go:34,720`;
  intentional for deterministic `docker login`).
- **Performance / Leaks:** none remaining.
- **UI:** `GetAuthorizationToken` has no console surface.
- _Recently closed:_ opaque tokens for DescribeRepositories/ListImages/PullThroughCache; ScanFrequency
  modes; LayerInaccessibleException; all three O(n) scans indexed; lifecycle/repo-policy/scan/pull-through UI.

### ecs
- **Parity:** `ExecuteCommand` returns a synthetic non-connectable SSM `StreamURL`/`TokenValue` with no
  honesty signal (`backend_ext.go:679-687`); `DiscoverPollEndpoint` returns synthetic hosts
  (`handler_stubs.go:492`).
- **Performance:** `Purge` holds the write lock across a full nested scan of every task-def family/revision
  (`backend.go:404-439`).
- **Leaks:** `Reconciler.sems` grows one entry per cluster, never deleted (`reconciler.go:39-50`);
  `realDockerRunner.containers` keeps stale entries for containers that exit on their own
  (`docker_runner.go:120-122,283`).
- **UI:** no RunTask/StopTask actions, no Daemon ops, no ExecuteCommand (read views + UpdateService only).
- _Recently closed:_ Daemon CRUD backend-backed; Submit*StateChange mutate real state; real Docker task
  runner; capacity providers with managed scaling; cached cluster counters.

## Messaging & streaming

### sqs
- **Performance:** `computeMD5OfMessageAttributes` re-sorts+re-encodes all attributes on every receive
  rather than memoizing at send (`backend.go:427-453`; low severity).
- **Parity / Leaks / UI:** none remaining — region-scoped URL lookup, activity-filtered prune, and the
  message-move-task panel are all in place.
- _Recently closed:_ region-scoped `lookupQueueByURL`; activity-filtered `pruneState`; move-task UI.

### sns
- **Parity:** HTTP/HTTPS delivery is fire-once with no AWS retry/backoff on 5xx — failures go straight to
  DLQ or are dropped (`backend.go:2925-2937`).
- **Leaks:** `smsDeliveries`/`applicationDeliveries`/`emailDeliveries` slices append on every delivery and
  are only cleared by `Reset()`/test drains — no age-based purge (`backend.go:2262,4229-4231`;
  `lambda_firehose_delivery.go:181`; `Purge` at `4105-4119` skips them).
- **UI:** SMS-sandbox ops (Create/Verify/Delete/List SandboxPhoneNumber) and opt-out ops are docs-only,
  no interactive panel (`+page.svelte:1033-1036`).
- _Recently closed:_ persisted SMS-sandbox state; O(1) dedup eviction; fifoSeqNums delete-on-DeleteTopic;
  platform-app + filter-policy UI.

### eventbridge
- **Parity:** `PutEvents`/`PutPartnerEvents` always return `FailedEntryCount: 0` even when entries carry
  an `ErrorCode` (`handler.go:673-676,1434-1437`). API-destination target ARNs fall through to a `default`
  warn branch and are silently dropped (`delivery.go:418-421`). InputTransformer string-variable
  substitution doesn't quote/escape, can emit invalid JSON (`delivery.go:627-632`). `ListRules`/
  `ListTargetsByRule` silently ignore the request `Limit` (`handler.go:54,88` defined, dropped at
  `556-561,649-654`; backend has no limit param `backend.go:967,1173`).
- **Performance:** `PutEvents` holds the write lock across `captureEventInArchives`, which recompiles each
  pattern per event via `matchPattern`/`compilePattern` rather than the cache (`backend.go:1211,1251,2865`).
- **Leaks:** none material (tag map cleared on bus/rule delete; patternCache reset + janitor-cleared).
- **UI:** Pipes, Endpoints, Schema Registry, PutPermission/RemovePermission, DescribeReplay,
  ListApiDestinations are docs-only, no controls (`+page.svelte:320-346`).
- _Recently closed:_ opaque base64 nextToken; per-account bus quota; ListEventBuses Limit; worker
  ctx-cancel on Shutdown; patternCache Reset-safe; archives Replay + connection CRUD UI.

### kinesis
- **Parity:** `CreateStream` accepts an invalid non-empty `StreamMode` string verbatim (no reject)
  (`backend.go:395-410`).
- **UI:** Split button passes the shard's own `StartingHashKey`, which the backend always rejects — should
  pass the range midpoint (`+page.svelte:406`; `backend.go:1791`); Merge picks an arbitrary non-adjacent
  shard, also rejected (`+page.svelte:396`; `backend.go:1716`); GetRecords hardcodes TRIM_HORIZON + one
  page, no `NextShardIterator` follow-up (`+page.svelte:167`); no UI for PutRecords/UpdateShardCount/
  consumers (enhanced fan-out)/tags/encryption/retention/stream-mode; "Consumers" stat hardcoded `0`.
- **Performance / Leaks:** none remaining (read-lock retention sweep; janitor Stop()).
- _Recently closed:_ handler-tag cleanup on every delete path; read-lock retention sweep; janitor shutdown;
  kinesis UI added (basic).

### firehose
- **Parity:** Lambda transform only runs for the S3 destination — HTTP/Redshift/OpenSearch/Splunk carry
  `ProcessingConfiguration` but never invoke it (`backend.go:970-999`). Transform/delivery failures
  silently drop records (buffer cleared before delivery, no retry/backup-prefix routing; `FailedRecords`
  metric never written) (`backend.go:961-980,293`). Record-format conversion (Parquet/ORC),
  DynamicPartitioning, CloudWatchLogging, S3 KMS, and `FileExtension` are stored but inert
  (`backend.go:165,171-172,1115-1130`). `UpdateDestination` can't switch/clear a destination type and skips
  the version check when `currentVersionID == ""` (`backend.go:708-730`). `ListDeliveryStreams` ignores the
  `DeliveryStreamType` filter (`handler.go:681`).
- **Performance:** `intervalFlusher` scans every region×stream each 1s tick regardless of activity
  (`backend.go:775-804`); `ListDeliveryStreams` re-sorts all names every call (`backend.go:562`).
- **Leaks:** `NewInMemoryBackend` defaults `svcCtx` to `context.Background()`, so deliveries dispatched
  via `b.svcCtx` are unbounded if the backend is built without a context (`backend.go:346-357,609,674`).
- **UI:** no Tag/Untag/ListTags or UpdateDestination controls; HTTP/Splunk destinations don't render
  (`+page.svelte:6-16,367-453`).
- _Recently closed:_ Start/Stop encryption persist real config; ListDeliveryStreams pagination; all-five
  destination UpdateDestination; encryption shown in DescribeDeliveryStream.

## Identity & security

### iam
- **Parity:** `SimulateCustomPolicy` ignores permission boundaries (no `AllowedByPermissionsBoundary`),
  disagreeing with `SimulatePrincipalPolicy` which intersects them (`backend_refinement.go:615` vs
  `backend.go:2322-2339`). `GetCredentialReport` returns canned `no_information` for last-used columns
  (`backend_refinement2.go:252-306`).
- **Leaks:** `DeletePolicy` never deletes `policyVersionCounters[arn]` or `deletedV1Policies[arn]`; `Reset`
  also omits `deletedV1Policies` — repeated create/delete grows both maps (`backend.go:863-907,2506-2512`).
- **UI:** policy simulation and credential-report are docs-only, no interactive panel (`+page.svelte:598,600`).
- _Recently closed:_ distinct not-found sentinels; real policy-evaluation engine with boundary
  intersection; O(1) pagination indexes; IAM UI CRUD.

### sts
- **Parity:** `DecodeAuthorizationMessage` falls back to decoding any base64 blob after the self-issued
  check fails (`handler.go:583-594`); `validateSAMLAssertion` checks base64 only, not well-formed SAML XML
  (`backend.go:1153-1165`).
- **UI:** no interactive form for AssumeRoleWithSAML / AssumeRoleWithWebIdentity / AssumeRoot /
  GetDelegatedAccessToken / GetAccessKeyInfo — counters only (`+page.svelte:362-372`).
- **Performance / Leaks:** none remaining.
- _Recently closed:_ ASIA-key GetCallerIdentity returns InvalidClientTokenId/ExpiredToken;
  GetWebIdentityToken removed from supported ops; SAML base64 validation; self-issued message verification;
  STS panels (AssumeRole/SessionToken/Federation/Decode).

### kms
- **Parity:** `PutKeyPolicy` stores the policy verbatim without JSON parse/validation, so
  `MalformedPolicyDocumentException` is never produced (`backend.go:2762-2791`; `handler.go:1043-1064`).
- **UI:** `DescribeCustomKeyStores` and `GetKeyLastUsage` have no console surface.
- **Performance / Leaks:** none remaining.
- _Recently closed:_ alias-resolution cache evicted on disable/delete; `lastUsage` purge; O(1)
  `clearResolutionCache`; full KMS UI (key-store/rotation/sign-verify/grants/policies/import).

### secretsmanager
- **Parity:** `ListSecrets`/`ListSecretVersionIDs`/`BatchGetSecretValue` tokens are plain `strconv.Itoa`
  offsets (`backend.go:814,973,2020`); reusing a `ClientRequestToken` with different content overwrites the
  version instead of returning `ResourceExistsException` (`backend.go:48-56`).
- **Performance:** `GetSecretValue` takes the full write lock just to stamp `LastAccessedDate`, serializing
  reads (`backend.go:423`); tag filters `Clone()` the whole tag map per secret per `ListSecrets`
  (`backend.go:883,899`).
- **Leaks:** rotation scheduler runs a 1s ticker doing an O(n) all-secrets scan every tick regardless of due
  rotations (`backend.go:105,2471,2503-2509`).
- **UI:** no coverage for PutSecretValue, resource-policy ops, BatchGetSecretValue, GetRandomPassword,
  ReplicateSecretToRegions, StopReplicationToReplica.
- _Recently closed:_ `ValidateResourcePolicy` parses JSON + checks Version/Statement; same-token
  same-content idempotency.

## Orchestration & APIs

### stepfunctions
- **Parity:** task history events (`RecordTaskScheduled/Succeeded/Failed`) emit empty detail payloads (no
  resource/parameters/output/cause), so `GetExecutionHistory` is lossy (`backend.go:1213-1235`).
- **Leaks:** `mapRuns`/`execMapRuns` are never pruned — `pruneExecutionsLocked` and `DeleteStateMachine`
  omit them, only `Reset()` clears them, so Map-state executions leak MapRun memory
  (`backend.go:621-647,1542-1543`).
- **Performance / UI:** none remaining (status-bucketed ListExecutions; two-phase token sweep; sfn route
  with StartExecution/CreateStateMachine/history/Redrive/Activity/version-alias/Express).
- _Recently closed:_ TestState executes the state; MapRun stored; prune tombstones; handler tags cleared;
  full sfn UI.

### apigateway (v1)
- **Parity:** `runRequestValidator` only checks `json.Valid`, never validates against the model schema
  (`proxy.go:700`); non-opaque integer-index pagination, default page size 500 vs AWS 25
  (`backend.go:289-309`); `GetUsage` returns empty `Items` (`backend.go:3422-3427`); backend
  `TestInvokeMethod`/`TestInvokeAuthorizer` are hardcoded mocks (`backend.go:2766-2772,3180-3187`); canned
  stubs persist (GetSdk/GetSdkType(s), ImportApiKeys, DomainNameAccessAssociations,
  `handler_stubs.go:245-284`); `FlushStageCache` is a no-op (`handler.go:3357-3366`).
- **Performance:** `dispatch` rebuilds the full op→handler table via ~20 sub-constructors + `maps.Copy` on
  every request (`handler.go:2972,2778-2795`); proxy rebuilds the resource-path trie per data-plane request
  (`proxy.go:1390-1393`); `GetAPIKeyByValue` is a linear scan per apiKey-required request (`backend.go:1899`).
- **Leaks:** `selRegexpCache` keyed by user patterns has no eviction (`handler.go:640`; `proxy.go:1093-1109`).
- **UI:** resources/methods/integrations read-only; no CreateResource/PutMethod/PutIntegration/createStage/
  createAuthorizer/createRequestValidator/TestInvokeMethod forms; no export/base-path-mapping/gateway-
  response/client-cert/VPC-link UI.
- _Recently closed:_ handler `testInvokeMethod` executes MOCK templates + Lambda integrations;
  FlushStageAuthorizersCache actually flushes.

### apigatewayv2
- **Parity:** `ExportApi` shape wrong (wraps spec instead of returning the raw OpenAPI body) and omits
  `components`/`securitySchemes` (`handler.go:1446`; `backend.go:~3255-3290`); `CreateApiMapping` doesn't
  reject a duplicate mapping key on a domain (`backend.go:1552-1591`); errors are `{"message"}` with no
  `x-amzn-ErrorType` header (`handler.go:626,651,665`; `models.go:413`); authorizationType enum not strictly
  validated.
- **Performance:** sub-resource dispatch maps rebuilt per request (`handler.go:698-740`); `Snapshot`
  marshals the whole backend under RLock (`persistence.go:42-72`); create ops do O(n) duplicate-key scans
  under write lock.
- **Leaks:** `DeletePortalProduct` leaves `portalProductSharingPolicies` entry behind (`backend.go:3080-3093`).
- **UI:** no Models, Integration/Route Responses, Api Mappings, Routing Rules, Portals,
  Import/Reimport/Export, ResetAuthorizersCache; hardcoded `supportedOperations` diverges from backend
  (`+page.svelte:180-183`).
- _Recently closed:_ ImportApi/ReimportApi parse OpenAPI and apply routes/integrations; real page.New
  pagination; ProtocolType enum validated; v2 create/delete UI for routes/stages/integrations/authorizers/
  deployments.

## Management, config & data

### ssm
- **Parity:** `DescribeInventoryDeletions` is a stub returning `[]` (`backend_ops.go:413`);
  `ListDocumentMetadataHistory` returns empty `ReviewerResponse` (`backend_ops.go:120`);
  `GetDeployablePatchSnapshotForInstance`/`GetDefaultPatchBaseline` synthesize fake IDs/URLs ignoring stored
  state (`backend_ops.go:989,717`); `DescribeAssociationExecutions/Targets` fabricate a fresh UUID
  ExecutionID per call, so results aren't stable/queryable (`backend_batch2.go:992,1027`).
- **Leaks:** terminated sessions marked `Terminated` but never evicted from `sessionsStore`, no cap
  (`backend_stubs.go:1664`).
- **UI:** only Parameter Store + Maintenance Windows tabs; no parameter history/labels, SecureString
  decrypt, Documents, Run Command, Sessions, Automation, Patch baselines, OpsItems, Inventory, Compliance,
  Associations (`+page.svelte:1-198`).
- _Recently closed:_ real per-instance AES-256 KMS encryptor; batch2 outputs populate stored data; empty
  region sub-maps GC'd; on-write expiry moved to janitor.

### cloudformation
- **Parity:** change-set `computeChanges` emits only Add/Modify, never Remove, so `DescribeChangeSet`
  under-reports deletions (runtime UpdateStack still deletes them — preview-only gap)
  (`backend.go` computeChanges); `Fn::ForEach` (Languages Extensions) unsupported (`template.go`; all other
  intrinsics present).
- **Performance / Leaks:** none remaining.
- **UI:** no type/registry management, resource scans, generated templates, stack refactors, hook results,
  stack-instance detail, nested-stack tree, or SignalResource/RollbackStack/ContinueUpdateRollback.
- _Recently closed:_ 183 resource types; YAML+JSON parse; nested stacks; real drift; Add/Modify change sets;
  export collision detection; change-sets/drift/stack-sets/policy UI.

### cloudwatch
- **Parity:** `GetMetricWidgetImage` returns a hardcoded 1×1 PNG (`handler.go:2655-2657`);
  `DescribeAlarmContributors` always empty (`backend.go:2455-2475`); EC2 (`arn:aws:automate:`) and
  AutoScaling alarm actions are logged-and-skipped — only SNS + Lambda fire (`backend.go:1791-1800`);
  `GetMetricData` doesn't paginate (`backend.go:915`).
- **UI:** no insight rules / contributor insights, alarm mute rules, managed insight rules, or
  GetMetricWidgetImage (`+page.svelte:44`).
- **Performance / Leaks:** none remaining (two-phase metric sweep; bounded metric storage; ticker-based
  alarm eval).
- _Recently closed:_ MetricStreams/AnomalyDetectors/MetricFilters UI; real mute-rules/managed-insight-rules;
  sweep + stream-delivery lock contention fixed; tag Close() leak fixed.

### cloudwatchlogs
- **Parity:** 5 list ops still emit raw `strconv.Itoa` tokens despite a base64 helper existing —
  DescribeLogAnomalyDetectors/ListScheduledQueries/account-policies/DescribeMetricFilters/query-definitions
  (`backend.go:3134,3238,3362,3488,3659`); `StopQuery` is cosmetic since queries complete synchronously
  (`backend.go:2393`).
- **Performance:** `collectQueryEvents` does an O(events) full scan per query under RLock
  (`backend.go:2254`); `retentionTargets` allocates O(regions×groups) per janitor tick (`janitor.go:106`).
- **UI:** no anomaly detectors, scheduled queries, account policies, deliveries, index policies, transformers.
- **Leaks:** none remaining (bounded event storage; bounded subscription-delivery goroutines; capped
  compiled-pattern cache).
- _Recently closed:_ ListAnomalies/GetScheduledQueryHistory/UpdateAnomaly real; BytesScanned computed;
  two-phase retention sweep; opaque base64 tokens; subscription/metric-filter/query-def/export-task UI.

### route53
- **Parity:** `TestDNSAnswer` ignores `Weight` for weighted routing (returns first by `SetIdentifier`
  sort) and resolves alias records to the literal `AliasTarget.DNSName` string without recursing or
  consulting `EvaluateTargetHealth` (`backend.go:2818-2821,2867-2880`).
- **Performance / Leaks / UI:** none remaining — HealthCheck/TrafficPolicy/CidrCollection/DelegationSet/
  QueryLogging/DNSSEC/KeySigningKey all have console coverage; counts are O(1).
- _Recently closed:_ backend tag ops + batch ListTags; ListHostedZonesByName/ByVPC filtering;
  health-check last-failure observations; count ops; missing-op UI.

### elasticache
- **Parity:** cluster/replication-group/snapshot lifecycle is instantaneous — status set straight to
  `available`, no `creating`/`modifying`/`deleting`/`snapshotting` transitions, so SDK waiters never see
  intermediate states (`backend.go:845,1176,1412,1685`).
- **Performance:** `createClusterLocked` calls `miniredis.Start()` while holding `b.mu.Lock`, serializing
  all backend ops behind listener startup (`backend.go:869,896`).
- **Leaks / UI:** none remaining (Reset closes miniredis; updateActions capped; serverless/global-RG/
  user-group/reserved-node UI present).
- _Recently closed:_ reserved-offerings seeded; updateActions capped; Reset closes miniredis; four UI ops.

### opensearch
- **Parity:** domain endpoint is a cosmetic `search-…es.amazonaws.com` string with no real/proxied search
  service, so `_search`/indexing is non-functional (`backend.go:585,600,2692`); no lifecycle —
  `Status:"Active"` immediately, `Processing:false` hardcoded, `Created`/`Deleted` booleans omitted
  (`handler.go:655-662,1418-1419`); `CancelServiceSoftwareUpdate` returns canned `CANCELLED`, mutates
  nothing (`backend.go:1030-1037`); `DeleteDomain` doesn't clear domainIndexes/scheduledActions/
  reservedInstances (`backend.go:649-666`).
- **UI:** no inbound/outbound connections, data sources, VPC endpoints, serverless collections, scheduled
  actions, reserved instances, auto-tune, dry-runs.
- **Performance / Leaks:** none remaining.
- _Recently closed:_ SAML JSON tags; AcceptInboundConnection not-found; AssociatePackages validation;
  DeleteDomain cascade; CancelDomainConfigChange mutates state; single-lock ListDomainNames; O(1)
  CreateApplication.
