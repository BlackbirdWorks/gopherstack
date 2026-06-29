# gopherstack Parity Audit — DynamoDB deep dive

**Goal: 100% real AWS emulation for DynamoDB (match and exceed LocalStack's open tier),
then carry the same bar to the other popular services.**

This document is the live punch-list for the **DynamoDB family** — `dynamodb`,
`dynamodbstreams`, and `dax` — audited against four axes: **AWS-emulation parity**,
**performance**, **resource leaks**, and **UI/console coverage**. Every bullet below is a
concrete, code-cited *remaining* gap (`file:line`). When an item lands, delete it from here.
Items that were in the previous audit and have since been fixed are not repeated; the
"Recently closed" section records them so the history is auditable without re-reading git.

**How this scan was produced.** The Go sources under `services/dynamodb/`,
`services/dynamodbstreams/`, and `services/dax/` (handlers, `*_ops.go`, janitor,
persistence, the `expr/` evaluator, and the DAX `dataplane/`) were read directly against
the AWS SDK v2 surface, and the Svelte console under `ui/src/routes/dynamodb/` and
`ui/src/routes/dax/` was compared to the backend operation surface.

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
