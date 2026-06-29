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

# Cross-service integration — does it interoperate like AWS?

A separate axis from per-service correctness: do the services wire together end-to-end the way
AWS does (S3→Lambda events, EventBridge→targets, API Gateway→Lambda, Step Functions→service
integrations, CloudFormation→real backends, alarms→SNS, etc.)? This was verified by tracing each
producer → transport → consumer path in code.

**How cross-service calls are wired.** Two mechanisms: (1) the `pkgs/events` in-memory emitter —
used only for SNS publish fan-out and S3-notification subscribers; (2) **explicit typed adapters
injected at startup in `cli.go`** via `Set*Invoker`/`Set*Integration`/`Set*Backend`. The advertised
`service.Registry.GetByName` is **not** the general data-plane mechanism (it's test/logging-only),
though a few stream consumers do resolve targets through a `byName` map built in `cli.go`.

**The recurring root cause of broken interop.** For many integrations the delivery/dispatch code
exists inside the service, but the corresponding `Set*` hookup is **missing in `cli.go`**, so the
dependency is `nil` and the call silently no-ops — frequently returning "success", so there is no
error, no retry, and no DLQ. The fixes are mostly one-line wiring additions in `cli.go`, not new
features.

**Scorecard.** Works end-to-end: S3 event fan-out; SNS→SQS; SQS→DLQ; EventBridge/Scheduler →
Lambda/SQS/SNS(/SFN); Pipes(SQS-source)→Lambda/SFN; DynamoDB-Streams/Kinesis/SQS → Lambda;
Kinesis→Firehose; CloudWatch-Logs subscriptions → Lambda/Kinesis/Firehose; API Gateway v1/v2 →
Lambda (+ real HTTP proxy); Step Functions → Lambda/SQS/SNS/DynamoDB and `.waitForTaskToken`;
CloudFormation → real backends (~60 types, shared state); CloudWatch alarm → SNS/Lambda; Secrets
Manager rotation → Lambda; Resource Groups Tagging aggregation; SSM SecureString → KMS. Broken or
unwired: **SNS→Lambda, SNS→Firehose**; EventBridge → Kinesis/Firehose/StepFunctions/ECS/Logs/
API-destination; Scheduler → EventBus/Kinesis/SageMaker/ECS; Pipes → all non-Lambda/SFN targets and
all non-SQS sources; Lambda ESM for Kafka/MSK/DocDB/MQ; ESM `FilterCriteria`; Lambda async DLQ/
destinations; API Gateway → AWS service integrations; Step Functions → ECS/Glue/EventBridge/
SfnStartExecution; CloudTrail/Config capture; Backup recovery points; Cognito→Lambda triggers; RAM
cross-account; Cloud Control (disjoint state); KMS use by S3/DynamoDB/Secrets Manager.

### Event fan-out — S3, SNS, SQS
- **S3 → SQS / SNS / Lambda / EventBridge:** WORKS — `dispatchToQueue`/`dispatchToTopic`/`dispatchToLambda`/
  `dispatchToEventBridge` call the real target backends through adapters (`s3/notification.go:402-486`;
  wired `cli.go:3250-3272`).
- **SNS → SQS:** WORKS — `Publish` emits `SNSPublishedEvent`; the SQS subscriber delivers to the real queue,
  raw + envelope (`sns/backend.go:2111,2183`; `sqs/sns_delivery.go:46-110`).
- **SNS → Lambda:** BROKEN — delivery code is correct but `b.lambdaBackend==nil` early-returns; `SetLambdaBackend`
  is **never called in `cli.go`** (tests only) (`sns/lambda_firehose_delivery.go:83-102`).
- **SNS → Firehose:** BROKEN — same pattern, `SetFirehoseBackend` never wired (`lambda_firehose_delivery.go:109`).
- **SNS → HTTP/SMS/email:** PARTIAL — one real HTTP POST, no retry despite `numRetries:3`; SMS/email go to
  in-memory sinks (`sns/backend.go:2925-2937`; `lambda_firehose_delivery.go:150-156`).
- **SQS redrive → DLQ:** WORKS in-region — moves real messages at `ReceiveCount>=maxReceiveCount`
  (`sqs/backend.go:377-404,1683-1692`); cross-region DLQ silently not configured.

### EventBridge / Pipes / Scheduler → targets
- **EB rule → Lambda / SQS / SNS:** WORKS — real `InvokeFunction`/`SendMessageToQueue`/`PublishToTopic`
  (`eventbridge/delivery.go:404-466`; wired `cli.go:3191-3203`).
- **EB rule → Kinesis / Firehose / Step Functions / ECS:** BROKEN — dispatch exists but `dt.KinesisStream`/
  `Firehose`/`StepFunctions`/`ECS` are never wired in `wireEventBridgeDelivery`, so `svc==nil` returns `false`
  (success) → **silent drop** (`delivery.go:468-517`; `cli.go:3187-3207`).
- **EB rule → CloudWatch Logs / API destination:** BROKEN — no ARN case; falls to the `default` "unsupported
  target" warn + drop (`delivery.go:418-421`); API destinations are CRUD-only (`backend.go:1530-1556`).
- **Scheduler → Lambda / SQS / SNS / SFN:** WORKS (`scheduler/runner.go:482-601`; wired `cli.go:5688-5710`);
  FIFO-SQS falls back to standard send (sqsFIFO unwired).
- **Scheduler → EventBus / Kinesis / SageMaker / ECS:** BROKEN — dispatch exists but the setters are omitted in
  `wireSchedulerRunner`; nil invoker logs "(no invoker)" and returns success (`runner.go:603-745`).
- **Scheduler `at()` one-time / FlexibleTimeWindow:** BROKEN — `isDue` only handles `rate(`/`cron(`, so `at()`
  never fires; FlexibleTimeWindow is stored but never read (`runner.go:201-214`).
- **Pipes (SQS source) → Lambda / SFN:** WORKS, with enrichment (`pipes/runner.go:357-433`; wired `cli.go:5749`).
- **Pipes → SNS/SQS/Kinesis/EventBridge/Logs/Firehose targets:** BROKEN — setters omitted; surfaces
  `ErrTargetInvokerUnwired` (routes to DLQ if configured, unlike EB's silent drop) (`runner.go:434-642`).
- **Pipes non-SQS sources (Kinesis/DynamoDB/MQ/MSK/Kafka):** BROKEN — `pollPipe` only routes `isSQSARN`; other
  sources are accepted at config but never polled (`runner.go:231-239`).

### Stream consumers & log subscriptions
- **DynamoDB Streams → Lambda:** WORKS — ESM polls the real stream (`DescribeStreamShards`/`GetStreamRecords`)
  and invokes the real function (`lambda/event_source_poller.go:269,486-632`; wired `cli.go:2632`).
- **Kinesis → Lambda:** WORKS (`event_source_poller.go:320-436`; poller `cli.go:3470`, wired `:2626`).
- **SQS → Lambda:** WORKS with correct partial-batch — `filterByBatchItemFailures` honors
  `ReportBatchItemFailures` (`event_source_poller.go:649,772`).
- **Kafka/MSK/DocumentDB/MQ → Lambda:** BROKEN — configs stored but `processOneMapping` only branches
  SQS/DDB/Kinesis; other ARNs resolve to `""` → silent no-op (`event_source_poller.go:278-281,448`).
- **Lambda ESM `FilterCriteria`:** BROKEN — stored/round-tripped but never referenced by the poller; all records
  delivered unfiltered.
- **Kinesis → Firehose:** WORKS — `launchKinesisPoller` reads the real Kinesis backend and the flush loop drains
  to the real destination (`firehose/kinesis_source.go:55-134`; `backend.go:497`).
- **CloudWatch Logs subscription → Lambda / Kinesis / Firehose:** WORKS — `PutLogEvents` matches compiled
  patterns and the deliverer dispatches to the real backend (`cloudwatchlogs/backend.go:1633`; `cli.go:3969-4004`).
  Caveat: the same matched events are sent to every filter (not re-filtered per pattern as AWS does).
- **Lambda async destinations / DLQ → SQS/SNS/EventBridge:** BROKEN — stored only; the exhausted retry loop just
  logs, and success routes to CloudWatch Logs (`backend.go:2121,2128`). No target ever reached.

### API Gateway & Step Functions integrations
- **API Gateway v1 → Lambda (AWS_PROXY / AWS):** WORKS — real `InvokeFunction` with proxy-event build + VTL
  response mapping (`apigateway/proxy.go:822-1023`; wired `cli.go:3370`).
- **API Gateway v1 → HTTP/HTTP_PROXY:** WORKS — genuine outbound `client.Do` (`proxy.go:1195-1258`).
- **API Gateway v1 → AWS service integrations (SQS/SNS/DDB/SFN direct):** BROKEN — `dispatchIntegration` switches
  only AWS_PROXY/AWS/HTTP/MOCK; other AWS URIs return `501` (`proxy.go:429-440`).
- **API Gateway v2 HTTP/WebSocket → Lambda:** WORKS — payload format 1.0/2.0 / WS event to real Lambda
  (`apigatewayv2/http_proxy.go:155-344`; `proxy.go:191-258`).
- **Step Functions → Lambda / SQS / SNS / DynamoDB:** WORKS — real backend adapters (`stepfunctions/asl/
  executor.go:1098-1292`; wired `cli.go:3441-3449`).
- **Step Functions → ECS / Glue / EventBridge / SfnStartExecution / APIGateway / EMR:** BROKEN — executor methods
  exist but `SetECSIntegration`/`SetGlueIntegration`/`SetEventBridgeIntegration` are never called in `cli.go`
  (nil → `…IntegrationNotConfigured`); the others error as unsupported (`executor.go:1059-1379`).
- **Step Functions `.waitForTaskToken`:** WORKS — crypto-random token, blocking channel resumed by
  `SendTaskSuccess/Failure` (`executor.go:818-852`; `backend.go:2122-2256`).

### CloudFormation & Cloud Control provisioning
- **CloudFormation → real service backends:** WORKS — `storeCLIHandlers`/`extractAllServiceBackends` hand CFN the
  *same* registered backend singletons the native APIs use, so created resources are immediately visible/usable
  via their native APIs — true shared state (`cli.go:2406`; `cloudformation/provider.go:159-221`). Verified REAL
  for S3::Bucket, DynamoDB::Table, SQS::Queue (Ref→real Queue URL), SNS::Topic, Lambda::Function, IAM::Role,
  EC2::Instance/VPC, StepFunctions::StateMachine, Logs::LogGroup, Events::Rule, KMS::Key, ApiGateway(V2)
  (`resources*.go`). The only stub path is the `backend==nil` guard returning `<logicalID>-stub`.
- **`Fn::ImportValue` / exports:** CFN-internal — resolved against CFN's own `exports` store, not a foreign
  backend (`template.go:585,650,886`).
- **Cloud Control → service backends:** BROKEN — `CreateResource` writes only its private `b.resources` map and
  never consults the registry or any backend, so Cloud Control state is **fully disjoint** from the native APIs
  (`cloudcontrol/backend.go:139`).

### Governance, observability & security
- **CloudWatch alarm → SNS / Lambda:** WORKS — `executeActions` dispatches to the real SNS/Lambda backends
  (`cloudwatch/backend.go:1773-1790`; wired `cli.go:3815-3848`); EC2/AutoScaling actions are logged-not-executed.
- **CloudWatch Logs ← services:** PARTIAL — only Lambda (and Pipes) emit real log lines (`cli.go:3853-3901`); ECS
  stores `LogConfiguration` as metadata only (`ecs/backend.go:73`); most services are isolated.
- **Secrets Manager rotation → Lambda:** WORKS — all four rotation steps invoke the real function
  (`secretsmanager/backend.go:1489-1500`; wired `cli.go:4101`).
- **Resource Groups Tagging API → cross-service tags:** WORKS — `GetResources` aggregates from real backends
  (DDB/SQS/SNS/Lambda/KMS/SM) (`resourcegroupstaggingapi/backend.go:259-279`; wired `cli.go:4327-4348`). The
  distinct Resource Groups service is isolated.
- **SSM SecureString → KMS:** WORKS — real `Encrypt`/`Decrypt` (`cli.go:3309-3357`).
- **KMS → S3 SSE-KMS / DynamoDB SSE / Secrets Manager:** BROKEN — these only persist key-id metadata and never
  call the KMS backend (SSM is the only real KMS consumer).
- **CloudTrail ← API calls:** BROKEN — no central call-recording hook; no service emits trail events
  (`services/cloudtrail` has zero subscribers).
- **Config ← resource changes:** BROKEN — configuration items are recorded only via the explicit
  `PutResourceConfig` API; no other service feeds it (`awsconfig/backend_real.go:544`).
- **AWS Backup → DynamoDB / EFS / RDS:** BROKEN — `StartBackupJob` validates and stores a job record but reads no
  source data and creates no real recovery point (`backup/backend.go:680-715`).
- **Cognito → Lambda triggers:** BROKEN — `LambdaConfig` stored but no invoker exists; triggers never fire
  (`cognitoidp/backend.go:83`).
- **RAM share → cross-account visibility:** BROKEN — shares are stored but no service consults RAM
  (`ram/backend.go:333-382`).

### Systemic: region/account, ARN & auth consistency
- **Region/account isolation is inconsistent.** Region-scoped (correct): dynamodb (`store.go:134`), s3
  (`backend_memory.go:114-115`), kms (`backend.go:300`), eventbridge (`backend.go:185-191`), stepfunctions
  (`backend.go:199`). **Region-less / flat (cross-region leakage):** lambda (`functions map[name]` + single
  `region`, `backend.go:217,1115`), ec2 (`backend.go:230-248`), xray (`backend.go:285-298`), swf, cloudwatchlogs
  (account-global). A same-named Lambda/EC2/X-Ray resource is shared across regions.
- **Cross-service delivery is name-scoped in the default region, not region-scoped.** SNS→SQS, S3→SQS/SNS, and
  EventBridge→SQS/SNS strip the region from the target ARN and resolve by name against the backend default
  region (`sqs/sns_delivery.go:76,131`; `sqs/backend.go:1050,326-331`), so a same-named queue/topic in another
  region can receive the event, or it silently misses.
- **ARN construction** is mostly correct (request region/account via `pkgs/arn`), but hardcoded
  `config.DefaultRegion`/`DefaultAccountID` literals produce wrong-region ARNs in `lambda/runtime_api.go:197`,
  `cloudformation/handler.go:345`, `timestreamwrite/backend.go:333`, and pervasively in omics/memorydb.
- **Auth is advisory only** across all services (LocalStack-style): S3 bucket policy is stored but never
  evaluated on object GET/PUT (`s3/acl_policy.go:114-127`); Lambda resource policy (`b.permissions`) is never
  consulted by `InvokeFunction` (`lambda/backend.go:1849-1856`); the IAM evaluator isn't gated on cross-service
  call paths. Uniform parity gap.

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

### s3 (deep dive)

S3 is broadly real: multipart upload, versioning + delete markers, SSE crypto that actually
round-trips (AES-256-GCM for SSE-S3/KMS/C), the lifecycle janitor (expiration/noncurrent/abort/
transition, with WORM-skip), and notification dispatch (SQS/SNS/Lambda/EventBridge fire for
real) are all genuinely implemented. Two **systemic** parity gaps dominate the remaining work:
(1) **access control and default encryption are stored but never enforced on the data plane**,
and (2) **there is no SigV4 header-auth or `aws-chunked` body decoding**. Details below.

- **Parity — auth / signature:**
  - No SigV4/SigV2 **header-auth** verification at all — the `Authorization` header is never
    validated (`handler.go:71`); presigned-URL verification is opt-in and **off by default**
    (`PresignSecret==""` accepts on structure + expiry only) (`presign.go:124`). SigV2 query
    presigns (`AWSAccessKeyId`/`Signature`) are unrecognized (`presign.go:35,101`).
  - No `aws-chunked` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` decoding: chunk-signature framing and
    `x-amz-decoded-content-length` are ignored, so chunked PutObject/UploadPart bodies are stored
    **with chunk headers inline**, corrupting content and ETag (no handling anywhere).
- **Parity — access control NOT enforced (stored-but-lies):**
  - Bucket Policy + ACL are stored but never checked on object GET/PUT/DELETE/List — object ops
    make zero authorization calls (only `PutObjectAcl` hits `enforceACLPolicy`, `object_ops.go:1233`);
    a `Deny`/public policy has no effect on real requests.
  - Public Access Block + Ownership Controls are enforced only at config-**write** time via
    substring checks (`acl_policy.go:36,117,135`), not on object access; `GetBucketPolicyStatus` is
    substring-based (`handler_stubs.go:151`). `GetBucketAcl` always returns a hardcoded FULL_CONTROL
    owner grant, never reflecting stored grants (`bucket_ops.go:999`).
- **Parity — encryption:**
  - Bucket **default** encryption is never applied: PutObject encrypts only from request-scoped
    `sseInfo` (`backend_memory.go:509`); the stored `PutBucketEncryption` config (`bucket_ops.go:1255`)
    is never consulted, so objects in an SSE-default bucket are stored **plaintext** with no SSE
    response headers.
  - SSE-KMS DEK is random, not wrapped under any CMK, and the KMS key is never validated to exist
    (`sse_crypto.go:67`).
- **Parity — Object Lock:**
  - GOVERNANCE bypass missing — `x-amz-bypass-governance-retention` is never read, so GOVERNANCE
    behaves identically to COMPLIANCE (`backend_memory.go:1106,1138`). Bucket `DefaultRetention` is
    parsed but never auto-applied on PutObject (`model.go:353`). MFA-Delete is dropped on
    `putBucketVersioning` (`bucket_ops.go:794`).
- **Parity — multipart:**
  - Complete result omits composite `x-amz-checksum-*`/`ChecksumType` (COMPOSITE/FULL_OBJECT) — only
    ETag (`multipart_ops.go:246`; `backend_memory.go:2178`). `ListParts` omits `StorageClass`/`Owner`/
    `Initiator` and per-part checksums (`multipart_ops.go:438`). `UploadPartCopy` ignores
    `x-amz-copy-source-if-*` preconditions and the copy-source-range 416 distinction, and never sets
    `x-amz-copy-source-version-id` (`multipart_ops.go:132-187`).
- **Parity — conditional / range / listing:**
  - Multi-range GET unsupported — only the first range is served, never `multipart/byteranges` 206
    (`object_ops.go:1539`); `Range` is ignored on HEAD (`object_ops.go:235`). `If-Match`/`If-None-Match`
    ignore weak-vs-strong and multi-value lists (`object_ops.go:1621`). `response-content-*` GET override
    query params are ignored (`object_ops.go:736`). ListObjects clamps out-of-range `max-keys` instead of
    `InvalidArgument`, with a v1 off-by-one that drops `max-keys=1000` to the default
    (`bucket_ops.go:590`); `ListObjectVersions` hardcodes `StorageClass=STANDARD` (`bucket_ops.go:928`).
- **Parity — checksums / errors:**
  - GET/HEAD emit only the first stored checksum, not all set algorithms (`object_ops.go:1286`);
    `x-amz-checksum-mode=ENABLED` fabricates a CRC32 when none is stored (`object_ops.go:1430`); no
    CRC64NVME branch (`utils.go:43`). Error XML never populates `<RequestId>`/`<Resource>`
    (`errors.go:314`). `GetObjectAttributes` ignores the `X-Amz-Object-Attributes` header and returns the
    full set (`handler_stubs.go:230-281`). `validateExpectedBucketOwner` compares a single hardcoded
    account `123456789012` (`accuracy.go:308`).
- **Parity — other subresources (store-and-echo only):** presigned POST policy/conditions/signature are
  not validated — any form upload is accepted (`post_object.go:39`); website/logging/accelerate/
  requestpayment/inventory/analytics/metrics/intelligent-tiering all store-and-echo (logging never writes
  access-log objects; website never serves index/error docs); replication ignores `Filter`/
  `ExistingObjectReplication` and replicates only new PUTs, not existing objects (`replication.go:85,143`);
  Object Lambda is a transform stub; Directory Buckets / S3 Express enforce no session-auth or zonal
  semantics (`bucket_ops.go:2131`).
- **Performance:**
  - `ListObjects`/V2/`ListObjectVersions` do a full O(n) scan + O(n log n) sort of **all** matching keys
    before MaxKeys truncation, with no sorted index (`backend_memory.go:1379-1429,1590,1629`).
  - `saveObjectVersion` iterates all existing versions under `bucket.mu.Lock`+`obj.mu.Lock` on every PUT
    (`backend_memory.go:4376`).
  - Whole-object in-memory buffering everywhere — GET decompresses the whole object into RAM
    (`backend_memory.go:746`; `object_ops.go:727`); `CopyObject` reads the full source into memory then
    re-PUTs it (`object_ops.go:490-553`); multipart concatenates all parts into one buffer then gzips
    (`backend_memory.go:2222-2284`). No streaming.
  - `dispatch`/`dispatchAccessLog` re-`xml.Unmarshal` the bucket notification/logging config on every
    event (`notification.go:370`; `access_log.go:42`); lifecycle transition sweeps hold `bucket.mu.RLock`
    across the whole scan (`janitor.go:1009-1010,1083-1084`).
- **Leaks:**
  - Access-log and notification dispatch goroutines are spawned with bare `go`, no WaitGroup, and are not
    drained at shutdown — one unbounded goroutine per logged/notified request (`access_log.go:58`;
    `object_ops.go:374,480,934,1031`; `handler_stubs.go:342`).
  - No handler-level `Shutdown` exists and `Provider.Init` never calls `backend.Shutdown()`
    (`provider.go:25-41`), so even the replication drain + serviceCtx cancel are effectively never invoked
    in production — only the janitor stops (via its ctx). `b.tags` keyed by `bucket/key/version` grows with
    versioned writes, evicted only on delete/lifecycle. (Multipart uploads ARE GC'd at 24h — not a leak.)
- **UI:** broad coverage (bucket CRUD; object upload/download/copy/rename/preview/bulk-delete; versioning
  toggle, encryption toggle, tagging, policy/ACL/PAB, lifecycle add/delete, CORS, multipart list/abort,
  object-lock config, notifications/replication/logging/ownership/analytics read; object subpage with
  versions/tags/presign; region-change handled). Missing: RestoreObject/Glacier (`handler.go:284`),
  SelectObjectContent (`handler.go:242`), per-object PutObjectRetention/PutObjectLegalHold
  (`handler.go:227-229`), object-level ACL put, Accelerate (`handler.go:281`), RequestPayment
  (`handler.go:276`), GetObjectAttributes (`handler.go:277`), **create** (not just read/delete) for
  notification/replication/analytics/metrics/inventory/tiering configs, and versioning Suspend distinction.
- _Recently closed:_ ABAC/inventory/journal/directory-bucket no-ops now persist; default-multipart and
  noncurrent-version sweeps de-locked; replication goroutine cancels+waits on Shutdown; lifecycle/CORS/
  policy/versioning/encryption/ACL/object-lock/replication/tagging/batch-delete UI all added.

### lambda (deep dive)
- **Parity — invoke:** `X-Amz-Invocation-Type` is never validated — a bad value silently degrades to
  RequestResponse instead of `InvalidParameterValueException` (`handler.go:1919-1922`; backend only tests
  `==Event`/`==DryRun`, `backend.go:1905,1954`). `LogType=Tail` unsupported — `X-Amz-Log-Type` never read,
  no base64 `X-Amz-Log-Result` header (`handler.go:1955-1979`). `X-Amz-Client-Context` ignored.
  `X-Amz-Function-Error` is hardcoded `"Unhandled"`, never `"Handled"` (`handler.go:1973`). Error responses
  omit the `x-amzn-errortype` header (`handler.go:2032-2037`).
- **Parity — async/DLQ/destinations:** the retry loop exhausts then only logs — never delivers to
  `DeadLetterConfig` or `DestinationConfig` OnFailure/OnSuccess, though both are stored
  (`backend.go:2119-2132,507,3565`).
- **Parity — event source mappings:** `FilterCriteria` is stored but **never applied** — no filter matching
  in the poller (`event_source_mapping.go:24`; absent from `event_source_poller.go`).
  `MaximumBatchingWindow`/`TumblingWindow`/`MaximumRecordAge`/`BisectBatchOnFunctionError`/
  `ParallelizationFactor` are stored and ignored. Kafka/MSK/SelfManagedKafka/DocumentDB/MQ source configs are
  accepted and `Enabled` but the poller only handles kinesis/sqs/dynamodb (`event_source_poller.go:259-487`),
  so those mappings never invoke. SQS event records omit `messageAttributes`/`md5OfMessageAttributes` and use
  the backend default region, not the ARN's (`event_source_poller.go:714-723`).
- **Parity — Function URLs:** `AuthType` is stored but never enforced — `AWS_IAM` URLs invoke without SigV4
  verification, no 403 (`backend.go:899-921`); CORS config is stored but no preflight headers are emitted;
  the event payload omits `cookies`/`queryStringParameters`/`pathParameters` (`backend.go:947-973`).
- **Parity — lifecycle:** functions are created directly `Active`, no Pending→Active (`handler.go:1524`);
  ProvisionedConcurrency jumps straight to `READY`, skipping `IN_PROGRESS` (`backend.go:3737-3747`).
  Code-signing config is stored but signatures are never verified on UpdateFunctionCode.
- **Parity — permissions/pagination:** the resource policy is never consulted on Invoke or Function-URL
  (advisory only); AddPermission ignores `Qualifier`/`RevisionId`/`EventSourceToken`/`PrincipalOrgID`/
  `FunctionUrlAuthType` (`backend.go:4028-4038`). All `Marker`/`NextMarker` tokens are base64-wrapped raw
  decimal offsets — decodable/forgeable (`pkgs/page/page.go:45-64`).
- **Performance:** `withInvocationChain` allocates a fresh slice per invocation on the hot path
  (`backend.go:116-122`; documented intentional).
- **Leaks:** `deleteFunctionMapsLocked` never deletes `permissions`, `runtimeManagementConfigs`,
  `functionRecursionConfigs`, `functionScalingConfigs`, or code-signing entries — stale config survives
  delete/recreate, cleared only by `Reset()` (`backend.go:3948-3973` vs `238-240,4020`).
- **UI:** no console for reserved/provisioned concurrency, event-invoke-config/DLQ/destinations, code-signing,
  layer-version permissions, runtime-management, Function-URL CRUD + invoke-via-URL, ESM update/filters,
  recursion/scaling config, SnapStart, resource-policy, or tag editing.
- _Recently closed:_ real vnd.amazon.eventstream framing; fire-and-forget InvokeAsync; SQS
  ReportBatchItemFailures partial-batch; async retry loop honoring MaximumRetryAttempts/EventAge; ESM health
  sweeps; `activeConcurrencies` zero-delete.

### ec2 (deep dive)
- **Parity — stub no-ops:** ~389 ops return a bare `stubResponse{Return:true}` with no state mutation — every
  `handleStub*` body is identical (`handler_stubs.go:949-955`). Advertised-but-no-op families: **IPAM**
  (AllocateIpamPoolCidr, AssociateIpam*), **TransitGateway** multicast/policy/peering attach, **TrafficMirror**,
  **VerifiedAccess** trust-provider attach, **CapacityReservation** billing transfer, **ClassicLink**,
  **VpnGateway** (AttachVpnGateway), **Bundle/Conversion** tasks (`handler_stubs.go:16-1622`) — they accept
  params, validate nothing, and return success, so clients see phantom success.
- **Parity — pagination:** `NextToken` is a base64-wrapped raw decimal offset, decodable/forgeable —
  DescribeInstances (`handler.go:631`), DescribeImages (`handler_ext.go:751-753`); DescribeInstanceTypes
  emits an even-barer un-base64'd offset (`strconv.Itoa(end)`, `handler.go:1077`). A forged/stale token
  silently re-pages instead of `InvalidPaginationToken`.
- **Parity — instances/attributes:** `ModifyInstanceAttribute` with an unknown/empty attribute returns
  `Return:true` instead of erroring (`handler_ext.go:2028-2033`); the generic `Attribute=`/`Value=` form
  bypasses the stopped-state guard (`handler_ext.go:2061,2098`). User-data is stored but not validated for
  base64/16KB limit (`handler.go:507-530`). EBS volume create ignores the gp3 iops/throughput coupling
  (`backend_accuracy.go:100-143`).
- **Performance:** `DeleteVpc` still does two O(n) full-map scans under the write lock — `natGateways`
  (`backend.go:1141`) and `networkInterfaces` (`backend.go:1163`) — no per-VPC index for either; ~24
  `range b.<map>` scans remain.
- **Leaks:** persistence snapshot is complete (~105 maps), but **`Restore` never rebuilds the secondary
  indexes** — `restoreCoreFields`/`restoreExtendedFields` reassign maps only (`persistence.go:621-674`),
  leaving `instanceIDsByVPC`/`subnetIDsByVPC`/`routeTableIDsByVPC`/`sgIDsByVPC`/`eniIDsByInstance` empty after
  reload, silently breaking DeleteVpc cascades and ENI-by-instance lookups post-restore.
- **UI:** tabs cover instances/secgroups/keypairs/amis/launchtemplates/vpcendpoints/nacls/vpcs/volumes/
  snapshots (`+page.svelte:635-662`) — no tabs for CreateFleet, transit gateways, IPAM, traffic mirror, VPN,
  verified access, reserved instances, dedicated hosts, capacity reservations, or network insights despite
  backend state.
- _Recently closed:_ stubs refactored to named funcs; DeleteVpc subnet/RT/SG/IGW cascades; correct EC2 error
  XML; comprehensive persistence snapshot; real CreateFleet; DescribeImages/InstanceTypes pagination plumbing.

### ecr (deep dive)
- **Parity — lifecycle policies:** `PutLifecyclePolicy` stores text but **never expires/deletes images** —
  there is no background evaluation job; only `StartLifecyclePolicyPreview`/`GetLifecyclePolicyPreview` compute
  expirations (`backend.go:1401-1445`). `GetLifecyclePolicy.LastEvaluatedAt` is faked to `time.Now()` per call
  (`backend.go:1346`).
- **Parity — image scanning:** ENHANCED scanning yields the same 12-CVE BASIC mock set (`scan.go:96-143`); no
  `PackageVulnerabilityDetails`, CVSS, `fixAvailable`, or Inspector enhanced shape. `DescribeImageScanFindings`
  has no `nextToken`/`maxResults` (`handler.go:1781-1800`); `scanOnPush` never auto-creates findings on PutImage.
- **Parity — error shapes:** `StartImageScan`/`DescribeImageScanFindings`/`UpdateImageStorageClass` return
  `RepositoryNotFoundException`/404 for a missing *image* instead of `ImageNotFoundException`/400
  (`backend.go:1849,1825,2189`; `DescribeImageReplicationStatus` correctly uses `ErrImageNotFound` at `2146` —
  inconsistent).
- **Parity — replication:** `DescribeImageReplicationStatus` hardcodes every destination to `COMPLETE`
  (`backend.go:2164`); `PutReplicationConfiguration` triggers no actual cross-region copy. `DescribeImages`
  `nextToken` is the raw `imageDigest` (`handler.go:988`). `GetAuthorizationToken` returns constant
  `dummy-password` (`handler.go:34`; intentional for `docker login`).
- **Performance / Leaks:** none material (layer-upload FIFO prune `backend.go:1106-1122`).
- **UI:** `GetAuthorizationToken` has no console surface.
- _Recently closed:_ opaque tokens for DescribeRepositories/ListImages/PullThroughCache; ScanFrequency
  resolution; LayerInaccessibleException; immutability exclusion filters.

### ecs (deep dive)
- **Parity — deployments/blue-green:** `DeploymentCircuitBreaker` is config-only — no failed-deployment
  detection ever flips a deployment to `FAILED` or rolls back (`backend.go:1156-1158`); rolling deploys rotate
  PRIMARY→ACTIVE but the reconciler keys tasks only by service, so old-revision tasks aren't progressively
  drained, and RolloutState jumps straight to `COMPLETED` (`backend.go:1058-1070`); no CODE_DEPLOY/blue-green.
- **Parity — capacity providers:** `ManagedScaling` (targetCapacityPercent, step sizes) is stored but inert
  (`handler_new_ops.go:121-128`) — no ASG ever scales container instances to honor target capacity.
- **Parity — task lifecycle:** `StopTask` jumps RUNNING→STOPPED with no DEACTIVATING/STOPPING intermediate
  (`backend.go:1540`); tasks **never self-stop when their containers exit** — no container-exit monitoring, so
  a crashed container leaves the task RUNNING forever (this also drives the stale-map leak below).
- **Parity — misc:** `ExecuteCommand`/`DiscoverPollEndpoint` return synthetic non-connectable SSM `StreamURL`/
  hosts with no honesty signal (`backend_ext.go:679-687`); `ServiceRegistries` are stored but no Cloud Map
  registration/DNS records are created (`backend.go:945`).
- **Performance:** `Purge` holds the write lock across a full nested scan of every task-def family/revision
  (`backend.go:404-439`).
- **Leaks:** `Reconciler.sems` grows one entry per cluster, never deleted (`reconciler.go:39-50`);
  `realDockerRunner.containers` keeps stale entries for self-exiting containers (`docker_runner.go:120-122,283`);
  `Purge` doesn't delete `serviceDeployments`/`daemonDeployments` for purged clusters (`backend.go:404-420`).
- **UI:** no RunTask/StopTask, no Daemon ops, no ExecuteCommand; capacity-provider view read-only.
- _Recently closed:_ MinimumHealthyPercent scale-down floor; real Docker runner; capacity-provider
  managed-scaling persistence; cached cluster counters; stopped-task janitor.

## Messaging & streaming

### sqs (deep dive)
- **Parity — DLQ/redrive:** `applyRedrivePolicy` never checks that source and DLQ share a type — AWS rejects a
  FIFO source pointing at a standard DLQ (and vice-versa) with `InvalidParameterValue`; here any same-region
  queue is accepted, and a missing/cross-region DLQ silently no-ops (`backend.go:377-404`). Over-`maxReceiveCount`
  messages are only routed to the DLQ lazily on a receive/janitor pick (`backend.go:1683-1692`), so a never-polled
  queue keeps them on the source.
- **Parity — system attributes:** `MD5OfMessageSystemAttributes` is never computed/returned on SendMessage(Batch)
  even when `MessageSystemAttributes` (AWSTraceHeader) is supplied (`backend.go:1151-1156`).
- **Performance:** `computeMD5OfMessageAttributes` re-sorts+re-encodes the attribute set on subset-receive
  (`handler.go:788-801`; full-set path now memoizes — low residual).
- **Leaks / UI:** none material (activity-gated prune; move-task panel, tags, redrive present).
- _Recently closed:_ send-time MD5 memoization; activity-gated prune; in-flight caps (120k/20k); FIFO per-group
  300 TPS; RedriveAllowPolicy validation.

### sns (deep dive)
- **Parity — delivery retry/backoff:** HTTP/HTTPS, Lambda, and Firehose deliveries are fire-once — any network
  error or non-2xx goes straight to DLQ or is dropped (`backend.go:2925-2937`; `lambda_firehose_delivery.go`).
  The `defaultEffectiveDeliveryPolicy` (numRetries:3) is returned by GetTopicAttributes but never honored, and
  per-protocol custom healthyRetryPolicy is ignored (`backend.go:138-140`).
- **Parity — delivery status logging:** `*SuccessFeedbackRoleArn`/`*FailureFeedbackSampleRate` are validated and
  stored (`backend.go:879-905`) but never emitted to CloudWatch Logs — status logging is cosmetic.
- **Performance:** none material (filter policies parsed once and cached).
- **Leaks:** `smsDeliveries`/`emailDeliveries`/`applicationDeliveries` slices grow per delivery and are only
  cleared by `Reset()`/test drains — `Purge` skips them, no age-based cap (`backend.go:539-541,4229-4231`).
- **UI:** SMS-sandbox + opt-out ops (Create/Verify/Delete/List SandboxPhoneNumber, Check/ListPhoneNumbersOptedOut)
  are docs-only, no interactive panel (`+page.svelte:1033-1038`).
- _Recently closed:_ full filter-operator set ($or/cidr/wildcard/numeric/prefix/suffix/anything-but/exists/
  equals-ignore-case); FilterPolicyScope=MessageBody; FIFO dedup + content-based; raw-vs-envelope SQS delivery;
  256 KB publish limit; platform apps/endpoints CRUD.

### eventbridge (deep dive)
- **Parity — PutEvents/FailedEntryCount:** `PutEvents` builds `EventResultEntry{ErrorCode:…}` for oversized
  entries (`backend.go:1219-1222`) but the handler hardcodes `FailedEntryCount: 0` and never counts entries
  with an `ErrorCode` (`handler.go:673-676`); `PutPartnerEvents` likewise (`handler.go:1435`).
- **Parity — target delivery:** API-destination ARNs have no case in the delivery switch and fall through to the
  `default` warn branch — silently dropped; no HTTP invocation of connections/API destinations at all
  (`delivery.go:403-421`). `applyInputTransformer` substitutes string variables **unquoted** into the template,
  emitting invalid JSON for `{"k":<v>}` (`delivery.go:618-630`).
- **Parity — Limit:** `ListRules`/`ListTargetsByRule` parse `Limit` but never forward it; the backends take no
  limit param and use a fixed page size (`handler.go:54,88`; `backend.go:967-993,1173,1323`).
- **Performance:** `PutEvents` holds the write lock across `captureEventInArchives`, which recompiles each
  pattern per archive per event rather than using the cache (`backend.go:1251,2857-2872`).
- **Leaks:** none material (scheduler prunes `lastFired`; tags cleared on delete).
- **UI:** ApiDestinations, Endpoints, Schema Registry, PutPermission/RemovePermission, DescribeReplay are
  docs-only, no controls (`+page.svelte:326-346`).
- _Recently closed:_ ListEventBuses Limit; opaque nextToken; per-account bus quota; PutTargets/RemoveTargets
  FailedEntryCount now real; archives Replay + connection CRUD UI.

### kinesis (deep dive)
- **Parity — stream modes:** `CreateStream` accepts any non-empty `StreamMode` verbatim, only defaulting the
  empty case — an invalid value like `"FOO"` is stored, not rejected (`backend.go:395-398`).
- **Parity — resharding:** `UpdateShardCount` creates child shards without `ParentShardID` lineage and allows
  arbitrary target counts (AWS caps at 2×/0.5× per call) (`backend.go:1492-1496`); merge doesn't verify parents
  are open before re-merging (`backend.go:1697-1718`). `nextSequenceNumber` is a plain `%020d` counter with no
  timestamp/shard encoding (`backend.go:320-323`).
- **Performance / Leaks:** none remaining (read-lock retention sweep; janitor Stop()).
- **UI:** Split passes the shard's own `StartingHashKey`, rejected by the strict-interior check
  (`+page.svelte:406`; `backend.go:1791`); Merge picks an arbitrary non-adjacent shard, rejected by adjacency
  (`+page.svelte:396`; `backend.go:1716`); GetRecords hardcodes TRIM_HORIZON, one page, no `NextShardIterator`
  (`+page.svelte:167`); "Consumers" stat hardcoded; no UI for PutRecords/UpdateShardCount/consumers/encryption/
  retention/stream-mode.
- _Recently closed:_ retention/encryption ops; event-stream SubscribeToShard (~5-min poll loop); janitor shutdown.

### firehose (deep dive)
- **Parity — Lambda transform:** runs only for the S3 destination (`backend.go:973-983`); HTTP/Redshift/
  OpenSearch/Splunk carry `ProcessingConfiguration` but receive untransformed records — `transformRecords` is
  hardwired to `*S3DestinationDescription` (`backend.go:1027-1032`).
- **Parity — failure routing/backup:** transform failure drops all records silently and `deliverToS3` errors are
  `_`-discarded (`backend.go:978-982`); `ProcessingFailed` records should route to the S3 backup
  `ErrorOutputPrefix` but are dropped (`transform.go:76-79`); `S3BackupMode=Enabled` accumulates `BackupRecords`
  that are never delivered (`backend.go:1359`); the `FailedRecords` metric is never written (`backend.go:293`).
- **Parity — formats/config:** Parquet/ORC `DataFormatConversionConfiguration` is entirely absent;
  DynamicPartitioning, CloudWatchLogging, `ErrorOutputPrefix`, `FileExtension` are stored but inert
  (`backend.go:164-171`). `UpdateDestination` can't switch/clear a type and skips the version check when
  `currentVersionID==""` (`backend.go:708-730`); `ListDeliveryStreams` ignores the `DeliveryStreamType` filter
  (`handler.go:681`).
- **Performance:** `intervalFlusher` scans every region×stream each 1s tick (`backend.go:775-801`);
  `ListDeliveryStreams` re-sorts all names every call (`backend.go:562`).
- **Leaks:** `NewInMemoryBackend` defaults `svcCtx` to `context.Background()`, so deliveries via `b.svcCtx` are
  unbounded if built without a context (`backend.go:347,609,674`).
- **UI:** HTTP/Splunk destinations don't render (only S3/Redshift/OpenSearch); no Tag/Untag/ListTags or
  UpdateDestination controls (`+page.svelte:234-235,390`).
- _Recently closed:_ non-S3 destination delivery; ListDeliveryStreams pagination; encryption persist/show;
  all-five UpdateDestination plumbing.

## Identity & security

### iam (deep dive)
- **Parity — policy simulation:** `SimulateCustomPolicy` omits permission boundaries entirely (no
  `PermissionsBoundaryPolicyInputList` param, no `AllowedByPermissionsBoundary` output), diverging from
  `SimulatePrincipalPolicy` which intersects them (`backend_refinement.go:615-655` vs `backend.go:2322-2347`).
  `SimulatePrincipalPolicy` never evaluates a resource/trust policy or honors `CallerArn`/`ResourceOwner`. The
  condition engine handles only string/bool/null/IP/ARN operators — `Date*`, `Numeric*`, `Binary*`, and the
  `ForAllValues:`/`ForAnyValue:` set qualifiers fall through to "unknown operator → no match", **silently
  mis-evaluating** any policy using them (`conditions.go:119-126`). Policy variables cover only
  `${aws:username|userid|sourceip}`; `${aws:PrincipalTag/…}`/`${aws:RequestTag/…}` resolve to literals
  (`variables.go:60-74`). No SCP/session-policy layering.
- **Parity — MFA/keys/roles:** `EnableMFADevice`/`ResyncMFADevice` accept any auth codes (no 6-digit/distinct
  validation), so `InvalidAuthenticationCode` is never produced (`backend_comprehensive.go:250-252`); a role's
  trust-policy principal is checked only for an `arn:aws` prefix, not account/service existence
  (`backend_accuracy.go:586-592`). SAML/OIDC metadata is stored verbatim with no XML/thumbprint validation.
- **Performance:** `CreateAccessKey`/credential-row build scan the full `accessKeys` map per user — O(n)
  (`backend.go:1227`; `backend_refinement2.go:280`).
- **Leaks:** `DeletePolicy` never clears `policyVersionCounters[arn]` or `deletedV1Policies[arn]`; `Reset` omits
  `deletedV1Policies` — repeated create/delete grows both maps (`backend.go:901-907,2506-2512`).
- **UI:** policy simulation and credential-report are docs-only, no panel (`+page.svelte:598,600`).
- _Recently closed:_ IAM UI CRUD; realistic credential report; boundary intersection in SimulatePrincipalPolicy;
  O(1) opaque pagination tokens.

### sts (deep dive)
- **Parity — AssumeRole:** when a `RoleLookup` is wired but the role ARN is unknown, `roleDerivedMaxDuration`
  returns the default max with no error, so **AssumeRole succeeds for a non-existent role** instead of
  `NoSuchEntity` (`backend.go:590-593`). No call validates that the role's trust policy permits the caller/
  federated principal (only `ExternalId` is checked, and only when a lookup exists);
  `AssumeRoleWithSAML`/`WithWebIdentity` skip trust evaluation entirely (`backend.go:1026-1051`).
- **Parity — SAML/web-identity:** `validateSAMLAssertion` checks base64 only, not well-formed SAML XML or
  audience/issuer (`backend.go:1153-1164`); the web-identity JWT is parsed for claims but never validated for
  `exp`/signature/`aud`, so expired/forged tokens are accepted (`backend.go:1041-1051,1550`).
  `DecodeAuthorizationMessage` falls back to decoding any base64 blob after the self-issued HMAC check fails,
  so non-STS messages still decode (`handler.go:583-594`).
- **Performance / Leaks:** none remaining (ticker eviction + lazy expiry-delete).
- **UI:** counters-only plus a `GetAccessKeyInfo` validator; no interactive forms for AssumeRole /
  AssumeRoleWithSAML / WithWebIdentity / AssumeRoot / GetSessionToken / GetFederationToken (`+page.svelte:357-372`).
- _Recently closed:_ ASIA-key GetCallerIdentity InvalidClientTokenId/ExpiredToken; session-token-mismatch 400;
  role-chaining 1h cap; backend self-issued auth-message HMAC verification.

### kms (deep dive)
- **Parity — key policies:** `PutKeyPolicy` stores the policy verbatim with no JSON parse/validation
  (`backend.go:2789`; `handler.go:609-628`), so `MalformedPolicyDocumentException` (invalid JSON, missing
  Version/Statement, unresolvable principal) is never produced — the only check is `PolicyName == "default"`.
  This is the **sole remaining op-level divergence**: crypto, encryption-context AAD binding, grants +
  constraints, asymmetric Sign/Verify/GetPublicKey, HMAC, multi-region keys, rotation (auto + on-demand +
  history), and custom key stores are all real and at parity (`crypto.go:166-558,704-730`;
  `backend.go:1809-1933,2232-2544`).
- **Performance / Leaks:** none remaining (`lastUsage` purged on janitor finalization, `janitor.go:238`).
- **UI:** `DescribeCustomKeyStores` and `GetKeyLastUsage` have no console surface.
- _Recently closed:_ real crypto + AAD context binding; grant constraint enforcement; rotation history; MRK
  config; lastUsage purge; O(1) `clearResolutionCache`; full sign/verify/grant/import UI.

### secretsmanager (deep dive)
- **Parity — resource policies:** `PutResourcePolicy` stores the policy verbatim (`backend.go:2170`) without the
  JSON/Version/Statement validation `ValidateResourcePolicy` already performs (`backend.go:2704-2732`), so a
  malformed policy never yields `MalformedPolicyDocumentException`; `BlockPublicPolicy` is unenforced.
- **Parity — idempotency/pagination:** reusing a `ClientRequestToken` with *different* content does not raise
  `ResourceExistsException` — `PutSecretValue` silently creates a new version (`backend.go:535-563`).
  `ListSecrets`/`ListSecretVersionIds`/`BatchGetSecretValue` tokens are plain `strconv.Itoa` offsets
  (`backend.go:814,973`).
- **Performance:** `GetSecretValue` takes the full write lock just to stamp `LastAccessedDate`, serializing
  reads (`backend.go:423`); tag filters `Clone()` the whole tag map per secret per call (`backend.go:883,899`).
- **Leaks:** the rotation scheduler runs a 1s ticker doing an O(n) all-secrets scan every tick regardless of due
  rotations (`backend.go:2471,2503-2514`).
- **UI:** no coverage for PutSecretValue, resource-policy ops, BatchGetSecretValue, GetRandomPassword,
  StopReplicationToReplica.
- _Recently closed:_ `ValidateResourcePolicy` JSON/Version/Statement checks; same-token same-content idempotency;
  `X-Amzn-Errortype` header on errors.

## Orchestration & APIs

### stepfunctions (deep dive)
- **Parity — ASL JSONPath:** `jsonPathGet` splits on `.` only (`asl/executor.go:2026-2068`) — no array index
  (`$.a[0]`), wildcard (`$.a[*]`), slice, or filter `[?(…)]`; unsupported expressions error as `States.Runtime`.
- **Parity — Map/Parallel:** Retry (`tryRetry`) is wired only into `executeTask` (`asl/executor.go:797`); Map
  applies neither Retry nor Catch, Parallel applies Catch but never Retry (`asl/executor.go:1408-1481`). AWS
  allows both on Map and Parallel.
- **Parity — Distributed Map:** no `ProcessorConfig`/`Mode:DISTRIBUTED`/`ExecutionType`, no
  `ToleratedFailureCount/Percentage`, no `ResultWriter`, no `MaxConcurrencyPath` — any single item error fails
  the whole Map regardless of tolerance (`asl/parser.go:48-81`; `finalizeMap` `1888-1905`).
- **Parity — JSONata/Variables/Catch/history:** no JSONata `QueryLanguage` and no 2024 `Assign`/`$states`
  Variables; catcher output is `{"Error":…}` only — missing `Cause` (`asl/executor.go:1005-1008`); history events
  emit empty payloads (no resource/parameters/output/cause) and whole-second `Unix()` timestamps, not millis
  (`backend.go:1198,1212-1233`). APIGateway/EMR service integrations hard-fail; SQS/SNS/ECS/Glue support only one
  action each (`asl/executor.go:1059-1378`).
- **Leaks:** `mapRuns`/`execMapRuns` never pruned — `pruneExecutionsLocked` and DeleteStateMachine omit them,
  cleared only by `Reset()` (`backend.go:623-647,1542-1543`).
- **UI:** no `DescribeMapRun`/`ListMapRuns` or `TestState` panels (otherwise broad coverage).
- _Recently closed:_ crypto task tokens; intrinsics (Format/Array/Math/String/Hash/UUID/JsonMerge); ItemReader
  CSV/JSONL; ItemBatcher; bounded JSONPath cache; status-bucketed ListExecutions.

### apigateway (v1, deep dive)
- **Parity — request validation:** `runRequestValidator` only checks `json.Valid` on the body, never validating
  against the model JSON Schema; `ValidateRequestParameters` (required headers/query/path) is entirely absent
  (`proxy.go:689-705`).
- **Parity — usage plans / API keys:** the data plane validates only key existence + `Enabled`
  (`proxy.go:387-401`) — **no usage-plan association and no quota/throttle/burst enforcement** anywhere;
  `GetUsage` returns empty `Items` (`backend.go:3422-3427`); `GetAPIKeyByValue` is a linear scan per
  apiKey-required request (`backend.go:1899-1911`).
- **Parity — TestInvoke/canary/VTL:** backend `TestInvokeMethod`/`TestInvokeAuthorizer` are hardcoded mocks
  (`backend.go:2766-2772,3180-3187`); HTTP/HTTP_PROXY integrations fall back to the mock (`handler.go:2075-2077`);
  `CanarySettings` are stored but never split traffic; `$util.*` operate only on string literals and JSONPath
  supports only `.key`/`[n]` (`vtl.go:141-226`). `GetSdk`/`ImportDocumentationParts`/DomainNameAccessAssociations
  remain canned (`handler_stubs.go:245-296`).
- **Parity — pagination:** non-opaque integer-index tokens (`strconv.Itoa(end)`, `backend.go:304,527`);
  `defaultPageSize=500` vs AWS 25 (`backend.go:253`); `GetResources` hardcodes 500.
- **Performance:** `dispatch` rebuilds the full op→handler table via 13 sub-constructors + `maps.Copy` per
  request (`handler.go:2778-2795,2972`); the proxy rebuilds the resource-path trie per data-plane request after a
  full `GetResources` copy (`proxy.go:287,1390-1395`).
- **Leaks:** `selRegexpCache` keyed by user patterns has no eviction (`handler.go:640`; `proxy.go:1093-1109`).
- **UI:** resources/methods/integrations read-only; only API/deploy/apiKey/usagePlan/domainName create forms —
  no CreateResource/PutMethod/PutIntegration/createStage/createAuthorizer/createRequestValidator/TestInvokeMethod/
  export/base-path/gateway-response/client-cert/VPC-link.
- _Recently closed:_ GetExport oas30/swagger; VpcLink/ClientCert/GatewayResponse stateful update; handler
  testInvokeMethod executes MOCK+Lambda; FlushStageAuthorizersCache flushes.

### apigatewayv2 (deep dive)
- **Parity — errors:** every error is `{"message"}` with **no `x-amzn-ErrorType` header** anywhere
  (`models.go:413`; `handler.go:626,651,665,886`).
- **Parity — export:** `ExportApi` wraps the spec as `{"body":…,"specification":…}` instead of returning the raw
  OpenAPI blob (`handler.go:1446`) and omits `components`/`securitySchemes`, emitting placeholder `200` responses
  only (`backend.go:3314-3320`).
- **Parity — authorizers:** the data plane enforces only JWT — `enforceRouteAuth` returns early unless
  `AuthorizationType==JWT`, so REQUEST/Lambda (`CUSTOM`) authorizers are never invoked on HTTP-API requests
  (`http_proxy.go:170-191`); `authorizationType` is not validated against the enum (`backend.go:830-837`).
- **Parity — mappings/auto-deploy:** `CreateAPIMapping` doesn't reject a duplicate `APIMappingKey` on a domain
  (`backend.go:1577-1586`); `AutoDeploy` is stored but never triggers an implicit deployment on route/integration
  change (`backend.go:673,773`).
- **Performance:** four sub-dispatch maps rebuilt per request (`handler.go:693,720,758,2152`); `Snapshot` marshals
  the whole backend under RLock (`persistence.go:42-71`); create ops do O(n) duplicate-key scans under write lock.
- **Leaks:** `DeletePortalProduct` leaves the `portalProductSharingPolicies` entry behind (`backend.go:3088-3092`).
- **UI:** no Models, Integration/Route Responses, Api Mappings, Routing Rules, Portals, Import/Reimport/Export,
  ResetAuthorizersCache (`+page.svelte:172-192`).
- _Recently closed:_ ImportApi/ReimportApi parse OpenAPI; WebSocket proxy + route-selection-expression eval; real
  JWT authorizer validation; ProtocolType validated.

## Management, config & data

### ssm (deep dive)
- **Parity — run command/automation:** `SendCommand` drives Pending→InProgress→Success synchronously but never
  populates `StandardOutputContent`/`StandardErrorContent` — no script executes, so `GetCommandInvocation`
  returns empty output and waiters never see `InProgress` (`backend.go:2012,2114`). `StartAutomationExecution`
  leaves `Status:InProgress` forever — no step execution or terminal transition (`backend_batch2.go:596,719`).
- **Parity — associations/patch/inventory/docs:** `DescribeAssociationExecutions/Targets` fabricate a fresh UUID
  ExecutionID per call (`backend_batch2.go:992,1027`); `GetDeployablePatchSnapshotForInstance`/
  `GetDefaultPatchBaseline` synthesize fake IDs/URLs (`backend_ops.go:989,717`);
  `DescribeEffectivePatchesForPatchBaseline`/`DescribeInventoryDeletions`/`ListDocumentMetadataHistory` return
  empty (`backend_batch2.go:982`; `backend_ops.go:120,412`).
- **Parity — sessions:** `StartSession` returns a synthetic non-connectable `wss://gopherstack-ssm-session/…`
  URL (`backend_stubs.go:1621`).
- **Leaks:** terminated sessions are marked `Terminated` but never evicted from `sessionsStore`, no cap
  (`backend_stubs.go:1664`).
- **UI:** only Parameters + Maintenance Windows tabs (`+page.svelte:198`) — no document/run-command/session/
  automation/patch/OpsItem/inventory/compliance/association panels.
- _Recently closed:_ per-instance AES-256 KMS encryptor; command Pending→InProgress→Success state machine;
  param-policy janitor; empty region sub-maps GC'd.

### cloudformation (deep dive)
- **Parity — change sets:** `computeChanges` emits only `Add`/`Modify`, never `Remove` for resources dropped from
  the new template, so `DescribeChangeSet` under-reports deletions (runtime UpdateStack still deletes them —
  preview-only gap); it reports no `Replacement` flags or property-level `Details`/`Scope`, and marks everything
  pre-existing as `Modify` even when unchanged (`backend.go:1190,1403`).
- **Parity — intrinsics/macros:** `Fn::ForEach` (Languages Extensions) is unsupported, and `invokeMacroTransform`
  is a no-op so `Fn::Transform` inside resource bodies returns the literal map (`template.go:1293-1320`).
  `Fn::GetAtt` attribute derivation is hardcoded per-type and returns the physical ID for unknown attrs
  (`template.go:994`).
- **Parity — provisioning/drift/stacksets:** ~183 real types across ~50 backends (`resources.go:14-69`); unmapped
  types get stub physical IDs. Drift diffs *stored template properties* against *the parsed template*, not live
  backend state, so out-of-band mutations are never detected, and `propertiesDiffer` is whole-object JSON equality
  (`backend_ext.go:110,154`). `CreateStackInstances` records rows but never provisions child stacks
  (`backend_ops.go:118`); `RollbackStack`/`SignalResource` just set status/append a record, and signals never gate
  `CreateStack` (no WaitCondition/CreationPolicy). Lifecycle is synchronous (CREATE_COMPLETE immediately).
- **Performance / Leaks:** none remaining.
- **UI:** 7 tabs (overview/resources/events/template/changesets/drift/policy); no type/registry mgmt, resource
  scans, generated templates, refactors, hook results, stack-instance detail, nested-stack tree, or Signal/
  Rollback/ContinueUpdateRollback controls.
- _Recently closed:_ 183 types; YAML+JSON; nested stacks; real drift; export-collision; ContinueUpdateRollback.

### cloudwatch (deep dive)
- **Parity — GetMetricData:** no pagination — `MaxDatapoints` ignored, no `NextToken` returned/parsed
  (`backend.go:924`; `handler.go:1234-1293`); `MetricDataResult` has no `Messages` field, so per-result
  `Messages`/`PartialData` and top-level `Messages` are never surfaced (`models.go:160-170`).
- **Parity — extended statistics:** `computePercentiles` handles only `pNN`; trimmed/winsorized/percentile-rank
  stats `TM(x:y)`/`TC()`/`TS()`/`WM()`/`PR()`/`IQM` are silently dropped, and percentiles over `StatisticValues`
  lose distribution (`metricmath.go:500-577`).
- **Parity — alarm actions / widget:** only SNS + Lambda fire; EC2 (`arn:aws:automate:`) and AutoScaling actions
  are logged-and-skipped (`backend.go:1791-1798`); `DescribeAlarmContributors` always empty (`backend.go:2457`);
  `GetMetricWidgetImage` returns a hardcoded 1×1 PNG (`handler.go:2657`); anomaly band is a flat mean±k·stddev
  with no seasonal model (`backend.go:900-908`).
- **Performance / Leaks:** none remaining (two-phase sweep; bounded metric storage).
- **UI:** no insight rules / contributor insights, alarm mute rules, managed insight rules, composite-alarm
  builder, GetMetricWidgetImage, or alarm contributors (`+page.svelte:44`).
- _Recently closed:_ MetricStreams/AnomalyDetectors/MetricFilters UI; sweep/stream-delivery lock contention;
  tag Close() leak; topo-sorted metric-math.

### cloudwatchlogs (deep dive)
- **Parity — Logs Insights:** the engine supports only `fields`/`filter @x like /re/`/`sort`/`limit`/`stats
  count(*) by` (`insights.go:142-160,242-252,368-401`) — no `parse`/`dedup`/`display`, no aggregations beyond
  count, no comparison operators (`=`,`!=`,`<`,`>`,`in`,`and`/`or`), and only `@timestamp`/`@message`/
  `@ingestionTime` resolve (any other field → `""`, `insights.go:355-366`), so no JSON/`@message` field
  extraction. `StopQuery` is cosmetic (`backend.go:2393-2411`).
- **Parity — filter patterns:** `filterPatternMatches` is plain-text only (AND/`?`/`-`/quoted/`*`) — no JSON
  `{$.field = val}` selectors, numeric comparisons, or `[w1,w2]` space-delimited patterns; metric filters reuse
  the same matcher (`backend.go:1689,1991`).
- **Parity — export/data-protection:** `CreateExportTask` never writes to S3 — status advances by janitor age
  only (`backend.go:2678-2718`); the data-protection policy is stored but never applies PII masking on
  Get/FilterLogEvents, `Unmask` ignored (`backend.go:2499-2517`). Five list ops still emit raw `strconv.Itoa`
  tokens despite the base64 helper (`backend.go:3134,3238,3362,3488,3659`).
- **Performance:** `collectQueryEvents` O(events) full scan per query under RLock (`backend.go:2254`);
  `retentionTargets` allocates O(regions×groups) per janitor tick (`janitor.go:118`).
- **Leaks:** none remaining (bounded event storage; capped compiled-pattern cache; bounded delivery goroutines).
- **UI:** no anomaly detectors, scheduled queries, account/index policies, deliveries, transformers, or
  data-protection (`+page.svelte:66`).
- _Recently closed:_ ListAnomalies/GetScheduledQueryHistory real; BytesScanned computed; two-phase retention
  sweep; subscription/metric-filter/query-def/export-task UI.

### route53 (deep dive)
- **Parity — routing-policy resolution:** `TestDNSAnswer` ignores `Weight` for weighted routing (returns first by
  `SetIdentifier` sort, no weighted/random selection); latency ignores `Region`; geolocation ignores
  `GeoLocation`/client location; multivalue returns one record, not the set; failover picks `PRIMARY` blindly
  without consulting health-check status (`backend.go:2834-2880`).
- **Parity — health checks / alias:** records store `HealthCheckID` and `GetHealthCheckStatus` exists, but
  TestDNSAnswer never fails over on health status — an unhealthy PRIMARY still wins (`backend.go:253,1466-1476`);
  alias targets resolve to the literal `AliasTarget.DNSName` string without recursing into the target set or
  consulting `EvaluateTargetHealth` (`backend.go:2817-2819`).
- **Parity — record validation:** A/AAAA/MX/SRV/CAA validated; TXT/CNAME/NS/PTR/NAPTR/DS/SPF values unvalidated
  (lenient, minor) (`backend.go:707-732`).
- **Performance / Leaks / UI:** none material — TestDNSAnswer does an O(records) prefix scan per call
  (`backend.go:2856-2860`, fine at typical zone sizes); full console coverage; no janitor state.
- _Recently closed:_ backend tag ops + batch ListTags; ListHostedZonesByName/ByVPC filtering; health-check
  last-failure observations; O(1) count ops; routing-policy mutual-exclusion validation.

### elasticache (deep dive)
- **Parity — lifecycle states:** clusters, replication groups, snapshots, serverless caches, and global RGs all
  jump straight to `available`/`active` with no `creating`/`modifying`/`deleting`/`snapshotting`/`restoring`
  transitions, so SDK waiters never observe intermediate states (`backend.go:845,1176,1685`;
  `backend_new_ops.go:256,308,346`); `Modify*` mutate in place with no `PendingModifiedValues` reflected back
  (`backend.go:1333,1394`); `RebootCacheCluster`/`FailoverReplicationGroup` set `available` instantly.
- **Parity — errors/validation:** `xmlError` omits the `<Type>Sender</Type>` element and uses a static stub
  `RequestId` (`handler.go:1923-1936`); every error returns HTTP 400, even `*NotFound` faults AWS returns 404 for
  (`handler.go:441-504`). `EngineVersion` is not validated against real published versions (`backend.go:1317`).
- **Parity — connectivity:** embedded miniredis binds a real port (`backend.go:865`) but the published
  `Endpoint` is a synthetic DNS hostname while the data port lives elsewhere; node-level `CacheNodes` carry no
  per-node ports and Memcached clusters get no real engine (`backend_audit1.go:71,94`).
- **Performance:** `createClusterLocked` calls `miniredis.Start` while holding `b.mu.Lock`, serializing all ops
  behind listener startup (`backend.go:865,869`).
- **Leaks:** none (Reset closes miniredis; events ring-bounded; updateActions capped at 1000).
- **UI:** missing global-replication-group, users (RBAC, distinct from groups), and security-group tabs.
- _Recently closed:_ valkey engine + families; reserved offerings seeded; updateActions capped; Reset closes
  miniredis.

### opensearch (deep dive)
- **Parity — lifecycle/processing:** `CreateDomain` sets `Status:"Active"` immediately; `toDomainStatusJSON`
  hardcodes `Processing:false`/`DomainProcessingStatus:Active` and never emits `Created`/`Deleted`/
  `UpgradeProcessing`, so waiters see no processing window after create/update/delete (`backend.go:600`;
  `handler.go:1419-1420`).
- **Parity — search/index engine:** the domain `Endpoint` is a cosmetic `search-…es.amazonaws.com` string with no
  real/proxied search service; `CreateIndex`/`UpdateIndex`/`GetIndex` store only Mappings/Settings metadata — no
  documents, no `_search`, no doc counts (`backend.go:585,1993,2084`); serverless collections expose no query
  endpoint.
- **Parity — async states / software updates:** `CreateServerlessCollection` returns `ACTIVE` instantly though a
  `CREATING` constant exists unused (`backend_serverless.go:14,140`); inbound/outbound-connection and
  VPC-endpoint deletes set `DELETED` synchronously (`backend.go:1209,1248,1359`); `CancelServiceSoftwareUpdate`
  returns a canned `CANCELLED` envelope and mutates nothing (`backend.go:1030-1037`). SAML/fine-grained options
  are stored without enforcement.
- **Performance:** `toDomainStatusJSON` allocates empty EBS/Cognito/AdvancedSecurity structs per call
  (`handler.go:1424-1428`); domain maps are flat (not region-nested) but unbounded.
- **Leaks:** none — `DeleteDomain` cascades data-sources/packages/maintenances/upgradeHistory/autoTunes/dryRuns
  (`backend.go:649-666`).
- **UI:** only domains + packages surfaced — missing serverless collections/policies, inbound/outbound
  connections, VPC endpoints, data sources, scheduled actions, auto-tune, dry-runs, applications.
- _Recently closed:_ DeleteDomain full cascade; SAML JSON tags; AssociatePackages validation; single-lock
  ListDomainNames.

---

# Extended services — remaining gaps

Tier-2 of the LocalStack-core set, same four axes and code-cited rule. As with the popular
tier, most prior-audit gaps are fixed (see `_Recently closed_` lines). The dominant remaining
themes here: **synchronous lifecycles** (most clusters/jobs/deployments jump straight to a
terminal state with no intermediate transition, so SDK waiters never observe `CREATING`/
`IN_PROGRESS`/`PENDING`), **query/exec engines that return synthetic or empty result sets**
(athena non-SELECT, timestreamquery, redshiftdata, cloudtrail Lake, config evaluation),
**non-opaque pagination tokens** (raw index/ARN), **not-found mapped to HTTP 400** across the
code* and several data services, and **console trailing the backend** on advanced ops.

## Email & auth

### ses
- **Parity:** `GetSendQuota` Max24Hour/Rate are hardcoded constants (`SentLast24Hours` now tracked)
  (`backend.go:778`); `VerifyEmailIdentity` marks identities verified synchronously, no `Pending`
  (`backend.go:360`).
- **Performance:** `GetSendQuota` does an O(n) reverse scan of all retained emails under RLock per call
  (`backend.go:769`).
- **UI:** GetSendQuota and suppression-list ops not surfaced.
- _Recently closed:_ janitor Shutdowner + cancel/done channel; send-quota counter.

### sesv2
- **Parity:** `BatchGetMetricData` returns one timestamp with hardcoded `0` (`backend.go:606`);
  `GetDomainDeliverabilityCampaign`/`GetDomainStatisticsReport` fully-shaped but all-zero
  (`backend_ops.go:632,652`).
- **Performance:** `SendEmail` holds the write lock during the compaction copy (`backend.go:556`).
- **UI:** Tenant, MultiRegionEndpoint, ReputationEntity, BatchGetMetricData have no panels.
- _Recently closed:_ Tenant/MultiRegion/PutAccount* persist; Tag ops real; ListEmailIdentities sorts
  outside the lock.

### cognitoidp
- **Parity:** `randomAlphanumeric` swallows `crypto/rand` errors, substituting `chars[0]` and weakening
  token entropy (`backend.go:2431`); `AdminListUserAuthEvents` always empty (`handler_completeness.go:194`);
  ~40 completeness-table ops are validation-only stubs (`handler_completeness.go:13`).
- **Performance:** `sweepExpiredRefreshTokens` holds the write lock across the full scan + deletes
  (`janitor.go:57`).
- **Leaks:** `tokenRevokedBefore` (pool:username) never purged on DeleteUserPool/AdminDeleteUser
  (`backend.go:187,345,777`).
- **UI:** WebAuthn, UserImportJob, AuthEvents, device ops uncovered.
- _Recently closed:_ AssociateSoftwareToken per-user secret; real AdminLinkProviderForUser.

### cognitoidentity
- **Parity:** `ListIdentityPools` uses the pool name as the `nextToken` cursor (`backend.go:413,436`);
  `GetOpenIDToken` ignores `logins`, issuing tokens without validating provider tokens (`backend.go:671`);
  `GetCredentialsForIdentity` mints a synthetic non-STS `SecretAccessKey` (`backend.go:660`).
- **Performance:** `mergeExistingIdentity`/`lookupOrCreateDeveloperIdentity` O(n) per-pool scans
  (`backend.go:571,881`); `DeleteIdentities` O(deleted·n) per-id filter (`backend.go:819`).
- _Recently closed:_ developer-identity ops UI; GetCredentialsForIdentity login-token matching.

## Databases & data APIs

### rds
- **Parity:** `GetPerformanceInsightsData` ignores the StartTime/EndTime/period window — returns all stored
  points regardless of range (`batch3.go:79-107`). Otherwise parity is strong.
- _Recently closed:_ error sentinels → `awserr.New`; handler_stubs removed; PI no longer synthesized;
  events bounded (512); reconciler → one lazy self-terminating goroutine (leak gone).

### rdsdata
- **Parity:** `BeginTransaction` mints sequential `txn-%06d` IDs, not random (`backend.go:259`);
  `BatchExecuteStatement` always returns empty `GeneratedFields` even for INSERT…RETURNING (`backend.go:245`).
- **Performance:** `appendStatementLocked` does make+copy on every trim instead of a ring buffer
  (`backend.go:163`).
- **Leaks:** `executedStatements`/`transactions` grow one bucket per region key with no eviction (per-region
  slice capped at 1000, region count unbounded) (`backend.go:95,107`).
- _Recently closed:_ real per-resource SQLite engine executes SQL with genuine result sets/transactions.

### redshift
- **Parity:** `ListRecommendations` empty (`handler_completeness.go:920`); `DescribeNodeConfigurationOptions`
  one static option (`:866`); `ModifyAquaConfiguration` no-op fixed `auto`/`disabled` (`:935`);
  `GetIdentityCenterAuthToken` canned (`:888`); every mapped error returns HTTP 400/`Sender` even for
  not-found (`handler.go:716,736`).
- **Performance:** serverless list ops `sort.Slice` on every read, no index (`backend_serverless.go:215,398,
  602,728,893`).
- **Leaks:** `CreateCluster` spawns a raw unmanaged `go func(){time.Sleep…}` per cluster, no stop channel/WG
  (`backend.go:571`; dormant unless `clusterActivationDelay>0`).
- **UI:** IdcApplication, ScheduledAction, Register/DeregisterNamespace, AQUA have no pages.
- _Recently closed:_ ~35 completeness ops wired to real handlers; wire-level AWS error envelope.

### redshiftdata
- **Parity:** `GetStatementResult`/`V2` return synthetic `mock_value`/`mockColumnSize=256` regardless of SQL
  (`handler.go:374-389`); `BatchExecuteStatement` sub-statements hardcode `HasResultSet:false` even for
  SELECT subs (`backend.go:401`; single-statement path is correct at `:329`).
- **Performance:** `ListStatements` clones all matches under RLock then `sort.Slice` even for one page
  (`backend.go:516-530`).
- _Recently closed:_ ring-buffer eviction + age-based janitor; UUID IDs; `sqlHasResultSet` for single stmt.

### neptune
- **Parity:** `Marker` pagination is a numeric offset (`handler.go:2738,2762`); `DescribeGlobalClusters`
  ignores Marker/MaxRecords (`handler.go:1089`); `DescribeDBParameters`/`DescribeDBClusterParameters` return
  empty lists (`handler.go:1354,1399`); `ApplyPendingMaintenanceAction` validates then no-ops
  (`backend.go:1773`); clusters created directly `available`, no `creating→available` (`backend.go:740`).
- **Performance:** `DescribeDBClusters` clones every match (`backend.go:816`).
- **UI:** no GlobalCluster/Failover/Switchover/EventSubscriptions/ApplyPendingMaintenance.
- _Recently closed:_ ClusterParameterGroups pagination + Marker; ModifyDBClusterParameterGroup wired.

### docdb
- **Parity:** `DescribeDBEngineVersions` response has no `Marker` (`handler.go:843`); `DescribeGlobalClusters`
  un-paginated (`handler.go:865`); `DescribePendingMaintenanceActions` empty (`backend.go:1938`); clusters
  created directly `available` (`backend.go:88-90`).
- **Performance:** `GetClusterMembers` scans all instances per cluster on `DescribeDBClusters`
  (`backend.go:1017`).
- **UI:** FailoverGlobalCluster/ModifyGlobalCluster/SwitchoverGlobalCluster uncovered.
- _Recently closed:_ ClusterParameterGroups marker + real writes; DescribeDBClusterParameters overlay;
  unified tag store; cert filter-by-ID; GlobalCluster/EventSubscription UI.

### timestreamwrite
- **Parity:** `Handler.Backend` is the concrete `*InMemoryBackend`, not the `StorageBackend` interface, so
  alternative backends can't be injected (`handler.go:316,322`).
- **Leaks:** `StartWorker` launches `go janitor.Run(ctx)` fire-and-forget, unawaited (inner sweeper ticker is
  worker.Group-managed) (`handler.go:371`).
- **UI:** no `timestreamwrite` route; the shared Timestream console manages DB/table but exposes no
  `WriteRecords` ingestion path (`ui/src/routes/timestream/+page.svelte`).
- _Recently closed:_ StorageBackend interface + assertion; sweeper lifecycle-managed; DB/table UI.

### timestreamquery
- **Parity:** `QueryWithOptions` returns `[]Row{}` for every query — schema inferred from SQL but data always
  empty (`backend.go:412`); `queries` map not region-isolated, UUID-keyed (`backend.go:118`).
- **Performance:** eviction iterates the map to delete an arbitrary key, no LRU (`backend.go:434-439`);
  `regionFromARN` uses unbounded `strings.Split` not `SplitN` (`backend.go:50`).
- _Recently closed:_ query cache bounded (`maxRetainedQueries`); dedicated Query/Cancel/Prepare/scheduled UI.

### qldb
- **Won't-fix:** service intentionally removed (AWS end-of-support 2025-07-31); only `services/qldb/README.md`
  remains with migration guidance (issues #2073/#1819). Not a parity gap.

## Analytics & ML

### glue
- **Parity:** `DescribeEntity` returns empty `Fields` after validating the connection (`handler_stubs.go:1955`);
  `GetEntityRecords` returns empty `Records` (`handler_stubs.go:2536`); `DeleteConnectionType` is a no-op
  (`handler_stubs.go:1425`).
- **Performance:** `runReconciler` ticks unconditionally and takes the global write lock each tick even with
  nothing pending (`backend.go:475`).
- **Leaks:** `NewInMemoryBackend` starts `go b.runReconciler()` but `Close()` has zero callers, so the
  goroutine + ticker leak (`backend.go:460`; `provider.go:29`).
- _Recently closed:_ CheckSchemaVersionValidity/CreateScript/DeleteSchemaVersions/GetDataflowGraph et al. now
  real; entire Glue UI.

### athena
- **Parity:** all four sentinels equal `InvalidRequestException`, indistinguishable (`backend.go:39-46`);
  `paginateQueryExecutionIDs` token is the raw next exec ID, enumerable (`handler.go:710-735`); SQL engine is
  SELECT-only — non-SELECT silently returns empty (`backend_sql.go:52-54`).
- **Performance:** `ExtractResource` JSON-unmarshals the whole body per request (`handler.go:150-165`); janitor
  holds the global write lock for the whole execution sweep (`janitor.go:79-91`).
- **Leaks:** `queryResults` never evicted (janitor deletes only `queryExecutions`) (`backend.go:1169`;
  `janitor.go:88`); sessions/calculations have no sweep.
- _Recently closed:_ workgroup/named-query/catalog/prepared-statement pagination; derived
  GetQueryRuntimeStatistics; extended-op UI.

### emr
- **Parity:** clusters created directly `WAITING`, no STARTING→BOOTSTRAPPING→RUNNING (`backend.go:1005`); steps
  created `PENDING` and never advance except `CancelSteps`→CANCELLED (`backend.go:922-923,1664-1667`);
  `GetPersistentAppUIPresignedURL` returns a static URL without verifying the UI exists; `ErrNotFound`→
  `InvalidRequestException`/400, not `ClusterNotFoundException` (`handler.go:305-306`).
- **Performance:** `ListClusters` re-sorts all clusters every call (`backend.go:1112`).
- **Leaks:** janitor started with bare `go h.janitor.Run(ctx)`, not tied to Shutdown (honors ctx) (`handler.go:57`);
  empty region stores never GC'd (`backend.go:726`).
- **UI:** no RunJobFlow, DescribeJobFlows, SetVisibleToAllUsers, PutBlockPublicAccessConfiguration.
- _Recently closed:_ CancelSteps per-step results; release-label regex; on-cluster presigned-URL validation.

### lakeformation
- **Parity:** `GetWorkUnitResults` returns the stored query string, not result data; `GetQueryStatistics`
  synthetic (hardcoded 1s); `GetDataLakePrincipal` synthetic `:user/gopherstack-user`; pagination token is
  `base64(strconv.Itoa(idx))`, guessable (`NewHMAC` helper exists but unused) (`pkgs/page/page.go:47`).
- **Performance:** `ListPermissions` deep-copies the full filtered slice every call (`backend.go:544-575`).
- **UI:** no Get/PutDataLakeSettings, query-planning, or credential-vending panels.
- _Recently closed:_ SearchByLFTags MaxResults; real GetTableObjects; ExtendTransaction; O(1) permission
  lookup; single-lock BatchGrantPermissions.

### sagemaker
- **Parity:** core lifecycle (models/endpoints/configs/training/processing/transform/notebooks) is now real
  FSM-backed, but ~120 peripheral ops remain canned stubs — Create stubs return `""` ARN, List stubs return
  `[]`, `DescribePipelineExecution` always `Succeeded` (`handler_stubs.go:226,233-294,437,486-598`).
- **Performance:** stub List ops bypass pagination (`handler_stubs.go:486+`); real List ops paginate.
- **UI:** real ProcessingJob/TransformJob backends have no panels; ~120 stub ops unsurfaced.
- _Recently closed:_ core model/endpoint/training/processing/notebook lifecycle moved from stubs to real FSM.

### sagemakerruntime
- **Parity:** `InvokeEndpoint` returns hardcoded `"mock response from Gopherstack"` ignoring input/endpoint
  config (`handler.go:32,159`); stream hardcoded (`handler.go:201`); no endpoint-existence check, so the
  unknown-endpoint error shape is never produced (`handler.go:130-133`).
- **Performance:** `evictOldest` O(n) scan, but only when the 1000-cap map overflows (`backend.go:218-241`).
- _Recently closed:_ Shutdown + FIFO caps; async output honors caller `X-Amzn-Sagemaker-Outputlocation`; UI.

### appsync
- **Parity:** real GraphQL execution exists, but nested selection sets are NOT projected — `executeSelectionSet`
  resolves only top-level fields, returning whole resolver payloads (`graphql.go:139-178`); HTTP/Relational/
  OpenSearch data sources return `ErrUnsupportedDataSource` (`graphql.go:259`); `EvaluateCode` rejects
  non-trivial JS (`jseval.go:31-34`); Event APIs have no real event-bus/WebSocket.
- **Performance:** `randomAPIID`/`randomAPIKeyID` read `crypto/rand` per char while the write lock is held
  (`backend.go:279-309,414-437`).
- **Leaks:** `sourceAssocs` orphaned on API delete (`DeleteGraphqlAPI` doesn't prune it) (`backend.go:605-639`).
- **UI:** ExecuteGraphQL, EvaluateCode/EvaluateMappingTemplate, schema-merge ops uncovered.
- _Recently closed:_ List pagination; GraphQL execution from empty-stub to real resolver dispatch.

## Networking, edge & DNS

### cloudfront
- **Parity:** ~80 ops route through `dispatchStubs` returning empty/minimal XML — tenants, FLE, key-groups,
  public-keys, realtime-logs, KV-stores, streaming/VPC origins, trust stores, monitoring subs remain stubs
  (`handler.go:1969-2199`); `ListDistributionsBy*` do raw `strings.Contains` over `RawConfig` → false positives
  (`backend_batch2.go:320-331`); `GetManagedCertificateDetails` fabricates a SUCCESS cert when none stored
  (`backend_batch2.go:457-470`).
- **Performance:** `distributionsByConfigSearch` O(n×config) under RLock (`backend_batch2.go:320-331`);
  `runInvalidationReconciler` ticks every 20ms taking the global write lock with no idle short-circuit
  (`backend.go:617-632`).
- **UI:** no KV stores, key groups, public keys, FLE, realtime logs, streaming/VPC origins, trust stores,
  monitoring subs, tenants.
- _Recently closed:_ Distribution List Marker/MaxItems pagination; cache/origin/response-policy + OAC +
  invalidation UI; reconciler stoppable via Close().

### acm
- **Parity:** `RequestCertificate` never validates ValidationMethod/CT-logging preference — unknown method
  issues immediately (`backend.go:480-485`); `ImportCertificate` hardcodes `KeyAlgorithm:EC`, no key/cert match
  (`backend.go:676`); `ExportCertificate` returns a hardcoded chain when missing (`backend.go:813-815`);
  `RenewCertificate` on IMPORTED returns RequestInProgressException not ValidationException (`backend.go:714`);
  `PutAccountConfiguration` accepts DaysBeforeExpiry >45 (`backend.go:1456-1458`); Revoke has no InUseBy guard
  (`backend.go:1480-1545`).
- **Performance:** `ListCertificates` deep-copies + sorts all certs before paginating (`backend.go:955-985`);
  janitor sweeps all certs/timers under the full write lock hourly (`janitor.go:80-133`).
- **Leaks:** terminal-state certs only transition status, never deleted (`janitor.go:88-99`).
- **UI:** RevokeCertificate uses a hand-rolled `fetch` bypassing the SDK (`+page.svelte:279`).
- _Recently closed:_ KeyAlgorithm validation → ValidationException; request modal adds KeyAlgorithm/CAArn.

### route53resolver
- **Parity:** `ListResolverEndpoints`/`ListResolverRules` ignore Filters (the input structs don't declare a
  `Filters` field) (`handler.go:394-397,418-421`); pagination tokens encode a raw position index
  (`pkgs/page/page.go:46,52`); `PutResolverQueryLogConfigPolicy`/`PutFirewallRuleGroupPolicy` store raw policy
  strings with no validation.
- **Performance:** a single `b.mu` guards all resource types — any write blocks all cross-type reads
  (`backend.go:323`); `DeleteResolverEndpoint` cascades over all rules/associations under the write lock.
- **UI:** Outpost resolvers and rule-association management absent.
- _Recently closed:_ firewall rule-groups/domain-lists, QueryLog configs, DNSSEC configs, ResolverConfig UI.

## Governance & accounts

### cloudtrail
- **Parity:** `GetQueryResults` returns hardcoded empty rows/zero stats (`handler.go:1343-1347`;
  `backend.go:1311`); `ListQueries` ignores EventDataStore/QueryStatus filters (`backend.go:1325`);
  `ListImportFailures` hardcoded `[]` (`backend.go:1581`); Register/DeregisterOrganizationDelegatedAdmin are
  validation-only no-ops (`backend.go:962,1474`); channel IDs sequential not UUID (`backend.go:758`).
- **Leaks:** `events` is append-only, never capped/evicted; LookupEvents copies the whole slice each call
  (`backend.go:1614,227,1680`).
- **UI:** only Trails/Event-History/EDS tabs; no channels, imports/exports, Lake queries, insight selectors,
  dashboards, delegated admin.
- _Recently closed:_ LookupEvents NextToken pagination + newest-first; EDS insight selectors.

### config
- **Parity:** `StartConfigRulesEvaluation` marks every rule COMPLIANT regardless of resources/logic
  (`backend_ext.go:187-195`); `PutEvaluations`/`PutExternalEvaluation` store only last `ComplianceType` per
  rule, dropping per-resource granularity (`backend_real.go:369-388`); conformance-pack compliance ops return
  empty stubs (`backend_real.go:469,655,678,681`; `backend_ext.go:139`); `GetResourceConfigHistory` keeps only
  the latest item per resource (`backend_real.go:544-575`); `StartResourceEvaluation` returns constant
  `"eval-stub"` (`backend_ext.go:208`); org rule/pack statuses hardcoded (`backend_real.go:475-490`);
  `DescribeConfigRules` accepts but never echoes NextToken (`handler.go:558-560`).
- **UI:** no delivery-channel, configuration-aggregators, resource config-item browser, or manual
  evaluate/PutEvaluations trigger.

### organizations
- **Parity:** `CreateAccount`/`CreateGovCloudAccount` complete synchronously to `SUCCEEDED`, no `IN_PROGRESS`
  (`backend.go:643-651`); `AttachPolicy` records attachment but enforces nothing (no effective-policy eval).
- **Leaks:** `createStatuses` map grows unbounded, never pruned (`backend.go:653`; negligible).
- **UI:** no delegated-administrator console, handshake/invitation management, policy-type enable/disable, or
  CreateAccountStatus tracking.
- _Recently closed:_ all list ops paginate via `page.New`; `extractErrorType` string-splitting → `awserr.Classify`.

## Resource & tag management

### ram
- **Parity:** `GetResourcePolicies` emits a hardcoded empty-statement policy for every ARN, ignoring real state
  (`backend.go:1315`); `ramPaginate` token is base64 around a raw slice index (`handler.go:769`).
- **Performance:** `CreateResourceShare` O(n) name-collision scan (`backend.go:344`); `clonePermission`
  deep-copies the whole `Versions` map per read (`backend.go:162-170`); `ListResourceShares` `sort.Slice` per
  read (`backend.go:443`).
- **Leaks:** `DeleteResourceShare` soft-deletes (`Status=statusDeleted`) and never removes from the map;
  associations retained — unbounded growth (`backend.go:560`).
- **UI:** no GetResourcePolicies, PromotePermission/ResourceShareCreatedFromPolicy, ReplacePermissionAssociations.
- _Recently closed:_ ListResources/ListPrincipals filters; Promote no longer a no-op.

### resourcegroupstaggingapi
- **Parity:** `GetResources` token is the raw last `ResourceARN`, non-opaque (`backend.go:~664`);
  `ExcludeCompliantResources` hard-empties the result set, no tag-policy engine (`backend.go:594-595`);
  `GetComplianceSummary` and `ListRequiredTags` are empty stubs (`backend.go:1219,1285`).
- **Performance:** `GetResources` takes the full write lock on the read-and-cache path; `GetTagKeys`/
  `GetTagValues` likewise `Lock()` despite being reads (`backend.go:584,794,830`).
- _Recently closed:_ per-region cache TTL (30s); Tag/Untag/GetComplianceSummary UI.

### resourcegroups
- **Parity:** `ErrTagSyncTaskNotFound` returns `{"message":…}` with no `__type`/`x-amzn-errortype`, so SDKs
  can't extract the code (`backend.go:44`; `handler.go:358`); `SearchResources` only iterates explicitly
  grouped ARNs, no cross-service fan-out (`backend.go:1409-1424`); `paginate[T]` encodes the item key directly
  as the token (`backend.go:367-388`).
- **Performance:** `ListTagSyncTasks` takes a full write lock to evict stale tasks on a read path
  (`backend.go:1529`); `groupMatchesFilters` O(n²) `ListGroups` under config filters (`backend.go:878-906`).
- **Leaks:** `groupingStatuses` appends per ARN per `GroupResources` with no trim/TTL (`backend.go:1167`).
- **UI:** no StartTagSyncTask/CancelTagSyncTask/GetTagSyncTask, ListGroupingStatuses, SearchResources,
  UpdateAccountSettings.
- _Recently closed:_ PutGroupConfiguration validation; GroupResources/UngroupResources/ListTagSyncTasks UI.

### cloudcontrol
- **Parity:** no cross-service dispatch — resources live in a private `b.resources` map, so CloudControl and
  native APIs see disjoint state (`backend.go:103,167`); every op returns synchronous `SUCCESS`, `IN_PROGRESS`
  only via test-only `AddProgressEvent` (`backend.go:180,283,315,478`); `CancelResourceRequest` returns
  ValidationException not `UnsupportedActionException` for terminal requests (`backend.go:348-349`);
  `applyPatch` flattens JSON-Pointer paths, breaking nested patches (`backend.go:583-590`).
- **Performance:** `ListResources` O(n) `HasPrefix` scan over all resources (`backend.go:221-225`).
- **Leaks:** `requests`/`clientTokens` grow unbounded, cleared only by `Reset()` (`backend.go:104-105`).
- **UI:** no CancelResourceRequest or progress polling beyond a single status lookup.

## Compute & deployment

### batch
- **Parity:** job state machine collapses the chain — `getJobsToAdvance` jumps SUBMITTED/PENDING/RUNNABLE/
  STARTING straight to RUNNING in one tick (`janitor.go:202-204`); array jobs store `ArrayProperties` but never
  fan out into child jobs or a status summary (`backend.go:1899-1934`); all sentinels collapse to
  `ClientException`/400, no `ResourceNotFoundException` (`handler.go:357-360`).
- **Performance:** multiple list paths `sort.Slice` the full key set per call (`backend.go:1980,2411,2626`).
- **UI:** no scheduling-policies or consumable-resources tabs (`+page.svelte:203-208`).
- _Recently closed:_ job-state simulation; HMAC pagination; full-sweep-under-lock fixed.

### eks
- **Parity:** `DeleteCluster` cascades nodegroup deletion immediately, no `ResourceInUseException` precondition
  (`backend.go:556-590`); `CreateNodegroup` doesn't validate required `nodeRole`/`subnets` (`backend.go:610-720`);
  `stableID` is FNV-1a 32-bit (8-hex collision space) for ARNs (`backend.go:1260`).
- **Performance:** `findTagsForARNLocked` + helpers do O(n×m) nested ARN scans across all resource maps
  (`backend.go:901,914,936,965`).
- **UI:** OIDC/identity-provider association ops uncovered (access-entries/addons/fargate/pod-identity present).
- _Recently closed:_ AfterFunc delete-race → worker.Group; nodegroup/addon/fargate UI.

### elasticbeanstalk
- **Parity:** environments created directly `Ready`, no Launching→Ready / Terminating lifecycle
  (`backend.go:683,923`); `ComposeEnvironments` is a stub returning existing envs (`backend.go:~1335`).
- **UI:** RebuildEnvironment, Request/RetrieveEnvironmentInfo, managed platform versions, composed environments
  uncovered.
- **Performance / Leaks:** none remaining (indexed DNS/ops-role lookups; events ring-capped at 1000).
- _Recently closed:_ ResourceNotFoundException; real timestamps + DateUpdated; config-template key separator;
  bounded events.

## Storage & transfer

### efs
- **Parity:** `creating→available` lifecycle only simulated when `fsActivationDelay>0` (default 0, so
  out-of-box file systems skip `creating`) (`backend.go:711,740`).
- **Performance:** every Describe* allocates + sorts the full slice each call (`backend.go:798,1056`).
- **Leaks:** the activation goroutine uses a raw `time.Sleep` with no cancellation (gated by non-default delay)
  (`backend.go:743`).
- **UI:** no replication, backup policy, account preferences, or resource-tag UI.
- _Recently closed:_ mount-target VPC/AZ fields; O(1) idempotency + subnet indexes; binary-search pagination.

### transfer
- **Parity:** `TestConnection`/`TestIdentityProvider` validate then return canned/synthetic results
  (`handler.go:3380,3411`).
- **Performance:** `ListServers` deep-clones every server under lock via `cloneServer` (`backend.go:975`).
- **Leaks / UI:** none remaining (worker-managed state transitions; full Transfer UI present).
- _Recently closed:_ security-policy full shape; worker-managed transitions; entire Transfer UI.

### backup
- **Parity:** `GetBackupPlanFromJSON` returns empty Rules (`handler.go:4114-4122`); `GetBackupPlanFromTemplate`
  always `"template-plan"` (`handler.go:4123-4130`); `ListBackupPlanTemplates` empty (`handler.go:4131-4133`).
  (Note: a leftover untracked build artifact `handler.go.tmp.*` sits in the service dir.)
- **Performance:** `ListBackupVaults`/`ListBackupJobsFiltered`/`ListRecoveryPointsFiltered` sort before
  paginate (`backend.go:489`; `backend_parity.go:117,179`); `paginateByID` O(n) linear cursor scan
  (`backend_parity.go:544`).
- **UI:** no selections, frameworks, report plans, restore testing, legal holds, vault policies/notifications,
  copy jobs.
- _Recently closed:_ protected-resource, restore-job, copy-job, legal-hold, report ops now real.

## Messaging & integration

### mq
- **Parity:** `DeleteConfiguration` is a phantom op (AWS MQ has no such API) (`handler.go:87`); `Promote` is a
  read-locked no-op with no failover/switchover (`backend.go:1514-1531`); `RebootBroker` returns 200 NoContent
  empty body, not `{}` (`handler.go:680`).
- **Performance:** `DescribeBroker`/`ListBrokers` take the full write lock to call `promoteRebootingToRunning`,
  penalizing reads (`backend.go:722,745`); O(n) creator-request-ID + config-name dup scans (`backend.go:692,1167`).
- **UI:** Promote and configuration create/delete absent.
- _Recently closed:_ RebootBroker + ListConfigurations UI.

### apigatewaymanagementapi
- **Parity:** `GetConnection` still emits a non-AWS `connectionId` field and `Connection` carries
  `postedMessages`/`bytesSent` (`types.go:45-46`); oversized payload returns plain `{message}` 413 not modeled
  `PayloadTooLargeException` (`handler.go:199-216`); `DeleteConnection` returns 204, AWS returns 200
  (`handler.go:261`); no Forbidden/LimitExceeded modeling.
- **Performance:** `Broadcast` holds the write lock for the whole fan-out with per-conn make+copy
  (`backend.go:278-306`); `Stats` iterates all connections per poll (`backend.go:342-344`).
- **Leaks:** `connections` map unbounded; only manual `PruneIdle`, no reaper (`backend.go:310`).
- **UI:** no `GetConnection` call; server `?q=` search path dead.
- _Recently closed:_ manual PruneIdle endpoint + UI modal.

### scheduler
- **Parity:** `nextToken` is a transparent `group/name` composite (`backend.go:494`; `paginate:801`); an unknown
  token silently restarts from index 0 (`backend.go:802-814`); `FlexibleTimeWindow` stored/echoed but never
  honored at delivery (`runner.go:170-175,282`); one-time `at(...)` schedules never fire — `isDue` handles only
  `rate(`/`cron(` (`runner.go:201-213`).
- **Performance:** the runner calls `ListSchedules(...,0)` cloning all schedules every tick (`runner.go:156`).
- _Recently closed:_ List clone narrowed to matched items.

### pipes
- **Parity:** runner routes only SQS sources; Kinesis/DynamoDB-Streams/MSK/Kafka/MQ sources accepted by the
  handler are silently unrouted (`runner.go:231-235`).
- **Performance:** `sortedPipeNames` hand-rolled O(n²) selection sort (`backend.go:1135-1148`); `ListPipes`
  holds RLock for the entire collect/filter/clone (`backend.go:1092-1094`).
- **Leaks / UI:** none material (delayed transitions tracked via `b.wg`/`svcCtx`); no non-SQS source guidance
  or DLQ/filter editor surfaced.
- _Recently closed:_ StartPipe already-RUNNING → ConflictException.

### kafka
- **Parity:** `ListNodes`/`ListScramSecrets` lack pagination (`handler.go:2452,2019`); CREATING→ACTIVE advances
  only inside `DescribeCluster`, so a cluster only ever `ListClusters`'d stays CREATING forever
  (`backend.go:707`; `handler.go:1318`); `DeleteCluster` removes synchronously, no DELETING (`backend.go:757`).
- **Leaks:** `maxClustersPerRegion` cap silently evicts an arbitrary existing cluster on overflow — non-AWS,
  data-losing (`backend.go:605`).
- **UI:** no `kafka` route — every op uncovered.
- _Recently closed:_ list pagination for clusters/configs/replicators/topics/revisions; CREATING initial state;
  O(1) name index.

## Discovery, workflow & tracing

### servicediscovery
- **Parity:** `CreateNamespace` completes synchronously, stamping `Status:SUCCESS` while returning an
  `OperationId` implying async work — `GetOperation` never simulates PENDING/IN_PROGRESS (`backend.go:351-359,844`);
  `DiscoverInstances` honors MaxResults but ignores NextToken (`handler.go:1029`).
- **Performance:** List ops do full map iteration + `sort.Slice` every call, no index (`backend.go:453-477,
  585-606,699-721`); `countServicesInNamespace` adds an O(n) scan per namespace per list (`backend.go:441`).
- **UI:** no DeleteNamespace/DeleteService/DeregisterInstance/DiscoverInstances/tag/health ops.
- _Recently closed:_ DiscoverInstances MaxResults; full pagination helpers + cursors.

### swf
- **Parity:** `openCountsLocked` hardcodes `openTimers:0`/`openChildWorkflowExecutions:0` (`backend.go:1250-1251`);
  count handlers never set `Truncated` (`handler.go:881,908`); `DescribeWorkflowExecution` type-asserts the
  concrete backend, bypassing the interface (`handler.go:1082`); StartTimer/CancelTimer/child/signal decisions
  append history events with empty attribute maps and timers never fire `TimerFired` (`backend.go:1740-1748`);
  domain ARN uses `defaultRegion`/`defaultAccountID`, never `awsmeta.Region(ctx)` (`backend.go:540,396`).
- **Performance:** `openCountsLocked` O(n)+O(n·m) scans per call (`backend.go:1232-1245`); `PollForDecisionTask`
  copies the full history slice under the write lock (`backend.go:1454-1462`).
- **Leaks:** `activeActivityTasks` has no heartbeat-timeout reaper (`backend.go:1415,1475`); flat
  executions/history/domains maps with no region dimension.
- **UI:** no RegisterWorkflowType/RegisterActivityType, CountOpen/ClosedWorkflowExecutions, Deprecate* ops.
- _Recently closed:_ executionOrder duplicate-key growth bounded; SWF UI route added.

### xray
- **Parity:** group & sampling-rule ARNs built from construction-time `b.region`/`b.accountID`, no per-request
  region (`backend.go:375-381`); flat resource maps with no region key, cross-region leak (`backend.go:283-308`).
- **Leaks:** `insights`/`insightEvents`/`serviceWindows`/`samplingStats` maps never evicted (janitor sweeps only
  traces + retrievals) (`backend.go:289-298`; `janitor.go:71-101`).
- **UI:** no sampling-rule CRUD, insights view, PutTraceSegments, group create/update/delete, or trace retrieval.
- _Recently closed:_ groupsByARN O(1) index; insight detection; service-graph topology from segments;
  ListRetrievedTraces segment payloads; retrieval-token janitor sweep.

## Developer tools (CI/CD)

_Cross-cutting across all four: client-guessable decimal-index pagination tokens and `*NotFoundException`
mapped to HTTP 400 instead of 404._

### codebuild
- **Parity:** `StartBuild` builds are created `IN_PROGRESS` with a single SUBMITTED phase, never advancing to
  SUCCEEDED/phases/logs (`backend.go:842-854`); `DescribeCodeCoverages`/`DescribeTestCases`/`GetReportGroupTrend`
  empty (`backend.go:2004,2009,2014`); `ListShared*`/`ListCuratedEnvironmentImages` stubbed (`backend.go:2019-2029`);
  `StartSandboxConnection` returns `wss://localhost:9999/<id>` (`handler.go:1604`); list inputs empty, no
  pagination (`handler.go:472,603`).
- **Performance:** env-override merge O(n·m) nested loop (`backend.go:771-786`).
- **Leaks:** janitor sweeps only `builds`; `sandboxesByProject`/`batchesByProject`/`commandsBySandbox` unbounded
  (`janitor.go:81`; `backend.go:391-397`).
- **UI:** sandbox/batch ops absent.
- _Recently closed:_ janitor single-pass delete + `buildsByProject` cleanup.

### codecommit
- **Parity:** `GetFile`/`GetFolder`/`GetFolderFiles` ignore `commitSpecifier`, reading HEAD
  (`backend_ops.go:818,841,867`); `DeleteFile` ignores `parentCommitID` (`backend_ops.go:895`);
  `EvaluatePullRequestApprovalRules` always `Satisfied:true` (`backend_ops.go:565`); `GetMergeConflicts` always
  false (`backend_ops.go:1272`); `repoMetadata` emits `"Arn"` not `"arn"` (`handler.go:434`).
- **Performance:** comment getters O(n) scans (`backend_ops.go:688,708`); `GetBlob` O(n) over all files
  (`backend_ops.go:1120`).
- **UI:** merge ops (MergePullRequestByFastForward, …) absent.
- _Recently closed:_ ListFileCommitHistory filePath filtering; CreatePullRequest UI.

### codepipeline
- **Parity:** `GetPipelineExecution` returns synthetic `Succeeded` for unknown IDs instead of not-found
  (`backend.go:1301-1306`); `StartPipelineExecution` sets `InProgress` and never finalizes the parent
  (`backend.go:1244,1258-1270`); `RetryStageExecution`/`RollbackStage` ignore inputs, mutate nothing
  (`backend.go:1454,1477-1481`); `OverrideStageCondition` no-op (`backend.go:1499-1500`); `StopPipelineExecution`
  discards `reason` (`backend.go:1335`).
- **Performance:** `DeleteCustomActionType` iterates all pipelines/stages under the write lock (`backend.go:867,879`).
- **Leaks:** `executions`/`actionExecutions` slices grow unbounded per StartPipelineExecution (`backend.go:1251,1270`).
- **UI:** job-polling ops (PollForJobs, AcknowledgeJob, PutJobSuccessResult) absent.
- _Recently closed:_ ListPipelines pagination; webhook tag storage.

### codedeploy
- **Parity:** `CreateDeployment` immediately `Succeeded` with faked `completeTime`, no lifecycle
  (`backend.go:902-916`); `ContinueDeployment` validates only (`backend.go:1347-1356`);
  `BatchGetDeploymentInstances`/`BatchGetDeploymentTargets` hardcoded `Succeeded` (`backend.go:1267,1294`);
  `BatchGetApplicationRevisions` stores no revision data (`backend.go:1188`); list ops have no NextToken/
  MaxResults (`handler.go:354,884,1047`).
- **Leaks:** `AddTagsToOnPremisesInstances` auto-registers unknown names, growing `onPremisesInstances`
  unboundedly (`backend.go:1168-1180`); **no janitor exists** — `deployments` map never evicted (`backend.go:894`).
- **UI:** GetDeploymentTarget/BatchGetDeploymentTargets absent.
- _Recently closed:_ ListDeploymentInstances + BatchGetDeploymentInstances UI.
