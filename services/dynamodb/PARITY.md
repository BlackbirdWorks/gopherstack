---
service: dynamodb
sdk_module: aws-sdk-go-v2/service/dynamodb@v1.63.1   # version audited against (go.mod pin)
last_audit_commit: 778a69f0e
last_audit_date: 2026-08-13
overall: A   # gopherstack-rkmp deep pass: fixed 6 wire-layer field-drop bugs (GetItem/Scan/Query/BatchGetItem/BatchWriteItem/TransactWriteItems all silently discarded ReturnConsumedCapacity/ReturnItemCollectionMetrics/ConsistentRead/AttributesToGet/Select on the way in or the computed value on the way out), added BatchWriteItem/BatchGetItem/TransactWriteItems/TransactGetItems throughput enforcement (previously bypassed provisioned-capacity throttling entirely), wired BatchWriteItem/TransactWriteItems ItemCollectionMetrics (never populated). Flagged, not fixed: GSI/LSI Query always full-scans the base table (no per-index structure exists; measured ~380x-5900x slower than a PK query at 10k/100k items) -- tracked as a follow-up, see gaps.
protocol: json-1.0 (DynamoDB_20120810 targets)
families:
  item_crud:    {status: ok, note: PROVEN — condition eval, all ReturnValues, ItemCollectionMetrics/LSI 10GB, WCU/RCU formulas. 2026-08-13: GetItem's wire model (models.GetItemInput/GetItemOutput) was silently dropping ReturnConsumedCapacity, ConsistentRead, AttributesToGet on input and ConsumedCapacity on output even though the backend computed everything correctly -- fixed at the wire boundary in models/convert_ops.go, not the backend.}
  query_scan:   {status: ok, note: PROVEN pagination (LastEvaluatedKey w/ base-PK fusion for GSI/LSI, 1MB/Limit); FIXED Select/COUNT omits Items + Select constraint validation. 2026-08-13: ToSDKQueryInput never copied ReturnConsumedCapacity/Select despite models.QueryInput declaring ReturnConsumedCapacity (Select wasn't declared at all); models.ScanInput was missing ReturnConsumedCapacity/ConsistentRead/Select entirely and models.ScanOutput was missing ConsumedCapacity -- all fixed at the wire boundary. A real client could never get ConsumedCapacity from Query/Scan, nor a COUNT-only Scan/Query response, regardless of what it requested. GSI/LSI Query still always falls through to a full table scan (filterCandidatesForKeyCondition only tries the authoritative pkIndex/pkskIndex when IndexName=="") -- flagged, not fixed this pass; see gaps.}
  batch:        {status: ok, note: FIXED BatchWriteItem duplicate-key validation (was missing; BatchGetItem had it). 2026-08-13: BatchGetItem/BatchWriteItem never called the throttler at all (db.throttler.ConsumeRead/ConsumeWrite) despite ProvisionedThroughputExceededException being a real, documented error for both ops (confirmed against deserializers.go's per-op error switch) -- provisioned-capacity tables never throttled batch calls even though every single-item op did; fixed with per-table charging mirroring the existing single-item formulas, PAY_PER_REQUEST still bypasses. Also: models.BatchGetItemInput/BatchWriteItemInput had no ReturnConsumedCapacity field at all (BatchWriteItemInput also missing ReturnItemCollectionMetrics) -- silently dropped on the wire regardless of client request; fixed in models/convert_ops.go. BatchWriteItemOutput.ItemCollectionMetrics (a real, conditionally-populated SDK field per api_op_BatchWriteItem.go) was never populated by the backend even when requested on an LSI table -- wired using the same per-item SizeEstimateRangeGB formula PutItem/DeleteItem already use.}
  transactions: {status: ok, note: FIXED TransactWriteItems Update key-mutation — was NOT validated, silently corrupted pkIndex/pkskIndex (state corruption bug). 2026-07-24: FIXED gopherstack-daa — Put/Update/Delete/ConditionCheck now reject an ExpressionAttributeNames/Values placeholder unused by that item's expression(s), matching plain PutItem/UpdateItem/DeleteItem (checkUnusedExpressionAttributeNames/Values in expressions.go); Update correctly considers both UpdateExpression AND ConditionExpression when deciding "used". Enforced pre-lock in validateTransactWriteItems (transact_validation.go) so it's a plain ValidationException, not wrapped in CancellationReasons — matches AWS request-validation-time semantics. 2026-08-13: TransactWriteItems/TransactGetItems never called the throttler (same gap as BatchWriteItem/BatchGetItem above); fixed with per-table charging. ToSDKTransactWriteItemsInput had a dangling `// ReturnItemCollectionMetrics` comment instead of actually copying the field, so it was always dropped on the wire even though models.TransactWriteItemsInput declared it and TransactWriteItemsOutput.ItemCollectionMetrics (a real SDK field) was never populated by the backend regardless; both fixed.}
  streams:      {status: ok, note: PROVEN shard-iterator sequence clamping, trim-horizon; streamARNIndex now a store.Table, verified Put/Delete key derivation unchanged. 2026-07-24 (gopherstack-exg7): (1) DescribeStream's ShardFilter{Type:CHILD_SHARDS,ShardId} was accepted on the wire but silently ignored — now filters found.streamShards by ParentShardID (parseShardFilter/filterChildShards in streams_ops.go), rejecting unsupported filter Types and a missing ShardId with ValidationException; verified a filter that legitimately matches zero shards returns a real empty Shards list rather than the "stream just enabled" placeholder shard (buildSDKShardsList's synthesizePlaceholder flag). (2) ShardIteratorStore gained a clock-injection seam (now func() time.Time, SetClock/Now) — resolveIterator's expiry check now reads db.iteratorStore.Now() instead of time.Now() directly, so ExpiredIteratorException is exercised end-to-end via GetShardIterator -> advance fake clock -> GetRecords in a test, not just via the pre-existing ExpireAllShardIteratorsForTest backdate-hack. (3) De-duplicated the wire<->SDK AttributeValue conversion functions that were split across streams_ops.go (wire->SDK: toStreamAttributeValue/dispatchStreamType/buildSDKStreamItem/buildSDKRecord) and streams_wire.go (SDK->wire: FromStreamAttributeValue/FromStreamItem) — both directions (and their shared sentinel errors) now live together in streams_wire.go; streams_ops.go keeps only shard/record-management logic.}
  janitor_ttl:  {status: ok, note: PROVEN batched-lock, ctx-cancel, quickselect eviction, ring-buffer compaction}
  datalayer:    {status: ok, note: RE-AUDITED — ce30166a converted db.Tables/Backups/GlobalTables/exports/imports/streamARNIndex from raw maps to pkgs/store.Table+Index (composite key tableKey(region,name), region derived by parsing TableArn via tableRegion()). Verified every insertion site (CreateTable, RestoreTable, CreateGlobalTable replicas, cloneTableSchema, applyOneReplicaTableEntry) builds TableArn with the same region string used as the store key *before* Put, so tableRegion(t) round-trips correctly; TableArn is never mutated post-insert. No stale map-key leaks (tablesByRegion Index auto-empties groups on last delete, unlike the old per-region submap). Persistence snapshot reshaped map->sorted slice + added a schema version gate (old snapshots discarded cleanly on upgrade, matching the sqs/ec2 precedent) — intentional, not a parity bug.}
gaps:
  - "2026-08-13 (gopherstack-rkmp, PERFORMANCE, flagged not fixed): Query against a
    GSI or LSI always does a full O(table size) linear scan. store.go's Table only
    maintains pkIndex/pkskIndex for the BASE table key; item_ops_query.go's
    filterCandidatesForKeyCondition only calls tryFilterUsingAuthoritativeIndex when
    IndexName=='' and falls through to filterCandidatesScan (a plain range over
    table.Items) for every GSI/LSI query regardless of how selective the key
    condition is. Measured with a new benchmark (BenchmarkQuery_GSI in
    benchmarks_test.go): a primary-key Query is a flat ~4.7us regardless of table
    size (BenchmarkQuery/WithIndex_10k); a GSI Query is 1.82ms at 10k items and
    28.0ms at 100k items (BenchmarkQuery_GSI/10000, /100000) -- roughly linear in
    table size, ~380x-5900x slower than the PK path. Not fixed this pass: a correct
    fix needs a genuine per-GSI/LSI secondary-index structure (GSI keys are not
    unique the way the base table's are, so it can't reuse pkIndex/pkskIndex's
    shape), with maintenance on every PutItem/UpdateItem/DeleteItem/BatchWriteItem/
    TransactWriteItems path plus backfill on CreateTable-with-GSI and UpdateTable
    GSI-add, and correct behavior for sparse GSIs (items missing the GSI key). That
    is real feature work with real correctness risk across many write paths, not a
    quick data-structure swap, so it is flagged for a dedicated follow-up rather
    than rushed here. Scan against a GSI/LSI is NOT part of this gap: Scan already
    scans the whole table by design (matching real DynamoDB Scan's own complexity
    class), it doesn't regress from an index lookup the way Query does."
  - "2026-08-05: SearchVectors (new in SDK v1.63.1) — DynamoDB vector indexes have no
    backend model here: CreateTable/UpdateTable have no field or code path that attaches a
    vector index to a table, so no vector index can ever exist in this backend. Fabricating
    similarity scores for a search against an index that was never created would violate
    the no-fabricated-data rule. search_vectors.go implements full request validation
    (TableName/IndexName/SearchVector/TopK required, matching the SDK's
    validateOpSearchVectorsInput) and a real table-existence check, then honestly returns
    ResourceNotFoundException for the named index — the same response real DynamoDB gives
    for any index name on a table with no vector indexes. Wire types/converters
    (SearchVectorsInput/Output, VectorCapacity, SearchResultItem) are implemented in full
    for shape-correctness even though the success path is never reached. Full vector-index
    support (CreateTable VectorIndex, index storage, real similarity scoring) is out of
    scope for this pass — tracked as a follow-up if vector search ever becomes a priority."
deferred:
  - expr/ lexer/parser/evaluator subpackage (has own aws_spec_test.go/evaluator_test.go) — not line-by-line re-audited this sweep; genuinely large surface, out of scope for this streams/transactions-focused follow-up pass. No known bugs, just not freshly field-diffed against the SDK this cycle.
  - PartiQL execution (partiql.go, ~37KB) — not re-audited this sweep, same reason as above.
leaks: {status: clean, note: TTL sweeper + stream trimming verified, ctx-cancel present. ShardIteratorStore's SetClock mutates the store's `now` field under s.mu (same lock Put/Get/Sweep already take), so the clock-injection seam introduces no new race — verified via `go test -race`.}
---

## Notes
- 2026-08-13 (gopherstack-rkmp): two existing tests exercised the SDK-typed backend
  method directly (or patched the SDK struct after conversion) rather than going
  through the wire-format models.*Input -> ToSDK*Input path a real client's JSON
  body actually takes, which is exactly why they never caught the ReturnConsumedCapacity
  wire-drop bugs above: TestTransactWriteItems_ConsumedCapacity calls
  db.TransactWriteItems with a hand-built *sdk.TransactWriteItemsInput{ReturnConsumedCapacity: ...}
  (bypassing ToSDKTransactWriteItemsInput entirely), and TestQuery_ConsistentRead_ConsumedCapacity
  calls models.ToSDKQueryInput then manually overwrites
  sdkQuery.ReturnConsumedCapacity afterward (masking that ToSDKQueryInput itself
  never set it). Neither test was asserting anything false — they just couldn't see
  the gap they were standing next to. New tests (`*_SurvivesWireConversion`) added
  alongside each to close that blind spot; the old tests are left as-is since they
  still correctly cover the backend-level ConsumedCapacity math.
- BatchWriteItem rejects same-key Put+Delete / Put+Put / Delete+Delete in one call: "Provided list of item keys contains duplicates" (verified docs + boto3 history). A prior test asserted the opposite — corrected.
- Select=COUNT returns Count/ScannedCount only, Items omitted.
- Select=SPECIFIC_ATTRIBUTES requires a projection; ALL_PROJECTED_ATTRIBUTES invalid on bare table.
- 2026-07-11 re-audit: aws-sdk-go-v2/service/dynamodb bumped f459c9fa's v1.59.2 -> HEAD's v1.60.0 (e51c0de9); diffed api_op_*.go/types.go between the two module versions — zero surface change (v1.60.0's only changelog entry is "Add request serialization snapshot tests"), so no new-op audit was needed this cycle.
- 2026-07-11 re-audit: no real bugs found. All gates pass (build, vet, race tests, go fix -diff empty, golangci-lint 0 issues) with zero working-tree changes required.
- 2026-07-24 follow-up sweep: verified against dynamodbstreams@v1.35.0 (go.mod) that ShardFilter is the only new DescribeStreamInput field this backend hadn't wired up (types.ShardFilter{ShardId, Type}, only CHILD_SHARDS defined in ShardFilterType.Values()). services/dynamodbstreams (the sibling client-facing service that reads this backend's stream buffer) required no changes — it passes ShardFilter straight through the SDK input struct, which already carried the field; the gap was entirely on the dynamodb-backend side. `go build ./...` and `go test -race ./services/dynamodbstreams/...` both verified clean after the change.
