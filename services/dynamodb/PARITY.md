---
service: dynamodb
sdk_module: aws-sdk-go-v2/service/dynamodb@v1.63.1   # version audited against (go.mod pin)
last_audit_commit: 97805509b
last_audit_date: 2026-08-14
overall: A   # gopherstack-rkmp deep pass (this audit, 2026-08-14): struct-field-diffed every wire model against the pinned SDK (see Notes) and fixed 3 more wire drops -- Query/Scan AttributesToGet (undeclared, and even where declared elsewhere the projection resolver never consulted it for these two ops), GSI/LSI IndexArn (+GSI IndexSizeBytes/Backfilling), ListBackups BackupSummary.BackupSizeBytes. PARITY.md itself was stale by 6 commits (7a2189b06..bc2e6285a) before this update -- see Notes. CONFIRMED FIXED, previously an open gap here: GSI/LSI Query full-scan (17c0ac7a7 added real per-GSI/LSI indexes; gopherstack-anlc verified 4.8-5.0us flat vs 1.82-28.0ms before). Flagged not fixed: legacy pre-expression API params (gopherstack-lze5), ReturnConsumedCapacity=INDEXES dead code (gopherstack-glfv) -- see gaps.
protocol: json-1.0 (DynamoDB_20120810 targets)
families:
  item_crud:    {status: ok, note: PROVEN — condition eval, all ReturnValues, ItemCollectionMetrics/LSI 10GB, WCU/RCU formulas. 2026-08-13: GetItem's wire model (models.GetItemInput/GetItemOutput) was silently dropping ReturnConsumedCapacity, ConsistentRead, AttributesToGet on input and ConsumedCapacity on output even though the backend computed everything correctly -- fixed at the wire boundary in models/convert_ops.go, not the backend. Same day, separately: CreateTable dropped SSESpecification/OnDemandThroughput on input (7a2189b06); UpdateTable dropped DeletionProtectionEnabled/TableClass/BillingMode/SSESpecification (7a2189b06); DescribeBackup/DeleteBackup dropped two required SourceTableDetails members (bc2e6285a). 2026-08-14 (this audit): ListBackups' BackupSummary had no BackupSizeBytes field at all, even though CreateBackup/DescribeBackup's BackupDetails already carried it for the same backup via the real per-backup b.SizeBytes -- fixed in models/types.go + backup_ops.go's collectBackupSummaries.}
  query_scan:   {status: ok, note: PROVEN pagination (LastEvaluatedKey w/ base-PK fusion for GSI/LSI, 1MB/Limit); FIXED Select/COUNT omits Items + Select constraint validation. 2026-08-13: ToSDKQueryInput never copied ReturnConsumedCapacity/Select despite models.QueryInput declaring ReturnConsumedCapacity (Select wasn't declared at all); models.ScanInput was missing ReturnConsumedCapacity/ConsistentRead/Select entirely and models.ScanOutput was missing ConsumedCapacity -- all fixed at the wire boundary. A real client could never get ConsumedCapacity from Query/Scan, nor a COUNT-only Scan/Query response, regardless of what it requested. GSI/LSI Query full-scan gap (previously documented here) is FIXED -- see overall. 2026-08-14 (this audit): AttributesToGet (the legacy pre-expression projection parameter, still real and wire-serialized per api_op_Query.go:92/api_op_Scan.go) was declared on neither models.QueryInput nor models.ScanInput, so it was silently dropped by json.Unmarshal regardless of what a client sent. Fixing the wire model alone was not enough: item_ops_query.go's collectQueryPage and item_ops_scan.go's doScan built their Projector from ProjectionExpression only, never falling back to AttributesToGet the way GetItem/BatchGetItem's resolveProjection() already did -- so even a correctly-wired AttributesToGet would have been silently ignored by the projection logic itself. Fixed both layers (models/types.go + convert_ops.go for the wire, item_ops_query.go/item_ops_scan.go for resolveProjection() reuse), and added the AttributesToGet+ProjectionExpression mutual-exclusion validation Query/Scan were missing (validateProjectionParams, already used by GetItem/BatchGetItem). Test: TestQueryScan_AttributesToGet_SurvivesWireConversion (hand-verified to fail against unfixed code: both subtests failed with "AttributesToGet should have excluded 'other'").}
  batch:        {status: ok, note: FIXED BatchWriteItem duplicate-key validation (was missing; BatchGetItem had it). 2026-08-13: BatchGetItem/BatchWriteItem never called the throttler at all (db.throttler.ConsumeRead/ConsumeWrite) despite ProvisionedThroughputExceededException being a real, documented error for both ops (confirmed against deserializers.go's per-op error switch) -- provisioned-capacity tables never throttled batch calls even though every single-item op did; fixed with per-table charging mirroring the existing single-item formulas, PAY_PER_REQUEST still bypasses. Also: models.BatchGetItemInput/BatchWriteItemInput had no ReturnConsumedCapacity field at all (BatchWriteItemInput also missing ReturnItemCollectionMetrics) -- silently dropped on the wire regardless of client request; fixed in models/convert_ops.go. BatchWriteItemOutput.ItemCollectionMetrics (a real, conditionally-populated SDK field per api_op_BatchWriteItem.go) was never populated by the backend even when requested on an LSI table -- wired using the same per-item SizeEstimateRangeGB formula PutItem/DeleteItem already use. BatchExecuteStatement dropped per-statement ConsistentRead on input, already forwarded correctly once it arrived (fbc2cfe1f).}
  transactions: {status: ok, note: FIXED TransactWriteItems Update key-mutation — was NOT validated, silently corrupted pkIndex/pkskIndex (state corruption bug). 2026-07-24: FIXED gopherstack-daa — Put/Update/Delete/ConditionCheck now reject an ExpressionAttributeNames/Values placeholder unused by that item's expression(s), matching plain PutItem/UpdateItem/DeleteItem (checkUnusedExpressionAttributeNames/Values in expressions.go); Update correctly considers both UpdateExpression AND ConditionExpression when deciding "used". Enforced pre-lock in validateTransactWriteItems (transact_validation.go) so it's a plain ValidationException, not wrapped in CancellationReasons — matches AWS request-validation-time semantics. 2026-08-13: TransactWriteItems/TransactGetItems never called the throttler (same gap as BatchWriteItem/BatchGetItem above); fixed with per-table charging. ToSDKTransactWriteItemsInput had a dangling `// ReturnItemCollectionMetrics` comment instead of actually copying the field, so it was always dropped on the wire even though models.TransactWriteItemsInput declared it and TransactWriteItemsOutput.ItemCollectionMetrics (a real SDK field) was never populated by the backend regardless; both fixed.}
  streams:      {status: ok, note: PROVEN shard-iterator sequence clamping, trim-horizon; streamARNIndex now a store.Table, verified Put/Delete key derivation unchanged. 2026-07-24 (gopherstack-exg7): (1) DescribeStream's ShardFilter{Type:CHILD_SHARDS,ShardId} was accepted on the wire but silently ignored — now filters found.streamShards by ParentShardID (parseShardFilter/filterChildShards in streams_ops.go), rejecting unsupported filter Types and a missing ShardId with ValidationException; verified a filter that legitimately matches zero shards returns a real empty Shards list rather than the "stream just enabled" placeholder shard (buildSDKShardsList's synthesizePlaceholder flag). (2) ShardIteratorStore gained a clock-injection seam (now func() time.Time, SetClock/Now) — resolveIterator's expiry check now reads db.iteratorStore.Now() instead of time.Now() directly, so ExpiredIteratorException is exercised end-to-end via GetShardIterator -> advance fake clock -> GetRecords in a test, not just via the pre-existing ExpireAllShardIteratorsForTest backdate-hack. (3) De-duplicated the wire<->SDK AttributeValue conversion functions that were split across streams_ops.go (wire->SDK: toStreamAttributeValue/dispatchStreamType/buildSDKStreamItem/buildSDKRecord) and streams_wire.go (SDK->wire: FromStreamAttributeValue/FromStreamItem) — both directions (and their shared sentinel errors) now live together in streams_wire.go; streams_ops.go keeps only shard/record-management logic.}
  janitor_ttl:  {status: ok, note: PROVEN batched-lock, ctx-cancel, quickselect eviction, ring-buffer compaction}
  datalayer:    {status: ok, note: RE-AUDITED — ce30166a converted db.Tables/Backups/GlobalTables/exports/imports/streamARNIndex from raw maps to pkgs/store.Table+Index (composite key tableKey(region,name), region derived by parsing TableArn via tableRegion()). Verified every insertion site (CreateTable, RestoreTable, CreateGlobalTable replicas, cloneTableSchema, applyOneReplicaTableEntry) builds TableArn with the same region string used as the store key *before* Put, so tableRegion(t) round-trips correctly; TableArn is never mutated post-insert. No stale map-key leaks (tablesByRegion Index auto-empties groups on last delete, unlike the old per-region submap). Persistence snapshot reshaped map->sorted slice + added a schema version gate (old snapshots discarded cleanly on upgrade, matching the sqs/ec2 precedent) — intentional, not a parity bug.}
gaps:
  - "2026-08-14 (gopherstack-rkmp/gopherstack-lze5, CORRECTNESS, flagged not fixed):
    DynamoDB's legacy pre-expression API (Expected, ConditionalOperator,
    AttributeUpdates, KeyConditions, QueryFilter, ScanFilter) is a real,
    wire-serialized part of the service (confirmed against serializers.go, not a
    doc comment -- it writes these exact JSON keys when a real client sets the
    corresponding SDK-struct field) but none of these seven fields is declared
    anywhere in models/types.go's QueryInput/ScanInput/PutItemInput/DeleteItemInput/
    UpdateItemInput. A client using the legacy API gets every one of these keys
    silently dropped by json.Unmarshal: ScanFilter/QueryFilter ignored means
    Scan/Query returns unfiltered results; AttributeUpdates ignored with no
    UpdateExpression means UpdateItem is a silent no-op; Expected/ConditionalOperator
    ignored means PutItem/UpdateItem/DeleteItem's conditional check never runs and
    the write always succeeds. Zero backend support exists for any of it (no
    reference to ComparisonOperator/AttributeValueUpdate/ConditionalOperator
    anywhere in services/dynamodb/*.go) -- this is real feature work (a second,
    legacy Condition-evaluation surface across 5 operations), not a quick wire fix,
    so it's flagged rather than rushed. AttributesToGet, the one legacy field with
    no evaluation-engine complexity, WAS fixed this pass (see families.query_scan)."
  - "2026-08-14 (gopherstack-rkmp/gopherstack-glfv, CORRECTNESS, flagged not fixed):
    ReturnConsumedCapacity=INDEXES never returns a per-index breakdown on any
    operation. capacity.go's buildConsumedCapacityWithIndexes/applyIndexBreakdowns
    correctly build types.ConsumedCapacity.Table/GlobalSecondaryIndexes/
    LocalSecondaryIndexes and are unit-tested in isolation, but grep confirms they
    are called from nowhere except export_test.go -- every real operation
    (PutItem/UpdateItem/DeleteItem/Query/Scan/BatchGetItem/BatchWriteItem/
    TransactGetItems/TransactWriteItems) builds a bare ConsumedCapacity{TableName,
    CapacityUnits, Read/WriteCapacityUnits} literal directly, so INDEXES and TOTAL
    produce byte-identical output everywhere. TestConsumedCapacityIndexes_PutItem
    is misleadingly named: despite the name and a GSI fixture, it actually requests
    TOTAL and never exercises the INDEXES path -- the same 'test looked like
    coverage and wasn't' pattern noted below for the pre-53cfd590b tests. Read-side
    fix (100% of RCU to the queried index) is straightforward; write-side fix
    (attributing WCU across every GSI/LSI a written item's key populates) needs
    AWS billing semantics not verified against a real account this pass, so it's
    flagged rather than guessed, per the no-fabrication rule."
  - "2026-08-14 (gopherstack-rkmp, minor/structural, not filed individually):
    struct-field-diffing every wire model against dynamodb@v1.63.1 turned up a
    long tail of fields absent because the underlying AWS feature has no backend
    model at all (same category as the SearchVectors gap below, not a wire drop):
    WarmThroughput and VectorIndexes on CreateTable/UpdateTable/GSI actions;
    GlobalTableWitnesses and MultiRegionConsistency (MRSC witness regions) on
    CreateTable/TableDescription; ResourcePolicy on CreateTableInput (resource-based
    policy IS modeled via the separate Put/GetResourcePolicy ops, just not the
    at-creation shortcut); VectorIndexOverride/LocalSecondaryIndexOverride on
    RestoreTableFromBackup/RestoreTableToPointInTime; several ReplicaDescription
    v2-global-table fields (ReplicaArn, KMSMasterKeyId, OnDemand/ProvisionedThroughputOverride,
    ReplicaStatusDescription/PercentProgress, ReplicaTableClassSummary,
    ReplicaInaccessibleDateTime); ProvisionedThroughputDescription's
    LastIncrease/DecreaseDateTime and NumberOfDecreasesToday (AWS itself rarely
    populates the latter post-2018 throttling changes); SSEDescription's
    InaccessibleEncryptionDateTime (only set when a KMS key becomes unreachable,
    a failure mode this backend doesn't model); BackupSummary/BackupDetails'
    BackupExpiryDateTime (only set on the SYSTEM auto-backups DynamoDB creates on
    table deletion with PITR enabled -- this backend only ever creates USER
    backups via CreateBackup, so there's genuinely no SYSTEM-backup expiry to
    report). None fabricated; all are honest absences, listed here so a future
    pass doesn't have to rediscover them by re-running the same diff."
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
- 2026-08-14 (gopherstack-rkmp): methodology for this audit was a mechanical
  struct-field diff, not another manual read-through: a small Go/AST program
  (not checked in) parsed every `*Input`/`*Output` struct from the pinned
  aws-sdk-go-v2/service/dynamodb@v1.63.1 (both the top-level api_op_*.go files
  and types/types.go) and every struct in services/dynamodb/models/types.go,
  normalized Go's `Id`/`Arn`/`Kms`/`Sse` vs `ID`/`ARN`/`KMS`/`SSE` naming
  variance, and reported SDK fields with no same-named counterpart in the
  matching gopherstack struct. This is exactly the "required response member
  never populated" bug class this pass was asked to hunt, made systematic
  instead of op-by-op. It over-reports (ResultMetadata is SDK-internal
  middleware state, not a wire field; a handful of hits were the Go-naming
  false positives the normalization pass didn't fully catch, e.g. TableId vs
  TableID before normalization was added) so every hit was hand-verified
  against the actual SDK serializer/type before being treated as a bug --
  three were real and fixed (AttributesToGet, IndexArn, BackupSizeBytes,
  see families above), two are real and flagged as feature work (see gaps),
  the rest are honest structural absences (see gaps) or SDK-internal noise.
  The diff also caught `SearchVectorsInput.SearchConditionExpression` as
  "declared but never read outside models/" -- checked against search_vectors.go
  and confirmed this is the ALREADY-documented SearchVectors gap below (the
  field is validated for wire-shape correctness but the success path that
  would consume it is never reached), not a new bug -- included here as a
  cross-check that the diff produces real signal rather than only false
  positives.
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
