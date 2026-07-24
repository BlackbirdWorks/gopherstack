---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: dynamodbstreams
sdk_module: aws-sdk-go-v2/service/dynamodbstreams@v1.35.0   # version audited against
last_audit_commit: 95ab0584                                  # HEAD when this manifest was written
last_audit_date: 2026-07-24
overall: B            # A = ~1k genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ListStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "delegates to ddbbackend.StreamsBackend; region-scoped, ExclusiveStartStreamArn/Limit pagination verified"}
  DescribeStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "ShardFilter input field accepted but unused by backend (cross-service gap, see gaps below)"}
  GetShardIterator: {wire: ok, errors: ok, state: ok, persist: ok, note: "iterators are opaque server-side tokens (ddbbackend.iteratorStore); genuinely ephemeral per real AWS 15-min TTL, correctly NOT persisted (see leaks/persist note)"}
  GetRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "NextShardIterator always present unless shard closed+drained; verified against real records written by dynamodb PutItem/UpdateItem/DeleteItem via GetRecentEvents-backed ring buffer"}
# Families audited as a group (when per-op is impractical):
families:
  wire_shapes: {status: ok, note: "field-by-field diff against aws-sdk-go-v2/service/dynamodbstreams@v1.35.0 deserializers.go for DescribeStreamOutput/GetRecordsOutput/ListStreamsOutput/GetShardIteratorOutput, Shard, SequenceNumberRange, StreamDescription, Record, StreamRecord, Identity, KeySchemaElement, AttributeValue (S/N/B/BOOL/NULL/M/L/SS/NS/BS) — no mismatches found"}
  errors: {status: ok, note: "errCodeLookup surface matches SDK-modeled exceptions per op (DescribeStream: ResourceNotFoundException/InternalServerError; GetShardIterator: +TrimmedDataAccessException; GetRecords: +ExpiredIteratorException/LimitExceededException/TrimmedDataAccessException; ListStreams: ResourceNotFoundException/InternalServerError). handleError() correctly rewrites com.amazonaws.dynamodb.v20120810# -> com.amazonaws.dynamodbstreams.v20120810# namespace and maps InternalServerError to HTTP 500, all else to 400."}
  route_matcher: {status: ok, note: "X-Amz-Target prefix 'DynamoDBStreams_20120810.' verified against serializers.go (all 4 ops); Content-Type 'application/x-amz-json-1.0' (awsjson1.0, not 1.1) verified; no collision with DynamoDB_20120810. prefix used by the main dynamodb service"}
  persistence: {status: ok, note: "Handler intentionally has no Snapshot/Restore -- stream state (StreamARN/StreamsEnabled/StreamViewType/StreamCreatedAt/StreamRecords ring buffer) lives entirely in the shared *ddbbackend.InMemoryDB object (services/dynamodb), which already persists it under the 'DynamoDB' key. Guarded by persistence_test.go:TestHandler_OwnsNoState. Shard iterators are correctly NOT persisted (ephemeral, matches real AWS 15-min iterator TTL)."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "DescribeStreamInput.ShardFilter (CHILD_SHARDS filtering) is accepted on the wire but ignored by the backend (services/dynamodb/streams_ops.go DescribeStream never reads input.ShardFilter) -- cross-service, backend lives in services/dynamodb, not editable from this service (bd: file follow-up)"
  - "services/dynamodb/streams_wire.go duplicates the wire-marshaling helpers in this package's handler.go (toWireDescribeStreamOutput, wireStreamDescription, wireGetRecordsOutput, wireStreamRecord, fromStreamItem, fromStreamAttributeValue) nearly verbatim, used by services/dynamodb/handler.go's own internal stream-passthrough endpoints -- cross-service duplication/reuse opportunity, not editable from this service (bd: file follow-up)"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - none (all 4 routed ops fully audited this pass)
leaks: {status: clean, note: "Handler owns zero maps/goroutines; all state lives in the shared dynamodb backend which manages its own ring buffer eviction (streamTrimSeq) and iterator TTL expiry (iteratorStore)"}
---

## Notes

- Protocol is `awsjson1.0` (X-Amz-Target: `DynamoDBStreams_20120810.<Op>`, Content-Type
  `application/x-amz-json-1.0`) — confirmed against the real SDK's generated
  `serializers.go`. This is a distinct target prefix from the main DynamoDB API's
  `DynamoDB_20120810.` prefix, so `RouteMatcher`'s `strings.HasPrefix` check cannot
  collide between the two services.

- Only 4 operations exist in the real API: `ListStreams`, `DescribeStream`,
  `GetShardIterator`, `GetRecords`. Confirmed exhaustive via
  `sdk_completeness_test.go` (`sdkcheck.CheckCompleteness` against
  `dynamodbstreamssdk.Client{}`).

- Timestamps (`CreationRequestDateTime`, `ApproximateCreationDateTime`) are wire-encoded
  as Unix epoch-seconds `float64`, NOT ISO8601 strings, per the DynamoDB Streams JSON
  1.0 protocol. This package's `handler.go` builds parallel `wire*` structs specifically
  to override the SDK struct's native `*time.Time` marshaling (which would otherwise
  produce an RFC3339 string) — this is correct and required, not incidental duplication.
  Note: `handler_streams_test.go`'s `TestHandler_DescribeStream_CreationRequestDateTime`
  has a stale comment claiming the SDK marshals `*time.Time` as RFC3339 "so we verify
  presence rather than format" — the actual wire output IS a float64 via
  `toWireDescribeStreamOutput`; the test still passes because its assertion only checks
  presence/non-nil, not format. Harmless (comment-only staleness), left as-is since it's
  a test comment, not incorrect behavior.

- `GetShardIterator`/`GetRecords` shard-closing semantics: real AWS returns a nil
  `NextShardIterator` from `GetRecords` ONLY when the shard is closed (split) AND fully
  drained past its `EndingSequenceNumber`; otherwise `NextShardIterator` is always
  present (even with zero `Records`) so pollers keep advancing. Verified this is
  implemented correctly in `services/dynamodb/streams_ops.go` (`GetShardIterator`
  carries the shard's `EndingSequenceNumber` through `iteratorStore.PutWithEnd`, and
  `GetRecords` nils the iterator only when `endSeq > 0 && nextSeq > endSeq`). This is the
  single highest-risk bug class called out in the audit brief (disguised no-op /
  infinite-poll trap) and it is NOT present here.

- Record data (`GetRecentEvents`) is genuinely sourced from the DynamoDB table's own
  stream ring buffer (`table.StreamRecords`, populated by PutItem/UpdateItem/DeleteItem
  in the `dynamodb` service) — `GetRecords` is not a disguised empty stub; it reads real
  mutation history.

- `dynamodbstreams.Handler` deliberately does NOT implement `Snapshot`/`Restore` — it
  holds zero independent state. All durable stream state lives on the shared
  `*ddbbackend.InMemoryDB` object already persisted under the `"DynamoDB"` snapshot key.
  Implementing persistable methods here would double-register and double-restore the
  same backend object. This invariant is guarded by
  `persistence_test.go:TestHandler_OwnsNoState`.

## Audit outcome

No real bugs found in `services/dynamodbstreams/` this pass. All 4 routed operations
were verified op-by-op against the real `aws-sdk-go-v2/service/dynamodbstreams@v1.35.0`
serializers/deserializers (wire field names/casing, timestamp encoding, attribute-value
shapes, error taxonomy per operation). Gates (build/test -race/vet/go fix/lint) were all
green with zero changes required. This package had already been through four prior
parity passes (`da1b1da1`, `45524338`, `b1146508`, `ce30166a`) plus a dedicated
persistence-invariant doc commit (`cfdae261`); this fresh audit corroborates that prior
work rather than finding new regressions.

## Re-audit 2026-07-24

Re-verified field-by-field against the same SDK module already checked out locally
(`aws-sdk-go-v2/service/dynamodbstreams@v1.35.0` source read directly from the module
cache: `types/types.go`, `api_op_*.go`, `validators.go`, `deserializers.go`). No wire,
error-taxonomy, or state-mutation bugs found; confirms the `overall: B` grade still
holds. This pass focused on closing test-coverage gaps in *this* package (the sibling
`services/dynamodb` backend already had thorough coverage of the underlying iterator/
sequence-number/trim logic in `streams_ops_test.go` and `streams_shard_iterator_test.go`
— those were read and cross-checked, not duplicated) so that `services/dynamodbstreams`
proves its own wire-translation layer independently:

- `handler_records_test.go`: added HTTP-level coverage for `AT_SEQUENCE_NUMBER` /
  `AFTER_SEQUENCE_NUMBER` iterator boundary semantics, the `ValidationException` path
  when `SequenceNumber` is omitted for those two types, `TrimmedDataAccessException`
  (writes 1001 items to age sequence 1 out of the 1000-record ring buffer, then requests
  it — verifies the `dynamodbstreams` error namespace and HTTP 400), `StreamViewType`-
  driven record shape (`KEYS_ONLY`/`NEW_IMAGE`/`OLD_IMAGE`/`NEW_AND_OLD_IMAGES`, table-
  driven over INSERT vs MODIFY records), and `GetRecords` `Limit` pagination.
- `handler_streams_test.go`: added `DescribeStream` `Limit` + `ExclusiveStartShardId`
  pagination coverage (forces a shard split via 1001 `PutItem`s, same technique already
  used by `TestHandler_DescribeStream_ShardGenealogy`).

**Known test-coverage limitation (not a wire/state bug, left open):**
`ExpiredIteratorException` (`services/dynamodb/streams_ops.go`'s `resolveIterator`,
backed by `ShardIteratorStore`'s real 15-minute TTL in
`services/dynamodb/streams_shard_iterator.go`) is implemented correctly by inspection —
`Get()` compares `time.Now()` against `entry.ExpiresAt` and deletes+returns
`ExpiredIteratorException` past that point — but is **not exercised by any test** in
either package, because `shardIteratorTTL` is an unexported `const` with no clock-
injection seam, and no test may sleep 15 real minutes. Adding a seam (an exported/
overridable duration, or an injectable clock) would require a non-trivial change to
`services/dynamodb/streams_shard_iterator.go`, which is out of this task's scope ("do
NOT edit services/dynamodb unless a tiny read-accessor is genuinely required" — this
would be a mutator/testability seam, not a read-accessor, so it was deliberately not
made). Flagging as a follow-up: a bd issue should track adding a clock seam to
`ShardIteratorStore` so both packages can add a real `TestExpiredIteratorException`.
Separately, `GetRecords`' modeled `LimitExceededException` is never produced by this
backend (there is no artificial per-subscriber rate-limiting anywhere in this
emulator, consistent with how throttling-style exceptions are treated elsewhere in the
codebase); this is intentional, not a gap.
