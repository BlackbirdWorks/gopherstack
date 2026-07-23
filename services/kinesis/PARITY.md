---
service: kinesis
sdk_module: aws-sdk-go-v2/service/kinesis@v1.43.2
last_audit_commit: 2b2086c9
last_audit_date: 2026-07-23
overall: A            # this pass: closed 4 of the 5 open gaps for real (KMS KeyId validation, UpdateStreamMode auto-reshard, AT_TIMESTAMP required-Timestamp, ListShards true timestamp/AT_TRIM_HORIZON/AFTER_SHARD_ID filtering); deleted an invented "AT_SHARD_ID" ShardFilterType that doesn't exist in the real SDK; corrected a stale "Lambda ESM deferred" note (it's already wired in cli.go). Only remaining gap is KMSAccessDeniedException, honestly undeliverable without an IAM policy engine.
ops:
  IncreaseStreamRetentionPeriod: {wire: ok, errors: ok, state: ok, persist: ok, note: "reverted 2b2086c9: that commit made equal-to-current RetentionPeriodHours return InvalidArgumentException (a strict reading of the aws-sdk-go-v2 doc comment 'Must be more than the current retention period'), which broke TestTerraform_Kinesis in CI -- terraform's aws_kinesis_stream resource issues IncreaseStreamRetentionPeriod even when the requested value already equals the stream's current retention (confirmed live: CreateStream -> 24h default -> Increase(48) OK -> a second Increase(48) against the already-48h stream 400'd with InvalidArgumentException before this fix). Real AWS tolerates the equal case rather than erroring on every no-drift re-apply, so restored equal-value == no-op success. Strictly-lower and out-of-[24,8760] values are still rejected."}
  DecreaseStreamRetentionPeriod: {wire: ok, errors: ok, state: ok, persist: ok, note: "reverted 2b2086c9, mirrored: equal-to-current RetentionPeriodHours is a no-op success again (not InvalidArgumentException), matching real AWS/terraform tolerance. Strictly-greater and below-24h-min values are still rejected."}
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ON_DEMAND now defaults to 4 shards (was 1); inline Tags now validated pre-mutation and persisted via TagResource instead of a lost handler-local map"}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Shards list now paginates (Limit/ExclusiveStartShardId/HasMoreShards); previously returned every shard in one page with HasMoreShards hardcoded false"}
  DescribeStreamSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStreams: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRecord: {wire: ok, errors: ok, state: ok, persist: ok, note: "MD5 hash routing, explicit hash key, per-shard monotonic sequence numbers verified correct"}
  PutRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: empty Records list now rejected (was silently 200); stream-not-found now fails the whole call with top-level ResourceNotFoundException instead of InternalFailure on every result entry"}
  GetShardIterator: {wire: ok, errors: ok, state: ok, persist: n/a, note: "TRIM_HORIZON/LATEST/AT_(AFTER_)SEQUENCE_NUMBER/AT_TIMESTAMP all verified; iterator token carries region so cross-region record stores stay isolated; fixed: AT_TIMESTAMP with a genuinely omitted Timestamp (JSON field absent, distinguished from an explicit epoch-zero value via *float64) now rejected InvalidArgumentException instead of silently reading from position 0"}
  GetRecords: {wire: ok, errors: ok, state: ok, persist: n/a, note: "10k-record / 10MiB caps, NextShardIterator empty-on-closed-and-drained, MillisBehindLatest verified"}
  ListShards: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: deleted invented 'AT_SHARD_ID' ShardFilterType (not in the real SDK enum) and its lineage-matching behavior; AFTER_SHARD_ID now implements the real exclusive-start-cursor-over-all-shards semantics; AT_TRIM_HORIZON/AT_TIMESTAMP/FROM_TIMESTAMP now do true per-shard-timestamp filtering (Shard.StartedAt/ClosedAt) instead of approximating as 'include everything'; AT_TIMESTAMP/FROM_TIMESTAMP now require ShardFilterTimestamp (InvalidArgumentException if omitted)"}
  RegisterStreamConsumer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: added missing 20-consumers-per-stream limit (LimitExceededException)"}
  DescribeStreamConsumer: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStreamConsumers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterStreamConsumer: {wire: ok, errors: ok, state: ok, persist: ok}
  SubscribeToShard: {wire: ok, errors: ok, state: ok, persist: n/a, note: "event-stream binary framing verified byte-for-byte (prelude/CRC/headers); polling goroutine bounded by idle-poll count and 5-min deadline, no leak; fixed: AT_TIMESTAMP with a genuinely omitted Timestamp now rejected InvalidArgumentException (was previously ambiguous between omitted and explicit-zero, both silently read from position 0)"}
  UpdateShardCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "double/half scaling window, parent/adjacent-parent lineage, old shards kept CLOSED verified"}
  EnableEnhancedMonitoring: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableEnhancedMonitoring: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLimits: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: OnDemandStreamCountLimit set via UpdateAccountSettings was never in backendSnapshot, silently reset to default on every restart"}
  UpdateAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMaxRecordSize: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStreamWarmThroughput: {wire: ok, errors: ok, state: ok, persist: n/a, note: "intentional no-op (no throughput model to warm); existence-checked"}
  MergeShards: {wire: ok, errors: ok, state: ok, persist: ok, note: "adjacency check (either shard may be passed first), closed-parent lineage verified"}
  SplitShard: {wire: ok, errors: ok, state: ok, persist: ok, note: "NewStartingHashKey must be strictly inside parent range, verified"}
  StartStreamEncryption: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: KeyId is now required and format-validated (UUID/key ARN/alias ARN/alias name, matching the four shapes the SDK doc comment enumerates) -- InvalidArgumentException if malformed; optional KMSKeyValidator (WithKMSValidator, wired to the real kms backend by cli.go's wireKinesisKMS) additionally verifies the key exists and is usable, returning KMSNotFoundException/KMSDisabledException/KMSInvalidStateException -- all three are real types.KMSNotFoundException-class exceptions confirmed present in the SDK's StartStreamEncryption error set (deserializers.go), contradicting the previous audit's claim that no KMS-specific exception exists for this op. With no validator wired, only the format check applies (a well-formed but nonexistent KeyId is accepted, same permissive behavior as before)."}
  StopStreamEncryption: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: KeyId is now required and format-validated like StartStreamEncryption (matches the SDK's required-field validator); never calls the KMS validator since disabling encryption must succeed even if the key was later disabled/deleted"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "resource policies not yet in backendSnapshot; see gaps"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now reads backend stream.Tags via Backend.ListTagsForResource instead of a handler-local map that was previously the ONLY store for tags applied via CreateStream/AddTagsToStream/RemoveTagsFromStream"}
  AddTagsToStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now writes through Backend.TagResource (stream.Tags) instead of a handler-local map dropped on Snapshot"}
  RemoveTagsFromStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now writes through Backend.UntagResource"}
  ListTagsForStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now reads Backend.ListTagsForResource"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now enforces the 50-tag cap consistently with AddTagsToStream (previously uncapped)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStreamMode: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: PROVISIONED -> ON_DEMAND now auto-reshards up to defaultOnDemandShardCount (4, matching CreateStream's ON_DEMAND default) when the stream is currently below that floor, closing the old open shards (CLOSED, retained for lineage) and opening new ones spanning the full hash range -- reuses the same reshardTo helper UpdateShardCount uses. This approximates AWS's documented 'scale to double the max/peak-30-day throughput, whichever is higher' behavior, which requires a throughput-history model this emulator doesn't have; see gaps for the remaining approximation gap. ON_DEMAND -> PROVISIONED never reshards (keeps current shard count as the new baseline), matching AWS."}
families:
  hash_key_routing: {status: ok, note: "MD5-based partition-key routing and explicit-hash-key routing verified against big.Int range math; shardForHashKey fallback-to-first-open-shard behavior documented"}
  sequence_numbers: {status: ok, note: "per-shard monotonic NextSeq counter, 49-prefixed AWS-shaped sequence string, persisted via Shard.NextSeq"}
  reshard_lineage: {status: ok, note: "SplitShard/MergeShards/UpdateShardCount/UpdateStreamMode all set ParentShardID/AdjacentParentShardID correctly; closed shards retained forever for DescribeStream/ListShards lineage (see leaks note); Shard gained StartedAt/ClosedAt (set by every shard-creation/closeShard call site) so ListShards' timestamp-bounded ShardFilter types can do real time-bounded filtering instead of approximating"}
  error_codes: {status: ok, note: "ResourceNotFoundException/ResourceInUseException/InvalidArgumentException/ProvisionedThroughputExceededException/ExpiredIteratorException/LimitExceededException/UnknownOperationException all verified exact string + 400 status. fixed: KMSNotFoundException/KMSDisabledException/KMSInvalidStateException are now modeled and reachable via StartStreamEncryption's optional KMSKeyValidator (see StartStreamEncryption note) -- the previous audit's claim that Kinesis has no KMS-specific exceptions was wrong; deserializers.go's awsAwsjson11_deserializeOpErrorStartStreamEncryption lists KMSAccessDeniedException/KMSDisabledException/KMSInvalidStateException/KMSNotFoundException/KMSOptInRequired/KMSThrottlingException/AccessDeniedException as real modeled errors for this op. KMSAccessDeniedException specifically remains unreachable -- see gaps."}
gaps:
  - "KMSAccessDeniedException (types.KMSAccessDeniedException) is a real modeled StartStreamEncryption/StopStreamEncryption error but has no trigger path: it requires evaluating a KMS key policy/grant against a calling principal, and gopherstack has no IAM policy evaluation engine anywhere (not just in kinesis) to produce an access-denied decision from. The sentinel (ErrKMSAccessDenied) and its InvalidArgumentException-style wire mapping (KMSAccessDeniedException, 400) are defined for wire-shape completeness, matching the real error type string exactly, but nothing in the backend can ever return it. Fabricating a fake denial rule (e.g. 'deny if KeyId contains X') would itself be a stub, so this stays an honest gap rather than a fake implementation. (bd: gopherstack-ud2)"
  - "UpdateStreamMode's PROVISIONED -> ON_DEMAND auto-reshard (see UpdateStreamMode note) approximates AWS's real throughput-history-based scaling with a fixed floor (defaultOnDemandShardCount = 4); it does not scale further for streams whose sustained load would earn a higher on-demand shard count in real AWS, since that requires tracking throughput history this emulator has no model for. Low priority: most callers re-describe the stream after the transition and adapt to whatever shard count comes back. (bd: gopherstack-ud2)"
  - "AT_TRIM_HORIZON's trim-horizon instant is computed from the stream's RetentionPeriod but clamped to never predate the stream's own oldest tracked shard StartedAt (see trimHorizon in shards.go), so it degrades gracefully for young streams instead of AWS's true 'oldest data still available' semantics that would require tracking exactly when each record was trimmed, not just when its shard opened/closed. Close enough for shard-lineage filtering (the documented ShardFilter use case); would diverge from AWS in a scenario with partial mid-shard trimming, which this emulator's record ring-buffer model doesn't represent per-shard trim timestamps for."
  - "CORRECTED this pass: the previous gap entry claiming resource policies (PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy) are lost across a persistence restart was stale/incorrect. persistence.go's backendSnapshot already has a ResourcePolicies field wired into both Snapshot (line ~60) and Restore (line ~119), and TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip already exercises PutResourcePolicy through an actual snapshot/restore cycle and passes. No code change needed; this was a documentation-only correction (carried forward unchanged from the prior ledger)."
  - "CORRECTED this pass: the deferred entry below claiming Lambda event-source-mapping trigger wiring 'lives in cli.go per task constraints; not touched' was stale -- cli.go's wireKinesisLambda (called at cli.go:2657) already wires services/kinesis to services/lambda's event-source poller via kinesisReaderAdapter, and this has been true since before this pass. Moved out of deferred; documentation-only correction, no code changed for this item."
deferred:
  - "Enhanced fan-out SubscribeToShard real streaming cadence / HTTP2 push semantics beyond the polling emulation already in place"
leaks: {status: clean, note: "stream.mu (lockmetrics) and stream.Tags always Close()'d on DeleteStream/Purge; SubscribeToShard polling goroutine bounded by subscribeToShardMaxIdlePolls (3) and a 5-minute deadline, exits on ctx.Done(); FIS throughput-fault goroutines bound to experiment ctx or scheduled cleanup, lazily evict on read; janitor retention sweep is a single ticker goroutine stopped via context cancellation, no per-stream goroutines; this pass's reshardTo/closeShard/KMSKeyValidator additions introduce no goroutines, tickers, or new lock-acquisition orderings -- KMS validation is a synchronous in-process call into the kms package's own locked backend while kinesis holds stream.mu, safe because kms never calls back into kinesis"}
---

## Notes

### KMS KeyId validation (this pass: closed the KMSAccessDeniedException gap for real, minus the truly undeliverable part)

The previous audit's `gaps:` entry claimed "there is no KMS-specific exception in the Kinesis API
itself" for `StartStreamEncryption`. This was wrong: `aws-sdk-go-v2/service/kinesis`'s
`deserializers.go` (`awsAwsjson11_deserializeOpErrorStartStreamEncryption`) explicitly lists
`KMSAccessDeniedException`, `KMSDisabledException`, `KMSInvalidStateException`, `KMSNotFoundException`,
`KMSOptInRequired`, and `KMSThrottlingException` as modeled errors for this op, and
`types/errors.go` defines all of them with `ErrorFault() smithy.FaultClient` (400-class). The op was
also missing `KeyId`'s required-field validation entirely -- any string, including empty, was accepted.

Fixed in two layers:

1. **Format validation** (`validateKMSKeyIDFormat` in `stream_encryption.go`, always active): `KeyId`
   must match one of the four shapes the SDK's own doc comment enumerates -- a bare key UUID, a key
   ARN (`arn:*:kms:*:*:key/<uuid>`), an alias ARN (`arn:*:kms:*:*:alias/<name>`), or an alias name
   (`alias/<name>`, including the Kinesis-owned `alias/aws/kinesis`). A malformed or empty `KeyId`
   returns `InvalidArgumentException`. This applies to both `StartStreamEncryption` and
   `StopStreamEncryption` (the SDK requires `KeyId` on both, even though stopping encryption never
   needs to resolve it).
2. **Optional cross-service existence/state check** (`KMSKeyValidator` interface + `WithKMSValidator`
   setter, mirroring `services/ssm`'s `KMSEncryptor`/`WithKMS` pattern exactly): when wired --
   `cli.go`'s `wireKinesisKMS` does this in production, adapting the real `services/kms` backend's
   `DescribeKey` -- `StartStreamEncryption` additionally verifies the key exists and is `Enabled`,
   returning `KMSNotFoundException` (key doesn't exist), `KMSDisabledException` (key is `Disabled`), or
   `KMSInvalidStateException` (any other non-`Enabled` state, e.g. `PendingDeletion`/`PendingImport`).
   With no validator wired (e.g. a bare `kinesis.NewInMemoryBackend()` in a unit test), only the format
   check applies -- a well-formed but nonexistent key is accepted, identical to pre-existing permissive
   behavior, so no existing caller regresses.

`KMSAccessDeniedException` remains genuinely undeliverable: it requires evaluating a KMS key policy or
grant against a calling principal, and gopherstack has no IAM policy evaluation engine to produce that
decision from anywhere in the codebase, not just kinesis. The sentinel and wire mapping are defined (so
the error type string is correct if a future IAM engine ever wires into it), but nothing can trigger it
today -- see `gaps`.

### UpdateStreamMode PROVISIONED -> ON_DEMAND auto-reshard (this pass: closed for real, with a documented approximation)

AWS's docs for `UpdateStreamMode` state that switching to on-demand "automatically scales your data
stream to handle up to double the maximum throughput ... or up to double the peak throughput within
the last 30 days, whichever is higher." This emulator tracks no throughput history, so an exact
implementation isn't possible without inventing one. Instead, `UpdateStreamMode` now reuses the same
`reshardTo` helper `UpdateShardCount` uses (extracted from it this pass) to reshard a stream up to
`defaultOnDemandShardCount` (4 -- the same floor `CreateStream` gives a fresh `ON_DEMAND` stream)
whenever the transitioning stream is currently below it, closing the old open shards (retained CLOSED
for lineage, exactly like `UpdateShardCount`/`MergeShards`/`SplitShard` do) and opening new ones
spanning the full hash range. A stream already at or above the floor is left alone. The reverse
transition (`ON_DEMAND -> PROVISIONED`) still never reshards, matching AWS (the current auto-scaled
shard count becomes the new provisioned baseline). See
`TestUpdateStreamMode_OnDemandTransitionReshardsUpToFloor` and
`TestUpdateStreamMode_OnDemandToProvisionedKeepsShardCount`.

### GetShardIterator / SubscribeToShard AT_TIMESTAMP now requires Timestamp (this pass: closed for real)

Both ops silently treated an *omitted* `Timestamp` the same as an *explicit* epoch-zero `Timestamp`
(both decoded to the Go zero value), reading from position 0 either way. The wire types for both
(`jsonGetShardIteratorReq.Timestamp`, `jsonStartingPosition.Timestamp`) are now `*float64` instead of
`float64`, so JSON-field-absent (nil) is distinguished from an explicit `"Timestamp": 0` (non-nil,
pointing at 0.0) -- the existing `TestGetShardIteratorAtTimestamp` test, which explicitly sends
`"Timestamp": 0` and expects success, continues to pass unchanged, while a genuinely omitted
`Timestamp` on `AT_TIMESTAMP` now returns `InvalidArgumentException` from both the backend
(`GetShardIteratorInput.Timestamp`/`SubscribeToShardInput.StartingPosition.Timestamp` are now
`*time.Time`) and the HTTP layer. See `TestGetShardIterator_AtTimestampRequiresTimestamp`,
`TestGetShardIterator_AtTimestampNilRejectedAtBackend`, `TestSubscribeToShard_AtTimestampRequiresTimestamp`.

### ListShards ShardFilter: deleted an invented type, implemented the real ones for real (this pass)

Two separate problems, found while field-diffing `ListShards` against
`aws-sdk-go-v2/service/kinesis/types.ShardFilterType`'s real enum
(`AFTER_SHARD_ID`/`AT_TRIM_HORIZON`/`FROM_TRIM_HORIZON`/`AT_LATEST`/`AT_TIMESTAMP`/`FROM_TIMESTAMP`):

1. **Invented op**: the backend special-cased a `"AT_SHARD_ID"` filter type that does not exist in the
   real SDK, with behavior (`listShardsAtShardID`: return every shard whose ID *or*
   `ParentShardID`/`AdjacentParentShardID` equals the target) that doesn't correspond to any real
   `ShardFilterType`'s documented semantics either. No test exercised it. Deleted per the no-invented-ops
   rule, replaced with a real implementation of `AFTER_SHARD_ID` (exclusive-start cursor over *all*
   shards, open and closed -- unlike the default/`AT_LATEST` filter, which only ever considers open
   shards).
2. **Approximated filters**: `AT_TRIM_HORIZON`/`AT_TIMESTAMP`/`FROM_TIMESTAMP` were previously
   approximated as "include every shard, open and closed" (`shardFilterIncludesAll`), which is not what
   any of them mean -- `AT_TIMESTAMP`/`AT_TRIM_HORIZON` should return only shards *open at* a given
   instant, and `FROM_TIMESTAMP` only open shards plus closed shards whose end postdates the instant.
   `Shard` gained `StartedAt`/`ClosedAt time.Time` fields (set at every shard-creation site --
   `buildInitialShards`, `SplitShard`, `MergeShards`, `reshardTo` -- and by a new `closeShard` helper
   that keeps `Closed`/`ClosedAt` in sync everywhere a shard retires), and `resolveShardFilter` in
   `shards.go` now implements real per-shard timestamp predicates (`shardOpenAt`/`shardClosedAtOrAfter`)
   against them. `AT_TIMESTAMP`/`FROM_TIMESTAMP` now require `ShardFilterTimestamp`
   (`InvalidArgumentException` if omitted, using the same `*float64`-presence-detection fix as
   `GetShardIterator`). `AT_TRIM_HORIZON`'s trim-horizon instant is clamped to never predate the
   stream's own oldest shard (see `trimHorizon`), so a freshly created stream doesn't spuriously return
   an empty result just because its retention window is mathematically older than the stream itself --
   see `gaps` for the residual approximation this clamp represents.
   Legacy Go-level callers that set the plain `ShardFilter` string field (not `ShardFilterType`, which
   only the HTTP handler populates) continue to work unchanged -- `resolveShardFilter` falls back to
   `ShardFilter` when `ShardFilterType` is empty.
   See `TestListShards_ShardFilterType_AfterShardID`, `_AtTimestamp`, `_FromTimestamp`,
   `_AtTrimHorizon`, `_TimestampRequired`, `_UnrecognizedRejected`.

### Retention-period equality bug — reverted after breaking real terraform apply (CI: TestTerraform_Kinesis)

A previous pass (`2b2086c9`) changed `IncreaseStreamRetentionPeriod`/`DecreaseStreamRetentionPeriod`
from treating an equal `RetentionPeriodHours` as an idempotent no-op (200 OK) to rejecting it with
`InvalidArgumentException`, reasoning from the literal aws-sdk-go-v2 doc comments:
`IncreaseStreamRetentionPeriodInput.RetentionPeriodHours` "Must be more than the current retention
period" and `DecreaseStreamRetentionPeriodInput.RetentionPeriodHours` "Must be less than the current
retention period" — both strict inequalities on their face.

That change broke `TestTerraform_Kinesis` in CI: `aws_kinesis_stream` with `retention_period = 48`
started failing at `apply` with `IncreaseStreamRetentionPeriod, 400, InvalidArgumentException`.
Reproduced live against a running gopherstack instance with the real `aws-sdk-go-v2/service/kinesis`
client: `CreateStream` (shard count 1) → stream ACTIVE at the 24h default → first
`IncreaseStreamRetentionPeriod(48)` succeeds → **a second `IncreaseStreamRetentionPeriod(48)` issued
against the now-already-48h stream (mimicking the OpenTofu/Terraform AWS provider's create-then-set /
idempotent-reapply flow) returns `InvalidArgumentException`** even though the stream is already at the
requested value. Real AWS tolerates this — the terraform provider relies on the API being idempotent
for a no-drift re-apply, and a strict reading of the doc comment that rejects the equal case does not
survive contact with the provider's actual call pattern.

Reverted both backend methods to treat an equal `RetentionPeriodHours` as a no-op success again, while
keeping strict rejection of the wrong direction (lower value on Increase, higher value on Decrease) and
the min/max bounds (24h floor / 8760h ceiling, unaffected). Updated the three tests that `2b2086c9` had
changed to assert the strict-rejection behavior
(`backend_test.go`'s `increase_same_value_rejected`/`decrease_same_value_rejected` table cases, and
`handler_refinement3_test.go`'s `TestRefinement3_RetentionPeriod_IncreaseToSameValueRejected`) back to
asserting the no-op-success behavior, and added
`TestRefinement3_RetentionPeriod_IncreaseFromDefaultEqualsDefault` to cover the exact default-24h
create-then-set-24h terraform pattern.

### Resource-policy persistence gap correction (documentation-only)

The previous ledger's `gaps:` list claimed `PutResourcePolicy`/`GetResourcePolicy`/
`DeleteResourcePolicy` state was not part of `backendSnapshot` and was lost across a persistence
restart. This was stale/incorrect: `persistence.go`'s `backendSnapshot.ResourcePolicies` field is
already wired into both `Snapshot` and `Restore`, and
`TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip` already exercises `PutResourcePolicy` through
an actual snapshot→restore cycle and passes. No backend/handler code changed for this item — the gap
entry has been corrected to reflect reality.

### Tag persistence bug (fixed in a prior pass)

Before this pass, `Handler` kept a **second, parallel tag store** (`h.tags map[string]*svcTags.Tags`,
keyed by `region+"/"+streamName`) that was the *only* backing store for tags applied via
`CreateStream` (inline `Tags`), `AddTagsToStream`, `RemoveTagsFromStream`, `ListTagsForStream`, and
`ListTagsForResource`. The backend's own `stream.Tags` field — the one that actually participates in
`Snapshot`/`Restore` (`backendSnapshot.Streams[region][name].Tags` via the `Stream` struct's `json:"tags"`
tag) — was only ever written by `TagResource`/`UntagResource` (the ARN-based API), and the handler's
`ListTagsForResource` never read it (it read `h.tags` too, so this went unnoticed operationally). Net
effect: **every tag applied through the legacy AddTagsToStream/CreateStream API path silently vanished
on process restart**, even though the stream itself, its shards, and its records persisted correctly —
a textbook "persist when persistence is enabled" violation per the no-stub rule, made worse by two
existing tests (`TestRefinement1_ListTagsForResource_SortedOutput`,
`TestRefinement2_ListTagsForResource_UsesHandlerTags`) that had *rationalized the bug as intentional
design* rather than flagging it (the exact "looks-wrong-but-correct" trap the parity playbook warns
about, except here the previous audit got the call wrong).

Fix: every tag-mutating handler now writes through `Backend.TagResource`/`Backend.UntagResource`
(single source of truth = `stream.Tags`), and every tag-reading handler reads through
`Backend.ListTagsForResource`. The handler-local `h.tags`/`tagsMu` map, `tagKey`/`setTags`/`getTags`/
`removeTags`, and the `OnStreamPurged` tag-cleanup closure in `WithJanitor` were deleted entirely (dead
weight once the backend is the only store — `stream.Tags.Close()` is already called by the backend on
`DeleteStream`/`Purge`). `CreateStream` now validates inline `Tags` (length + 50-tag cap) *before*
creating the stream, matching AWS's all-or-nothing semantics, and `TagResource` now enforces the same
50-tag cap `AddTagsToStream` always enforced (previously uncapped). See
`TestRefinement2_Tags_SurvivePersistenceRestore` for the regression test that exercises all three
write paths (CreateStream / AddTagsToStream / TagResource) through an actual Snapshot→Restore cycle.

### PutRecords request-level vs. per-record errors

AWS's `PutRecords` contract distinguishes **request-level** failures (fail the whole call with a
single top-level exception, HTTP 4xx, no `Records` envelope) from **per-record** failures (200 OK,
each failed entry gets its own `ErrorCode`/`ErrorMessage`, `FailedRecordCount` > 0). Stream-not-found
and an empty `Records` list are request-level. Before this pass, the backend looped
`PutRecord` per entry with no upfront existence check, so a `PutRecords` call against a nonexistent
stream returned **200 OK** with every entry marked `"InternalFailure"` — wrong on three counts (wrong
HTTP status class, wrong error code, wrong response shape). Fixed by resolving the stream once before
the loop and returning `ErrStreamNotFound` at the top level; an empty `Records` slice is now rejected
the same way (AWS's SDK model has `MinItems: 1`).

### ON_DEMAND default shard count

AWS allocates **4 shards** to a freshly created `ON_DEMAND` stream (capacity is auto-managed
thereafter); a caller-supplied `ShardCount` is ignored for `ON_DEMAND`. The backend previously fell
through to `defaultShardCount = 1` for `ON_DEMAND` streams with no explicit `ShardCount`, which is
wrong for any test/tool that inspects shard count immediately after creating an on-demand stream (e.g.
to compute expected parallelism). `streamMode` is now resolved *before* `shardCount`, and `ON_DEMAND`
always gets `defaultOnDemandShardCount = 4` regardless of the caller's `ShardCount`.

### DescribeStream shard pagination

`DescribeStream`'s `Shards` list has an AWS-documented page contract: default 100, max 10000,
resumed via `ExclusiveStartShardId`, with `HasMoreShards` signaling truncation. The previous
implementation returned **every** shard in the stream in one response and hardcoded
`HasMoreShards: false` — invisible on a fresh stream (≤100 shards from `CreateStream`, capped by
`maxShardsPerStream`), but real once a stream has been resharded enough times: `MergeShards`/
`SplitShard`/`UpdateShardCount` never remove CLOSED shards from `stream.Shards` (correctly — AWS keeps
closed shards visible for lineage), so a long-lived, heavily-resharded stream's total shard count
(open + closed) is unbounded and can exceed one page. `DescribeStreamInput` gained
`ExclusiveStartShardID`/`Limit` fields (additive; every existing call site that only sets `StreamName`
is unaffected) and `DescribeStreamOutput` gained `HasMoreShards`.

### Shard hash-range and sequence-number traps (unchanged this pass, re-confirmed correct)

- **Hash key range**: the full space is `[0, 2^128-1]`. `shardForHashKey` matches a partition key's
  MD5-derived `big.Int` against `[HashKeyRangeStart, HashKeyRangeEnd]` inclusive on both ends; the
  fallback to "first open shard" (and then `shards[0]` even if closed) only fires if no shard's stored
  range covers the hash, which should not happen for internally-generated shards but protects against
  a corrupted/hand-seeded stream (see `AddStreamInternal`, test-only) from panicking on `nil`.
- **Sequence numbers** are per-shard monotonic (`Shard.NextSeq`), formatted as
  `49<14-digit ms timestamp><4-digit shard idx><20-digit seq>` — this is a plausible-shaped AWS
  sequence number (49-prefix) but is **not** globally comparable across shards the way real AWS
  sequence numbers are within a single call; `findSequencePosition` binary-searches assuming
  ascending order *within one shard's own record list*, which holds because records are always
  appended in arrival order — do not assume cross-shard ordering from the string value.
  `EndingSequenceNumber` is only populated once a shard is `Closed` — an open shard with records
  reports `SequenceNumberRangeEnd: ""` deliberately (real AWS/KCL treats the *presence* of
  `EndingSequenceNumber` as the "this shard is closed, move to children" signal; populating it on an
  open shard would make consumers abandon it prematurely). This is intentional, not a gap.
- **Reshard lineage**: `MergeShards` accepts either shard as "first" as long as they are hash-range
  adjacent (`s1.end+1 == s2.start` or `s2.end+1 == s1.start`); `SplitShard` requires
  `NewStartingHashKey` strictly inside `(shard.start, shard.end)` (not equal to either bound).
  `UpdateShardCount`'s `findOverlappingParents` assigns up to 2 parent IDs per new shard based on hash
  range overlap with the previously-open shard set — this can only ever find 0, 1, or 2 overlapping
  parents given the reshard math, matching AWS's parent/adjacent-parent model.

### Consumer registration limit

`RegisterStreamConsumer` had no upper bound; AWS caps enhanced fan-out consumers at 20 per stream
(`LimitExceededException` beyond that). Added the check; see
`TestAudit2_RegisterStreamConsumer_LimitExceeded`.

### Account settings persistence

`UpdateAccountSettings`'s `OnDemandStreamCountLimit` was backend in-memory state not included in
`backendSnapshot` — every restart silently reset the account's on-demand limit back to the compiled-in
default (10), even if an operator had explicitly raised or lowered it. Added to the snapshot.

### KMS error codes (deferred, not fabricated)

The task brief calls out `KMSAccessDenied` as an error code to verify. The real Kinesis API (per the
`aws-sdk-go-v2/service/kinesis` model) does not define a KMS-specific exception for
`StartStreamEncryption`/`StopStreamEncryption`/`UpdateMaxRecordSize` — the modeled exceptions are
`InvalidArgumentException`, `LimitExceededException`, `ResourceInUseException`,
`ResourceNotFoundException`, and a generic `AccessDeniedException` (not currently in this package's
error set at all). Actually validating a `KeyId` would require calling into the `kms` service's
backend, which is a cross-service dependency out of scope for a `services/kinesis/`-only pass per this
sweep's constraints. Fabricating a KMS validation error path not backed by real state would itself be
a stub, so this is left as a documented gap rather than implemented.
