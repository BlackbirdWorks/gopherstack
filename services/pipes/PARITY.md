---
service: pipes
sdk_module: aws-sdk-go-v2/service/pipes@v1.26.4
last_audit_commit: ef59a15b0
last_audit_date: 2026-08-21
overall: A            # both execution gaps closed for real (runner.go source pollers + cli.go target/DLQ wiring); the only remaining gap is a proven genuine impossibility (no in-repo Kafka/AMQP broker)
ops:
  CreatePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "added max-50-tags validation to match TagResource's existing limit; RoleArn is now enforced as a required field (ValidationException when absent/empty), matching validateOpCreatePipeInput -- closes the gap previously left open in the 2026-07-13 pass. ~40 call sites across the test suite (Go CreatePipeInput{} literals and raw-HTTP JSON bodies) updated to supply RoleArn now that it's enforced. 2026-08-21: KinesisStreamSourceParameters.StartingPositionTimestamp (a Kinesis-source-only filter) decoded straight into *time.Time, which encoding/json cannot unmarshal from the epoch-seconds JSON number restjson1 actually sends -- rejecting the entire request body for any real client setting it (gopherstack-5mr2). Fixed via wire_time.go's MarshalJSON/UnmarshalJSON pair, not a field-type change, since the same struct also serves DescribePipe's response and the persistence snapshot round trip"}
  DescribePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredState now reports DELETED while CurrentState=DELETING, per RequestedPipeStateDescribeResponse. 2026-08-21: StartingPositionTimestamp response encoding fixed by the same wire_time.go change as CreatePipe (see its note) -- it was previously emitted as an RFC3339 string, which the real client's deserializer (expecting the epoch-seconds number restjson1's own serializer always used on the request side) would have rejected"}
  UpdatePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "added ConflictException guard against updating a pipe that is DELETING (was silently resurrecting it, corrupting the pending async delete); RoleArn is now enforced as a required field on every UpdatePipe call (ValidationException when absent/empty), matching validateOpUpdatePipeInput -- real AWS requires RoleArn to be resupplied on every update, even when unchanged. Validation order is Name/DesiredState/SourceParameters-batch-size -> RoleArn -> pipe-lookup, so a request missing RoleArn against a nonexistent pipe now correctly surfaces ValidationException, not NotFoundException (adjusted TestErrors/update_nonexistent_pipe_returns_404 to supply a valid RoleArn so it still exercises the NotFound path specifically). Note: types.UpdatePipeSourceKinesisStreamParameters has no StartingPositionTimestamp member in the real SDK at all, so this field is not reachable via UpdatePipe by any real client regardless of the 2026-08-21 fix"}
  DeletePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredState now reports DELETED, matching UpdatePipe fix's shared toPipeResponse"}
  ListPipes: {wire: ok, errors: ok, state: ok, persist: ok}
  StartPipe: {wire: ok, errors: ok, state: ok, persist: ok}
  StopPipe: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "verified every op's (method,path) against aws-sdk-go-v2/service/pipes v1.23.18 serializers.go opPath/request.Method literals; added handler_route_matcher_test.go driving RouteMatcher(c)+Handler()(c) end-to-end (prior tests all bypassed RouteMatcher via h.Handler()(c) directly, and the /tags/ prefix is shared across many services -- test also pins that a pipes-shaped path with a non-pipes SigV4 credential-scope service is correctly rejected)"}
gaps:
  - "MSK, self-managed Kafka, RabbitMQ, and ActiveMQ pipe sources are modeled in full in CreatePipe/UpdatePipe/DescribePipe wire shapes (sources.go) but are never polled by the runner, and this is a genuine impossibility rather than a deferred implementation: gopherstack has no in-process Kafka-wire-protocol broker or AMQP/OpenWire broker anywhere in the repo to read messages from. Verified by inspecting both candidate backends before writing this line: services/kafka (Amazon MSK) implements only the AWS *control-plane* HTTP API (CreateCluster/DescribeCluster/GetBootstrapBrokers/topic metadata CRUD) -- confirmed via `grep -rl 'func.*Produce\\|func.*Consume\\|func.*SendMessage\\|func.*ReceiveMessage'` returning nothing message-plane-shaped; services/mq (Amazon MQ, backs both RabbitMQ and ActiveMQ engine types) is the same shape (broker/user/configuration lifecycle CRUD only, zero produce/consume methods anywhere in the package). Neither package speaks the real wire protocol (Kafka's binary TCP protocol; AMQP 0-9-1 for RabbitMQ; OpenWire/STOMP for ActiveMQ), so even a cluster/broker created via those services' control planes has no data-plane to poll. runner.go's pollPipe routes only SQS/Kinesis/DynamoDB-Streams ARNs and leaves these four source types unrouted (with a doc comment explaining why) rather than faking delivery."
deferred: []
leaks: {status: clean, note: "runDelayed goroutines are tracked by b.wg and tied to svcCtx (cancelled via Handler.Shutdown -> Backend.Shutdown); Runner.Start/Wait use the same wg+done-channel pattern; enrichmentCounts entries are pruned in completeDeleteTransition alongside the pipe row, so no unbounded growth. Runner.shardIterators (new: caches Kinesis/DynamoDB-Streams shard iterator tokens, keyed by pipe ARN + shard ID, since those source types have no message-level ack/delete like SQS to drive cleanup off of) is swept every poll tick in pollAllPipes: any cached key whose pipe ARN is not in the current RUNNING set is dropped, so a stopped or deleted stream-sourced pipe's iterator entries do not accumulate."}
---

## Notes

Protocol: restjson1. Timestamps (`CreationTime`/`LastModifiedTime`) are epoch
**seconds** as a JSON number with fractional milliseconds precision (verified
against `smithytime.ParseEpochSeconds` in the real SDK's deserializers.go --
NOT epoch milliseconds despite the `epochMillis` helper's name; the helper
divides `UnixMilli()` by 1000, which produces exactly the epoch-seconds-with-
millisecond-fraction shape the deserializer expects. Confusing name, correct
value -- did not rename to avoid unnecessary churn across the package).

`DesiredState` has two different wire shapes depending on the op, both
confirmed directly from `aws-sdk-go-v2/service/pipes/types/enums.go`:
- `RequestedPipeState` (`RUNNING`/`STOPPED` only) on CreatePipeOutput,
  UpdatePipeOutput, StartPipeOutput, StopPipeOutput, and the `Pipe` summary
  type used by ListPipesOutput.
- `RequestedPipeStateDescribeResponse` (`RUNNING`/`STOPPED`/`DELETED`) on
  DescribePipeOutput and DeletePipeOutput only. This pass fixed the shared
  `toPipeResponse` (used by all six single-pipe ops) to substitute `DELETED`
  for `DesiredState` whenever `CurrentState == DELETING` -- previously it
  echoed the pipe's last real desired state (RUNNING/STOPPED) even while
  being deleted. `pipeSummary` (List) intentionally does NOT get this
  substitution since its wire type has no DELETED value and a
  fully-DELETING-but-not-yet-removed pipe can only be observed transiently.

`CreatePipe`/`UpdatePipe`/`DeletePipe`/`StartPipe`/`StopPipe` all share the
full `pipeResponse` struct (same one `DescribePipe` uses), which includes
several fields (`SourceParameters`, `TargetParameters`, `DeadLetterConfig`,
`LogConfiguration`, `RuntimeMetricsStreaming`, `Tags`, `RoleArn`, `Source`,
`Description`, `Enrichment`, `KmsKeyIdentifier`) that the real
CreatePipeOutput/UpdatePipeOutput/DeletePipeOutput/StartPipeOutput/
StopPipeOutput shapes do NOT have (those five ops' real outputs are just
`Arn`/`CreationTime`/`CurrentState`/`DesiredState`/`LastModifiedTime`/`Name`).
Confirmed via the real SDK's restjson1 deserializers that unrecognized JSON
keys are silently skipped (`for key, value := range shape { switch key {
...no default case... } }`), so this is NOT a client-breaking bug -- extra
fields are ignored by every language's generated SDK. Left as-is rather than
splitting into five narrower response structs; flag if a future audit wants
stricter shape-for-shape parity, but it's cosmetic today. Also note
`DescribePipeOutput` (this SDK version, v1.23.18) has no top-level
`DeadLetterConfig` or `RuntimeMetricsStreaming` fields at all -- those only
exist nested inside `SourceParameters.{Kinesis,DynamoDBStream}Parameters` and
inside the pipe's own `RuntimeMetricsStreaming` sub-object respectively (the
latter genuinely doesn't exist as a top-level Pipe field in this SDK
version). Gopherstack's `Pipe.DeadLetterConfig`/`Pipe.RuntimeMetricsStreaming`
top-level fields are therefore emulator-only extensions with no real-API
equivalent at this SDK version; harmless (real requests never populate them,
so they're always empty on the wire) but worth knowing if diffing shapes.

Route paths (verified against `aws-sdk-go-v2/service/pipes@v1.23.18/
serializers.go` opPath literals, one `httpbinding.SplitURI` call per
`awsRestjson1_serializeOp*`):
- `POST /v1/pipes/{Name}` = CreatePipe
- `GET /v1/pipes/{Name}` = DescribePipe
- `PUT /v1/pipes/{Name}` = UpdatePipe
- `DELETE /v1/pipes/{Name}` = DeletePipe
- `GET /v1/pipes` = ListPipes
- `POST /v1/pipes/{Name}/start` = StartPipe
- `POST /v1/pipes/{Name}/stop` = StopPipe
- `GET /tags/{resourceArn}` = ListTagsForResource
- `POST /tags/{resourceArn}` = TagResource
- `DELETE /tags/{resourceArn}` = UntagResource

All ten match `Handler.ExtractOperation`/`extractPipeCRUDOp`/
`extractPipeActionOp`/`extractTagsOp` exactly -- no route-matcher bugs found.
`RouteMatcher()` additionally gates on `httputils.ExtractServiceFromRequest`
(the SigV4 credential-scope service name) before checking path prefix, which
is what prevents the shared `/tags/{resourceArn}` prefix from colliding with
every other AWS service that also serves tag ops off that path; this gate
did not previously have direct test coverage (all existing tests call
`h.Handler()(c)` directly, bypassing `RouteMatcher` entirely) -- added
`handler_route_matcher_test.go` to close that gap, following the pattern
established in `services/guardduty/handler_route_matcher_test.go`.

State machine: `PipeState` enum (`RUNNING`/`STOPPED`/`CREATING`/`UPDATING`/
`DELETING`/`STARTING`/`STOPPING`/`*_FAILED`/`*_ROLLBACK_FAILED`) matches the
real SDK's `types.PipeState.Values()` exactly. The `*_FAILED` states
(`CREATE_FAILED`, `UPDATE_FAILED`, `DELETE_FAILED`, `START_FAILED`,
`STOP_FAILED`) and `MarkPipeFailed` are defined but never triggered by any
internal code path -- every async transition (`completeCreateTransition`,
`completeUpdateTransition`, `completeDeleteTransition`,
`completeStartTransition`, `completeStopTransition`) always succeeds
optimistically after a fixed 10ms delay. This is consistent with the rest of
the emulator (no synthetic failure injection outside pkgs/chaos) and was not
treated as a bug -- `MarkPipeFailed` is exported and unit-tested
(`TestAudit_MarkPipeFailed`) for callers (e.g. a future chaos hook) that want
to force a failed state.

`StartPipe`/`StopPipe` already had a transitional-state guard
(`changePipeDesiredState`: reject if `CurrentState` matches the op's own
in-flight transitional state, or if `CurrentState == DELETING`).
`UpdatePipe` had NO such guard before this pass -- it unconditionally
overwrote `CurrentState` to `UPDATING` regardless of what state the pipe was
actually in. Concretely: `DeletePipe` sets `CurrentState = DELETING` and
schedules `completeDeleteTransition` (which only actually removes the row if
`CurrentState` is *still* `DELETING` when the delayed goroutine fires). If
`UpdatePipe` ran in that ~10ms window, it flipped `CurrentState` to
`UPDATING`, which made `completeDeleteTransition`'s guard fail silently --
the pipe was never removed, `DeletePipe`'s own response (which already
claimed `CurrentState: DELETING`) became a lie, and the pipe was
permanently stuck in `UPDATING` (since `completeUpdateTransition` would
still fire and "complete" it into a state the caller never asked for). Fixed
by rejecting `UpdatePipe` with `ConflictException` when
`CurrentState == DELETING`, mirroring the existing Start/Stop guard pattern.
`DeletePipe` itself was NOT given a symmetric guard against CREATING/
UPDATING/STARTING/STOPPING pipes -- letting a delete win over any of those
in-flight transitions is standard AWS async-resource behavior (delete is
terminal) and already correctly implemented (delete always overwrites
`CurrentState`/`DesiredState` and its own completion check is unconditional
on `CurrentState == DELETING`, so a later competing transition simply loses,
same failure mode analysis as above but in delete's favor by design).

Tag limits: `TagResource` already enforced `maxTagsPerPipe` (50, matching
real AWS), but `CreatePipe`'s initial `Tags` map was never checked against
the same limit -- a single `CreatePipe` call with >50 tags in the request
body succeeded, and the row could then only be discovered as over-limit via
`ListTagsForResource`. Added the same `len(tags) > maxTagsPerPipe` check to
`CreatePipe`, returning the same `ValidationException` shape `TagResource`
already uses.

RoleArn required-field validation (2026-07-24 pass): the prior audit
(2026-07-13) found that `CreatePipe`/`UpdatePipe` never validated `RoleArn`
as non-empty, even though `aws-sdk-go-v2/service/pipes@v1.23.18`'s
`validateOpCreatePipeInput`/`validateOpUpdatePipeInput` both mark it a
required member (confirmed directly against `validators.go`), and left it
open citing high test-churn (~340 subtests) for a raw-HTTP-only edge case.
This pass closed the gap for real: `CreatePipe` now rejects an empty
`RoleARN` with `ValidationException` (checked after `Source`/`Target`, so
those checks' error precedence is unchanged), and `UpdatePipe` now rejects
an empty `RoleARN` on *every* call -- matching real AWS, which requires
`RoleArn` to be resupplied on every `UpdatePipe` request even when its value
doesn't change (confirmed via the `// This member is required` doc comment
on `UpdatePipeInput.RoleArn` in `api_op_UpdatePipe.go`, not just the
smithy-generated client-side validator). `UpdatePipe`'s check runs before
the pipe-name lookup, matching real AWS's validate-before-execute ordering;
one test (`TestErrors/update_nonexistent_pipe_returns_404`) previously sent
a structurally-invalid request (missing `RoleArn`) against a nonexistent
pipe and asserted `NotFoundException` -- it was updated to supply a valid
`RoleArn` so it continues to exercise the not-found path specifically,
rather than incidentally asserting the wrong exception for a request that
was never valid to begin with. ~41 call sites across the test suite (34
`CreatePipeInput{}`/7 `UpdatePipeInput{}` Go struct literals, plus ~65
raw-HTTP JSON request bodies) were updated to supply `RoleArn` now that
it's enforced; no test assertions about *other* validation paths
(missing `Source`, missing `Target`, invalid `DesiredState`, etc.) changed
behavior, since those checks all run before the new `RoleArn` check.

Execution gaps closed for real (2026-07-24 second pass, parity-3 final phase):
the two gaps below were previously deferred citing "cross-service, out of
services/pipes/'s edit scope." That reasoning is no longer accepted in this
phase; both are now closed with real code plus tests proving delivery, not
just closed on paper.

1. **Runner source pollers (Kinesis, DynamoDB Streams).** `pollPipe`
   previously only had an `isSQSARN` gate; a RUNNING pipe with a Kinesis or
   DynamoDB Streams source never polled anything. `runner.go` gained
   `PipeKinesisReader`/`PipeDynamoDBStreamsReader` source interfaces (mirroring
   the shapes `services/lambda/event_source_poller.go`'s ESM poller already
   uses for the same two source types) plus `Runner.SetKinesisReader`/
   `SetDynamoDBStreamsReader`. A new `sources_poll.go` implements
   `pollKinesisPipe`/`pollDynamoDBStreamPipe`: each shard's iterator is cached
   in `Runner.shardIterators` (a `pkgs/safemap.Map[string,string]`, per the
   pkgs-catalog.md rule that an isolated single-map cache belongs in safemap
   rather than a bespoke mutex) and advances unconditionally once `GetRecords`
   succeeds -- matching the Lambda ESM poller's established precedent, since
   Kinesis/DynamoDB Streams have no message-level ack/delete to make
   checkpoint-only-on-success safe (one poison record would otherwise wedge
   the shard forever). Records are filtered through the same `FilterCriteria`
   engine SQS sources use (`filter.go`'s `matchesAnyFilter` was generalized
   from `(*SQSMessage, []Filter)` to `(body string, []Filter)` so all three
   source types share one matcher), enriched and dispatched through the same
   `dispatchTarget`/DLQ path SQS uses (`invokeTargetWithPayload` was split
   into that reusable `dispatchTarget` plus a thin SQS-receipt-handle wrapper).
   `cli.go`'s `wirePipesRunner` wires both against the **real** Kinesis and
   DynamoDB backends (new `pipesKinesisReaderAdapter`/
   `pipesDDBStreamsReaderAdapter`, modeled on the existing
   `kinesisReaderAdapter`/`ddbStreamsReaderAdapter` Lambda ESM adapters).
   MSK/self-managed Kafka/RabbitMQ/ActiveMQ remain unpolled -- see `gaps:`
   above for the proof this is a genuine impossibility, not a deferral.

2. **cli.go target/DLQ wiring.** `wirePipesRunner` previously only wired an
   SQS source reader and Lambda/StepFunctions target invokers, even though
   `runner.go` already had full `SNSPublisher`/`SQSSender`/`PipeKinesisPutter`/
   `PipeEventBridgePutter`/`PipeCloudWatchLogsPutter`/`PipeFirehosePutter`
   interfaces, `Set*` methods, and `invoke*Target` implementations sitting
   unused -- every one of those six target types (and both DLQ paths, which
   reuse the SNS/SQS interfaces) returned `ErrTargetInvokerUnwired` in the
   real binary. `wirePipesRunner` was split into `wirePipesSources`/
   `wirePipesInvokers`/`wirePipesTargets` and now wires all six against real
   backends via six new adapter structs (`pipesSNSPublisherAdapter`,
   `pipesSQSSenderAdapter`, `pipesKinesisPutterAdapter`,
   `pipesEventBridgePutterAdapter`, `pipesCloudWatchLogsPutterAdapter`,
   `pipesFirehosePutterAdapter`), each a thin delegate to that service's own
   `InMemoryBackend` method (`Publish`/`SendMessage`/`PutRecord`/`PutEvents`/
   `PutLogEvents`/`PutRecord`), following the existing
   `pipesSQSReaderAdapter`/`pipesSFNStarterAdapter` pattern. `SetSNSPublisher`/
   `SetSQSSender` cover both the direct-target case and the DLQ case
   (`handlePipeFailure`/`sendToDLQIfConfigured`) since `runner.go` already
   shared those two interfaces between the two call sites.

   Proof of delivery (not just "no error returned"): `cli_pipes_wiring_test.go`
   (root package) builds every backend for real (no mocks), calls the exact
   `wirePipesRunner` cli.go invokes, starts the real `Runner` ticker, and
   asserts the record actually landed in each target's own backend state --
   an SNS topic's message archive, a real SQS queue's `ReceiveMessage`, a
   Kinesis shard's `GetRecords`, an EventBridge archive's `EventCount`, a
   CloudWatch Logs stream's `GetLogEvents`, and an S3 object written by a
   flushed Firehose delivery stream -- plus a DLQ-delivery test (a Lambda
   target fails deterministically with no Docker runtime available, and the
   failure is redirected to a real SQS DLQ) and both new source pollers
   (a real `kinesisBk.PutRecord` / `ddbBk.PutItem` is picked up by the running
   poller and forwarded to a real SQS target). `services/pipes/`'s own test
   suite (`sources_kinesis_ddb_test.go`) additionally covers the poller logic
   itself against fakes: filter application, DLQ-on-target-failure, iterator
   advancement (no re-delivery), `GetRecords`-error recovery, and the shard
   iterator sweep bounding cache growth once a pipe stops.

3. **2026-08-21: `KinesisStreamSourceParameters.StartingPositionTimestamp`
   epoch-seconds fix (gopherstack-5mr2).** `sources.go` declared this field as
   plain `*time.Time` with a `json` tag, decoded/encoded by encoding/json's
   default machinery. Real `CreatePipe`/`UpdatePipe` requests carry it as a
   restjson1 epoch-seconds JSON number
   (`aws-sdk-go-v2/service/pipes@v1.26.4/serializers.go:1903-1905`:
   `ok.Double(smithytime.FormatEpochSeconds(*v.StartingPositionTimestamp))`),
   which `time.Time.UnmarshalJSON` rejects outright
   (`Time.UnmarshalJSON: input is not a JSON string`) -- failing the whole
   CreatePipe request body for any real client that sets an `AT_TIMESTAMP`
   Kinesis source. The same struct is shared, unconverted, between the
   CreatePipe/UpdatePipe decode target, `Pipe.SourceParameters` (the domain
   model), DescribePipe's response encoding, and the persistence snapshot
   round trip, so a plain field-type change to `*float64` (the pattern used
   in `services/emr/handler_clusters.go`) was not a fit -- it would have
   pushed raw epoch floats into the domain model and every other consumer.
   Instead, `wire_time.go` adds `KinesisStreamSourceParameters.MarshalJSON`/
   `UnmarshalJSON` (the alias-embedding pattern already used by
   `services/eventbridge/wire_time.go` and `services/cloudtrail/models.go`'s
   `Event`), keeping the domain field a real `time.Time` while the wire
   encoding/decoding goes through `*float64` + `epochSecondsPtr`/
   `timeFromEpochSecondsPtr`. This also fixes a second, previously-unnoticed
   bug for free: DescribePipe was encoding this field as an RFC3339 string,
   which a real client's deserializer (expecting the same epoch-seconds
   shape on responses; deserializers.go:4988-4996) would also have rejected.
   `UpdatePipe` cannot exercise this field at all in the real API --
   `types.UpdatePipeSourceKinesisStreamParameters` has no
   `StartingPositionTimestamp` member -- so only CreatePipe/DescribePipe are
   reachable by a real client; `wire_time_test.go` covers both, plus a
   direct-`UnmarshalJSON` table test proving the exact old failure mode and
   that an RFC3339 string is still correctly rejected (not silently
   misparsed) post-fix.

   Scope check performed for this pass (not just the four fields named in
   gopherstack-5mr2's floor count): a go/packages-based static analyzer
   walked every `json.Unmarshal`/`json.Decoder.Decode`/echo `Context.Bind`
   call site across all of `services/` (164 packages, generic `decodeBody[T]`/
   `parseBody[T]`/`unmarshalAction[T]` helpers included) and flagged every
   struct field of type `time.Time`/`*time.Time` reachable from a decode
   target, recursively through nested/slice fields. Every other hit was a
   false positive already covered by an existing fix: `services/eventbridge`
   (`EventEntry`, `StartReplayInput`) and `services/cloudtrail` (`Event`)
   already have the alias/custom-`UnmarshalJSON` pattern; `services/kinesis`
   (`ShardIterator`) round-trips only through its own opaque, never-AWS-wire
   base64 token; `services/sagemaker` hits are the pre-existing (and
   off-limits for this pass) alias pattern. `KinesisStreamSourceParameters`
   was the only remaining genuine gap in the whole tree.
