---
service: pipes
sdk_module: aws-sdk-go-v2/service/pipes@v1.23.18
last_audit_commit: 5d5b2188
last_audit_date: 2026-07-24
overall: B            # RoleArn-required gap closed; two cross-service execution gaps remain (out of pipes/ scope)
ops:
  CreatePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "added max-50-tags validation to match TagResource's existing limit; RoleArn is now enforced as a required field (ValidationException when absent/empty), matching validateOpCreatePipeInput -- closes the gap previously left open in the 2026-07-13 pass. ~40 call sites across the test suite (Go CreatePipeInput{} literals and raw-HTTP JSON bodies) updated to supply RoleArn now that it's enforced"}
  DescribePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredState now reports DELETED while CurrentState=DELETING, per RequestedPipeStateDescribeResponse"}
  UpdatePipe: {wire: ok, errors: ok, state: ok, persist: ok, note: "added ConflictException guard against updating a pipe that is DELETING (was silently resurrecting it, corrupting the pending async delete); RoleArn is now enforced as a required field on every UpdatePipe call (ValidationException when absent/empty), matching validateOpUpdatePipeInput -- real AWS requires RoleArn to be resupplied on every update, even when unchanged. Validation order is Name/DesiredState/SourceParameters-batch-size -> RoleArn -> pipe-lookup, so a request missing RoleArn against a nonexistent pipe now correctly surfaces ValidationException, not NotFoundException (adjusted TestErrors/update_nonexistent_pipe_returns_404 to supply a valid RoleArn so it still exercises the NotFound path specifically)"}
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
  - "Runner (runner.go) only polls SQS sources (isSQSARN gate in pollPipe) -- Kinesis, DynamoDB Streams, MSK, self-managed Kafka, RabbitMQ, and ActiveMQ sources are modeled in CreatePipe/UpdatePipe/DescribePipe wire shapes but a RUNNING pipe with one of those sources never actually polls or forwards events. No control-plane bug (Describe/List still report CurrentState=RUNNING correctly), but a real EXECUTION gap. Cross-service follow-up, not in services/pipes/'s edit scope to fix (would need new source-reader adapters in cli.go plus backend hooks in kinesis/dynamodb/kafka-shaped services)."
  - "cli.go's wirePipesRunner (cli.go:6592) only wires SQS as source and Lambda+StepFunctions as target/enrichment invokers. SNS, SQS, Kinesis, EventBridge, CloudWatchLogs, and Firehose TARGET invokers (Runner.Set{SNSPublisher,SQSSender,KinesisPutter,EventBridgePutter,CloudWatchLogsPutter,FirehosePutter}) and both DLQ senders (sqsSender/sns for handlePipeFailure) are never set, so a RUNNING SQS-sourced pipe targeting any of those services will poll+enrich correctly but every invokeXTarget call returns ErrTargetInvokerUnwired and the source message is left unconsumed (and DLQ delivery silently fails the same way). Cross-service wiring gap in cli.go, out of services/pipes/'s edit scope; needs adapter structs analogous to pipesSQSReaderAdapter/pipesSFNStarterAdapter for each target service's backend."
deferred: []
leaks: {status: clean, note: "runDelayed goroutines are tracked by b.wg and tied to svcCtx (cancelled via Handler.Shutdown -> Backend.Shutdown); Runner.Start/Wait use the same wg+done-channel pattern; enrichmentCounts entries are pruned in completeDeleteTransition alongside the pipe row, so no unbounded growth"}
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
