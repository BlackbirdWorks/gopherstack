---
service: stepfunctions
sdk_module: aws-sdk-go-v2/service/sfn@v1.40.8
last_audit_commit: 43aa6d65
last_audit_date: 2026-07-11
overall: A            # zero code drift vs. baseline ce30166a (previous pass); 0 LOC changed this
                       # pass -- confirmed via git diff ce30166a..HEAD -- services/stepfunctions/
                       # (empty) and identical sfn SDK pin (v1.40.8, no new ops: 34/34 match). All
                       # gates green (build/vet/fix/race-test/lint). One severe cli.go wiring gap
                       # newly discovered and documented under gaps (out of scope to fix here).
ops:
  CreateStateMachine: {wire: ok, errors: ok, state: ok, persist: ok, note: "STANDARD/EXPRESS, roleArn validation, tags, logging/tracing config; unchanged this pass"}
  UpdateStateMachine: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStateMachine: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStateMachine: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStateMachines: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page[T] pagination"}
  DescribeStateMachineForExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  PublishStateMachineVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStateMachineVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStateMachineVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStateMachineVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "routingConfiguration weighted versions validated"}
  UpdateStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStateMachineAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ValidateStateMachineDefinition: {wire: ok, errors: ok, state: ok, persist: n/a, note: "JSON/structural validation only, no deep ASL semantic checks (e.g. JitterStrategy enum, ToleratedFailure+INLINE combos) -- see gaps"}
  StartExecution:
    wire: ok
    errors: fixed
    state: ok
    persist: ok
    note: >
      FIXED this pass: StartExecution on an EXPRESS state machine was
      incorrectly rejected with InvalidExecutionType. AWS supports
      asynchronous "Express Workflows" via StartExecution for EITHER type;
      only StartSyncExecution is EXPRESS-only. Removed the incorrect check
      (backend.go). ClientRequestToken idempotency and EXPRESS's
      immediate-name-reuse semantics are not modeled either way (gap,
      bd: gopherstack-1sf).
  StartSyncExecution:
    wire: ok
    errors: fixed
    state: ok
    persist: ok
    note: >
      FIXED this pass: calling StartSyncExecution on a STANDARD state
      machine returned "InvalidExecutionType"; AWS returns
      "StateMachineTypeNotSupported". Added ErrStateMachineTypeNotSupported
      and rewired backend.go + handler.go's error-code table.
  StopExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "cancels the execution's context via cancelFns; goroutine exits promptly"}
  RedriveExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExecutionHistory:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass (severe, broad): StateEnteredEventDetails.Input,
      StateExitedEventDetails.Output, TaskScheduledEventDetails.Resource,
      TaskSucceededEventDetails.Output, and TaskFailedEventDetails.Error/Cause
      were ALL parsed into well-shaped Go structs (json tags already
      correct) but the historyRecorder methods in backend.go silently
      discarded every parameter and only ever wrote {Type, Timestamp} --
      every Task/state history event was an empty shell. Event
      types/ordering/IDs/pagination were already correct (that's all prior
      tests checked, which is why this went undetected). Added
      TaskScheduledEventDetails/TaskSucceededEventDetails/
      TaskFailedEventDetails structs+population; still missing
      resourceType/timeoutInSeconds/heartbeatInSeconds/outputDetails and
      TaskSubmitted/TaskStarted events (bd: gopherstack-996).
  CreateActivity: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActivity: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActivity: {wire: ok, errors: ok, state: ok, persist: ok}
  ListActivities: {wire: ok, errors: ok, state: ok, persist: ok}
  GetActivityTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "long-poll with WaitTimeSeconds; task-token issuance"}
  SendTaskSuccess: {wire: ok, errors: ok, state: ok, persist: ok}
  SendTaskFailure: {wire: ok, errors: ok, state: ok, persist: ok}
  SendTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "States.HeartbeatTimeout enforced against HeartbeatSeconds"}
  DescribeMapRun: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMapRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMapRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "ToleratedFailureCount/Percentage on the MapRun *resource* API were already real; the ASL-definition-level Map state fields were the gap (fixed, see families.asl_map)"}
  TestState: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  asl_task:
    status: ok
    note: >
      Resource ARN resolution (Lambda/SQS/SNS/DynamoDB/ECS/Glue/EventBridge/
      Activity), Parameters/ResultSelector/ResultPath/InputPath/OutputPath,
      TimeoutSeconds (context.WithTimeout), HeartbeatSeconds,
      .waitForTaskToken and .sync/.sync:2 patterns all verified against
      aws-sdk-go-v2 sfn behavior and already correct. Retry
      (MaxAttempts/IntervalSeconds/BackoffRate default 3/1s/2.0 -- matches
      AWS defaults) and Catch (ErrorEquals incl. States.ALL/Timeout/Runtime/
      Permissions/TaskFailed) were already correct for Task.
      FIXED this pass: Retry.MaxDelaySeconds and Retry.JitterStrategy
      ("FULL"/"NONE", AWS default NONE) were not parsed or applied at all --
      only an internal 24h safety cap existed. Added both fields to
      asl.Retrier and applyRetryDelayCapAndJitter().
      FIXED this pass: the Catch error-output object only ever set a single
      combined "Error": "<code>: <cause>" key; AWS's documented shape is
      separate "Error" and "Cause" keys. Also, TaskFailed history events
      always recorded errCode=<combined string> and cause="" (see
      GetExecutionHistory finding). Fixed via new
      stepFunctionsErrorCause() + reworked checkCatchers()/recordTaskFailed().
  asl_choice:
    status: ok
    note: >
      All comparison families verified: String/Numeric/Timestamp/Boolean
      Equals+LessThan+GreaterThan(+Equals) and their *Path variants,
      And/Or/Not, IsPresent/IsNull/IsString/IsNumeric/IsBoolean/IsTimestamp,
      StringMatches (custom glob matcher handles '*', literal-escaping via
      backslash, backtracking for multiple wildcards -- verified against the
      AWS doc example "log-*.txt"). No changes needed.
  asl_map:
    status: fixed
    note: >
      SEVERE FIX: runMapItem only checked the Go `error` return of the
      per-item sub-Executor.Execute() call. But Execute() returns a Fail
      state (or an unhandled Task failure inside the iterator) as
      `(&ExecutionResult{Error: code, Cause: cause}, nil)` -- a *successful*
      Go call with res.Error populated, NOT a Go error. So EVERY failing Map
      iteration was silently swallowed: errs[idx] stayed nil, results[idx]
      stayed nil, and the Map state always "succeeded" with nil holes in its
      output array. Verified runParallelBranch already had the correct
      `if res.Error != "" { errs[idx] = &FailError{...} }` check --
      runMapItem was missing the equivalent, asymmetric disguised-stub bug.
      Fixed by mirroring runParallelBranch's handling.
      ADDED (previously entirely absent): Map-level Retry and Catch (AWS
      supports Retry/Catch directly on Map/Parallel, not just Task; a retry
      re-runs every item from scratch). Extracted a shared
      executeWithStateRetryAndCatch() helper used by both executeMap and
      executeParallel.
      ADDED (previously entirely absent): ToleratedFailureCount/Percentage
      and their *Path variants on the Map state definition (AWS applies
      these to Distributed Map; the emulator applies them uniformly since
      Map processing mode is not otherwise distinguished -- see
      bd: gopherstack-8im). When both a count and percentage threshold are
      configured, the Map fails when either is crossed, matching AWS. On
      threshold-exceeded, fails with States.ExceedToleratedFailureThreshold;
      with no tolerance configured (the common/default case, threshold=0),
      the original per-item error is preserved unwrapped so existing
      ErrorEquals matching on Catch/Retry keeps working exactly as before.
      ItemsPath, MaxConcurrency, ItemBatcher (MaxItemsPerBatch/
      MaxInputBytesPerBatch), ItemReader (S3 CSV/JSON/JSONL via S3Reader),
      ItemSelector were already correct and unchanged.
      DEFERRED: ResultWriter (S3 write-out for Distributed Map) is not
      parsed at all -- results are always returned inline. Implementing this
      needs a new S3Writer integration wired from cli.go, outside this
      pass's services/stepfunctions/-only scope (bd: gopherstack-8j8).
      DEFERRED: ItemProcessor.ProcessorConfig.Mode (INLINE/DISTRIBUTED) is
      not parsed, so the emulator can't reject INLINE+ToleratedFailure
      combos the way AWS's definition validation does (bd: gopherstack-8im).
  asl_parallel:
    status: fixed
    note: >
      ADDED (previously entirely absent): Parallel-level Retry, via the same
      executeWithStateRetryAndCatch() helper added for Map (Catch already
      existed). Also fixed a latent bug where executeParallel hardcoded
      "Parallel" as the stateName passed to checkCatchers/history recording
      instead of the actual state's name from the ASL definition (threaded
      the real stateName through executeState -> executeParallel).
      Branch result aggregation, error propagation (first branch error wins,
      after ctx.Err() check), and per-branch FailError reconstruction
      (runParallelBranch) were already correct.
  asl_wait:
    status: ok
    note: "Seconds/Timestamp/SecondsPath/TimestampPath all verified; waitForDuration respects ctx cancellation promptly (no goroutine leak). No changes."
  asl_pass_succeed_fail:
    status: ok
    note: "Pass (Result/Parameters), Succeed, Fail (Error/Cause, static only -- no intrinsic in Error/Cause per AWS spec) all verified correct."
  asl_intrinsics:
    status: ok
    note: >
      All 18 real AWS intrinsics verified present and correct: States.Format,
      StringToJson, JsonToString, Array, ArrayPartition, ArrayContains,
      ArrayRange, ArrayGetItem, ArrayLength, ArrayUnique, Base64Encode,
      Base64Decode, Hash, JsonMerge (shallow-only, correctly rejects
      deep-merge arg per AWS's "third arg must be false" restriction),
      MathRandom, MathAdd, StringSplit, UUID.
      NOTE (non-AWS extras, informational only): the package also implements
      non-standard/invented intrinsics (StringConcat, StringLength,
      StringToLower/Upper, StringIndex, ArraySlice, ArrayFlatten,
      ArrayReverse, ArraySort, MathSubtract/Multiply/Divide/Mod/Min/Max,
      MathMax) that AWS does NOT support. This is permissive (accepts more
      than AWS would) rather than a correctness bug for real AWS
      definitions, but a definition using these against a real AWS account
      would fail at validation time where the emulator accepts it silently.
      Not fixed this pass (removing working functionality is net-negative;
      flagging for awareness only).
  json_1_0_protocol:
    status: ok
    note: "AWSStepFunctions.<Op> X-Amz-Target headers, json content-type, error shapes (__type + message) verified consistent with other json-1.0 services in this codebase. No changes."
gaps:
  - "SEVERE, cli.go-only (out of scope for this service dir): cli.go wires SetLambdaInvoker/
    SetSQSIntegration/SetSNSIntegration/SetDynamoDBIntegration onto the Step Functions backend
    (cli.go ~L3487-3514) but NEVER calls SetECSIntegration/SetGlueIntegration/
    SetEventBridgeIntegration. asl.Executor fully implements ecs:runTask/glue:startJobRun/
    events:putEvents Task-state routing (asl/executor.go ECSIntegration/GlueIntegration/
    EventBridgeIntegration interfaces, unit-tested with mocks in
    asl/service_integration_ecs_glue_eb_test.go) and the target backends already satisfy
    those interfaces directly with matching method signatures with zero adapter code needed
    (services/ecs/sfn_integration.go SFNRunTask, services/glue/sfn_integration.go
    SFNStartJobRun, services/eventbridge/sfn_integration.go SFNPutEvents -- verified signatures
    match asl's interfaces exactly). Because cli.go never wires them, every real (non-test)
    gopherstack process will hard-fail any ASL Task using an ecs:/glue:/events: resource ARN
    with ErrECSIntegrationNotConfigured/ErrGlueIntegrationNotConfigured/
    ErrEventBridgeIntegrationNotConfigured, even though the emulator's own PARITY.md previously
    claimed ECS/Glue/EventBridge resource ARN resolution was verified 'ok' -- that verification
    was executor-level (mocks) only and never checked end-to-end wiring. FIX is 3 lines in
    cli.go: sfnBk.SetECSIntegration(ecsH.Backend), sfnBk.SetGlueIntegration(glueH.Backend),
    sfnBk.SetEventBridgeIntegration(ebH.Backend), alongside the existing SQS/SNS/DynamoDB calls.
    Not fixed here per this pass's services/stepfunctions/-only scope; file bd issue and fix in
    cli.go directly (no new services/stepfunctions/ code needed)."
  - "Map Distributed Map ResultWriter (S3 write-out) not implemented -- needs new S3Writer integration wired from cli.go (bd: gopherstack-8j8)"
  - "Map ItemProcessor.ProcessorConfig.Mode (INLINE/DISTRIBUTED) not parsed/validated (bd: gopherstack-8im)"
  - "Retry.JitterStrategy accepts any string; only \"FULL\" is special-cased, invalid values silently behave as NONE instead of ValidationException at Create/UpdateStateMachine (bd: gopherstack-xtl)"
  - "StartExecution has no ClientRequestToken idempotency; EXPRESS's immediate-name-reuse semantics (vs STANDARD's reuse restriction) are not modeled (bd: gopherstack-1sf)"
  - "TaskScheduledEventDetails/TaskSucceededEventDetails still omit resourceType/region/parameters/timeoutInSeconds/heartbeatInSeconds/outputDetails.truncated; no TaskSubmitted/TaskStarted history events for .sync/.waitForTaskToken (bd: gopherstack-996)"
  - "Non-standard intrinsic functions (StringConcat, ArraySlice, MathSubtract, etc.) are accepted by this emulator but do not exist in real AWS Step Functions -- permissive superset, not a correctness bug against valid AWS definitions, but a definition that only works here would fail on real AWS (no bd filed; informational)"
deferred:
  - "State machine CRUD (Create/Update/Delete/Describe/List, versions/aliases, logging/tracing config) -- spot-checked only; last deep audit was PRs #1937/#1742/#2110 (batch1/batch2 audits) and appeared unchanged/correct"
  - "Activities (CreateActivity/GetActivityTask/SendTaskSuccess/Failure/Heartbeat) -- spot-checked only, appeared correct"
  - "Distributed Map ItemReader S3 CSV/JSON/JSONL decoding -- spot-checked only (unchanged), appeared correct"
leaks: {status: clean, note: "StopExecution/DeleteStateMachine cancel the execution's context via b.cancelFns; Wait/waitForRetry/execSem/semaphore all select on ctx.Done(); Map/Parallel goroutines (wg.Go) all respect ctx cancellation. No new goroutines introduced this pass beyond the existing patterns (executeWithStateRetryAndCatch runs synchronously in the calling goroutine, not a new one)."}
---

## Notes

**The big one this pass**: `runMapItem` (asl/executor.go) checked only the Go
`error` return of the per-iteration sub-executor's `Execute()` call. But
`Execute()` deliberately returns Fail-state/unhandled-Task failures as a
*successful* call — `(&ExecutionResult{Error: code, Cause: cause}, nil)` — so
that the top-level `Execute()` caller can distinguish "the state machine
executed successfully and *produced* a FAILED status" from "the Go call
itself errored" (e.g. bad JSON). `runParallelBranch` already had the correct
`if res.Error != "" { errs[idx] = &FailError{...} }` check for this; only the
Map path was missing it. This meant every Map state with a failing branch
silently succeeded with a `nil` hole in its output array instead of failing
the whole state (and thus never triggered any Catch/Retry, whether or not one
was even defined at the Map level — moot, since Map didn't support state-level
Catch/Retry at all before this pass either). **Trap for the next auditor**:
whenever a state type builds a sub-`Executor` and calls `.Execute()`, always
check `res.Error` in addition to the Go `error` — the two are orthogonal
result channels, and only checking one is what makes this bug so easy to miss
in review (the code "looks complete").

**AWS's Catch/Retry belong on Map and Parallel too, not just Task.** Before
this pass, `executeParallel` had ad-hoc Catch handling and `executeMap` had
none at all; neither had Retry. Extracted `executeWithStateRetryAndCatch()`
(shared by both) which re-runs the *entire* state body per attempt — that's
correct AWS semantics (a Map/Parallel retry restarts every branch/item, not
just the failed ones), but is a meaningful behavior difference from Task
Retry (which re-invokes a single resource call). Don't "simplify" this later
by trying to retry only failed items — that would silently diverge from AWS.

**GetExecutionHistory's Task/State detail objects were empty shells.**
`StateEnteredEventDetails`, `StateExitedEventDetails`, `TaskScheduledEventDetails`
(new), `TaskSucceededEventDetails` (new), and `TaskFailedEventDetails` (new)
all have AWS-correct field shapes and json tags, but the five
`historyRecorder` methods in backend.go took the real values as parameters
and then *threw them away*, writing back only `{Type, Timestamp}`. Every
existing `GetExecutionHistory` test only asserted on event `Type`/ordering/
pagination — never on the detail payload — which is exactly why this was
never caught. **Trap for the next auditor**: a green test suite for
`GetExecutionHistory` does not mean the event *bodies* are populated; check
that recorder methods actually use their parameters, not just that they
append an event of the right `Type`.

**AWS's Catch error-output shape is `{"Error": <code>, "Cause": <description>}`
as two separate keys**, not one combined string. The pre-existing code built
`{"Error": err.Error()}` where `err.Error()` on a `*FailError` returns
`"<code>: <cause>"` — so downstream states reading `$.error.Cause` after a
`ResultPath` would get nothing, and `$.error.Error` would contain a mangled
combined string instead of just the code. Fixed via `stepFunctionsErrorCode`/
`stepFunctionsErrorCause` (the former already existed for `catchesError`
matching; the latter is new).

**StartExecution vs StartSyncExecution / STANDARD vs EXPRESS**: AWS allows
*asynchronous* `StartExecution` on EXPRESS state machines ("Asynchronous
Express Workflows", a documented, commonly-used feature) — only
`StartSyncExecution` is restricted to EXPRESS. The emulator had this
backwards in two ways: (1) it rejected `StartExecution` on EXPRESS entirely
(a real functional gap blocking a valid, common integration pattern), and (2)
it used the wrong error code (`InvalidExecutionType`) for the one case that
*is* correctly rejected (`StartSyncExecution` on STANDARD) — AWS's actual
error there is `StateMachineTypeNotSupported`. Fixed both; added
`ErrStateMachineTypeNotSupported` and its `handler.go` error-code mapping.
`ClientRequestToken`/EXPRESS-name-reuse nuances remain unmodeled
(bd: gopherstack-1sf) — lower priority since the core functional gap
(EXPRESS StartExecution being rejected) was the severe part.

**Retry jitter**: AWS's `Retry.JitterStrategy` default is `"NONE"` (not
`"FULL"` — verify this if you're tempted to "fix" it the other way; it's
counter-intuitive since jitter is usually a sane default elsewhere).
`MaxDelaySeconds` caps the per-attempt delay *before* jitter is applied.
Both were entirely unparsed before this pass (only an internal 24h safety
cap existed, unrelated to the ASL-level `MaxDelaySeconds` field).

**Protocol**: json-1.0 (`X-Amz-Target: AWSStepFunctions.<Operation>`),
consistent with the rest of the codebase's json-1.0 services. No wire-format
regressions found in this family.

## 2026-07-11 re-audit (zero-drift pass)

`last_audit_commit` in the previous ledger (`e5a9ac69`) turned out to be a
stale/wrong hash (it's actually a **kinesis** commit, not a stepfunctions
one, and isn't an ancestor of the current HEAD). Used `ce30166a` ("Parity
sweep 3", the commit that actually authored this file) as baseline per the
re-audit protocol instead. `git diff ce30166a..HEAD -- services/stepfunctions/`
is **empty** — no commits touched this service directory since the last deep
audit. The SDK pin is also unchanged (`aws-sdk-go-v2/service/sfn v1.40.8`,
same 34 `api_op_*.go` files, no new ops). Per protocol, with zero drift and
zero not-ok rows there was no changed/new surface requiring re-audit; all
`ops:`/`families:` rows above are carried forward unchanged from the prior
pass and still trusted.

**No code changes made this pass.** All scoped gates pass clean: `go build`,
`go vet`, `go test -race` (both `stepfunctions` and `stepfunctions/asl`
packages), `go fix -diff` (empty), `golangci-lint run` (0 issues).

**New finding (reported, not fixed — lives in cli.go, out of this pass's
services/stepfunctions/-only scope)**: while spot-checking the
service-integration delivery path called out in this pass's brief, found
that `cli.go` wires Lambda/SQS/SNS/DynamoDB onto the Step Functions backend
but never calls `SetECSIntegration`/`SetGlueIntegration`/
`SetEventBridgeIntegration` — see the new first entry under `gaps:` above
for the full trace and 3-line fix. This means `ecs:runTask`/
`glue:startJobRun`/`events:putEvents` Task-state resource ARNs, despite
being fully implemented and unit-tested at the `asl.Executor` level, can
never actually deliver in a real running gopherstack process today. **Trap
for the next auditor**: an `asl_task` family marked "ok" for resource ARN
resolution only proves the *executor* dispatches correctly against a mock —
it says nothing about whether the concrete integration is ever wired up by
the process entrypoint. Cross-check `cli.go`'s `SetXIntegration` calls
against every interface `asl/executor.go` defines whenever auditing this
family again.
