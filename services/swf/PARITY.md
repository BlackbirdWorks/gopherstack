---
service: swf
sdk_module: aws-sdk-go-v2/service/swf@v1.33.14
last_audit_commit: d9aee9cb
last_audit_date: 2026-07-13
overall: A            # genuine fixes found this pass (see Notes)
ops:
  RegisterDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkflowTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op this pass, was entirely missing"}
  RegisterActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListActivityTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActivityType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op this pass, was entirely missing"}
  StartWorkflowExecution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not scheduling the initial decision task -- see Notes"}
  TerminateWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  GetWorkflowExecutionHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOpenWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClosedWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  RequestCancelWorkflowExecution: {wire: ok, errors: fixed, state: ok, persist: ok, note: "was ValidationException on closed exec; real AWS is UnknownResourceFault"}
  SignalWorkflowExecution: {wire: ok, errors: fixed, state: ok, persist: ok, note: "was ValidationException on closed exec; real AWS is UnknownResourceFault"}
  CountOpenWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountClosedWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingActivityTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingDecisionTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  PollForActivityTask: {wire: ok, errors: ok, state: ok, persist: partial, note: "activityQueues intentionally ephemeral, see Notes"}
  PollForDecisionTask: {wire: ok, errors: ok, state: ok, persist: partial, note: "decisionQueues intentionally ephemeral, see Notes"}
  RecordActivityTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondActivityTaskCanceled: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondActivityTaskCompleted: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondActivityTaskFailed: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondDecisionTaskCompleted: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "4 decision types silently dropped their attrs, see Notes"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  decision_processing: {status: fixed, note: "ContinueAsNewWorkflowExecution/StartChildWorkflowExecution/SignalExternalWorkflowExecution/RequestCancelExternalWorkflowExecution decision types record a history event but carry no attributes and don't perform the underlying semantic action (no new run created, no child workflow started, no signal delivered) -- see gaps"}
gaps:
  - "ContinueAsNewWorkflowExecution decision closes the execution as CONTINUED_AS_NEW but never starts the new run (no fresh WorkflowExecution/RunID, no re-seeded decision task) -- deciders that rely on continue-as-new see the workflow simply end. Bigger feature, out of scope for a bug-fix pass. (bd: TODO -- file follow-up)"
  - "StartChildWorkflowExecution/SignalExternalWorkflowExecution/RequestCancelExternalWorkflowExecution decisions record an *Initiated history event but never actually start/signal/cancel the target execution, and their wire-level attrs (workflowId, control, input, etc.) still are not parsed into the Decision struct -- only Started/TimerStarted/CancelTimer/RecordMarker/RequestCancelActivityTask attrs were wired this pass. Cross-execution orchestration is a bigger feature. (bd: TODO -- file follow-up)"
  - "activityQueues/decisionQueues (FIFO pending-task lists) are intentionally NOT part of backendSnapshot (pre-existing, documented design choice in persistence.go/backend.go -- order-sensitive plain maps). A restart loses in-flight pending tasks that haven't been polled yet, while their corresponding history events and active-task records DO survive. Not fixed this pass (would require reworking backendSnapshot's shape); flagged for awareness. (bd: TODO -- file follow-up)"
  - "openTimers/openChildWorkflowExecutions/openLambdaFunctions in DescribeWorkflowExecution's openCounts are hardcoded to 0 -- consistent with the timer/child-workflow gaps above, not independently fixed."
deferred:
  - "DescribeWorkflowExecution's openCounts.openLambdaFunctions (always 0 -- SWF Lambda task support is out of scope for a JSON-wire-shape/state-mutation audit)"
leaks: {status: clean, note: "no goroutines/timers spawned by this service; all state lives in InMemoryBackend maps/store.Tables guarded by lockmetrics.RWMutex"}
---

## Notes

Protocol: SWF is **awsjson1.0** (`application/x-amz-json-1.0`, `X-Amz-Target:
SimpleWorkflowService.<Op>`) -- confirmed against the real
`aws-sdk-go-v2/service/swf` serializers (every op sets
`Content-Type: application/x-amz-json-1.0`). This is easy to mis-key as
awsjson1.1 (the more common AWS JSON protocol) since SWF's dispatch shape
looks identical otherwise.

### Real bugs fixed this pass

1. **Wrong response Content-Type** (`handler.go`): was
   `application/x-amz-json-1.1`, should be `application/x-amz-json-1.0`. Every
   other awsjson1.0 service in this repo (dynamodb, dynamodbstreams,
   stepfunctions, apprunner, cloudcontrol, codestarconnections,
   verifiedpermissions, timestreamquery) gets this right; swf was the
   exception. `TestSWFHandler_ResponseContentType` guards this now.

2. **StartWorkflowExecution never scheduled the initial decision task**
   (`backend.go`): a freshly started workflow execution recorded
   `WorkflowExecutionStarted` in history but never enqueued anything onto
   `decisionQueues`. Only a *subsequent* stimulus (signal, cancel request,
   activity completion) called `enqueueDecisionTaskLocked` and got the ball
   rolling. A workflow with no other stimulus after Start could never be
   polled by a decider -- classic disguised-no-op ("workflow stuck OPEN, no
   decision processing"). The entire pre-existing test suite worked around
   this by calling the test-only `EnqueueDecisionTaskInternal` immediately
   after every `StartWorkflowExecution` call, which is itself a strong signal
   the real path was never exercised. Fixed by calling
   `enqueueDecisionTaskLocked` at the end of `StartWorkflowExecution`.
   `TestStartWorkflowExecution_EnqueuesInitialDecisionTask` covers it.

3. **RequestCancelWorkflowExecution / SignalWorkflowExecution returned the
   wrong fault type on a closed execution** (`backend.go`): both returned
   `ErrValidation` (`ValidationException`, HTTP 400) when the target
   execution wasn't RUNNING. Real AWS's own SDK doc comments are explicit:
   *"If the specified workflow execution isn't open, this method fails with
   UnknownResource."* -- and neither op's fault model even includes
   `ValidationException` (only `OperationNotPermittedFault` and
   `UnknownResourceFault`, confirmed against the generated deserializer
   switch). A real SDK client would fail to type-assert the emulator's
   response into any known SWF exception. Fixed to return `ErrNotFound`
   (`UnknownResourceFault`, HTTP 404) instead.

4. **RespondDecisionTaskCompleted silently dropped 4 decision types' wire
   attributes** (`handler.go` + `backend.go`): `RequestCancelActivityTask`,
   `StartTimer`, `CancelTimer`, `RecordMarker` decisions were parsed off the
   wire into handler-local structs (`requestCancelActivityDecisionAttrs`
   etc.) but never copied into the `Decision` struct passed to the backend --
   the backend's `Decision` type simply had no fields for them. The right
   history event type was still recorded (so tests asserting "an event of
   type X exists" passed), but every attribute the decider sent
   (`activityId`, `timerId`, `startToFireTimeout`, `markerName`, `details`)
   was discarded and replaced with an empty `{}`. This is exactly the
   "real-looking but disguised stub" trap from the parity playbook: correct
   event *type*, fabricated (empty) event *payload*. Fixed by adding
   `RequestCancelActivityTaskAttrs`/`StartTimerAttrs`/`CancelTimerAttrs`/
   `RecordMarkerAttrs` to `Decision`, wiring them through in `handler.go`, and
   populating the corresponding history event attributes in
   `processDecisionLocked`. Covered by
   `TestRespondDecisionTaskCompleted_TaskTimerMarkerAttrsPropagate` (drives
   the real HTTP wire path end-to-end).

5. **DeleteActivityType / DeleteWorkflowType were entirely unimplemented**
   (missing op, not a stub): the real SWF API added these two ops (delete a
   *deprecated* type permanently, `TypeNotDeprecatedFault` if not yet
   deprecated -- confirmed against the real SDK's `api_op_Delete*.go` doc
   comments and generated error-deserializer switch, which lists exactly
   `OperationNotPermittedFault`/`TypeNotDeprecatedFault`/`UnknownResourceFault`).
   gopherstack's own `sdk_completeness_test.go` had them explicitly
   whitelisted in a `notImplemented` list, and a stale `parity_test.go` test
   (`TestParity_DeleteOps_NotSupported`) asserted they should 400 as unknown
   operations -- both signals this was a known, tracked gap rather than an
   intentional decision to skip. Implemented for real: `Backend.DeleteWorkflowType`/
   `DeleteActivityType` require the type to already be DEPRECATED, then
   remove it from the `workflows`/`activities` `store.Table` (which also
   drops it from the `byDomain` index and from Snapshot/Restore for free,
   since it's a "clean" table). Registered in the dispatch table,
   `GetSupportedOperations`, and the `StorageBackend` interface.
   `TestParity_DeleteOps_RequireDeprecatedFirst` replaces the stale
   not-supported test; `TestDeleteWorkflowType`/`TestDeleteActivityType`
   cover the backend directly.

### Traps for the next auditor

- `RegisterDomain`/`RegisterWorkflowType`/`RegisterActivityType`'s "type
  already active" error on Undeprecate is correctly `TypeAlreadyExistsFault`
  / `DomainAlreadyExistsFault` (verified against the real deserializer
  switches for `UndeprecateDomain`/`UndeprecateWorkflowType`/
  `UndeprecateActivityType`) -- do NOT "fix" this to
  `TypeNotDeprecatedFault`/`DomainNotDeprecatedFault`; those fault types
  don't even exist for these ops (only for `Delete*`). Easy to get backwards
  since the naming is confusingly close.
- `activityQueues`/`decisionQueues` are plain maps, not `store.Table`s, and
  are deliberately excluded from `backendSnapshot` (documented in both
  `backend.go`'s `InMemoryBackend` doc comment and `persistence.go`'s
  `restoreDirtyTablesLocked` doc) -- this predates this audit pass and was
  not changed. Don't flag it as a fresh persistence regression; it's a
  pre-existing, intentional simplification (see gaps above for the
  follow-up).
- `CreationDate`/`StartTimestamp`/etc. use hand-rolled
  `float64(time.Now().UnixMilli()) / milliDivisor` instead of
  `pkgs/awstime.Epoch`. Output is equivalent (epoch-seconds float64, matches
  the real Timestamp shape), so this is a reuse/style nit, not a wire bug --
  left alone this pass to stay within a bug-fix scope, but worth a `pkgs`
  reuse cleanup later.
