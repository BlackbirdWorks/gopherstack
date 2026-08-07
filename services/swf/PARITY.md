---
service: swf
sdk_module: aws-sdk-go-v2/service/swf@v1.37.4   # verified this pass; go.mod pin, was stale at v1.33.14
last_audit_commit: pending (agent instructed not to commit; see git log for this pass's commit)
last_audit_date: 2026-08-07
overall: A            # genuine fixes found this pass (see Notes)
ops:
  RegisterDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateDomain: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not cascading DEPRECATED onto the domain's registered workflow/activity types, see Notes"}
  UndeprecateDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkflowTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListActivityTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  StartWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "childPolicy was parsed off the wire into handleTerminateWorkflowExecutionInput and then silently discarded -- the backend call took no such parameter, so a client's per-call override never applied and only the policy stored at StartWorkflowExecution time governed. Now threaded through and, combined with a new TERMINATE/REQUEST_CANCEL child-policy cascade onto open children, actually takes effect; also propagates ChildWorkflowExecutionTerminated to the parent execution, see Notes"}
  DescribeWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "openCounts.openTimers/openChildWorkflowExecutions were hardcoded 0; executionInfo.parent was entirely missing; ADDITIONALLY (gopherstack-jsi8, 2026-08-07): the wire's Execution.RunId (a real, required field per types.WorkflowExecution) was parsed off the request and then silently discarded -- the Go-level backend method took no runID parameter at all, so a client asking for a specific historical run always got whatever run currently occupied the domain+workflowId slot instead. Now threaded through end to end; see Notes"}
  GetWorkflowExecutionHistory: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "gopherstack-jsi8, 2026-08-07: same Execution.RunId-discarded bug as DescribeWorkflowExecution above, same fix -- see Notes"}
  ListOpenWorkflowExecutions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution"}
  ListClosedWorkflowExecutions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution"}
  RequestCancelWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  SignalWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  CountOpenWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountClosedWorkflowExecutions: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingActivityTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingDecisionTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  PollForActivityTask: {wire: ok, errors: ok, state: ok, persist: partial, note: "activityQueues intentionally ephemeral, see Notes"}
  PollForDecisionTask: {wire: ok, errors: ok, state: ok, persist: partial, note: "decisionQueues intentionally ephemeral, see Notes"}
  RecordActivityTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondActivityTaskCanceled: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondActivityTaskCompleted: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now propagates ChildWorkflowExecutionCompleted to the parent execution, see Notes"}
  RespondActivityTaskFailed: {wire: ok, errors: ok, state: ok, persist: ok}
  RespondDecisionTaskCompleted: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ContinueAsNewWorkflowExecution/StartChildWorkflowExecution/SignalExternalWorkflowExecution/RequestCancelExternalWorkflowExecution now perform real state mutation instead of recording an empty *Initiated event; decisionTaskCompletedEventId was missing from every decision-derived history event; StartTimer/CancelTimer now validate timerId state; see Notes"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  decision_processing: {status: ok, note: "all 12 SWF decision types now perform real state mutation and carry decisionTaskCompletedEventId + full wire attrs -- see Notes. Dispatch table decomposed into decisionHandlers() (decision_tasks.go) + decision_orchestration.go, removing the historical cyclop/funlen nolint on processDecisionLocked."}
  multi_run_history: {status: ok, note: "FIXED (gopherstack-jsi8, 2026-08-07): executions/history were keyed by domain+workflowId alone, so a second/later run of the same workflowId silently overwrote the first run's row and history -- confirmed against the real aws-sdk-go-v2/service/swf@v1.37.4 types.WorkflowExecution, where RunId is a required member alongside WorkflowId (not an optional disambiguator), and DescribeWorkflowExecutionInput/GetWorkflowExecutionHistoryInput's Execution field requires both. Re-keyed executions (store.Table) and history (map) to domain+workflowId+runId (workflowExecutionKeyFn/executionKey, store.go/store_setup.go); added an executionsByWorkflow store.Index grouping every run (open or closed) under domain+workflowId so the currently-open run (real AWS guarantees at most one) can still be found without a full-domain scan. New resolveExecutionLocked/openExecutionLocked helpers centralize 'find a run, optionally pinned to a runId' -- a non-empty runId is an exact lookup (works for ANY run, open or long-closed); an empty runId first tries the open run, then falls back to the most-recently-started run for that workflowId (a deliberate leniency beyond real AWS, which would error UnknownResource with no open run -- kept so callers that don't track runId, including this backend's own internal cross-execution decision handlers, keep working exactly as before for the still-common single-run-per-workflowId case). DescribeWorkflowExecution/GetWorkflowExecutionHistory both gained a runID parameter (the wire's Execution.RunId was already being parsed but silently discarded -- see their ops rows above); Terminate/RequestCancel/SignalWorkflowExecution's pre-existing runID parameters now actually disambiguate instead of being checked against a single shared row. Every appendHistoryEventLocked/enqueueDecisionTaskLocked call site across activity_tasks.go/decision_tasks.go/decision_orchestration.go/signals.go/workflow_executions.go now threads the specific run's runID through (~25 call sites) rather than resolving 'the' execution for a workflowId. propagateChildClosureLocked now does a direct domain+parentWorkflowId+parentRunId lookup instead of an ambiguous domain+workflowId one, so a parent that has since continued-as-new (a newer run under the same workflowId) no longer incorrectly receives a child-closure event meant for the run that actually started that child. Also fixed the closely-related 'LRU-eviction ghost queue rows' gopherstack-jsi8 finding: registerExecutionOrderLocked's eviction (at the pre-existing maxWorkflowExecutions=10_000 cap) previously deleted only the execution row and history, leaving any still-pending decisionQueues/activityQueues entry or activeDecisionTasks/activeActivityTasks record for that run behind as a ghost referencing data that no longer existed; new evictExecutionLocked purges all four. New tests: TestMultiRunHistory (multirun_test.go, 6 cases covering explicit-old-run/explicit-new-run/empty-run-id resolution, history isolation between runs, the already-open-run rejection, and the falls-back-to-most-recent behavior) and TestLRUEvictionPurgesPendingDecisionTasks (creates 10_000 executions, confirms the evicted run's pending decision task is gone from its task list and the evicted execution is truly not found -- verified this test fails without the evictExecutionLocked fix). One existing test (TestRespondDecisionTaskCompleted_ContinueAsNew) asserted the old run's WorkflowExecutionContinuedAsNew event appeared in the NEW run's history, which was only true because both runs shared one history blob under the old keying -- corrected to check each run's own history/DescribeWorkflowExecution independently, which is the actual point of this fix. Interface changes: Backend.DescribeWorkflowExecution and Backend.GetWorkflowExecutionHistory each gained a runID parameter; no external callers found via full-repo grep (cloudformation/dashboard/cli.go reference services/swf but not these methods directly)."}
gaps:
  - "activityQueues/decisionQueues (FIFO pending-task lists) are intentionally NOT part of backendSnapshot (pre-existing, documented design choice in store.go/persistence.go -- order-sensitive plain maps). A restart loses in-flight pending tasks that haven't been polled yet, while their corresponding history events and active-task records DO survive. Not fixed this pass (would require reworking backendSnapshot's shape); flagged for awareness. (bd: TODO -- file follow-up)"
  - "Child-policy cascade is now implemented for TerminateWorkflowExecution (this pass, see Notes): TERMINATE recursively terminates open children (cascading each child's own stored ChildPolicy to grandchildren in turn), REQUEST_CANCEL records WorkflowExecutionCancelRequested (cause CHILD_POLICY_APPLIED) on each open child and gives it a fresh decision task, and ABANDON is correctly a no-op. This closes the TerminateWorkflowExecution half of gopherstack-jsi8's child-policy finding. Real AWS's *other* child-policy trigger -- an execution auto-closing via WorkflowExecutionTimedOut when ExecutionStartToCloseTimeout/TaskStartToCloseTimeout expires -- is unreachable here because this backend has no timeout-enforcement mechanism at all (statusTimedOut is defined in models.go but nothing ever sets it; no background timer, no check on poll/describe). That is a separate, materially larger gap (a whole missing feature, not a cascade bug) that predates this pass, was not previously documented, and is out of scope for this fix; flagging it here since it was surfaced while auditing this exact mechanism. (bd: TODO -- file follow-up for timeout enforcement)"
  - "Complete/Fail/Cancel workflow-closing decisions do NOT cascade child policy onto their own open children, and this is correct, not a gap: real AWS's child policy is only ever invoked when a workflow execution is terminated (explicitly, via TerminateWorkflowExecution) or times out -- never on a normal Complete/Fail/Cancel close, where child executions are simply independent and keep running. Parent-closure IS still propagated to already-open children as history events in all four cases (ChildWorkflowExecutionCompleted/Failed/Canceled/Terminated, see Notes), so deciders learn of parent closure either way."
  - "ScheduleLambdaFunction decision type (Lambda activity tasks) is not implemented -- consistent with the pre-existing openLambdaFunctions deferral below; SWF Lambda task support as a whole is out of scope for this service."
deferred:
  - "DescribeWorkflowExecution's openCounts.openLambdaFunctions (always 0) and the ScheduleLambdaFunction decision type -- SWF Lambda task support is out of scope for a JSON-wire-shape/state-mutation audit."
leaks: {status: clean, note: "no goroutines/timers spawned by this service, including the new cross-execution decision handlers in decision_orchestration.go -- every SWF 'timer' is purely decision-driven state (OpenTimerIDs on WorkflowExecution, mutated only by StartTimer/CancelTimer decisions) with no autonomous firing, consistent with the pre-existing no-goroutine design. All state lives in InMemoryBackend maps/store.Tables guarded by lockmetrics.RWMutex; every lock path uses defer-release."}
---

## Notes

Protocol: SWF is **awsjson1.0** (`application/x-amz-json-1.0`, `X-Amz-Target:
SimpleWorkflowService.<Op>`) -- confirmed against the real
`aws-sdk-go-v2/service/swf` serializers (every op sets
`Content-Type: application/x-amz-json-1.0`). This is easy to mis-key as
awsjson1.1 (the more common AWS JSON protocol) since SWF's dispatch shape
looks identical otherwise.

### Real bugs fixed this pass (2026-08-07, gopherstack-jsi8)

executions/history were keyed by `domain+":"+workflowID` alone (not
`+runID`), so a second or later run of the same `workflowId` silently
collided with -- overwrote -- an earlier, already-closed run's row and
history. Confirmed against the real `aws-sdk-go-v2/service/swf@v1.37.4`
(go.mod's pin; the `sdk_module` line above was stale at v1.33.14, corrected
this pass): `types.WorkflowExecution.RunId` is a **required** member
alongside `WorkflowId`, not an optional disambiguator, and both
`DescribeWorkflowExecutionInput.Execution` and
`GetWorkflowExecutionHistoryInput.Execution` require it. Real AWS keeps every
run of a `workflowId` as an independently, permanently queryable record; this
backend did not.

Full detail (the re-keying itself, the new `resolveExecutionLocked`/
`openExecutionLocked` helpers and their deliberate empty-`runID` leniency, the
`propagateChildClosureLocked` fix, the closely-related LRU-eviction
ghost-queue-row fix, and the test list) is in the `multi_run_history` family
note above rather than duplicated here. Two points worth calling out
separately:

1. **This was a real, wire-reachable bug, not just an internal-storage
   nicety.** `DescribeWorkflowExecution`/`GetWorkflowExecutionHistory`'s
   handlers (`handler_workflow_executions.go`/`handler_history.go`) already
   parsed `Execution.RunId` off the wire into `in.Execution.RunID` -- and then
   never passed it to the backend call, which had no `runID` parameter to
   receive it. A real client asking for a specific historical run by RunId
   got whatever run currently occupied that `workflowId` slot instead,
   silently wrong data rather than an error.

2. **The empty-`runID` fallback is intentionally more lenient than real
   AWS.** Real AWS requires `RunId` on Describe/GetHistory and would reject a
   call with none; the ops here whose `RunId` is genuinely optional
   (Terminate/RequestCancel/SignalWorkflowExecution) target only the
   currently-*open* run and error `UnknownResource` otherwise. This backend's
   `resolveExecutionLocked` accepts an empty `runID` everywhere, first trying
   the open run and then falling back to the most-recently-started run if
   none is open. This is a deliberate compatibility choice, not an oversight:
   it preserves this backend's own pre-existing behavior (and every existing
   test/internal caller) for the still-overwhelmingly-common
   single-run-per-`workflowId` case, while a caller that actually needs a
   *specific* run -- the entire point of this fix -- still gets it by passing
   a real `runID`. The three mutating ops (Terminate/RequestCancel/Signal)
   cannot be led astray by this leniency: each independently re-checks
   `exec.Status == RUNNING` after resolution regardless of which run was
   returned, so they can never act on a closed run just because none was
   open.

### Real bugs fixed this pass (2026-07-31)

1. **TerminateWorkflowExecution's `childPolicy` override was parsed and
   thrown away.** `handler_workflow_executions.go`'s
   `handleTerminateWorkflowExecutionInput` had a `ChildPolicy` field, but the
   call into the backend passed only `(Domain, WorkflowID, RunID, Reason,
   Details)` -- `InMemoryBackend.TerminateWorkflowExecution` had no
   childPolicy parameter at all. Confirmed against the real
   `aws-sdk-go-v2/service/swf` `TerminateWorkflowExecutionInput.ChildPolicy`
   doc comment that this is a genuine, real, per-call override ("This policy
   overrides the child policy specified for the workflow execution at
   registration time or when starting the execution"), not an
   invented/misread field.

   Fixing the signature alone would have created a new false claim -- the API
   would *appear* to accept an override that still did nothing -- because the
   child-policy cascade itself did not exist: no code path ever acted on
   `WorkflowExecution.ChildPolicy` for a closing execution's children (a
   child just kept running regardless, equivalent to always applying
   ABANDON; `propagateChildClosureLocked` only ever notifies a closing
   execution's *own parent*, not the other direction). So this pass
   implements the cascade too, scoped to what real AWS actually defines:
   `TerminateWorkflowExecution` now resolves an effective policy (the
   override if given, else `exec.ChildPolicy`), records it accurately on the
   `WorkflowExecutionTerminated` event, and applies it to open children via
   `applyChildPolicyLocked`:
   - `TERMINATE`: each open child is itself terminated
     (`terminateExecutionLocked`, cause `CHILD_POLICY_APPLIED`), which
     recursively cascades *that child's own* stored `ChildPolicy` onto its
     own children -- the override argument governs only the one execution
     named in the API call, exactly as AWS's doc says.
   - `REQUEST_CANCEL`: each open child gets a `WorkflowExecutionCancelRequested`
     event (cause `CHILD_POLICY_APPLIED` -- the only cause value real AWS's
     `WorkflowExecutionCancelRequestedCause` enum defines) and a fresh
     decision task, mirroring `RequestCancelWorkflowExecution`'s own
     behavior; it does not itself close the child.
   - `ABANDON`: no-op, matching the pre-existing (and correct) behavior.

   The stored default (`exec.ChildPolicy`, set at `StartWorkflowExecution`)
   is never mutated by an override -- it continues to govern any later close
   that doesn't supply one, exactly as real AWS specifies.

   `gopherstack-jsi8`'s note that "executions/history are keyed by
   `domain+workflowId`, not `+runId`, so Terminate's `runId` parameter is
   decorative" is real but does **not** block this fix: child-matching in
   `applyChildPolicyLocked` filters on `ParentRunID == parent.RunID` (the
   same pattern `openCountsLocked` already used), so the cascade correctly
   targets only the children of the run actually being terminated. That
   out-of-scope re-keying issue was left untouched.

   New tests in `decision_orchestration_test.go`:
   `TestTerminateWorkflowExecution_ChildPolicyOverride_Terminate` (override
   cascades TERMINATE despite the parent's stored default being ABANDON),
   `_RequestCancel` (override cascades REQUEST_CANCEL, child stays open),
   `_Absent` (no override falls back to the stored ABANDON default -- pins
   the correct no-op behavior), `_Invalid` (a bogus override is rejected,
   same validation as `StartWorkflowExecution`'s `childPolicy`). No
   pre-existing test asserted the discarded-override bug or an
   always-ABANDON cascade; the gap was present but unencoded in tests.

### Real bugs fixed this pass (2026-07-23)

1. **ContinueAsNewWorkflowExecution never started the new run**
   (`decision_orchestration.go`, new file): the decision closed the execution
   as `CONTINUED_AS_NEW` and stopped -- no fresh `RunID`, no re-seeded
   decision task, so a decider that relied on continue-as-new saw the
   workflow simply dead-end forever. Fixed by resolving the (possibly
   re-versioned) `WorkflowType`'s defaults exactly like `StartWorkflowExecution`
   does (both now share `createExecutionLocked`), closing the old run, and
   starting a fresh one under the same `domain+workflowId` with a new `RunID`,
   `WorkflowExecutionContinuedAsNew` (carrying `newExecutionRunId`) followed by
   a fresh `WorkflowExecutionStarted` (carrying `continuedExecutionRunId`), and
   a decision task enqueued on the new run's task list. If the workflow type
   can't be resolved, the execution stays open and
   `ContinueAsNewWorkflowExecutionFailed` is recorded instead (matching real
   AWS: a rejected decision never closes the run). Architectural caveat: since
   `executions`/`history` are keyed by `domain+workflowId` only (not
   `+runId`), the completed old run isn't independently queryable after
   continuation -- see gaps.
   `TestRespondDecisionTaskCompleted_ContinueAsNew`/
   `_ContinueAsNew_UnknownWorkflowType` cover both paths.

2. **StartChildWorkflowExecution/SignalExternalWorkflowExecution/
   RequestCancelExternalWorkflowExecution recorded an empty `*Initiated` event
   and did nothing else** (`decision_orchestration.go`): none of their
   wire-level attrs (`workflowId`, `runId`, `signalName`, `control`, etc.) were
   even parsed off the wire into the `Decision` struct, and the target
   execution was never touched. Fixed:
   - `StartChildWorkflowExecution` now actually creates the child execution
     (reusing `createExecutionLocked`), links it back to its parent
     (`WorkflowExecution.ParentWorkflowID/ParentRunID/ParentInitiatedEventID/
     ParentStartedEventID`, new fields), and records
     `ChildWorkflowExecutionStarted` on the parent. Failure (unknown/deprecated
     workflow type, or the child's workflowId already has an open run) records
     `StartChildWorkflowExecutionFailed` with the matching real cause
     (`WORKFLOW_TYPE_DOES_NOT_EXIST`/`WORKFLOW_TYPE_DEPRECATED`/
     `WORKFLOW_ALREADY_RUNNING`).
   - `SignalExternalWorkflowExecution` now delivers the signal: appends
     `WorkflowExecutionSignaled` to the target's history and enqueues it a
     decision task, or `SignalExternalWorkflowExecutionFailed`
     (`UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION`) if the target isn't
     found/open/run-matching.
   - `RequestCancelExternalWorkflowExecution` now actually requests
     cancellation: sets the target's `CancelRequested`, appends
     `WorkflowExecutionCancelRequested`, and enqueues it a decision task, or
     `RequestCancelExternalWorkflowExecutionFailed` on the same
     not-found/not-open condition.
   - A closing child (Complete/Fail/Cancel decision, or
     `TerminateWorkflowExecution`) now propagates `ChildWorkflowExecutionCompleted/
     Failed/Canceled/Terminated` back onto its parent's history and gives the
     parent a fresh decision task (`propagateChildClosureLocked`), so a parent
     decider learns the outcome without polling the child directly.
   - `DescribeWorkflowExecution`/`ListOpen/ClosedWorkflowExecutions` gained the
     `executionInfo.parent` field (real AWS's `WorkflowExecutionInfo.Parent`),
     and `openCounts.openChildWorkflowExecutions` is now the real count of
     open children (previously hardcoded 0).
   All four wire-level attrs are now parsed in `handler_decision_tasks.go`
   (`convertDecisionOrchestrationAttrs`) using the exact field names confirmed
   against the real SDK's `serializers.go`
   (`continueAsNewWorkflowExecutionDecisionAttributes`,
   `startChildWorkflowExecutionDecisionAttributes`,
   `signalExternalWorkflowExecutionDecisionAttributes`,
   `requestCancelExternalWorkflowExecutionDecisionAttributes`).
   `decision_orchestration_test.go` covers success/failure for all three
   cross-execution decisions plus parent-closure propagation for all four
   closure paths (Complete/Fail/Cancel/Terminate);
   `TestSnapshotRestore_ChildLinkAndOpenTimers` covers the new
   `WorkflowExecution` fields surviving Snapshot/Restore.

3. **`decisionTaskCompletedEventId` was missing from every decision-derived
   history event** (`decision_tasks.go`, `decision_orchestration.go`,
   `workflow_executions.go`): field-diffed against the real SDK's
   `types.go` -- every one of `WorkflowExecutionCompleted/Failed/Canceled`,
   `ActivityTaskScheduled/CancelRequested`, `TimerStarted/Canceled`,
   `MarkerRecorded`, `WorkflowExecutionContinuedAsNew`, and all three new
   `*Initiated` events require this field (it's how a decider traces an event
   back to the decision that caused it), and it was absent everywhere. Fixed
   by threading `decisionTaskCompletedEventId` (captured once per
   `RespondDecisionTaskCompleted` call, from the `DecisionTaskCompleted` event
   it appends) through `decisionCtx` into every handler.

4. **StartTimer/CancelTimer never validated timerId state** (`decision_tasks.go`):
   `StartTimer` always recorded `TimerStarted` even for an already-open
   `timerId`, and `CancelTimer` always recorded `TimerCanceled` even for a
   `timerId` that was never started -- real AWS rejects both with
   `StartTimerFailed(TIMER_ID_ALREADY_IN_USE)` /
   `CancelTimerFailed(TIMER_ID_UNKNOWN)` (confirmed against the real SDK's
   `StartTimerFailedCause`/`CancelTimerFailedCause` enums). Fixed by adding
   `WorkflowExecution.OpenTimerIDs` (a per-execution open-timer set, mutated
   only by these two decisions -- there is still no autonomous timer-firing
   goroutine, consistent with this service's no-goroutine design) and
   validating against it. This also makes `openCounts.openTimers` real instead
   of hardcoded 0. `TestStartTimerDecision_AlreadyInUse`/
   `TestCancelTimerDecision_UnknownID` cover both faults.

5. **DeprecateDomain didn't cascade to the domain's registered types**
   (`domains.go`): the real SDK's doc comment on `DeprecateDomain` is explicit:
   *"Deprecating a domain also deprecates all activity and workflow types
   registered in the domain. Executions that were started before the domain
   was deprecated continue to run."* -- the emulator only flipped the domain's
   own status, leaving every workflow/activity type `REGISTERED`. Fixed to
   cascade `DEPRECATED` onto every `REGISTERED` type in the domain (already-
   deprecated types are left alone; open executions are deliberately
   untouched, matching the doc comment).
   `TestDeprecateDomain_CascadesToRegisteredTypes` covers it. (`UndeprecateDomain`
   does *not* cascade back -- the real doc comment for it makes no such claim,
   so this is intentionally one-directional.)

6. **`processDecisionLocked`'s `cyclop,funlen` nolint** (`decision_tasks.go`):
   the historical 12-decision-type `switch` carried a
   `//nolint:cyclop,funlen // 12 SWF decision types; cannot reduce without
   artificial splitting` comment. Decomposed into a `DecisionType -> handler`
   dispatch table (`decisionHandlers()`, a `sync.OnceValue`-backed
   package-level map, mirroring `services/apigatewayv2`'s `onceOpTable`
   pattern) with one small function per decision type in `decision_tasks.go`
   (simple decisions) and `decision_orchestration.go` (the four
   cross-execution ones) -- no nolint suppression needed.
   `TestDecisionHandlers_CoverAllDecisionTypes` is a table-driven test over
   the dispatch table asserting every decision type has a handler.

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
  `store.go`'s `InMemoryBackend` doc comment and `persistence.go`'s
  `restoreDirtyTablesLocked` doc) -- this predates this audit pass and was
  not changed. Don't flag it as a fresh persistence regression; it's a
  pre-existing, intentional simplification (see gaps above for the
  follow-up).
- `CreationDate`/`StartTimestamp`/etc. use hand-rolled
  `float64(time.Now().UnixMilli()) / milliDivisor` instead of
  `pkgs/awstime.Epoch`. Output is equivalent (epoch-seconds float64, matches
  the real Timestamp shape), so this is a reuse/style nit, not a wire bug --
  left alone this pass to stay within scope, but worth a `pkgs` reuse cleanup
  later.
- UPDATE (2026-08-07, gopherstack-jsi8): `executions`/`history` are now keyed
  by `domain+workflowId+runId` (see store.go's `InMemoryBackend` doc comment
  and the `multi_run_history` family note above) -- the paragraph this
  replaces described the old, since-fixed `domain+workflowId`-only keying.
  `StartChildWorkflowExecution`'s child still must have a workflowId with no
  *currently open* run anywhere in the domain (that invariant is real AWS
  behavior, `WorkflowExecutionAlreadyStartedFault`, not a storage-shape
  limitation, so it did not change). If you find yourself re-deriving a
  `domain+":"+workflowID` key anywhere in this package outside
  `workflowGroupKey`/`executionsByWorkflow`, that is very likely a
  reintroduction of this exact bug class -- every per-run lookup must go
  through `executionKey`/`resolveExecutionLocked`, not a bare
  `domain+":"+workflowID` string.
- `WorkflowExecution.OpenTimerIDs`/`ParentWorkflowID`/`ParentRunID`/
  `ParentInitiatedEventID`/`ParentStartedEventID` (new fields this pass) are
  internal-only: they're never marshaled onto any AWS wire response directly
  (`DescribeWorkflowExecution`/list handlers project them into
  `openCounts`/`executionInfo.parent` via separate DTOs in
  `handler_workflow_executions.go`), but they DO round-trip through
  Snapshot/Restore for free since `executions` is a "clean" `store.Table`
  (see store.go) -- don't add a wire-visible field with the same name by
  accident, and don't assume they need special `persistence.go` wiring if you
  add more.
