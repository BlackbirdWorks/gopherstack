---
service: swf
sdk_module: aws-sdk-go-v2/service/swf@v1.37.4   # verified this pass; go.mod pin, was stale at v1.33.14
last_audit_commit: fd65c414d
last_audit_date: 2026-08-10
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
  StartWorkflowExecution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now sweeps expired executions first so a workflowId whose prior run has timed out (but not yet been observed) can be restarted instead of falsely hitting WorkflowExecutionAlreadyStartedFault -- see Notes: timeout enforcement (gopherstack-7gse)"}
  TerminateWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "childPolicy was parsed off the wire into handleTerminateWorkflowExecutionInput and then silently discarded -- the backend call took no such parameter, so a client's per-call override never applied and only the policy stored at StartWorkflowExecution time governed. Now threaded through and, combined with a new TERMINATE/REQUEST_CANCEL child-policy cascade onto open children, actually takes effect; also propagates ChildWorkflowExecutionTerminated to the parent execution, see Notes. ADDITIONALLY (gopherstack-7gse, 2026-08-10): now sweeps expired executions first, same as StartWorkflowExecution above -- see Notes: timeout enforcement"}
  DescribeWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "openCounts.openTimers/openChildWorkflowExecutions were hardcoded 0; executionInfo.parent was entirely missing; ADDITIONALLY (gopherstack-jsi8, 2026-08-07): the wire's Execution.RunId (a real, required field per types.WorkflowExecution) was parsed off the request and then silently discarded -- the Go-level backend method took no runID parameter at all, so a client asking for a specific historical run always got whatever run currently occupied the domain+workflowId slot instead. Now threaded through end to end; see Notes. ADDITIONALLY (gopherstack-7gse, 2026-08-10): now sweeps expired executions (EXECUTION_START_TO_CLOSE only) before resolving, so a RUNNING execution whose timeout has elapsed reads back as TIMED_OUT instead of staying RUNNING forever -- see Notes: timeout enforcement"}
  GetWorkflowExecutionHistory: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "gopherstack-jsi8, 2026-08-07: same Execution.RunId-discarded bug as DescribeWorkflowExecution above, same fix -- see Notes. Also sweeps expired executions first, same as DescribeWorkflowExecution (gopherstack-7gse)"}
  ListOpenWorkflowExecutions: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution. Also sweeps expired executions first (gopherstack-7gse) so a timed-out execution moves from the open list to the closed list on the next call instead of staying open forever -- see Notes: timeout enforcement"}
  ListClosedWorkflowExecutions: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution. Also sweeps expired executions first (gopherstack-7gse), same effect as ListOpenWorkflowExecutions above"}
  RequestCancelWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), defense-in-depth consistency with the other execution-touching ops -- see Notes: timeout enforcement"}
  SignalWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as RequestCancelWorkflowExecution"}
  CountOpenWorkflowExecutions: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "now sweeps expired executions first (gopherstack-7gse) so a timed-out execution is no longer counted as open -- see Notes: timeout enforcement"}
  CountClosedWorkflowExecutions: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "now sweeps expired executions first (gopherstack-7gse), same as CountOpenWorkflowExecutions above"}
  CountPendingActivityTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingDecisionTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  PollForActivityTask: {wire: ok, errors: ok, state: ok, persist: partial, note: "activityQueues intentionally ephemeral, see Notes. Now also sweeps expired executions first (gopherstack-7gse), defense-in-depth consistency -- see Notes: timeout enforcement"}
  PollForDecisionTask: {wire: ok (fixed), errors: ok, state: ok, persist: partial, note: "decisionQueues intentionally ephemeral, see Notes. Now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask. 2026-08-21 (gopherstack-r80d batch 17): required StartedEventId was always 0 (no struct field anywhere tracked a real DecisionTaskStarted event) -- now a real DecisionTaskStarted event is recorded and its ID threaded through, see Notes"}
  RecordActivityTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskCanceled: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskCompleted: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now propagates ChildWorkflowExecutionCompleted to the parent execution, see Notes. Also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskFailed: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondDecisionTaskCompleted: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ContinueAsNewWorkflowExecution/StartChildWorkflowExecution/SignalExternalWorkflowExecution/RequestCancelExternalWorkflowExecution now perform real state mutation instead of recording an empty *Initiated event; decisionTaskCompletedEventId was missing from every decision-derived history event; StartTimer/CancelTimer now validate timerId state; see Notes. Also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask. 2026-08-21 (gopherstack-r80d batch 17): DecisionTaskCompletedEventAttributes' required scheduledEventId/startedEventId had no struct field at all (dropped on every decision task completion); TimerCanceledEventAttributes' required startedEventId was likewise dropped on every CancelTimer decision; ChildWorkflowExecutionTimedOutEventAttributes' required timeoutType was dropped on every child-execution timeout propagation. All three fixed, see Notes"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  timeout_enforcement: {status: partial, note: "gopherstack-7gse, 2026-08-10: EXECUTION_START_TO_CLOSE is now enforced -- see Notes for the full mechanism and citations. TaskStartToCloseTimeout (decision tasks) and every activity-task timeout kind (ScheduleToStart/ScheduleToClose/StartToClose/Heartbeat) remain accepted-and-ignored, same as before this pass -- see gaps. Do not read 'partial' as 'mostly enforced': exactly one timeout kind, out of five SWF defines, is enforced."}
  decision_processing: {status: ok, note: "all 12 SWF decision types now perform real state mutation and carry decisionTaskCompletedEventId + full wire attrs -- see Notes. Dispatch table decomposed into decisionHandlers() (decision_tasks.go) + decision_orchestration.go, removing the historical cyclop/funlen nolint on processDecisionLocked."}
  multi_run_history: {status: ok, note: "FIXED (gopherstack-jsi8, 2026-08-07): executions/history were keyed by domain+workflowId alone, so a second/later run of the same workflowId silently overwrote the first run's row and history -- confirmed against the real aws-sdk-go-v2/service/swf@v1.37.4 types.WorkflowExecution, where RunId is a required member alongside WorkflowId (not an optional disambiguator), and DescribeWorkflowExecutionInput/GetWorkflowExecutionHistoryInput's Execution field requires both. Re-keyed executions (store.Table) and history (map) to domain+workflowId+runId (workflowExecutionKeyFn/executionKey, store.go/store_setup.go); added an executionsByWorkflow store.Index grouping every run (open or closed) under domain+workflowId so the currently-open run (real AWS guarantees at most one) can still be found without a full-domain scan. New resolveExecutionLocked/openExecutionLocked helpers centralize 'find a run, optionally pinned to a runId' -- a non-empty runId is an exact lookup (works for ANY run, open or long-closed); an empty runId first tries the open run, then falls back to the most-recently-started run for that workflowId (a deliberate leniency beyond real AWS, which would error UnknownResource with no open run -- kept so callers that don't track runId, including this backend's own internal cross-execution decision handlers, keep working exactly as before for the still-common single-run-per-workflowId case). DescribeWorkflowExecution/GetWorkflowExecutionHistory both gained a runID parameter (the wire's Execution.RunId was already being parsed but silently discarded -- see their ops rows above); Terminate/RequestCancel/SignalWorkflowExecution's pre-existing runID parameters now actually disambiguate instead of being checked against a single shared row. Every appendHistoryEventLocked/enqueueDecisionTaskLocked call site across activity_tasks.go/decision_tasks.go/decision_orchestration.go/signals.go/workflow_executions.go now threads the specific run's runID through (~25 call sites) rather than resolving 'the' execution for a workflowId. propagateChildClosureLocked now does a direct domain+parentWorkflowId+parentRunId lookup instead of an ambiguous domain+workflowId one, so a parent that has since continued-as-new (a newer run under the same workflowId) no longer incorrectly receives a child-closure event meant for the run that actually started that child. Also fixed the closely-related 'LRU-eviction ghost queue rows' gopherstack-jsi8 finding: registerExecutionOrderLocked's eviction (at the pre-existing maxWorkflowExecutions=10_000 cap) previously deleted only the execution row and history, leaving any still-pending decisionQueues/activityQueues entry or activeDecisionTasks/activeActivityTasks record for that run behind as a ghost referencing data that no longer existed; new evictExecutionLocked purges all four. New tests: TestMultiRunHistory (multirun_test.go, 6 cases covering explicit-old-run/explicit-new-run/empty-run-id resolution, history isolation between runs, the already-open-run rejection, and the falls-back-to-most-recent behavior) and TestLRUEvictionPurgesPendingDecisionTasks (creates 10_000 executions, confirms the evicted run's pending decision task is gone from its task list and the evicted execution is truly not found -- verified this test fails without the evictExecutionLocked fix). One existing test (TestRespondDecisionTaskCompleted_ContinueAsNew) asserted the old run's WorkflowExecutionContinuedAsNew event appeared in the NEW run's history, which was only true because both runs shared one history blob under the old keying -- corrected to check each run's own history/DescribeWorkflowExecution independently, which is the actual point of this fix. Interface changes: Backend.DescribeWorkflowExecution and Backend.GetWorkflowExecutionHistory each gained a runID parameter; no external callers found via full-repo grep (cloudformation/dashboard/cli.go reference services/swf but not these methods directly)."}
gaps:
  - "activityQueues/decisionQueues (FIFO pending-task lists) are intentionally NOT part of backendSnapshot (pre-existing, documented design choice in store.go/persistence.go -- order-sensitive plain maps). A restart loses in-flight pending tasks that haven't been polled yet, while their corresponding history events and active-task records DO survive. Not fixed this pass (would require reworking backendSnapshot's shape); flagged for awareness. (bd: TODO -- file follow-up)"
  - "Child-policy cascade is now implemented for TerminateWorkflowExecution (2026-07-31 pass, see Notes): TERMINATE recursively terminates open children (cascading each child's own stored ChildPolicy to grandchildren in turn), REQUEST_CANCEL records WorkflowExecutionCancelRequested (cause CHILD_POLICY_APPLIED) on each open child and gives it a fresh decision task, and ABANDON is correctly a no-op. That pass surfaced -- but explicitly left out of scope -- that real AWS's *other* child-policy trigger, an execution auto-closing via WorkflowExecutionTimedOut, was unreachable because this backend had no timeout-enforcement mechanism at all. THIS PASS (gopherstack-7gse, 2026-08-10) closes that: EXECUTION_START_TO_CLOSE is now enforced and reuses the exact same applyChildPolicyLocked cascade timeoutExecutionLocked calls -- see Notes: timeout enforcement. Both of real SWF's child-policy triggers are reachable now."
  - "TaskStartToCloseTimeout (the decision task timeout) and every activity-task timeout kind SWF defines -- ScheduleToStartTimeout, ScheduleToCloseTimeout, StartToCloseTimeout, HeartbeatTimeout -- remain UNENFORCED: accepted on RegisterWorkflowType/RegisterActivityType/ScheduleActivityTask, stored, echoed back on Describe, and never acted on. This is deliberate scope discipline (gopherstack-7gse), not an oversight: real AWS's child policy is invoked only by TerminateWorkflowExecution or a WorkflowExecutionTimedOut EXECUTION_START_TO_CLOSE close (see Notes), never by a decision-task or activity-task timeout, so none of these five feed the mechanism this pass exists to fix. Implementing them needs a materially different shape too -- DecisionTaskTimedOut/ActivityTaskTimedOut close the *task*, not the execution, and a real decision-task timeout must re-enqueue a fresh decision task rather than close anything. Do not read the sweep added this pass as covering these; it only ever inspects WorkflowExecution.ExecutionStartToCloseTimeout. (bd: TODO -- file follow-up if these are ever wanted)"
  - "StartTimer/CancelTimer decisions (TimerStarted/TimerCanceled events, WorkflowExecution.OpenTimerIDs) remain purely decision-driven with no autonomous TimerFired -- see leaks below. This predates gopherstack-7gse and is a different mechanism from ExecutionStartToCloseTimeout (a StartTimer decision's StartToFireTimeout is decider-chosen and per-timer, not the execution-wide deadline set at StartWorkflowExecution/RegisterWorkflowType), so this pass's sweep does not touch it. Left as a separate, pre-existing, still-undocumented-until-now gap."
  - "Complete/Fail/Cancel workflow-closing decisions do NOT cascade child policy onto their own open children, and this is correct, not a gap: real AWS's child policy is only ever invoked when a workflow execution is terminated (explicitly, via TerminateWorkflowExecution) or times out -- never on a normal Complete/Fail/Cancel close, where child executions are simply independent and keep running. Parent-closure IS still propagated to already-open children as history events in all four cases (ChildWorkflowExecutionCompleted/Failed/Canceled/Terminated, see Notes), so deciders learn of parent closure either way."
  - "ScheduleLambdaFunction decision type (Lambda activity tasks) is not implemented -- consistent with the pre-existing openLambdaFunctions deferral below; SWF Lambda task support as a whole is out of scope for this service."
deferred:
  - "DescribeWorkflowExecution's openCounts.openLambdaFunctions (always 0) and the ScheduleLambdaFunction decision type -- SWF Lambda task support is out of scope for a JSON-wire-shape/state-mutation audit."
leaks: {status: clean, note: "no goroutines/timers spawned by this service, including the new cross-execution decision handlers in decision_orchestration.go and the gopherstack-7gse timeout sweep (timeout_sweep.go). Every SWF timer/timeout mechanism here is either purely decision-driven state with no autonomous firing (OpenTimerIDs on WorkflowExecution, mutated only by StartTimer/CancelTimer decisions -- see gaps) or, for EXECUTION_START_TO_CLOSE only, lazily swept: sweepTimedOutExecutionsLocked takes now as a parameter (never calls time.Now() itself) and is invoked with the real clock at the top of every backend op that reads or mutates execution state (Describe/GetHistory/List/Count/Poll/Respond/Terminate/RequestCancel/Signal/Start -- see the ops table), so a timed-out execution becomes visible on the next such call rather than at a real background tick. This keeps the pre-existing no-goroutine design intact and makes the sweep trivially unit-testable (pass an arbitrary now, no sleeping/synctest needed). All state lives in InMemoryBackend maps/store.Tables guarded by lockmetrics.RWMutex; every lock path uses defer-release. Several previously-RLock-only ops (DescribeWorkflowExecution, GetWorkflowExecutionHistory, ListOpen/ClosedWorkflowExecutions, CountOpen/ClosedWorkflowExecutions, RecordActivityTaskHeartbeat) were upgraded to Lock so the sweep -- which mutates state -- can run under them; this is a coarse-lock design (see pkgs-catalog.md), so the change is a straightforward RLock->Lock swap, not a new locking scheme."}
---

## Notes

Protocol: SWF is **awsjson1.0** (`application/x-amz-json-1.0`, `X-Amz-Target:
SimpleWorkflowService.<Op>`) -- confirmed against the real
`aws-sdk-go-v2/service/swf` serializers (every op sets
`Content-Type: application/x-amz-json-1.0`). This is easy to mis-key as
awsjson1.1 (the more common AWS JSON protocol) since SWF's dispatch shape
looks identical otherwise.

### Timeout enforcement (2026-08-10, gopherstack-7gse)

**Real semantics, established before writing any code.** When a workflow
execution's `ExecutionStartToCloseTimeout` elapses, real SWF closes the
execution by recording a `WorkflowExecutionTimedOut` history event and
setting `CloseStatus` to `TIMED_OUT`. Confirmed against
`aws-sdk-go-v2/service/swf@v1.37.4`:

- `types.WorkflowExecutionTimedOutEventAttributes`
  (`types/types.go:3440-3463`) has exactly two members, both required:
  `ChildPolicy` and `TimeoutType`. `types.WorkflowExecutionTimeoutType`
  (`types/enums.go:689-694`) has exactly one defined value,
  `START_TO_CLOSE` -- there is no other timeout type this event can carry.
- `types.CloseStatus` (`types/enums.go:88-98`) defines
  `CloseStatusTimedOut = "TIMED_OUT"` alongside `COMPLETED`/`FAILED`/
  `CANCELED`/`TERMINATED`/`CONTINUED_AS_NEW` -- the same enum this backend's
  pre-existing `statusTimedOut` constant (`models.go`) already targeted, it
  was simply never assigned anywhere.
- The deserializer's field-name switch
  (`deserializers.go:9964-10010`,
  `awsAwsjson10_deserializeDocumentWorkflowExecutionTimedOutEventAttributes`)
  confirms the wire keys are `childPolicy` and `timeoutType` under the
  `workflowExecutionTimedOutEventAttributes` attribute block, matching this
  service's existing `eventAttrKey`/attribute-map convention exactly.

The `WorkflowExecutionTimedOutEventAttributes.ChildPolicy` member existing at
all is the confirmation that matters here: real SWF invokes the child policy
on exactly two events, `TerminateWorkflowExecution` and an execution timing
out (gopherstack-jsi8/2026-07-31's child-policy cascade pass established
this and documented the second trigger as unreachable -- see the gaps entry
above). `timeoutExecutionLocked` (`timeout_sweep.go`) therefore mirrors
`terminateExecutionLocked` (`workflow_executions.go`) almost exactly:
close the execution, append the closing history event, notify the parent
via the existing `propagateChildClosureLocked` (with `ChildWorkflowExecutionTimedOut`,
which real SWF also defines -- `types.ChildWorkflowExecutionTimedOutEventAttributes`,
`types/types.go:608`), then cascade `exec.ChildPolicy` onto open children via
the same `applyChildPolicyLocked` terminate already uses. Both of real SWF's
child-policy triggers are reachable as of this pass.

**Scope decision: EXECUTION_START_TO_CLOSE only, not the other four timeout
kinds SWF defines.** The issue (gopherstack-7gse) sketched full support as a
sweeper over open executions *and pending tasks*, plus `TimerStarted`/
`TimerFired` history events and their decision types -- deliberately not
attempted this pass. `TaskStartToCloseTimeout` (decision tasks) and every
activity-task timeout (`ScheduleToStartTimeout`/`ScheduleToCloseTimeout`/
`StartToCloseTimeout`/`HeartbeatTimeout`) remain accepted-and-ignored, same
as before. This is not an oversight: none of those four feed the
child-policy mechanism this pass exists to fix (only an execution closing
does), and they need a materially different shape --
`DecisionTaskTimedOut`/`ActivityTaskTimedOut` close the *task*, not the
execution, and a real decision-task timeout re-enqueues a fresh decision
task rather than closing anything. See the `gaps` entries above for the
precise, unambiguous list of what remains unenforced -- **do not read
"timeouts work now" as covering anything beyond EXECUTION_START_TO_CLOSE.**

**Design: lazy sweep, no background goroutine, clock as a parameter.** This
service has never spawned a goroutine (see `leaks` above), and a background
ticker would have been a materially larger, riskier change than this issue's
scope warrants. Instead, `sweepTimedOutExecutionsLocked(now time.Time)`
(`timeout_sweep.go`) is a synchronous scan over `b.executions.All()`,
closing any `RUNNING` execution whose `StartTimestamp + ExecutionStartToCloseTimeout`
deadline is at or before `now`. It takes `now` as a parameter rather than
calling `time.Now()` internally, and every call site
(`Describe`/`GetHistory`/`List`/`Count`/`Poll`/`Respond`/`Terminate`/
`RequestCancel`/`Signal`/`Start`) passes the real clock -- so a timed-out
execution becomes visible on the next such call, not at the real
wall-clock instant it expired. This makes the sweep itself trivially
testable with a fabricated `now` (no sleeping, no `testing/synctest`
needed -- see `timeout_sweep_whitebox_test.go`'s
`TestSweepTimedOutExecutionsLocked_Evaluation`), and lets an end-to-end test
prove the wiring by backdating `StartTimestamp` into the real past and
calling a public method with no fabricated clock at all
(`TestDescribeWorkflowExecution_SweepsOnRead`). Several ops that only ever
needed `RLock` before now take `Lock`, since the sweep can mutate state;
this is a plain `RLock`->`Lock` swap under the service's existing single
coarse lock, not a new locking scheme (see `pkgs-catalog.md`'s locking
rule).

No snapshot version bump: `Status`/`CloseStatus`/`CloseTimestamp`/
`ChildPolicy` are pre-existing, already-`omitempty` JSON fields on
`WorkflowExecution` that already round-trip through the "clean"
`executions` `store.Table` (see `store.go`). `TIMED_OUT` is simply a new
*value* passing through those same fields, not a new field --
`TestSnapshotRestore_TimedOutExecution` pins this.

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

### 2026-08-21: three required output members dropped entirely (gopherstack-r80d batch 17)

An AST-style walk of `swf@v1.37.4`'s 3,606-line `types/types.go` (brace-depth
block splitting, not a grep window -- the naive line-based version of this
walk silently skipped `ChildWorkflowExecutionTerminatedEventAttributes`
entirely; re-verified with a character-level brace matcher before trusting
the result) found 80 of 88 structs carry at least one required member --
almost entirely the `*EventAttributes`/`*DecisionAttributes` family, the
same "polymorphic `HistoryEvent` sub-object" undercount shape stepfunctions'
batch 10 named (each event type's own required members are invisible to
`cmd/requiredoutputfields`'s per-op scan, since every op's own
`<Op>Output` struct is mostly flat). Read every event type this backend
actually emits (`appendHistoryEventLocked` call sites across
`activity_tasks.go`, `decision_tasks.go`, `decision_orchestration.go`,
`workflow_executions.go`, `signals.go`, `timeout_sweep.go`) against its
matching struct's required members.

Three bugs, all in event types emitted on a real, common (not edge-case)
path:

1. **`DecisionTaskCompletedEventAttributes.scheduledEventId`/`.startedEventId`**
   (types.go:~2000, both `This member is required.`) had no struct field
   anywhere -- this backend never recorded a `DecisionTaskScheduled` or
   `DecisionTaskStarted` history event at all, so every
   `RespondDecisionTaskCompleted` call's `DecisionTaskCompleted` event
   carried only `executionContext`. This also meant
   `PollForDecisionTaskOutput.StartedEventId` (api_op_PollForDecisionTask.go,
   required) stayed at its Go zero value (0) forever -- present on the wire
   (no `omitempty` tag) but a value no real event ID can take (real AWS IDs
   start at 1). Fixed by mirroring the already-correct
   `ActivityTaskScheduled`/`ActivityTaskStarted`/`ActivityTaskCompleted`
   chain (`activity_tasks.go`): `enqueueDecisionTaskLocked` (store.go) now
   records `DecisionTaskScheduled` (required `taskList`) and threads its
   event ID onto the queued `DecisionTask`; `PollForDecisionTask` now
   records `DecisionTaskStarted` (required `scheduledEventId`) and threads
   both IDs onto `activeDecisionTaskRecord`; `RespondDecisionTaskCompleted`
   reads them back onto `DecisionTaskCompleted`. This is the single most
   common event in SWF's entire history stream (recorded on every decision
   task response), not an edge case.
2. **`ChildWorkflowExecutionTimedOutEventAttributes.timeoutType`**
   (types.go:625-628, required) -- `propagateChildClosureLocked`'s base attrs
   (`initiatedEventId`/`startedEventId`/`workflowExecution`/`workflowType`)
   cover every other Child* closure event's required set, but
   `timeoutExecutionLocked` (timeout_sweep.go) called it with `extra: nil`
   for the TimedOut case specifically, silently dropping the one member that
   family alone requires beyond the base four. Fixed by passing
   `{timeoutType: timeoutTypeStartToClose}` as `extra`, reusing the same
   constant already used two lines above for the parent's own
   `WorkflowExecutionTimedOut` event.
   `ChildWorkflowExecutionTerminatedEventAttributes` (types.go:577-605,
   passed `nil` the same way from `terminateExecutionLocked`) needs no
   `extra` at all -- its required set is exactly the base four -- so that
   call site was correctly left alone.
3. **`TimerCanceledEventAttributes.startedEventId`** (types.go, required)
   -- `handleCancelTimerDecision`'s `TimerCanceled` event carried only
   `decisionTaskCompletedEventId`/`timerId`; nothing on `WorkflowExecution`
   tracked which `TimerStarted` event a given open `timerId` referred to.
   Fixed by adding `WorkflowExecution.TimerStartedEventIDs
   map[string]int64`, populated in `handleStartTimerDecision` (whose own
   `appendHistoryEventLocked` return value was previously discarded) and
   consumed-then-deleted in `handleCancelTimerDecision`.

All three proven via real `aws-sdk-go-v2/service/swf` client round trips
(`wire_output_required_r80d_test.go`), hand-reverted (`store.go`,
`models.go`, `decision_tasks.go`, `timeout_sweep.go` together) /
confirmed-failing / restored, md5sum byte-identical. `go test
./services/swf/...` passed unchanged both before and after (no existing
test hard-coded an event-index/count that the two new
`DecisionTaskScheduled`/`DecisionTaskStarted` events per cycle would have
shifted), so no existing test needed correction.

**Named, not fixed -- structurally unreachable via any real client:**
`DecisionTaskScheduledEventAttributes.taskList` and
`DecisionTaskStartedEventAttributes.scheduledEventId` are satisfied by the
fix above and not separately counted. `TimerFiredEventAttributes` (required
`startedEventId`/`timerId`) is never emitted at all -- this backend has no
autonomous timer-firing mechanism (see the pre-existing gaps entry on
`OpenTimerIDs`/no `TimerFired`), so the type can never be violated; a
missing-feature gap (already documented), not a dropped-required-field bug.
Likewise never emitted, for the same reason (this backend's decision-task
lifecycle has no separate scheduled/started/timed-out task states beyond
what's now fixed above, and Lambda tasks are out of scope, see the
pre-existing gaps entry): `DecisionTaskTimedOutEventAttributes`,
`LambdaFunctionScheduled/Started/Completed/Failed/TimedOutEventAttributes`,
`ScheduleActivityTaskFailedEventAttributes`,
`RequestCancelActivityTaskFailedEventAttributes`,
`RecordMarkerFailedEventAttributes`,
`CompleteWorkflowExecutionFailedEventAttributes`,
`FailWorkflowExecutionFailedEventAttributes`. `WorkflowType`/`ActivityType`'s
`CreationDate` (required on `WorkflowTypeInfo`/`ActivityTypeInfo`, tagged
`omitempty` in `models.go`) is unreachable via any real client path --
`RegisterWorkflowType`/`RegisterActivityType` unconditionally stamp it at
registration time, and the only code path that skips it
(`AddWorkflowTypeInternal`) is a Go-only test-seed helper no real SDK client
can reach, the same class as `EnqueueDecisionTaskInternal` (batch 17's own
scope note applies equally here). Everything else read end to end came back
clean: `ActivityTaskScheduled/Started/Completed/Failed/Canceled`,
`ActivityTaskCancelRequested`, `StartTimerFailed`/`TimerStarted`/
`CancelTimerFailed`, `MarkerRecorded`, the four cross-execution `*Initiated`/
`*Failed` pairs (`StartChildWorkflowExecution`/
`SignalExternalWorkflowExecution`/`RequestCancelExternalWorkflowExecution`),
`ChildWorkflowExecutionStarted/Completed/Failed/Canceled`,
`WorkflowExecutionStarted/Completed/Failed/Canceled/Signaled/
CancelRequested/Terminated/ContinuedAsNew`, and
`DescribeDomain`/`DescribeWorkflowType`/`DescribeActivityType`/
`DescribeWorkflowExecution`'s `Configuration`/`TypeInfo`/`ExecutionInfo`/
`OpenCounts` wrapper members.

`fieldalignment -fix` was run on `models.go` after adding
`TimerStartedEventIDs`/`ScheduledEventID` (govet's `fieldalignment` flagged
the reordered struct); reordering only, no field renamed or retyped, `git
diff` confirmed.

`last_audit_commit: pending` was already the value from the prior pass
(2026-08-10) before this batch touched the file -- left as-is per this
campaign's standing rule (never write a fresh `pending`), not introduced
here.
