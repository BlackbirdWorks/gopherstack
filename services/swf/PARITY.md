---
service: swf
sdk_module: aws-sdk-go-v2/service/swf@v1.37.4   # verified this pass; go.mod pin, was stale at v1.33.14
last_audit_commit: 49cff86c4                     # HEAD at close of the 2026-09-18 ledger burn-down pass
last_audit_date: 2026-09-19
overall: A            # genuine fixes found this pass, plus a wrapper-key/nested-shape sweep
                       # (2026-08-20) that found and fixed 2 more real bugs; see Notes.
                       # 2026-08-29: one more genuine bug found and fixed (ListOpen/
                       # ListClosedWorkflowExecutions ReverseOrder + default sort order,
                       # see Notes) in the wrapper-key/silent-drop sweep (bd gopherstack-6flj/
                       # 21my). 2026-09-18 ledger burn-down: fixed StartTimer's
                       # StartToFireTimeout (TimerFired now lazily swept, see Notes) and
                       # removed 6 stale items_still_open entries that were already fixed
                       # at HEAD (DecisionTaskScheduled/Started recording, TimerCanceled's
                       # StartedEventId, DeprecationDate, and 3 non-gap audit-confirmation
                       # notes) -- confirmed by reading the code, not by cmd/staleclaims
                       # (which did not flag any of these; see Notes).
ops:
  RegisterDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomains: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-09-18 (reqfielddiff tier-1): ReverseOrder (documented default: ascending) was parsed nowhere -- always sorted ascending regardless of the request. Re-verified this pass (over-wide-response sweep, 2026-09-19): domainInfoOutput's four members match types.DomainInfo exactly -- no leaks, no gaps."}
  DeprecateDomain: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not cascading DEPRECATED onto the domain's registered workflow/activity types, see Notes"}
  UndeprecateDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkflowTypes: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "2026-08-23: DeprecationDate now populated (see gaps). FIXED 2026-09-18 (reqfielddiff tier-1): ReverseOrder was parsed nowhere -- same fix as ListDomains. Re-verified this pass (over-wide-response sweep, 2026-09-19): workflowTypeInfoOutput's five members match types.WorkflowTypeInfo exactly -- no leaks, no gaps."}
  DescribeWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-23: DeprecationDate now populated (see gaps)"}
  DeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkflowType: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListActivityTypes: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "2026-08-23: DeprecationDate now populated (see gaps). FIXED 2026-09-18 (reqfielddiff tier-1): ReverseOrder was parsed nowhere -- same fix as ListDomains. Re-verified this pass (over-wide-response sweep, 2026-09-19): activityTypeInfoOutput's five members match types.ActivityTypeInfo exactly -- no leaks, no gaps."}
  DescribeActivityType: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-23: DeprecationDate now populated (see gaps)"}
  DeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  UndeprecateActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActivityType: {wire: ok, errors: ok, state: ok, persist: ok}
  StartWorkflowExecution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now sweeps expired executions first so a workflowId whose prior run has timed out (but not yet been observed) can be restarted instead of falsely hitting WorkflowExecutionAlreadyStartedFault -- see Notes: timeout enforcement (gopherstack-7gse)"}
  TerminateWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "childPolicy was parsed off the wire into handleTerminateWorkflowExecutionInput and then silently discarded -- the backend call took no such parameter, so a client's per-call override never applied and only the policy stored at StartWorkflowExecution time governed. Now threaded through and, combined with a new TERMINATE/REQUEST_CANCEL child-policy cascade onto open children, actually takes effect; also propagates ChildWorkflowExecutionTerminated to the parent execution, see Notes. ADDITIONALLY (gopherstack-7gse, 2026-08-10): now sweeps expired executions first, same as StartWorkflowExecution above -- see Notes: timeout enforcement"}
  DescribeWorkflowExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "openCounts.openTimers/openChildWorkflowExecutions were hardcoded 0; executionInfo.parent was entirely missing; ADDITIONALLY (gopherstack-jsi8, 2026-08-07): the wire's Execution.RunId (a real, required field per types.WorkflowExecution) was parsed off the request and then silently discarded -- the Go-level backend method took no runID parameter at all, so a client asking for a specific historical run always got whatever run currently occupied the domain+workflowId slot instead. Now threaded through end to end; see Notes. ADDITIONALLY (gopherstack-7gse, 2026-08-10): now sweeps expired executions (EXECUTION_START_TO_CLOSE only) before resolving, so a RUNNING execution whose timeout has elapsed reads back as TIMED_OUT instead of staying RUNNING forever -- see Notes: timeout enforcement"}
  GetWorkflowExecutionHistory: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "gopherstack-jsi8, 2026-08-07: same Execution.RunId-discarded bug as DescribeWorkflowExecution above, same fix -- see Notes. Also sweeps expired executions first, same as DescribeWorkflowExecution (gopherstack-7gse)"}
  ListOpenWorkflowExecutions: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution. Also sweeps expired executions first (gopherstack-7gse) so a timed-out execution moves from the open list to the closed list on the next call instead of staying open forever -- see Notes: timeout enforcement. 2026-08-29 wrapper-key/wire sweep: ReverseOrder (real, per-op input member) was dropped entirely and results had no default sort order at all (arbitrary index-insertion order) instead of real AWS's documented descending-start-time default -- see Notes. Re-verified this pass (over-wide-response sweep, 2026-09-19): executionInfoOutput's nine members match types.WorkflowExecutionInfo exactly -- no leaks, no gaps."}
  ListClosedWorkflowExecutions: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "executionInfo.parent was missing, same fix as DescribeWorkflowExecution. Also sweeps expired executions first (gopherstack-7gse), same effect as ListOpenWorkflowExecutions above. 2026-08-29 wrapper-key/wire sweep: same ReverseOrder/default-order bug as ListOpenWorkflowExecutions, ordered by close time when closeTimeFilter selects the results, by start time when startTimeFilter does -- see Notes. Re-verified this pass (over-wide-response sweep, 2026-09-19): shares executionInfoOutput with ListOpenWorkflowExecutions, already confirmed exact above (real AWS also reuses one WorkflowExecutionInfo type for both ops)."}
  RequestCancelWorkflowExecution: {wire: fixed, errors: ok, state: ok, persist: ok, note: "2026-08-20 wire-parity sweep: WorkflowExecutionCancelRequestedEventAttributes.Cause was stamped OPERATOR_INITIATED for a direct call, a value the real WorkflowExecutionCancelRequestedCause enum does not define at all (its only value is CHILD_POLICY_APPLIED) -- see Notes. Also sweeps expired executions first (gopherstack-7gse), defense-in-depth consistency with the other execution-touching ops -- see Notes: timeout enforcement"}
  SignalWorkflowExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as RequestCancelWorkflowExecution"}
  CountOpenWorkflowExecutions: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "now sweeps expired executions first (gopherstack-7gse) so a timed-out execution is no longer counted as open -- see Notes: timeout enforcement"}
  CountClosedWorkflowExecutions: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "now sweeps expired executions first (gopherstack-7gse), same as CountOpenWorkflowExecutions above"}
  CountPendingActivityTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountPendingDecisionTasks: {wire: ok, errors: ok, state: ok, persist: n/a}
  PollForActivityTask: {wire: fixed, errors: ok, state: ok, persist: partial, note: "activityQueues intentionally ephemeral, see Notes. Now also sweeps expired executions first (gopherstack-7gse), defense-in-depth consistency -- see Notes: timeout enforcement. FIXED (2026-09-24, requiredoutputfields census): pollForActivityTaskOutput.TaskToken carried json:\"taskToken,omitempty\", so the idle case (no task available, TaskToken==\"\") dropped the required member entirely instead of emitting it as an empty string -- the real API's own doc comment defines exactly that empty-string TaskToken as how a client detects no task is available (api_op_PollForActivityTask.go), so a real client following the documented contract decoded TaskToken as nil instead of a pointer to \"\". Sibling PollForDecisionTask's decisionTaskOutput.TaskToken already had no omitempty; PollForActivityTask was the outlier. See TestPollForActivityTask_TaskTokenRequiredMember (SDK-client-proven, both idle and task-available cases)."}
  PollForDecisionTask: {wire: fixed, errors: ok, state: fixed, persist: partial, note: "decisionQueues intentionally ephemeral, see Notes. Now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask. 2026-08-21 (gopherstack-r80d batch 17): required StartedEventId was always 0 (no struct field anywhere tracked a real DecisionTaskStarted event) -- now a real DecisionTaskStarted event is recorded and its ID threaded through, see Notes. FIXED 2026-09-18 (reqfielddiff tier-1): StartAtPreviousStartedEvent was parsed nowhere and PreviousStartedEventId was always 0 -- now the previous DecisionTaskStarted event ID is tracked and returned, and StartAtPreviousStartedEvent trims Events to that event onward when set."}
  RecordActivityTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskCanceled: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskCompleted: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now propagates ChildWorkflowExecutionCompleted to the parent execution, see Notes. Also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondActivityTaskFailed: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask"}
  RespondDecisionTaskCompleted: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ContinueAsNewWorkflowExecution/StartChildWorkflowExecution/SignalExternalWorkflowExecution/RequestCancelExternalWorkflowExecution now perform real state mutation instead of recording an empty *Initiated event; decisionTaskCompletedEventId was missing from every decision-derived history event; StartTimer/CancelTimer now validate timerId state; see Notes. Also sweeps expired executions first (gopherstack-7gse), same as PollForActivityTask. 2026-08-21 (gopherstack-r80d batch 17): DecisionTaskCompletedEventAttributes' required scheduledEventId/startedEventId had no struct field at all (dropped on every decision task completion); TimerCanceledEventAttributes' required startedEventId was likewise dropped on every CancelTimer decision; ChildWorkflowExecutionTimedOutEventAttributes' required timeoutType was dropped on every child-execution timeout propagation. All three fixed, see Notes"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  timeout_enforcement: {status: partial, note: "gopherstack-7gse, 2026-08-10: EXECUTION_START_TO_CLOSE is now enforced -- see Notes for the full mechanism and citations. UPDATE (2026-08-20 wire-parity sweep): ChildWorkflowExecutionTimedOutEventAttributes.TimeoutType (required) was entirely missing from the parent-notification event a child's own timeout produces -- fixed by passing timeoutType through as extra, same as Completed/Failed/Canceled. FIXED 2026-09-18: StartTimer's StartToFireTimeout is now enforced the same lazy-sweep way -- see the TimerFired Notes entry. TaskStartToCloseTimeout (decision tasks) and every activity-task timeout kind (ScheduleToStart/ScheduleToClose/StartToClose/Heartbeat) remain accepted-and-ignored -- see items_still_open. Do not read 'partial' as 'mostly enforced': two timeout kinds (execution, timer), out of six SWF defines total, are enforced."}
  decision_processing: {status: ok, note: "all 12 SWF decision types now perform real state mutation and carry decisionTaskCompletedEventId + full wire attrs -- see Notes. Dispatch table decomposed into decisionHandlers() (decision_tasks.go) + decision_orchestration.go, removing the historical cyclop/funlen nolint on processDecisionLocked."}
  multi_run_history: {status: ok, note: "FIXED (gopherstack-jsi8, 2026-08-07): executions/history were keyed by domain+workflowId alone, so a second/later run of the same workflowId silently overwrote the first run's row and history -- confirmed against the real aws-sdk-go-v2/service/swf@v1.37.4 types.WorkflowExecution, where RunId is a required member alongside WorkflowId (not an optional disambiguator), and DescribeWorkflowExecutionInput/GetWorkflowExecutionHistoryInput's Execution field requires both. Re-keyed executions (store.Table) and history (map) to domain+workflowId+runId (workflowExecutionKeyFn/executionKey, store.go/store_setup.go); added an executionsByWorkflow store.Index grouping every run (open or closed) under domain+workflowId so the currently-open run (real AWS guarantees at most one) can still be found without a full-domain scan. New resolveExecutionLocked/openExecutionLocked helpers centralize 'find a run, optionally pinned to a runId' -- a non-empty runId is an exact lookup (works for ANY run, open or long-closed); an empty runId first tries the open run, then falls back to the most-recently-started run for that workflowId (a deliberate leniency beyond real AWS, which would error UnknownResource with no open run -- kept so callers that don't track runId, including this backend's own internal cross-execution decision handlers, keep working exactly as before for the still-common single-run-per-workflowId case). DescribeWorkflowExecution/GetWorkflowExecutionHistory both gained a runID parameter (the wire's Execution.RunId was already being parsed but silently discarded -- see their ops rows above); Terminate/RequestCancel/SignalWorkflowExecution's pre-existing runID parameters now actually disambiguate instead of being checked against a single shared row. Every appendHistoryEventLocked/enqueueDecisionTaskLocked call site across activity_tasks.go/decision_tasks.go/decision_orchestration.go/signals.go/workflow_executions.go now threads the specific run's runID through (~25 call sites) rather than resolving 'the' execution for a workflowId. propagateChildClosureLocked now does a direct domain+parentWorkflowId+parentRunId lookup instead of an ambiguous domain+workflowId one, so a parent that has since continued-as-new (a newer run under the same workflowId) no longer incorrectly receives a child-closure event meant for the run that actually started that child. Also fixed the closely-related 'LRU-eviction ghost queue rows' gopherstack-jsi8 finding: registerExecutionOrderLocked's eviction (at the pre-existing maxWorkflowExecutions=10_000 cap) previously deleted only the execution row and history, leaving any still-pending decisionQueues/activityQueues entry or activeDecisionTasks/activeActivityTasks record for that run behind as a ghost referencing data that no longer existed; new evictExecutionLocked purges all four. New tests: TestMultiRunHistory (multirun_test.go, 6 cases covering explicit-old-run/explicit-new-run/empty-run-id resolution, history isolation between runs, the already-open-run rejection, and the falls-back-to-most-recent behavior) and TestLRUEvictionPurgesPendingDecisionTasks (creates 10_000 executions, confirms the evicted run's pending decision task is gone from its task list and the evicted execution is truly not found -- verified this test fails without the evictExecutionLocked fix). One existing test (TestRespondDecisionTaskCompleted_ContinueAsNew) asserted the old run's WorkflowExecutionContinuedAsNew event appeared in the NEW run's history, which was only true because both runs shared one history blob under the old keying -- corrected to check each run's own history/DescribeWorkflowExecution independently, which is the actual point of this fix. Interface changes: Backend.DescribeWorkflowExecution and Backend.GetWorkflowExecutionHistory each gained a runID parameter; no external callers found via full-repo grep (cloudformation/dashboard/cli.go reference services/swf but not these methods directly)."}
gaps: []
items_still_open:
  - "(2026-09-18) TaskStartToCloseTimeout and every activity-task timeout kind (ScheduleToStart/ScheduleToClose/StartToClose/Heartbeat) remain unenforced -- accepted, stored, echoed on Describe, never acted on. Deliberate scope limit: DecisionTaskTimedOut/ActivityTaskTimedOut close the *task* and re-enqueue, a materially different mechanism than the execution-closing sweep this service already has for EXECUTION_START_TO_CLOSE and TimerFired. (bd: TODO)"
  - "(2026-09-18) ScheduleLambdaFunction decision type and openLambdaFunctions are not implemented -- SWF Lambda task support is out of scope for this service (no Lambda cross-service wiring exists)."
  - "(2026-09-18) ListActivityTypes/ListWorkflowTypes/ListDomains's RegistrationStatus is documented required but an empty value is accepted as no-filter server-side -- moot in practice: the pinned SDK's own client-side validateOpListActivityTypesInput/etc. reject an empty RegistrationStatus before any request is sent, so no real typed client can ever reach this code path."
  - "(2026-09-18) activityQueues/decisionQueues (FIFO pending-task lists) are intentionally excluded from backendSnapshot (order-sensitive plain maps, not store.Tables) -- a restart loses in-flight pending tasks not yet polled, while their history events and active-task records survive."
deferred: []
leaks: {status: clean, note: "no goroutines/timers spawned by this service, including the new cross-execution decision handlers in decision_orchestration.go and the gopherstack-7gse timeout sweep (timeout_sweep.go). Both EXECUTION_START_TO_CLOSE and (FIXED 2026-09-18) StartTimer's StartToFireTimeout are lazily swept, not autonomously fired: sweepTimedOutExecutionsLocked takes now as a parameter (never calls time.Now() itself) and is invoked with the real clock at the top of every backend op that reads or mutates execution state (Describe/GetHistory/List/Count/Poll/Respond/Terminate/RequestCancel/Signal/Start -- see the ops table), so a timed-out execution or fired timer becomes visible on the next such call rather than at a real background tick. This keeps the pre-existing no-goroutine design intact and makes the sweep trivially unit-testable (pass an arbitrary now, no sleeping/synctest needed). All state lives in InMemoryBackend maps/store.Tables guarded by lockmetrics.RWMutex; every lock path uses defer-release. Several previously-RLock-only ops (DescribeWorkflowExecution, GetWorkflowExecutionHistory, ListOpen/ClosedWorkflowExecutions, CountOpen/ClosedWorkflowExecutions, RecordActivityTaskHeartbeat) were upgraded to Lock so the sweep -- which mutates state -- can run under them; this is a coarse-lock design (see pkgs-catalog.md), so the change is a straightforward RLock->Lock swap, not a new locking scheme."}
---

## Notes

### 2026-09-26: closed-execution retention pruning (unbounded-growth audit)

Closed workflow executions were only ever removed by the unrelated
maxWorkflowExecutions=10_000 FIFO cap, never by the domain's own
workflowExecutionRetentionPeriodInDays (AWS RegisterDomain doc). Added
sweepExpiredClosedExecutionsLocked (timeout_sweep.go), wired into the same
lazy per-op sweep as timeout enforcement -- no new goroutine. See
TestSweepExpiredClosedExecutionsLocked_Evaluation /
TestListClosedWorkflowExecutions_SweepsRetentionOnRead.

### 2026-09-19 over-wide-response sweep

cmd/overwidecandidates flagged all 5 List ops. All 5 already emitted exactly
their real Info-summary member set (verified against swf@v1.37.4) -- no
leaks, no gaps. ListOpenWorkflowExecutions/ListClosedWorkflowExecutions
share one executionInfoOutput builder, matching real AWS's own reuse of
types.WorkflowExecutionInfo across both ops. See list_summary_shapes_test.go.

### 2026-09-18: ledger burn-down -- TimerFired implemented, 6 stale items_still_open entries removed

Adjudicated every `items_still_open`/`deferred` entry one at a time against the
code at HEAD, not just the manifest prose.

**Fixed (real behaviour, class b):** `StartTimer`'s `StartToFireTimeout` was
purely decision-driven with no autonomous firing -- a timer, once started,
never fired on its own, so a decider waiting on `TimerFired` would poll
forever. `WorkflowExecution.OpenTimerDeadlines` (new, `json:"-"`, same
non-persisted convention as the existing `TimerStartedEventIDs`) records each
open timer's fire deadline; `fireExpiredTimersLocked` (`timeout_sweep.go`),
called from the same `sweepTimedOutExecutionsLocked` already invoked at every
execution-state-touching op, appends `TimerFired`
(`TimerFiredEventAttributes` requires `startedEventId`/`timerId`, confirmed
against `aws-sdk-go-v2/service/swf@v1.37.4/types/types.go`) and gives the
execution a fresh decision task once a timer's deadline elapses -- the same
lazy-sweep, no-goroutine pattern `EXECUTION_START_TO_CLOSE` already uses.
Proven via `TestTimerFired_SDKRoundTrip` (`timer_fired_test.go`), a real
`aws-sdk-go-v2` client test using `StartToFireTimeout: "0"` so the deadline is
already elapsed by the next call; confirmed failing pre-fix by swapping in the
pre-fix `timeout_sweep.go`/`decision_tasks.go`/`models.go` (not stashed --
this session shares its working tree with other agents), re-running, and
restoring.

**Removed as already fixed at HEAD (class a, found by reading code, not by
`cmd/staleclaims`):**
- "DecisionTaskScheduled and DecisionTaskStarted history events are never
  recorded" -- false at HEAD; `store.go`'s `enqueueDecisionTaskLocked` and
  `decision_tasks.go`'s `PollForDecisionTask` both record them (landed in the
  2026-08-21 batch-17 pass, see that Notes entry) but the items_still_open
  bullet was never removed.
- "`TimerCanceledEventAttributes.StartedEventId` is never emitted" -- false at
  HEAD; `WorkflowExecution.TimerStartedEventIDs` populates it (same batch-17
  pass), same stale-removal miss.
- The "FIXED 2026-08-23" `DeprecationDate` bullet, and two audit-confirmation
  notes ("Value-semantics sweep... CLEAN", "Child-policy cascade is now
  implemented...") -- none of these describe an open gap; they document
  already-resolved or already-correct behavior and don't belong in
  `items_still_open` (parity-principles.md #6).
- "Complete/Fail/Cancel... do NOT cascade child policy... this is correct,
  not a gap" -- self-described as not a gap; removed rather than kept as a
  no-op entry (the underlying fact is still documented in this file's own
  `families`/Notes history).

**Kept, tightened (class c, structural):** decision/activity-task timeout
kinds beyond execution+timer (materially different task-closing mechanism);
Lambda task support (no cross-service wiring); `RegistrationStatus`
empty-value leniency (unreachable via any real typed client -- the SDK's own
`validateOpListActivityTypesInput` etc. refuse to send the request);
`activityQueues`/`decisionQueues` snapshot exclusion (pre-existing, documented
design choice).

`cmd/staleclaims` was run first and did NOT flag any of the 6 real stale
entries above -- all of its hits for this service were token collisions
(`ReverseOrder`/`RegistrationStatus`/`BranchOrder`-style false positives on
unrelated same-word text), consistent with this campaign's prior experience
that the tool is a lead, not a verdict. The 6 removals here were found by
reading `store.go`/`decision_tasks.go`/`models.go` directly against each
claim.

Gates: `gofmt -l services/swf` clean. `go build ./services/swf/` and
`go vet ./services/swf/` clean. `go test -race -count=1 ./services/swf/...`
and `go test -count=1 ./pkgs/persistence/` pass.
`golangci-lint run ./services/swf/` 0 issues. No `go.mod`/`go.sum` change. No
`snapshot_inventory.json` change (`OpenTimerDeadlines` is `json:"-"`, same
non-persisted convention as `TimerStartedEventIDs`).

- **2026-09-12 (typed coverage, gopherstack-n3zi)**: added
  `wire_sdk_roundtrip_workflow_and_activity_test.go`, driving all 23 previously
  typed-client-uncovered ops (domain/activity-type/workflow-type
  deprecate-undeprecate-delete lifecycles, DescribeWorkflowExecution,
  SignalWorkflowExecution, Count{Open,Closed}WorkflowExecutions,
  Count/PollForActivityTask, RecordActivityTaskHeartbeat,
  RespondActivityTask{Completed,Failed,Canceled}, tags) through the real
  `aws-sdk-go-v2` client. Zero real backend bugs found. One
  test-authoring trap worth recording for future slices on this service:
  `StartWorkflowExecution` (`createExecutionLocked`) always
  auto-schedules the workflow's first decision task; calling the
  test-only `EnqueueDecisionTaskInternal` helper afterward (a pattern
  copied from an existing test, `decision_lifecycle_test.go`) pushes a
  SECOND, malformed decision task (empty `RunID`, since the helper's
  4th arg was passed `""`) onto the same queue behind the real one. A
  test that polls only once never notices the stray leftover entry --
  but a test that polls a SECOND time after a `RespondActivityTaskFailed`
  auto-re-enqueue (which is real behavior, `enqueueDecisionTaskLocked`)
  pops that stray malformed entry instead of the real one, and any
  decision applied against it (e.g. `ScheduleActivityTask`) silently
  resolves against a nonexistent execution and does nothing. Not a
  backend bug -- the double-seed is redundant test setup, not something a
  real client would ever trigger, since a real client only calls
  `StartWorkflowExecution` once.

### 2026-08-29: ListOpen/ListClosedWorkflowExecutions dropped ReverseOrder and had no default sort order

Wrapper-key/silent-drop sweep (bd gopherstack-6flj/21my) against
`aws-sdk-go-v2/service/swf@v1.37.4` (pin unchanged, reconfirmed against
go.mod). `enumcheck`/`acceptguard`/`zeroguard`/`xmlitemwrap` all came back
clean for swf this pass -- this bug was found by hand (write-only-state
sweep over every List op's request struct), not by a tool.

`ListOpenWorkflowExecutionsInput.ReverseOrder` and
`ListClosedWorkflowExecutionsInput.ReverseOrder` (both real, documented
members -- `api_op_ListOpenWorkflowExecutions.go`/
`api_op_ListClosedWorkflowExecutions.go`: "When set to true, returns the
results in reverse order. By default the results are returned in
descending order of the start [or the close] time of the executions")
had no corresponding field anywhere in `handleListOpenWorkflowExecutionsInput`/
`handleListClosedWorkflowExecutionsInput` (`handler_workflow_executions.go`)
-- a real client's `ReverseOrder: true` was silently discarded on the way
in. Worse, there was no default ordering either:
`ListOpenWorkflowExecutions`/`ListClosedWorkflowExecutions`
(`workflow_executions.go`) built their result slice directly from
`executionsByDomain.Get(domain)`, a `pkgs/store.Index` group whose own doc
comment is explicit that "iteration order within the group is insertion
order, not any table-defined order" -- so even a caller that never touched
`ReverseOrder` at all did not get real AWS's documented
descending-start-time (or descending-close-time, for
`ListClosedWorkflowExecutions` when `closeTimeFilter` selected the page)
default; it got arbitrary insertion order.

Fixed: added `ReverseOrder bool` to both wire input structs, threaded it
into `ExecutionFilter.ReverseOrder` (`workflow_executions.go`), and added
`sortExecutionsByTimestamp` (`workflow_executions.go`), called from both
`ListOpenWorkflowExecutions` (always by `StartTimestamp` -- `ListOpen`'s
only filter is `startTimeFilter`, required) and `ListClosedWorkflowExecutions`
(by `CloseTimestamp` when the caller's `closeTimeFilter` populated
`filter.CloseOldestDate`, matching `ExecutionTimeFilter.OldestDate` being a
required member whenever `closeTimeFilter` is present at all; by
`StartTimestamp` otherwise, matching `CloseTimeFilter`'s own doc: "if this
parameter is specified, the returned results are ordered by their close
times" vs. `StartTimeFilter`'s "...ordered by their start times").
`CountOpenWorkflowExecutions`/`CountClosedWorkflowExecutions` do not carry
a `ReverseOrder` member in the real SDK (they return only a count, no
ordered list) and were correctly left untouched.

Proven via two real `aws-sdk-go-v2/service/swf` client round trips
(`wire_field_fixes_test.go`,
`TestListOpenWorkflowExecutions_ReverseOrder_SDKRoundTrip`/
`TestListClosedWorkflowExecutions_ReverseOrder_SDKRoundTrip`): each starts
three real executions (the second test closes them via
`TerminateWorkflowExecution` out of start order specifically so a fix that
accidentally sorted `ListClosedWorkflowExecutions` by `StartTimestamp`
instead of `CloseTimestamp` would produce a detectably different, still-wrong
sequence), asserts the default page comes back newest-first, then asserts
`ReverseOrder: true` flips it to oldest-first. Confirmed both tests fail
against the pre-fix code (default order came back as plain start-of-request
insertion order, not sorted; hand-reverted via `git show HEAD:` before this
pass touched the files, re-ran, restored the fix, re-ran clean) before
counting this as a real bug.

Everything else in the wrapper-key/silent-drop sweep for swf this pass came
back clean: re-read `ExecutionFilter`/`WorkflowExecutionFilter`/
`WorkflowTypeFilter`/`TagFilter`/`CloseStatusFilter` wiring in
`handler_workflow_executions.go` end to end (all four are correctly parsed
and applied, including the mutually-exclusive `executionFilter`/
`typeFilter`/`tagFilter`/`closeStatusFilter` combinations) and the
`enumcheck` findings on `decision_orchestration.go`/`decision_tasks.go`/
`workflow_executions.go` `cause` values (`OPERATION_NOT_PERMITTED`,
`UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION`, `TIMER_ID_ALREADY_IN_USE`,
`TIMER_ID_UNKNOWN`, `CHILD_POLICY_APPLIED`) -- all five are real values of
their own specific `*FailedCause`/`*Cause` enum (already cross-checked
against `types/enums.go` in the 2026-08-20 sweep below); `enumcheck` flags
them only because it cannot disambiguate which of the many same-named
`cause` enums applies at each call site (documented ~2.5% needs-review
precision), not because any value is actually wrong. Not re-litigated with
fresh citations here since the 2026-08-20 sweep already did that work.

**Not reached this pass:** activity type/task surface
(`activity_tasks.go`/`activity_types.go`), domain surface (`domains.go`),
decision-task/history internals beyond the `cause`-enum spot-check above,
tag operations. These were exhaustively covered by the 2026-08-20 and
2026-08-21 (`gopherstack-r80d` batch 17) sweeps below and were not
re-audited from scratch this pass; this pass's own new coverage is the
List/Count execution-filter surface only.

Protocol: SWF is **awsjson1.0** (`application/x-amz-json-1.0`, `X-Amz-Target:
SimpleWorkflowService.<Op>`) -- confirmed against the real
`aws-sdk-go-v2/service/swf` serializers (every op sets
`Content-Type: application/x-amz-json-1.0`). This is easy to mis-key as
awsjson1.1 (the more common AWS JSON protocol) since SWF's dispatch shape
looks identical otherwise.

### Wrapper-key / nested-shape wire-parity sweep (2026-08-20)

Full campaign-style sweep of all 39 ops against `aws-sdk-go-v2/service/swf@v1.37.4`
(confirmed unchanged, still pinned in go.mod). Protocol re-confirmed
awsjson1.0 from `X-Amz-Target: SimpleWorkflowService.<Op>` in serializers.go
and `deserializeOpDocument<Op>Output` helpers present and called in
deserializers.go for every op checked -- the restjson flat-body false-positive
trap this campaign warns about does not apply to a JSON-RPC service.

`eventAttrKey` (models.go) derives each `HistoryEvent` attributes wrapper key
programmatically (`strings.ToLower(eventType[:1]) + eventType[1:] +
"EventAttributes"`) rather than hand-writing 44 string literals. Checked this
convention against every case in
`awsAwsjson10_deserializeDocumentHistoryEvent`'s switch (deserializers.go:6589-6924,
all 44 `*EventAttributes` cases): every one follows exactly this
lowerCamel-plus-suffix pattern with zero exceptions, and every EventType string
literal gopherstack emits (grepped across activity_tasks.go/decision_tasks.go/
decision_orchestration.go/workflow_executions.go/signals.go/timeout_sweep.go)
matches a real `types.EventType` enum value (types/enums.go:229-286) exactly.
This means the wrapper-KEY layer of the HistoryEvent surface is structurally
immune to the dominant "wrong key" bug class the rest of this campaign kept
finding -- the two real bugs found this pass were both in per-event attribute
CONTENT (a missing required field, and a fabricated enum value), not in key
selection. See the ops/families/gaps entries above for the two fixes and three
disclosed-not-fixed gaps; full detail:

1. **RequestCancelWorkflowExecution stamped an enum value that does not
   exist** (workflow_executions.go). `types.WorkflowExecutionCancelRequestedCause`
   (types/enums.go:649-654) defines exactly one value, `CHILD_POLICY_APPLIED`
   -- there is no `OPERATOR_INITIATED` case for this specific cause enum
   (unlike `WorkflowExecutionTerminatedCause`, types/enums.go:667-673, which
   does define one). A direct, operator-initiated
   `RequestCancelWorkflowExecution` call was stamping
   `WorkflowExecutionCancelRequestedEventAttributes.cause = "OPERATOR_INITIATED"`
   regardless -- a pre-existing bug a prior pass's own code comment had already
   found and explicitly left unfixed ("that mismatch predates this change...
   is left alone here"). Since `Cause` is a bare string type, not smithy-enum
   validated on decode, this didn't fail the call -- a real typed client just
   silently got a cause value that cannot occur on real AWS. Real AWS leaves
   `Cause` unset entirely for a direct call (it's optional, and the enum's only
   value is reserved for the automatic child-policy cascade already handled
   correctly by `cascadeCancelRequestLocked`, which was and remains correct).
   Fixed by removing the fabricated cause key from the direct-call attrs.
   `TestRequestCancelWorkflowExecution_CancelRequestedCause_SDKRoundTrip`
   (wire_sdk_roundtrip_test.go) drives this through the real SDK client and
   asserts `Cause` decodes empty; hand-reverting reproduced the exact
   predicted symptom (`Cause == "OPERATOR_INITIATED"`).

2. **ChildWorkflowExecutionTimedOut was missing its one required field**
   (timeout_sweep.go). `types.ChildWorkflowExecutionTimedOutEventAttributes`
   (types/types.go, ~line 608) requires `TimeoutType` alongside
   `InitiatedEventId`/`StartedEventId`/`WorkflowExecution`/`WorkflowType` --
   the same real enum this backend's own `WorkflowExecutionTimedOut` handling
   already gets right (`types.WorkflowExecutionTimeoutType`'s only value,
   `START_TO_CLOSE`). `propagateChildClosureLocked`'s shared base attrs
   (decision_orchestration.go) correctly carry
   initiatedEventId/startedEventId/workflowExecution/workflowType for every
   Child* closure event, but `timeoutType` isn't one of those shared fields --
   it must come through the `extra` parameter, the same way Completed passes
   `result`, Failed passes `reason`/`details`, and Canceled passes `details`.
   `timeout_sweep.go`'s call site was passing `nil` for `extra`, so a real
   typed client reading a timed-out child's notification on its parent's
   history got a zero-value `""` `TimeoutType` on an otherwise-required field.
   Fixed by passing `map[string]any{attrTimeoutType: timeoutTypeStartToClose}`
   as `extra`, mirroring how `handleCompleteWorkflowExecutionDecision`/
   `handleFailWorkflowExecutionDecision`/`handleCancelWorkflowExecutionDecision`
   already do it for their own events.
   `TestChildWorkflowExecutionTimedOut_TimeoutType_SDKRoundTrip`
   (wire_sdk_roundtrip_test.go) starts a child workflow type registered with
   `DefaultExecutionStartToCloseTimeout: "0"` (so its deadline equals its
   StartTimestamp -- already expired the instant it starts, no sleep or
   fabricated clock needed) and drives the whole thing through the real SDK
   client, asserting the parent's `GetWorkflowExecutionHistory` response
   decodes `ChildWorkflowExecutionTimedOutEventAttributes.TimeoutType ==
   types.WorkflowExecutionTimeoutTypeStartToClose`; hand-reverting reproduced
   the exact predicted symptom (`TimeoutType == ""`).

Both fixes were proven by hand-revert (reintroduce the bug, run the test,
confirm the exact predicted failure message, restore, confirm
`go test -race ./services/swf/...` passes byte-identical to the fixed state).
Both round-trip tests use a real typed SDK-client field assertion (not a
raw-body/leaked-key check), since both `Cause` and `TimeoutType` are fields a
real `aws-sdk-go-v2` client actually surfaces.

**Layer-2 shape verification performed but found CLEAN** (no fabricated/
misnested/miscased/wrong-enum members beyond the two bugs above), checked
member-by-member against the real struct in types/types.go for every event
type gopherstack emits: `ActivityTaskStarted/Completed/Failed/Canceled/
CancelRequested/Scheduled`, `WorkflowExecutionStarted/Completed/Failed/
Canceled/Terminated/CancelRequested/Signaled/ContinuedAsNew`,
`StartTimerFailed/TimerStarted/CancelTimerFailed/TimerCanceled` (except the
disclosed StartedEventId gap), `MarkerRecorded`,
`ContinueAsNewWorkflowExecutionFailed`, `StartChildWorkflowExecutionInitiated/
Failed`, `ChildWorkflowExecutionStarted/Completed/Failed/Canceled/Terminated`
(TimedOut was the bug above), `SignalExternalWorkflowExecutionInitiated/
Failed`, `RequestCancelExternalWorkflowExecutionInitiated/Failed`,
`ExternalWorkflowExecutionSignaled/CancelRequested`. Every *FailedCause enum
value gopherstack emits (`WORKFLOW_TYPE_DEPRECATED`/`WORKFLOW_TYPE_DOES_NOT_EXIST`/
`WORKFLOW_ALREADY_RUNNING`/`UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION`/
`OPERATION_NOT_PERMITTED`/`TIMER_ID_ALREADY_IN_USE`/`TIMER_ID_UNKNOWN`) checked
against its own dedicated enum in types/enums.go and matches -- only
`WorkflowExecutionCancelRequestedCause`'s `OPERATOR_INITIATED` (bug #1 above)
was wrong.

The four summary/full pairs (`WorkflowExecutionInfo`+`WorkflowExecutionConfiguration`+
`WorkflowExecutionOpenCounts` vs `DescribeWorkflowExecutionOutput`,
`ActivityTypeInfo`+`ActivityTypeConfiguration` vs `DescribeActivityTypeOutput`,
`WorkflowTypeInfo`+`WorkflowTypeConfiguration` vs `DescribeWorkflowTypeOutput`,
`DomainInfo`+`DomainConfiguration` vs `DescribeDomainOutput`) were all
re-verified root-flat (no fabricated wrapper key) against each op's own
Output struct in `api_op_<Op>.go` -- clean, matching the existing PARITY.md
grade. `PollForActivityTaskOutput`/`PollForDecisionTaskOutput`/
`CountOpenWorkflowExecutionsOutput`/`CountPendingActivityTasksOutput`/
`CountPendingDecisionTasksOutput` also re-verified root-flat and clean (all
`{count, truncated}` root members present).

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

## 2026-09-18: ReverseOrder (List ops) and PollForDecisionTask.StartAtPreviousStartedEvent (reqfielddiff tier-1)

`reqfielddiff` flagged 4 tier-1 fields: `ListActivityTypes.ReverseOrder`,
`ListDomains.ReverseOrder`, `ListWorkflowTypes.ReverseOrder`,
`PollForDecisionTask.StartAtPreviousStartedEvent` (swf@v1.37.4, all real
request members with documented defaults). The 3 `ReverseOrder` findings were
already disclosed in `items_still_open` as "recorded, not fixed" by an
earlier sweep (the "Other axis" entry, now removed since this pass closes
it) -- `handleListDomainsInput`/`handleListActivityTypesInput`/
`handleListWorkflowTypesInput` never declared the field at all, so every
call sorted ascending by name regardless of the request.

Fixed: all three now take `ReverseOrder` and flip their `sort.Slice`
comparator, same shape as the 2026-08-29
`ListOpen`/`ListClosedWorkflowExecutions` fix. `PollForDecisionTask` never
declared `StartAtPreviousStartedEvent` and never tracked
`PreviousStartedEventId` (always the Go zero value, 0, even though
`DecisionTask.PreviousStartedEventID` has existed on the model since
2026-08-20's disclosed-not-fixed sweep) -- `lastDecisionTaskStartedEventID`
now finds the prior `DecisionTaskStarted` event before appending the new
one, `PollForDecisionTask` always echoes it as `PreviousStartedEventId`, and
`eventsFromID` trims `Events` to that event onward when
`StartAtPreviousStartedEvent` is set. `Backend.PollForDecisionTask` gained a
5th parameter; all ~18 backend-direct call sites in this package's tests
were updated to pass `false` (or `true` where relevant) -- no other
`services/` package calls this method directly.

Proven with `TestList_ReverseOrder_RealClient` (table of 3 subtests, one per
op) and `TestPollForDecisionTask_StartAtPreviousStartedEvent_RealClient`
(typed `aws-sdk-go-v2` client, `list_reverse_order_test.go` and
`decision_task_previous_started_event_test.go`) -- the latter drives 3
decision tasks across a real workflow execution and asserts the trimmed
poll's `Events` excludes everything before the previous
`DecisionTaskStarted` event (5 of 10 total history events) while the
untrimmed poll returns the full history (7 of 7 at that point).

No persisted (`backendSnapshot`) fields changed (`PreviousStartedEventID`
was already a model field, just never populated). Gates: `go build ./...`,
`go vet ./services/swf/`, `go test -race -count=1 ./services/swf/` (all
pass), `golangci-lint run --new-from-rev=HEAD ./services/swf/` (0 issues).
tier-1 (`cmd/reqfielddiff -dir swf`): 4 -> 0.
