---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codepipeline
sdk_module: aws-sdk-go-v2/service/codepipeline@v1.48.0   # version audited against
last_audit_commit: 0627d5d3                              # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # ~1k genuine fixes found (fresh audit, no prior PARITY.md existed)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreatePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPipeline: {wire: ok, errors: ok, state: ok, persist: ok, note: "version mismatch now returns PipelineVersionNotFoundException, not PipelineNotFoundException (fixed)"}
  UpdatePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePipeline: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also clears actionExecutions on delete (fixed leak/stale-data bug)"}
  ListPipelines: {wire: ok, errors: ok, state: ok, persist: ok}
  StartPipelineExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "was stuck at InProgress forever -- fixed to reach terminal Succeeded"}
  StopPipelineExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "abandon field was parsed but silently dropped -- now wired through; was stuck at Stopping forever -- fixed to reach terminal Stopped; unknown execution ID now returns PipelineExecutionNotFoundException instead of a fabricated Succeeded/Stopping stub"}
  GetPipelineExecution: {wire: partial, errors: ok, state: ok, persist: ok, note: "unknown execution ID now returns PipelineExecutionNotFoundException instead of a fabricated stub (fixed); response omits optional ArtifactRevisions/ExecutionMode/ExecutionType/Trigger/RollbackMetadata/Variables fields (all optional pointers -- SDK-safe to omit, but shallower than real AWS)"}
  ListPipelineExecutions: {wire: partial, errors: ok, state: ok, persist: ok, note: "summaries omit optional StartTime/LastUpdateTime/SourceRevisions/ExecutionMode/ExecutionType (all optional pointers)"}
  GetPipelineState: {wire: ok, errors: ok, state: ok, persist: n/a, note: "derived live from pipeline+actionExecutions each call; correctly reflects latestExecution per action"}
  ListActionExecutions: {wire: ok, errors: ok, state: ok, persist: deferred, note: "actionExecutions intentionally not persisted (derived, rebuilt on StartPipelineExecution) -- matches pre-existing design; now correctly cleared on DeletePipeline"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  webhooks: {status: ok, note: "PutWebhook/ListWebhooks/DeleteWebhook/Register|DeregisterWebhookWithThirdParty verified; ARN + URL generation, tag round-trip, persisted via store.Table"}
  customActionTypes: {status: ok, note: "Create/Delete/Get/UpdateActionType + ListActionTypes verified; DeleteCustomActionType correctly blocks in-use types with ResourceInUseException"}
  jobsAndThirdPartyJobs: {status: ok, note: "AcknowledgeJob(ThirdParty), PollForJobs(ThirdParty), GetJobDetails, GetThirdPartyJobDetails, PutJob(ThirdParty)SuccessResult/FailureResult verified; status transitions correct"}
  stageTransitions: {status: ok, note: "Enable/DisableStageTransition verified; validates stage exists (StageNotFoundException); persisted, cascade-deleted with pipeline"}
  ruleOps: {status: ok, note: "ListRuleExecutions/ListRuleTypes deliberately return empty/static data -- no condition-rule engine exists anywhere in this backend (documented in source)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "RetryStageExecution, RollbackStage, OverrideStageCondition, PutActionRevision, PutApprovalResult validate the pipeline exists but otherwise perform no real state mutation (RetryStageExecution/RollbackStage fabricate a PipelineExecution response that is never persisted to executionsStore, so a subsequent GetPipelineExecution/ListPipelineExecutions will not reflect the retry/rollback). Deep fix requires modeling per-action failure/approval state, which no other part of this backend tracks either (all actions succeed synchronously on Start). Out of scope for this pass; not in the priority family list."
  - "ListDeployActionExecutionTargets always returns an empty list -- no deploy-target model exists (documented in source, consistent with ListRuleExecutions' scoped-down design)."
  - "PutApprovalResult does not update actionStates/GetPipelineState output -- manual-approval actions are not modeled as a distinct action-state machine."
  - "UpdatePipeline rejects a version mismatch with ConflictException; real AWS documentation does not clearly specify this as a hard requirement (the input Version field is described as system-managed). Left as-is since it is a defensible, non-obviously-wrong emulator choice and no test/behavior contradicts it; flagged for a future audit to verify against SDK integration tests."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - RetryStageExecution / RollbackStage / OverrideStageCondition deep state modeling
  - PutActionRevision / PutApprovalResult deep state modeling
  - ListPipelineExecutions / GetPipelineExecution optional-field completeness (StartTime, LastUpdateTime, SourceRevisions, ExecutionMode, ExecutionType, Trigger, ArtifactRevisions, RollbackMetadata, Variables)
leaks: {status: clean, note: "DeletePipeline now clears both executionsStore and actionExecutionsStore for the deleted pipeline name (previously only cleared executionsStore, leaving actionExecutions to leak into a same-named pipeline recreated later); no goroutines/janitors in this service"}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CodePipeline_20150709.<Op>`).
Route matching in `handler.go`'s `RouteMatcher` is a simple header-prefix check; all 39
ops in `GetSupportedOperations()` are reachable through `dispatchTable()` -- verified no op
is registered in one list but missing from the other.

### Bugs found and fixed this pass (all in the "stuck state" / "disguised no-op" /
"missing errCodeLookup entry" bug classes called out in parity-principles.md):

1. **StartPipelineExecution left every execution stuck at `InProgress` forever**
   (`backend.go`). The synchronous emulator marks every action execution `Succeeded`
   immediately in the same call, but never updated the *pipeline* execution's own
   `Status`, so `GetPipelineExecution`/`ListPipelineExecutions` reported `InProgress`
   forever. A real client polling for completion (the intended usage pattern for an
   inherently-asynchronous AWS service) would spin indefinitely. Fixed: the pipeline
   execution now transitions to `Succeeded` once all of its (synchronously-completed)
   actions are recorded.

2. **StopPipelineExecution left every stopped execution stuck at `Stopping` forever**,
   and silently dropped the `abandon` request field entirely (parsed in `handler.go`
   but never passed to the backend). Real AWS: `Stopping` is a transient state while
   in-progress actions finish or are abandoned; since this backend has no in-progress
   actions by the time `StopPipelineExecution` can be called (everything already
   completed synchronously in `StartPipelineExecution`), the execution should reach
   the terminal `Stopped` state immediately. Fixed: `abandon` is now threaded through,
   and the matched execution's status is set to `Stopped`.

3. **GetPipelineExecution and StopPipelineExecution fabricated a fake `Succeeded`/
   `Stopping` response for a completely unknown execution ID** ("Return a stub for
   unknown execution IDs to maintain backward compatibility" -- a disguised no-op that
   also hid the missing `PipelineExecutionNotFoundException` error mapping). Real AWS
   returns `PipelineExecutionNotFoundException` (verified against
   `aws-sdk-go-v2/service/codepipeline/types/errors.go`). Fixed: both ops now return
   the real error; a new `ErrExecutionNotFound` sentinel was added and wired into
   `handleError`'s mapping table.

4. **GetPipeline returned `PipelineNotFoundException` for a version mismatch** on an
   *existing* pipeline. Real AWS has a distinct `PipelineVersionNotFoundException` for
   exactly this case (verified against the real SDK's error types) -- these are
   different wire error codes an SDK caller may branch on, not just different
   messages. Fixed: added `ErrVersionNotFound` sentinel, wired into `handleGetPipeline`
   and `handleError`.

5. **DeletePipeline did not clear `actionExecutions`** (only `executions` was cleared).
   Since `actionExecutions` is keyed only by pipeline name (not by a pipeline
   identity/generation), deleting a pipeline and recreating one with the same name
   resurrected the deleted pipeline's old `ListActionExecutions` history. Fixed:
   `DeletePipeline` now clears both stores.

### Traps for the next auditor (looks-wrong-but-correct)

- `ListRuleExecutions`, `ListRuleTypes`, and `ListDeployActionExecutionTargets`
  deliberately return empty/static data for a *known* pipeline and `ErrNotFound` for
  an unknown one. This is not a stub in the disguised-no-op sense -- there genuinely is
  no condition-rule or deploy-target engine anywhere else in this backend to be
  inconsistent with (confirmed by reading the backend methods, not just grepping for
  empty returns, per parity-principles.md rule 4).
- `OverrideStageCondition`, `PutActionRevision`, and `PutApprovalResult` validate that
  the referenced pipeline exists (real backend logic) and then return a void `{}`
  response, which is the AWS-correct wire shape for these ops. They do **not** mutate
  any modeled state beyond that existence check -- flagged under `gaps` above rather
  than fixed, since doing so properly requires an action-state machine (failed/
  approval-pending states) that nothing else in this backend tracks; StartPipeline­
  Execution always marks every action `Succeeded` synchronously, so there is currently
  no way to reach the "some actions failed" precondition `RetryStageExecution` expects
  in real AWS.
- `PipelineExecution.Status` values used here (`InProgress`, `Succeeded`, `Stopped`)
  are the real `PipelineExecutionStatus` enum values (`aws-sdk-go-v2/service/
  codepipeline/types/enums.go`); `Cancelled`, `Superseded`, and `Failed` are never
  produced since this backend has no path that fails an action or supersedes a
  running execution with a newer one.
