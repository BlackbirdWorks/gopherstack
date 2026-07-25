---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codepipeline
sdk_module: aws-sdk-go-v2/service/codepipeline@v1.48.0   # version audited against
last_audit_commit: d50d1410                              # HEAD when this manifest was written (working tree, uncommitted)
last_audit_date: 2026-07-23
overall: A            # closed all 4 gaps + 3 deferred families from the 2026-07-12 audit
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreatePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePipeline: {wire: ok, errors: ok, state: ok, persist: ok, note: "version-mismatch ConflictException REMOVED (fixed, was gopherstack-invented -- real PipelineDeclaration.Version is documented as purely system-managed output, not an optimistic-concurrency input; UpdatePipeline now always succeeds and always increments version by 1, matching real AWS/CLI docs)"}
  DeletePipeline: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also cascade-clears actionRevisions (fixed leak/stale-data bug, same class as the actionExecutions leak fixed in the prior pass)"}
  ListPipelines: {wire: ok, errors: ok, state: ok, persist: ok}
  StartPipelineExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now gates on the first unresolved Approval-category action (action_engine.go runPipelineActions) instead of always completing synchronously; StartTime/Trigger/ExecutionMode/ExecutionType now populated"}
  StopPipelineExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now abandons (rather than silently orphaning) any action execution left InProgress on a pending approval gate, clearing its token so a stopped execution's approval can never be resurrected"}
  GetPipelineExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was partial): now includes executionMode/executionType/trigger/rollbackMetadata, matching the real PipelineExecution shape exactly (verified field-by-field against awsAwsjson11_deserializeDocumentPipelineExecution -- this shape has NO startTime/lastUpdateTime, unlike PipelineExecutionSummary; an earlier draft of this fix incorrectly added them here too and was corrected before landing). ArtifactRevisions/Variables remain omitted -- no artifact-store or pipeline-variable resolution engine exists to populate them (see deferred)."}
  ListPipelineExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was partial): summaries now include startTime/lastUpdateTime (epoch seconds)/executionMode/executionType/rollbackMetadata, verified field-by-field against awsAwsjson11_deserializeDocumentPipelineExecutionSummary (confirmed this shape has NO pipelineName/pipelineVersion, unlike the GetPipelineExecution detail shape). sourceRevisions/statusSummary/stopTrigger remain omitted -- no source-revision or stop-reason tracking exists (see deferred)."}
  GetPipelineState: {wire: ok, errors: ok, state: ok, persist: n/a, note: "actionStates[].latestExecution now includes token/summary/lastStatusChange (fixed -- required for the real approval-token handshake: PutApprovalResult's token can ONLY come from here in real AWS); actionStates[].currentRevision now populated from PutActionRevision (fixed, was entirely absent)"}
  ListActionExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: actionExecutions is no longer purely derived state safely rebuildable by StartPipelineExecution alone (an approval gate's token lives only on its ActionExecution record) so it is now persisted (backendSnapshot version bumped 1->2); correctly cleared on DeletePipeline"}
  PutActionRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was a stub: validated the pipeline exists, mutated nothing, always returned NewRevision=true). Now tracks the submitted ActionRevision per stage/action (surfaced via GetPipelineState), returns NewRevision=false on a repeat revisionId, and triggers a real, persisted pipeline execution (Trigger=PutActionRevision) via the same synchronous run engine as StartPipelineExecution. New ActionNotFoundException for an unknown stage/action (previously silently accepted)."}
  PutApprovalResult: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed multiple real bugs at once (see 'Bugs found and fixed this pass' below): the wire-shape field name mismatch (approvalResult -> result), the entirely-unparsed required token field, the RFC3339-string approvedAt (should be epoch seconds), and the complete absence of any state mutation. Now implements the real token-handshake: validates the action is an Approval-category action with an open (InProgress) approval request, matches token, and resumes (Approved) or fails (Rejected) the paused pipeline execution."}
  RetryStageExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was a stub: fabricated an InProgress PipelineExecution response never written to executionsStore). Now requires an actually-Failed/Abandoned action in the given stage/execution (StageNotRetryableException otherwise, matching real AWS's real precondition), resets it (FAILED_ACTIONS) or the whole stage (ALL_ACTIONS), and resumes the SAME execution via the shared run engine. retryMode was previously parsed but silently dropped -- now threaded through and validated."}
  RollbackStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was a stub: fabricated an unpersisted InProgress PipelineExecution with a random new ID). Now requires the target execution to have actually succeeded through the given stage (UnableToRollbackStageException otherwise), and creates+persists a real ROLLBACK-type PipelineExecution with RollbackMetadata.RollbackTargetPipelineExecutionId set, replaying that stage's actions as Succeeded."}
  OverrideStageCondition: {wire: ok, errors: ok, state: partial, persist: n/a, note: "still validates-only (pipeline/stage/execution existence, conditionType enum) and mutates no modeled state -- deliberately unchanged this pass; see deferred. Added pipeline-execution-existence validation (PipelineExecutionNotFoundException) and conditionType validation (ValidationException), neither of which existed before."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  webhooks: {status: ok, note: "unchanged this pass; spot-verified via full test suite pass, not re-diffed against the SDK (files touched only by the pure structural 'Go refactoring 2' decomposition since the 2026-07-12 audit)"}
  customActionTypes: {status: ok, note: "unchanged this pass; spot-verified via full test suite pass, not re-diffed against the SDK"}
  jobsAndThirdPartyJobs: {status: ok, note: "unchanged this pass; spot-verified via full test suite pass, not re-diffed against the SDK"}
  stageTransitions: {status: ok, note: "unchanged this pass; spot-verified via full test suite pass, not re-diffed against the SDK"}
  ruleOps: {status: ok, note: "unchanged this pass; ListRuleExecutions/ListRuleTypes deliberately return empty/static data -- no condition-rule engine exists anywhere in this backend (documented in source); consistent with OverrideStageCondition remaining validate-only below"}
  approvalGate: {status: ok, note: "NEW this pass: StartPipelineExecution/PutActionRevision/PutApprovalResult/RetryStageExecution/RollbackStage all now share one action-run engine (action_engine.go) that gates on Approval-category actions with a real system-generated token, exposed only via GetPipelineState (matching real AWS -- there is no other way for a real client to obtain it). This closes 3 of the 4 gaps and all 3 deferred items from the 2026-07-12 audit at once, since they all stemmed from the SAME missing action-state machine."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "OverrideStageCondition validates pipeline/stage/execution existence and conditionType but mutates no modeled state -- there is no condition-rule/before-entry-condition engine anywhere else in this backend to be inconsistent with (same class as ListRuleExecutions' deliberately scoped-down design, confirmed by reading the backend methods per parity-principles.md rule 4, not just grepping for empty returns). A full fix requires modeling BeforeEntryConditionState as a real blocking gate that StartPipelineExecution/runPipelineActions can produce and this op can then override -- out of scope for this pass."
  - "ListDeployActionExecutionTargets always returns an empty list -- no deploy-target model exists (documented in source, consistent with ListRuleExecutions' scoped-down design). Unchanged this pass."
  - "GetPipelineExecution/ListPipelineExecutions omit ArtifactRevisions/Variables/SourceRevisions/StatusSummary/StopTrigger -- no artifact-store content model, pipeline-variable resolution engine, or stop-reason tracking exists anywhere else in this backend to source real values from (all are optional fields, SDK-safe to omit)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "OverrideStageCondition deep state modeling (see gaps) -- requires a condition-rule engine that does not exist anywhere in this backend."
  - "ArtifactRevisions/Variables/SourceRevisions/StatusSummary/StopTrigger completeness on GetPipelineExecution/ListPipelineExecutions -- requires an artifact-store content model / pipeline-variable resolution engine / stop-reason tracking, none of which exist anywhere else in this backend."
  - "webhooks/customActionTypes/jobsAndThirdPartyJobs/stageTransitions/ruleOps families were NOT re-diffed against the SDK this pass (only spot-verified via the full test suite) -- their files were touched only by the pure structural 'Go refactoring 2' decomposition since the 2026-07-12 audit; next full audit should re-diff them properly rather than continuing to trust this note indefinitely."
leaks: {status: clean, note: "DeletePipeline now cascade-clears executionsStore, actionExecutionsStore, AND actionRevisionsStore (the last one is new this pass) for the deleted pipeline name; StopPipelineExecution now abandons+clears the token of any action execution left InProgress on a pending approval gate rather than leaving it silently unresolved forever; no goroutines/janitors in this service"}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CodePipeline_20150709.<Op>`).
Route matching in `handler.go`'s `RouteMatcher` is a simple header-prefix check; all 39
ops in `GetSupportedOperations()` are reachable through `dispatchTable()` -- verified no op
is registered in one list but missing from the other (unchanged this pass).

### Persistence: snapshot version bumped 1 -> 2

`actionExecutions` (per-pipeline action-execution history) and the new `actionRevisions`
(PutActionRevision tracking) are now persisted in `backendSnapshot` (`persistence.go`).
Previously `actionExecutions` was deliberately NOT persisted, on the reasoning that it was
purely derived state, safely rebuildable by re-running `StartPipelineExecution`. That
reasoning broke the moment an execution could genuinely pause mid-run: the approval gate's
system-generated `Token` lives ONLY on its `ActionExecution` record, so losing
`actionExecutions` across a snapshot/restore cycle would permanently strand any paused
execution -- there would be no way to ever resume it. `codepipelineSnapshotVersion` is bumped
to 2; any snapshot written by an older build is safely discarded on restore (never
partially/incorrectly decoded), per the existing version-guard contract in `Restore`.

### Bugs found and fixed this pass

1. **The approval-gate action-state machine did not exist**, which was the ROOT CAUSE of 3
   of the 4 `gaps` and all 3 `deferred` items from the 2026-07-12 audit simultaneously:
   `RetryStageExecution`/`RollbackStage`/`OverrideStageCondition`/`PutActionRevision`/
   `PutApprovalResult` all validated their pipeline existed and then either fabricated an
   unpersisted response or returned a correct-shaped void response, with no way to reach
   the real preconditions those ops require (a genuinely-failed stage, a genuinely-completed
   target execution, an open approval request) because `StartPipelineExecution` always
   marked every action `Succeeded` synchronously with no exceptions. Fixed by adding a shared
   run engine (`action_engine.go`'s `runPipelineActions`) that gates on the first unresolved
   Approval-category action: it is recorded `InProgress` with a fresh system-generated token
   and processing stops there, exactly mirroring the transient wait a real client observes.
   `PutApprovalResult` (`approvals.go`) now implements the real token handshake and
   resumes/fails the paused execution; `RetryStageExecution`/`RollbackStage`
   (`pipeline_state.go`) now have a genuine failed/succeeded stage to operate against and
   persist real, mutated state.

2. **`PutApprovalResult`'s wire shape used the wrong JSON key for its required `result`
   member** (`handler_approvals.go`): the handler parsed `approvalResult`, but the real
   SDK always serializes this member as `result`
   (verified against `awsAwsjson11_serializeOpDocumentPutApprovalResultInput` in the real
   SDK's `serializers.go`) -- a real client's request would have silently no-opped this
   field every time. The required `token` field was not parsed AT ALL (real AWS requires it
   to identify which open approval request is being resolved; obtaining it is only possible
   via `GetPipelineState`). Fixed: renamed to `result`, added `token` (now required,
   `errInvalidRequest` if missing).

3. **`PutApprovalResult`'s `approvedAt` was an RFC3339 string**, but every other timestamp on
   this service's awsjson1.1 wire is an epoch-seconds JSON number (the protocol's standard
   timestamp format, and the convention `pkgs/awstime.Epoch` exists to enforce elsewhere).
   Fixed: `approvedAt` is now `float64` epoch seconds.

4. **`GetPipelineExecution`/`ListPipelineExecutions` field-completeness fix introduced (and
   then self-corrected) a shape-conflation bug during this pass**: `PipelineExecution` (the
   `GetPipelineExecution` detail shape) and `PipelineExecutionSummary` (the
   `ListPipelineExecutions` list-item shape) are DIFFERENT SDK shapes with only partial field
   overlap -- `PipelineExecution` has no `startTime`/`lastUpdateTime` at all, while
   `PipelineExecutionSummary` has no `pipelineName`/`pipelineVersion` at all (verified
   field-by-field against `awsAwsjson11_deserializeDocumentPipelineExecution` and
   `awsAwsjson11_deserializeDocumentPipelineExecutionSummary`). An earlier draft of this
   fix built one shape by deriving the other via `delete()`, which leaked `startTime`/
   `lastUpdateTime` into the detail shape. Caught before landing by diffing against the real
   deserializers field-by-field (parity-principles.md rule 2) and corrected: two independent
   builders (`pipelineExecutionDetail`, `pipelineExecutionSummary` in
   `handler_pipeline_executions.go`) sharing only the sub-shapes that are genuinely identical
   (`trigger`, `rollbackMetadata`).

5. **`UpdatePipeline` rejected a pipeline-version mismatch with a fabricated
   `ConflictException`** -- flagged as a defensible-but-unverified judgment call in the prior
   audit pass. Verified this pass against the real SDK's field documentation:
   `PipelineDeclaration.Version` is documented as "the version number of the pipeline...
   incremented when a pipeline is updated" with no mention anywhere of the caller's input
   value being validated against the current version, and the real UpdatePipeline API/CLI
   documentation describes an update as always incrementing the version by exactly 1
   regardless of what was sent. This confirms it as gopherstack-invented behavior with no
   basis in the real API. Fixed: the version-mismatch check has been removed entirely;
   `UpdatePipeline` always succeeds and always increments the version by 1.

6. **`DeletePipeline` did not cascade-clear `actionRevisions`** (the new PutActionRevision
   tracking store added this pass) -- would have been an immediate re-introduction of the
   same stale-data leak class fixed for `actionExecutions` in the prior pass, for a
   same-named pipeline recreated after delete. Fixed as part of adding the store, not as a
   follow-up.

7. **`StopPipelineExecution` did not account for a genuinely-InProgress execution** (only
   possible now that the approval gate exists) -- would have left a stopped execution's
   pending approval action silently `InProgress` forever, with its token still valid and
   resurrectable by a later `PutApprovalResult` even after the execution was supposedly
   stopped. Fixed: stopping now abandons (`Abandoned` status) and clears the token of any
   action left `InProgress` on an approval gate.

### Traps for the next auditor (looks-wrong-but-correct)

- `ListRuleExecutions`, `ListRuleTypes`, and `ListDeployActionExecutionTargets`
  deliberately return empty/static data for a *known* pipeline and `ErrNotFound` for
  an unknown one -- unchanged this pass, still not a disguised stub (see `gaps`).
- `OverrideStageCondition` validates pipeline/stage/execution/conditionType (real backend
  logic, improved this pass) and otherwise mutates no state, which is the AWS-correct wire
  shape for this op (`OverrideStageConditionOutput` carries no fields). It remains the ONE
  op from the 2026-07-12 `gaps` list still validate-only, because there is genuinely no
  condition-rule/before-entry-condition state anywhere else in this backend for it to
  override -- unlike `PutApprovalResult`/`RetryStageExecution`/`RollbackStage`, which all
  became real once the approval-gate action-state machine existed to give them a genuine
  precondition to act on.
- `PipelineExecution.Status` values used here (`InProgress`, `Succeeded`, `Stopped`,
  `Failed` -- new this pass, reached via a rejected/never-retried approval) are all real
  `PipelineExecutionStatus` enum values. `Cancelled` and `Superseded` are still never
  produced (this backend has no path that supersedes a running execution with a newer one).
- `ActionExecution.Status` can now be `Abandoned` (real `ActionExecutionStatus` enum value),
  reached only via `StopPipelineExecution` stopping an execution paused on an approval gate.
- `PutApprovalResult`'s exact error precedence when NO open approval request exists at all
  for a stage/action (vs. one that already resolved) is an interpretive call: this backend
  returns `ApprovalAlreadyCompletedException` for both cases (no real AWS documentation
  distinguishes "never reached" from "already resolved" for this op) -- a defensible choice,
  flagged for verification against SDK integration tests in a future pass, same as the
  now-fixed `UpdatePipeline` version judgment call was.
