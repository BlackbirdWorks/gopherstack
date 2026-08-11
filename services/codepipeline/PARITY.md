---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codepipeline
sdk_module: aws-sdk-go-v2/service/codepipeline@v1.49.4   # version audited against
last_audit_commit: d50d1410                              # stale -- git usage disallowed this pass; see last_audit_date
last_audit_date: 2026-07-30
overall: A            # gopherstack-ohm3 CLOSED (2026-07-30 follow-up pass): UpdateActionType now parses the real ActionTypeDeclaration shape (Executor/Id/InputArtifactDetails/OutputArtifactDetails required, Description/Permissions/Properties/Urls optional), validated against the real SDK's validators.go (validateActionTypeDeclaration/validateActionTypeExecutor/validateActionTypeIdentifier/validateExecutorConfiguration/validateLambdaExecutorConfiguration) field-for-field, and genuinely mutates the stored action-type record (merge semantics: only the fields ActionTypeDeclaration can express are replaced; legacy Settings/ConfigurationProperties/Tags are preserved, since AWS's real UpdateActionType input has no member that could ever express clearing them). GetActionType had the SAME bug -- despite the prior pass's customActionTypes note claiming it was "genuinely clean," its real output (GetActionTypeOutput.ActionType) is also *types.ActionTypeDeclaration, not the legacy *types.ActionType the prior pass verified it against (confirmed directly against api_op_GetActionType.go) -- fixed as part of this pass; see families. This closes the one SEVERE finding that was the sole stated reason for last pass's A->B downgrade; no other new gaps were introduced or discovered. Restored A.
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
  OverrideStageCondition: {wire: FIXED, errors: ok, state: partial, persist: n/a, note: "still validates-only (pipeline/stage/execution existence, conditionType enum) and mutates no modeled state -- see gaps. Fixed a real bug this pass: conditionType validation only accepted BEFORE_ENTRY, but the real types.ConditionType enum has exactly two values, BEFORE_ENTRY and ON_SUCCESS -- a real client requesting ON_SUCCESS override was wrongly rejected with ValidationException. Now both are accepted. Backend comment rewritten to precisely name the real mutation this op can't perform (StageState.{BeforeEntryConditionState,OnSuccessConditionState}.LatestExecution.Status -> Overridden) and exactly why (StageDeclaration here has no BeforeEntry/OnFailure/OnSuccess members at all -- CreatePipeline never parses them, so there is no condition state anywhere to flip)."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  webhooks: {status: FIXED, note: "Re-diffed against types.WebhookDefinition/ListWebhookItem/WebhookAuthConfiguration/WebhookFilterRule this pass (previously only spot-verified via test suite, not re-diffed). Definition/AuthenticationConfiguration/Filters/URL/ARN all confirmed correct. Found and fixed a real gap: PutWebhookInput.Tags was parsed into the handler's `in.Tags` but never passed to the backend (never stored) and never included in either PutWebhook's or ListWebhooks' response -- real PutWebhookOutput.Webhook/ListWebhookItem both carry Tags as a top-level member alongside Definition. Now stored (reusing the same Webhook.Tags map[string]string field TagResource/UntagResource/ListTagsForResource already used, previously write-only from PutWebhook's perspective) and returned under the real 'tags' key on both operations. ErrorCode/ErrorMessage (real ListWebhookItem members reporting third-party registration failures) remain unpopulated -- see gaps. LastTriggered is real-shaped as a string here but is never actually set by anything (no HTTP listener models an inbound webhook POST) so the type mismatch with the real *time.Time is latent, not yet observable -- flagged in Notes."}
  customActionTypes: {status: FIXED, note: "gopherstack-ohm3 follow-up pass (2026-07-30). CreateCustomActionType/ListActionTypes use the legacy types.ActionType shape (id/actionConfigurationProperties/inputArtifactDetails/outputArtifactDetails/settings) and remain genuinely clean, unchanged. GetActionType/UpdateActionType use a DIFFERENT real shape, types.ActionTypeDeclaration (confirmed directly against api_op_GetActionType.go/api_op_UpdateActionType.go: GetActionTypeOutput.ActionType and UpdateActionTypeInput.ActionType are both *types.ActionTypeDeclaration, never *types.ActionType) -- the prior pass's claim that GetActionType was 'genuinely clean' verified it against the WRONG real type (ActionType instead of ActionTypeDeclaration); it had the identical bug UpdateActionType did. Both now build/parse the real ActionTypeDeclaration shape: Executor (*ActionTypeExecutor: Configuration+Type required, Type validated against the real ExecutorType enum {Lambda,JobWorker}, Configuration.LambdaExecutorConfiguration.LambdaFunctionArn required when present -- matches validateActionTypeExecutor/validateExecutorConfiguration/validateLambdaExecutorConfiguration exactly), Id (Category/Owner/Provider/Version all required, matching validateActionTypeIdentifier -- Owner is a required plain string here, NOT the ActionOwner enum ActionTypeId.Owner is), InputArtifactDetails/OutputArtifactDetails (required wrapper, present-vs-nil distinguished via pointer types since the real validator checks for nil not zero value), Description/Permissions/Properties/Urls (optional). New CustomActionType fields (Description/Executor/Permissions/Properties/Urls) store this; a record only ever created via the legacy path (never updated) has Executor/Permissions/Properties/Urls genuinely nil -- GetActionType omits them (matching the real serializer's own `if v.X != nil` behavior) rather than fabricating placeholder Executor data. UpdateActionType now does a REAL merge, not a blind overwrite: only the ActionTypeDeclaration-expressible fields are replaced; Settings/ConfigurationProperties/Tags (which this op's real input has no member to carry) are preserved from the existing record, verified by TestGetActionType_AfterUpdateActionType_ReturnsDeclarationData. ListActionTypesInput.RegionFilter is parsed but never applied (see gaps, low severity, unchanged)."}
  jobsAndThirdPartyJobs: {status: FIXED, note: "Re-diffed against types.Job/JobDetails/JobData/ThirdPartyJob/ThirdPartyJobDetails/ThirdPartyJobData this pass. Found and fixed: (1) PollForJobs' response only ever included {id, nonce} -- real Job also carries AccountId and Data{ActionTypeId, ...}; now includes both (GetJobDetails already included Data.ActionTypeId correctly, so this closes the same gap PollForJobs had). (2) PollForThirdPartyJobs' response used {id, nonce} -- real ThirdPartyJob is {ClientId, JobId}, a DIFFERENT shape: the field is named JobId (wire key 'jobId'), not 'id', and there is NO Nonce on this type at all -- real AWS deliberately withholds the nonce until GetThirdPartyJobDetails (gated behind the ClientId/clientToken pairing), so leaking it at poll time was also a real-data-exposure-shape bug, not just a naming one. Fixed the key name and removed the fabricated 'nonce'; ClientId itself remains unpopulated (see gaps -- both SDK members are technically optional so this doesn't 400 a real client, but a worker has no clientId to present later). (3) GetThirdPartyJobDetails' response was missing Data entirely (real ThirdPartyJobDetails.Data.ActionTypeId) -- now included, matching the GetJobDetails fix pattern. Residual gaps (ActionConfiguration/ArtifactCredentials/ContinuationToken/EncryptionKey/InputArtifacts/OutputArtifacts/PipelineContext on JobData/ThirdPartyJobData, and PutJobFailureResult/PutThirdPartyJobFailureResult discarding FailureDetails.Message/Type entirely) are real and NOT fixed this pass -- see gaps."}
  stageTransitions: {status: ok, note: "unchanged this pass; spot-verified via full test suite pass, not re-diffed against the SDK this pass either -- still deferred, see deferred"}
  ruleOps: {status: partial, note: "Re-diffed against types.RuleType/RuleExecutionDetail/ListRuleTypesOutput/ListRuleExecutionsOutput this pass. ListRuleExecutions returning an empty list for a known pipeline is genuinely correct/honest, not a stub: this backend has no condition-rule engine anywhere (see OverrideStageCondition), so there is never a real rule execution to report -- confirmed by reading the backend method per parity-principles.md rule 4, not just grepping for an empty return. Found a real gap in ListRuleTypes: real types.RuleType requires InputArtifactDetails (ArtifactDetails{MinimumCount, MaximumCount}) as a non-optional member; this backend's ListRuleTypes only ever sets 'id', omitting it entirely. NOT fixed this pass -- see gaps for why (no verified-correct per-rule-type artifact count to populate it with)."}
  approvalGate: {status: ok, note: "NEW in the 2026-07-23 pass: StartPipelineExecution/PutActionRevision/PutApprovalResult/RetryStageExecution/RollbackStage all now share one action-run engine (action_engine.go) that gates on Approval-category actions with a real system-generated token, exposed only via GetPipelineState (matching real AWS -- there is no other way for a real client to obtain it). This closed 3 of the 4 gaps and all 3 deferred items from the 2026-07-12 audit at once, since they all stemmed from the SAME missing action-state machine. Unchanged, not re-diffed this pass."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "OverrideStageCondition validates pipeline/stage/execution existence and conditionType but mutates no modeled state -- there is no condition-rule/before-entry-condition engine anywhere else in this backend to be inconsistent with (same class as ListRuleExecutions' deliberately scoped-down design). A full fix requires modeling BeforeEntry/OnFailure/OnSuccess as real StageDeclaration input (parsed by CreatePipeline) and StageState.{BeforeEntryConditionState,OnSuccessConditionState,OnFailureConditionState} as real, gating output that StartPipelineExecution/runPipelineActions produces and this op can then flip to Overridden -- a new subsystem, out of scope for this pass. See the rewritten backend comment in pipeline_state.go for the precise real mutation this would need to perform."
  - "ListActionTypes' RegionFilter request parameter is parsed but never applied -- low severity, since this backend already implicitly scopes ListActionTypes to the request-context region (there is no cross-region action-type catalog to filter within in the first place)."
  - "ListRuleTypes omits the real, required RuleType.InputArtifactDetails member entirely -- not fixed this pass because there is no AWS-documented deterministic MinimumCount/MaximumCount per rule provider (Deployment/LambdaInvoke/CloudWatchAlarm/VariableCheck) this pass could verify with confidence; guessing counts would be a fabrication, not a fix."
  - "webhooks: ListWebhookItem.ErrorCode/ErrorMessage (real members reporting third-party webhook-registration failures) are never populated -- this backend's RegisterWebhookWithThirdParty always succeeds, so there is genuinely never a failure to report (same honest-always-empty rationale as ListRuleExecutions)."
  - "jobsAndThirdPartyJobs: JobData/ThirdPartyJobData are only ever populated with ActionTypeId (fixed this pass, see families) -- ActionConfiguration, ArtifactCredentials (AWSSessionCredentials), ContinuationToken, EncryptionKey, InputArtifacts, OutputArtifacts, and PipelineContext are real members with no equivalent anywhere in this backend's Job model (no artifact-store, no STS-session-credential issuance, no pipeline-context propagation from the owning execution to its jobs). A real job worker driven against this backend could not actually do its job (fetch input artifacts, write output artifacts) from this data alone. Not fixed this pass -- large gap, same class as GetPipelineExecution's pre-existing ArtifactRevisions/Variables gap below."
  - "jobsAndThirdPartyJobs: PutJobFailureResult/PutThirdPartyJobFailureResult parse FailureDetails.Message but discard it entirely (`_ = message`), and never parse FailureDetails.Type/ExternalExecutionId at all. Not fixed this pass: neither Job nor JobDetails (the only read-back shapes for a job) has anywhere to surface a stored failure message in the first place in real AWS either -- failure detail surfacing happens via GetPipelineExecution/GetActionExecution-style action-execution records, which this service DOES model for normal pipeline actions (ActionExecution.Summary) but jobs (the job-worker-facing side of a custom/third-party action) are a separate, unlinked record here. Fixing this properly means linking Job records back to their originating ActionExecution, out of scope for this pass."
  - "ListDeployActionExecutionTargets always returns an empty list -- no deploy-target model exists (documented in source, consistent with ListRuleExecutions' scoped-down design). Unchanged this pass."
  - "GetPipelineExecution/ListPipelineExecutions omit ArtifactRevisions/Variables/SourceRevisions/StatusSummary/StopTrigger -- no artifact-store content model, pipeline-variable resolution engine, or stop-reason tracking exists anywhere else in this backend to source real values from (all are optional fields, SDK-safe to omit)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "OverrideStageCondition deep state modeling (see gaps) -- requires a condition-rule engine that does not exist anywhere in this backend."
  - "JobData/ThirdPartyJobData completeness (see gaps) -- requires an artifact-store content model and STS-style session-credential issuance, neither of which exist anywhere else in this backend."
  - "ArtifactRevisions/Variables/SourceRevisions/StatusSummary/StopTrigger completeness on GetPipelineExecution/ListPipelineExecutions -- requires an artifact-store content model / pipeline-variable resolution engine / stop-reason tracking, none of which exist anywhere else in this backend."
  - "stageTransitions family was NOT re-diffed against the SDK this pass (only webhooks/customActionTypes/jobsAndThirdPartyJobs/ruleOps were, per this pass's scope) -- still only spot-verified via the full test suite since the 2026-07-12 audit; next pass should re-diff it properly."
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

(The "this pass"/"unchanged this pass" language in the two sections above refers to the
2026-07-23 audit. See the dated section below for the 2026-07-30 pass.)

### 2026-07-30 pass: re-diff of webhooks/customActionTypes/jobsAndThirdPartyJobs/ruleOps + OverrideStageCondition

This pass's brief was narrower than 2026-07-23's: (1) determine what `OverrideStageCondition`
should actually mutate and either fix it or document the gap plainly, and (2) re-diff the
families the 2026-07-23 pass explicitly flagged as "spot-verified via test suite, NOT
re-diffed against the SDK" (`webhooks`, `customActionTypes`, `jobsAndThirdPartyJobs`,
`ruleOps` -- `stageTransitions` was in that same flagged list but fell outside this pass's
explicit scope and remains un-re-diffed, see `deferred`).

**`OverrideStageCondition`**: confirmed via `types.ConditionType`/`types.ConditionExecutionStatus`/
`types.StageState` that the real mutation this op performs is flipping
`StageState.{BeforeEntryConditionState,OnSuccessConditionState}.LatestExecution.Status` to
`Overridden`. Confirmed via `types.StageDeclaration` that this backend's `StageDeclaration`
has no `BeforeEntry`/`OnFailure`/`OnSuccess` members at all -- `CreatePipeline` never parses
them, so there is no `Conditions`/`RuleDeclaration` state anywhere, and `StageState` here has
no `BeforeEntryConditionState`/`OnSuccessConditionState`/`OnFailureConditionState` to flip in
the first place. This is a genuine, confirmed architectural gap, not a disguised stub with a
self-serving comment: building it would mean a new condition-rule evaluation subsystem
(parsing stage `Conditions`, gating stage entry on rule results, tracking per-execution
`ConditionState`/`RuleState`), not a field patch. Left as an honest validate-only no-op, with
the backend comment rewritten to name the exact real mutation and exact reason it can't
happen. The one real, containable bug found and fixed: `conditionType` validation only ever
accepted `BEFORE_ENTRY`; the real enum also has `ON_SUCCESS`, which was being wrongly rejected
with `ValidationException`. Both values are now accepted.

**`webhooks`**: `PutWebhookInput.Tags` was parsed but silently dropped -- never stored, never
returned from either `PutWebhook` or `ListWebhooks`, even though the underlying
`Webhook.Tags` field already existed and was fully load-bearing for the *separate*
`TagResource`/`UntagResource`/`ListTagsForResource` API family. Fixed: `PutWebhook` now
stores the supplied tags (full-replace semantics on each call, consistent with how `Filters`
and `Authentication` are already treated as a declarative full-replace on every `PutWebhook`,
not a merge), and both `PutWebhook`'s and `ListWebhooks`' responses now include the real
`tags` member.

**`customActionTypes`**: `CreateCustomActionType`/`GetActionType`/`ListActionTypes` are
genuinely clean -- confirmed field-by-field against `types.ActionType`/`ActionTypeId`. The
severe finding is `UpdateActionType`: see `gaps` for the full breakdown. In short, AWS
introduced a newer "custom action type with a Lambda/JobWorker executor" model
(`ActionTypeDeclaration`, requiring `Executor`) for this specific operation, and this
backend's `UpdateActionType` still speaks the older `CreateCustomActionType`-era shape
(`id`/`settings`/`configurationProperties`/`inputArtifactDetails`/`outputArtifactDetails`),
missing `Executor` (required), `Permissions`, `Properties`, `Urls`, and `Description`
entirely. A real SDK client's `UpdateActionType` request would always include `Executor`
(client-side validation middleware requires it before the request is even serialized), so
this handler doesn't reject such a request -- it just silently ignores the executor
configuration a real caller sent, meaning the update "succeeds" while dropping the one thing
the caller was most likely trying to change. Not fixed this pass -- a new-subsystem-sized gap.

**`jobsAndThirdPartyJobs`**: found and fixed three real wire-shape bugs (see `families` for
the fix details): `PollForJobs` was missing `accountId`/`data.actionTypeId` (real `Job` has
both); `PollForThirdPartyJobs` used the wrong field name (`id` instead of the real `jobId`)
AND leaked `nonce`, a field the real `ThirdPartyJob` type doesn't have at all (real AWS
deliberately withholds the nonce until `GetThirdPartyJobDetails`, gated behind the
`ClientId`/`clientToken` pairing -- a real, if emulator-irrelevant, security-model detail);
`GetThirdPartyJobDetails` was missing `data.actionTypeId` entirely. The larger residual gap
(`JobData`/`ThirdPartyJobData` missing `ActionConfiguration`/`ArtifactCredentials`/
`ContinuationToken`/`EncryptionKey`/`InputArtifacts`/`OutputArtifacts`/`PipelineContext`, and
`PutJobFailureResult`/`PutThirdPartyJobFailureResult` discarding `FailureDetails` entirely)
was not fixed -- see `gaps` for why each specifically can't be derived from what this backend
already models.

**`ruleOps`**: `ListRuleExecutions` returning an empty list is genuinely honest (confirmed by
reading the backend method, not just the empty-looking return -- same verification standard
`parity-principles.md` rule 4 asks for): this backend has no condition-rule engine anywhere
(see `OverrideStageCondition` above), so there is never a real rule execution to report.
`ListRuleTypes`, however, has a real gap: the real `types.RuleType` requires
`InputArtifactDetails`, and this backend's `ListRuleTypes` only ever populates `id`. Left
unfixed rather than guessed, since there is no AWS-documented deterministic
`MinimumCount`/`MaximumCount` per rule provider this pass could verify with confidence.

### 2026-07-30 follow-up pass: gopherstack-ohm3 (UpdateActionType real-shape fix)

Closed the SEVERE finding from the same-day audit pass above. Field-diffed
`types.ActionTypeDeclaration` and its `types.ActionTypeExecutor`/`types.ExecutorConfiguration`/
`types.JobWorkerExecutorConfiguration`/`types.LambdaExecutorConfiguration`/
`types.ActionTypeIdentifier`/`types.ActionTypeArtifactDetails`/`types.ActionTypePermissions`/
`types.ActionTypeProperty`/`types.ActionTypeUrls` members directly against
`aws-sdk-go-v2/service/codepipeline@v1.49.0`'s `types/types.go`, `serializers.go`,
`deserializers.go`, and `validators.go` (the last one to get required-vs-optional exactly
right, not guessed): `Executor`/`Id`/`InputArtifactDetails`/`OutputArtifactDetails` are
required on `ActionTypeDeclaration`; within `Executor`, `Configuration`/`Type` are required
and `Configuration.LambdaExecutorConfiguration.LambdaFunctionArn` is required whenever a
Lambda executor configuration is supplied; within `Id` (`ActionTypeIdentifier`),
`Category`/`Owner`/`Provider`/`Version` are all required (unlike the legacy `ActionTypeId`,
`Owner` here is a plain required string, not the `ActionOwner` enum).

**Also found while checking the read side (in scope per the ticket)**: `GetActionType` had
the identical bug. `api_op_GetActionType.go` confirms `GetActionTypeOutput.ActionType` is
`*types.ActionTypeDeclaration` -- the SAME newer shape `UpdateActionType` uses, not the legacy
`*types.ActionType` shape `CreateCustomActionType`/`ListActionTypes` use. The prior pass's
`customActionTypes` note asserting `GetActionType` was "genuinely clean" was verified against
the wrong real type. Fixed as part of this pass -- `handleGetActionType` now builds the real
`ActionTypeDeclaration` response shape, and now also validates `owner` as required (it wasn't
checked at all before), matching `validateOpGetActionTypeInput`.

New model types added (`models.go`): `ActionTypeExecutor`, `ActionTypeExecutorConfiguration`,
`JobWorkerExecutorConfig`, `LambdaExecutorConfig`, `ActionTypePermissions`,
`ActionTypeProperty` (distinct from the legacy `ActionConfigurationProperty` -- same role,
different real field set: `NoEcho`/`Optional` here vs `Required`/`Secret` there), and
`ActionTypeUrls` (distinct from the legacy `ActionTypeSettings` -- `ConfigurationUrl` has no
legacy equivalent, `ThirdPartyConfigurationUrl` has no declaration equivalent). `CustomActionType`
gained `Description`/`Executor`/`Permissions`/`Properties`/`Urls`, populated only by
`UpdateActionType` (never fabricated for records that only went through `CreateCustomActionType`
-- `GetActionType` omits `executor` etc. entirely for those, matching the real serializer's own
`if v.X != nil` omission behavior rather than inventing placeholder executor data).

**Merge, not overwrite**: `UpdateActionType`'s backend method used to fully replace the stored
record from whatever the handler built. Since the handler now only ever builds
`ActionTypeDeclaration`-shaped fields, a blind overwrite would have silently wiped the legacy
`Settings`/`ConfigurationProperties` set by `CreateCustomActionType` on every real client's
update -- data `ActionTypeDeclaration`'s real input has no member to even express clearing.
Real AWS's `UpdateActionType` cannot destroy data its own input shape can't carry, so this
backend now fetches the existing record and replaces only the `ActionTypeDeclaration`-owned
fields, leaving `Settings`/`ConfigurationProperties`/`Tags` untouched. Verified end-to-end by
`TestGetActionType_AfterUpdateActionType_ReturnsDeclarationData`
(`custom_action_types_test.go`): creates a type with legacy `settings`, calls `UpdateActionType`
with `executor`/`description`, confirms both the new declaration data (via `GetActionType`) and
the untouched legacy `settings` (via `ListActionTypes`) are present afterward.

**Grade**: this was the sole stated reason for the same-day A->B downgrade above ("Flagged as
the primary reason for this pass's overall grade (A->B)"). No other new gap was found or
introduced closing it. Restored to **A**.
