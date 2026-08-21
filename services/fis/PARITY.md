---
service: fis
sdk_module: aws-sdk-go-v2/service/fis@v1.40.4   # version audited against
last_audit_commit: f8a54fdb                       # HEAD when this manifest was written
last_audit_date: 2026-07-31
overall: A            # genuine wire/error-code fixes found and applied
ops:
  CreateExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: experimentReportConfiguration now accepted + persisted}
  GetExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: experimentReportConfiguration now returned}
  UpdateExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: experimentReportConfiguration now accepted (wholesale replace) + persisted}
  DeleteExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades target-account-configs + idempotency-token entries}
  ListExperimentTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  StartExperiment: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'experimentOptions.actionsMode (run-all/skip-all) now accepted; template/lever/quota check-and-insert race fixed; see Notes'}
  GetExperiment: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'experimentReport/experimentReportConfiguration now returned; ExperimentTarget now carries filters/resourceTags/selectionMode; ExperimentAction now carries description; see Notes'}
  StopExperiment: {wire: ok, errors: ok, state: ok, persist: ok, note: 'was wrongly 409 ConflictException on not-running; StopExperiment has no ConflictException case in the SDK — fixed to 400 ValidationException (prior sweep); this sweep confirmed no regression'}
  ListExperiments: {wire: ok, errors: ok, state: ok, persist: ok, note: experimentTemplateId/status query filters applied before pagination}
  ListExperimentResolvedTargets: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: 'resolvedTargetDTO emitted invented resolvedArns/targetResourcesCount fields that do not exist on types.ResolvedTarget, and never paginated despite declaring nextToken; both fixed this sweep -- see Notes'}
  GetAction: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListActions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTargetResourceType: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTargetResourceTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSafetyLever: {wire: fixed, errors: ok, state: ok, persist: ok, note: 'removed gopherstack-invented "tags" field from the wire response — types.SafetyLever has no tags field in the real SDK; see Notes'}
  UpdateSafetyLeverState: {wire: fixed, errors: ok, state: ok, persist: ok, note: 'same "tags" field removal as GetSafetyLever'}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: 50-tag quota + aws:-prefix rejection enforced; safety-lever tag storage retained internally (see Notes)}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTargetAccountConfigurations: {wire: ok, errors: ok, state: fixed, persist: ok, note: 'declared nextToken but never paginated -- always returned the full list; fixed this sweep -- see Notes'}
  GetExperimentTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListExperimentTargetAccountConfigurations: {wire: ok, errors: ok, state: fixed, persist: n/a, note: 'same missing-pagination bug as ListTargetAccountConfigurations; fixed this sweep -- see Notes'}
families:
  route_matcher: {status: ok, note: 'RouteMatcher/parseFISPath path+method map verified 1:1 against every serializers.go SplitURI+request.Method in the pinned SDK; all 26 ops match exactly (an extra non-AWS POST /experiments/{id}/stop alias is additive and does not collide with any real route). Prior text said "25 ops"; GetSupportedOperations() has always returned 26 -- stale count, not a routing bug; corrected this sweep'}
  experiment_lifecycle: {status: fixed, note: 'real background goroutine state machine: pending→initiating→running→completed/stopped/cancelled/failed — matches types.ExperimentStatus exactly. A prior revision had invented a "completing" status/action-status pair not present in the real SDK enum; removed this sweep (see Notes). "cancelled" (real enum value, previously never emitted) is now used when StopExperiment interrupts an experiment before it reaches "running". actionsMode skip-all is now a real dry-run mode (all actions → "skipped", no fault rules/external calls). StopExperiment cancels via context; snapshot/restore cancels in-flight goroutines and marks non-terminal experiments failed (no stuck-pending disguised no-op)'}
  experiment_reports: {status: ok, note: 'ExperimentTemplateReportConfiguration (create/update/get on templates) and ExperimentReportConfiguration/ExperimentReport (on running experiments, inherited from the template at StartExperiment time) implemented end-to-end this sweep — see Notes. Was entirely unimplemented before (gaps/deferred item).'}
  error_taxonomy: {status: ok, note: 'four exception shapes (ValidationException/ResourceNotFoundException/ConflictException/ServiceQuotaExceededException@402) verified against deserializers.go this sweep; no regressions'}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - Experiment report generation is synchronous/immediate (terminal state computed the instant the owning experiment reaches a terminal status) rather than modeling the real async pending→running→completed/failed report lifecycle with its own timing. There is no real S3/CloudWatch backend to wait on in this emulator, so this is a reasonable simplification, not a wire-shape defect — the four modeled ExperimentReportStatus values pending/completed/cancelled/failed are all reachable (in the exact wire shape), "running" is skipped over.
  - CloudWatch dashboard snapshot capture (ExperimentReportConfigurationDataSources.CloudWatchDashboards) is accepted, validated, and echoed back on both the template and the running experiment's report configuration, but does not influence report generation (gopherstack has no real CloudWatch dashboard rendering to snapshot) — only the S3 output destination affects the generated ExperimentReportS3Report.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - Built-in action catalog completeness vs the full real AWS FIS action list (gopherstack ships a curated subset across EC2/RDS/ECS/EKS/DynamoDB/Lambda/SSM/network/CloudWatch/Kinesis + the aws:fis:inject-api-*/wait built-ins; real AWS has more actions per service and evolves this list independently of the API shape)
leaks: {status: clean, note: 'Restore() cancels in-flight experiment goroutines before replacing state; Shutdown() (service.Shutdowner) cancels all running experiments; janitor sweeps terminal experiments (completed/stopped/failed/cancelled) past TTL under the coarse lock with a pre-snapshotted slice so Delete-while-iterating is safe. No new goroutines/tickers were introduced for report generation — it is computed synchronously inside the same locked critical section that already finalizes the experiment''s terminal status (cleanupActions / markExperimentFailed), so there is nothing new to leak or drain on Shutdown.'}
---

## Notes

**Experiment reports were entirely unimplemented before this sweep** (a
previously-listed gap). Added end-to-end:

- `ExperimentTemplateReportConfiguration` (`dataSources.cloudWatchDashboards[].dashboardIdentifier`,
  `outputs.s3Configuration.{bucketName,prefix}`, `preExperimentDuration`,
  `postExperimentDuration`) on `CreateExperimentTemplate` / `UpdateExperimentTemplate`
  (input, wholesale-replace semantics on update) / `GetExperimentTemplate` /
  `ListExperimentTemplates` (output), field-diffed against
  `types.ExperimentTemplateReportConfiguration` and its `*Input` create/update
  counterparts, plus `deserializers.go`'s
  `awsRestjson1_deserializeDocumentExperimentTemplateReportConfiguration`.
- `ExperimentReportConfiguration` (identical shape, inherited from the template at
  `StartExperiment` time) and `ExperimentReport` (`s3Reports[].{arn,reportType}`,
  `state.{status,reason,error.code}`) on `GetExperiment` / `StartExperiment` output,
  field-diffed against `types.ExperimentReportConfiguration` / `types.ExperimentReport`
  / `types.ExperimentReportState` / `types.ExperimentReportError` and
  `deserializers.go`'s `awsRestjson1_deserializeDocumentExperimentReport*` family.
- Duration fields are validated as ISO 8601 durations (reusing the existing
  `isValidISODuration` used for action durations) — invalid values are rejected
  with `ValidationException` on create/update.
- Report generation: when an experiment inherits a report configuration and reaches
  a terminal status, the report's terminal state is computed synchronously in the
  same locked section that finalizes the experiment (`cleanupActions` /
  `markExperimentFailed`) — `completed` with one `ExperimentReportS3Report`
  (`reportType: "experiment-report"`, an `arn:aws:s3:::bucket/prefix/{expID}/report.json`
  ARN) when an S3 output destination is configured; `failed` with
  `error.code: "MissingReportOutputConfiguration"` when it is not; `cancelled` when
  the owning experiment itself was cancelled before it ever started running.
  `ExperimentReportS3Report.ReportType` and `ExperimentReportError.Code` are
  free-form strings in the real SDK model (no fixed enum), so these values label
  the artifact/failure without inventing a modeled enum member.

**A fabricated "completing" experiment/action status has been removed.** The real
`types.ExperimentStatus` enum is exactly `pending, initiating, running, completed,
stopping, stopped, failed, cancelled` — there is no `completing` value.
`types.ExperimentActionStatus` is exactly `pending, initiating, running, completed,
cancelled, stopping, stopped, failed, skipped` — same story. A prior gopherstack
revision invented `"completing"` as an extra broadcast state between `running` and
`completed` on both the experiment and every one of its actions; a real SDK client
polling `experiment.State.Status` could observe a status value that AWS FIS itself
never produces. `runExperiment` now transitions directly `running` → `completed`
(still pausing for `lifecycleDelay` internally so polling has a window to observe
`running`, just without broadcasting a fake intermediate label). The real
`"cancelled"` value — present in the enum but never reachable in gopherstack before
this sweep — is now emitted when `StopExperiment` interrupts an experiment still in
`pending`/`initiating` (before it reaches `running`), matching the real semantic
distinction between "cancelled before it started" and "stopped while running".

**`SafetyLever`'s wire response had a gopherstack-invented `"tags"` field.** The real
`types.SafetyLever` (confirmed via `deserializers.go`'s
`awsRestjson1_deserializeDocumentSafetyLever`) has exactly three fields: `arn`, `id`,
`state` — no `tags`. `GetSafetyLever` / `UpdateSafetyLeverState` were emitting a
`"tags"` key that no real AWS FIS response ever contains. Removed from
`safetyLeverDTO` / `toSafetyLeverDTO`. The safety lever's tags are still stored
internally (`SafetyLever.Tags`, an implementation detail used as the backing map
for the generic `TagResource`/`UntagResource`/`ListTagsForResource` operations
against its ARN, exactly as any other taggable FIS resource) — only the two direct
safety-lever response DTOs were leaking it onto the wire.

**`StartExperiment` gained `experimentOptions.actionsMode` (real field, previously
entirely absent).** `types.StartExperimentExperimentOptionsInput.ActionsMode` is a
real enum (`run-all` / `skip-all`, confirmed via `serializers.go`'s
`"actionsMode"` key and `enums.go`'s `ActionsMode` type) controlling whether an
experiment actually injects faults or only dry-run-validates its configuration.
gopherstack previously accepted no `experimentOptions` on `StartExperiment` at all.
Now: an invalid value is rejected with `ValidationException`; omitted defaults to
`run-all`; `skip-all` runs the full pending→initiating→running→completed lifecycle
(so stop conditions/targets/permissions can still be validated) but skips every
fault-rule application and every external action-provider call, setting every
action to `skipped` instead of `running`/`completed`. The resolved mode is echoed
back on `Experiment.ExperimentOptions.ActionsMode`.

**Running-experiment `ExperimentTarget` / `ExperimentAction` were missing fields
present on the real wire shape** (previously-listed gap, now fixed).
`types.ExperimentTarget` has `filters`, `resourceTags`, and `selectionMode` in
addition to `parameters`/`resourceArns`/`resourceType` (confirmed via
`deserializers.go`'s `awsRestjson1_deserializeDocumentExperimentTarget`);
`types.ExperimentAction` has `description` in addition to `actionId`/`parameters`/
`targets`/`startTime`/`endTime`/`state` (confirmed via
`awsRestjson1_deserializeDocumentExperimentAction`). Both are now carried through
from the owning template's target/action definitions when an experiment starts.

**A dead `startAfter` dependency-wait loop was removed from `executeActionsOrdered`.**
The loop's body (`if !completed[dep] { continue }`) had no effect on control flow —
`continue` inside the innermost `for` loop just advances to the next dependency
check, never affecting the outer action-execution loop. `topoSortActions` already
guarantees every action appears after all of its `startAfter` dependencies, so no
separate wait was ever needed; removed along with the now-write-only `completed`
map, rather than leaving dead code that misleadingly suggested real dependency
gating.

**`cleanupActions` was unconditionally clobbering the structured
`ExperimentStatusError` that `markExperimentFailed` had just set**, one call later
in the same code path. `runExperiment` called `markExperimentFailed` (setting
`Status: failed`, `Reason`, and the structured `Error{Code,Location,AccountID}`)
and then immediately called `cleanupActions(..., statusFailed, ...)`, which
unconditionally overwrote `exp.Status = ExperimentStatus{Status: expStatus}` —
discarding the `Reason` and `Error` that were only just set. This meant a real SDK
client reading `experiment.State.Error` on any provider-failure path would always
see `nil`, and `experiment.State.Reason` would always be empty, regardless of what
`markExperimentFailed` computed. Fixed: the failure path now calls a new
`releaseFaultRulesAndCancel` (fault-rule cleanup + context cancel only) instead of
`cleanupActions` after `markExperimentFailed`, so the structured error survives.
Covered by a new regression assertion in
`TestFISHandler_ExperimentFails_WhenActionProviderFails`.

**`StartExperiment`'s lever/quota/template-lookup checks and the experiment
insert had a TOCTOU race.** The checks (`safetyLever.State.Status`,
`experiments.Len() >= maxExperiments`, template lookup) were read under an
`RLock`, released, and then a separate `Lock` was taken to insert the new
experiment — two concurrent `StartExperiment` calls could both observe
`experimentCount < maxExperiments` before either had written, allowing the count to
exceed `maxExperiments`. Fixed: the checks and the `Put` now happen inside one
critical section under a single write lock.

**Duplicate `status`/`state` JSON keys are intentional and harmless, not a bug**
(unchanged from the prior sweep's finding — see `deserializers.go`'s
`case "state":` in `awsRestjson1_deserializeDocumentExperiment`; unrecognized keys
are silently ignored by every generated deserializer, so gopherstack's additional
`"status"` key does not break real clients).

**Route matcher** (unchanged from the prior sweep — still verified 1:1 against
every `serializers.go` `SplitURI`/`request.Method` pair across all 26 operations;
the prior text said "25 ops" — `GetSupportedOperations()` has always returned 26,
this was a stale count in the doc, not a routing gap — corrected this sweep).

**`resolvedTargetDTO` emitted a fabricated wire shape.** The real
`types.ResolvedTarget` (confirmed via `types/types.go` and the API reference) has
exactly three fields: `resourceType`, `targetName`, and `targetInformation` (a
generic `map[string]string` whose per-key contents AWS does not publicly
document beyond length/pattern constraints). gopherstack's `resolvedTargetDTO`
instead emitted invented `resolvedArns`/`targetResourcesCount` fields that do not
exist on the real type, and never populated `targetInformation` at all — a real
SDK client calling `ListExperimentResolvedTargets` would see an empty
`targetInformation` and none of the ARN/count data the emulator was returning
under made-up keys (unknown JSON keys are silently dropped by every generated
deserializer). Fixed: `resolvedTargetDTO` now emits the real three fields.
Because AWS does not publish the `targetInformation` key schema and gopherstack
does not model per-resource-type target metadata, `targetInformation` is
honestly left empty rather than inventing a key structure (e.g. stuffing ARNs
under a made-up `resourceArn` key) that would look official without being
verified against real AWS behavior — see `resolvedTargetDTO`'s doc comment in
`models.go`. The UI (`ui/src/routes/fis/+page.svelte`) has a documented
wire-augmented `ResolvedTarget` type that falls back to the old
`resolvedArns`/`targetResourcesCount` fields for its resolved-target count
display; that workaround can be removed once the UI is updated to read
`targetInformation` (or to stop displaying a fabricated-key-derived count) —
tracked as follow-up UI work, not fixed in this backend-only pass.

**`ListTargetAccountConfigurations`, `ListExperimentTargetAccountConfigurations`,
and `ListExperimentResolvedTargets` never paginated.** All three declare
`nextToken` on both the real SDK response and gopherstack's own response DTO, but
none of the three handlers called `paginateWithToken` — they always returned the
full list in one page and `nextToken` was always absent, unlike their siblings
(`ListExperimentTemplates`, `ListExperiments`, `ListActions`,
`ListTargetResourceTypes`), which paginate correctly. Fixed: all three handlers
now call `paginateWithToken`/`encodePageToken` exactly like their siblings.

**`ListExperimentTemplates` and `ListExperiments` over-wide summaries fixed.**
`ListExperimentTemplates` previously returned the full `experimentTemplateDTO`
(leaking targets, actions, stopConditions, logConfiguration, roleArn, and
experimentReportConfiguration) instead of `types.ExperimentTemplateSummary`
(`arn`, `creationTime`, `description`, `id`, `lastUpdateTime`, `tags`).
`ListExperiments` previously returned the full `experimentDTO` (leaking targets,
actions, stopConditions, roleArn, logConfiguration, startTime, endTime,
executionId, and experimentReport) instead of `types.ExperimentSummary`
(`arn`, `creationTime`, `experimentOptions`, `experimentTemplateId`, `id`,
`state`, `tags`). Dedicated `experimentTemplateSummaryDTO` and
`experimentSummaryDTO` structs now enforce exact SDK wire parity.

