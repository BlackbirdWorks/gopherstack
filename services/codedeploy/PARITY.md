---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codedeploy
sdk_module: aws-sdk-go-v2/service/codedeploy@v1.37.0   # version audited against
last_audit_commit: 59ab8f6a                             # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "createTime was UnixMilli int64, fixed to awstime.Epoch float64"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetApplications: {wire: ok, errors: n/a, state: ok, persist: ok, note: "same createTime fix as GetApplication"}
  CreateDeploymentGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeploymentGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDeploymentGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeploymentGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDeploymentGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetDeploymentGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "createTime/completeTime were UnixMilli int64, fixed to awstime.Epoch float64"}
  ListDeployments: {wire: ok, errors: n/a, state: ok, persist: ok, note: "createTimeRange.start/end request fields were parsed as epoch-millis (time.UnixMilli), fixed to epoch-seconds float64 matching smithytime.FormatEpochSeconds"}
  StopDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "output status was returning the deployment's own status literal (Stopped), fixed to the real StopStatus enum (Succeeded); deployment status itself still correctly becomes Stopped"}
  ContinueDeployment: {wire: ok, errors: ok, state: partial, note: "STILL OPEN: validates deployment exists and returns the correct empty envelope; does not model blue/green wait-state transitions (see gaps) -- genuinely not fixed this pass, see items_still_open"}
  SkipWaitTimeForInstanceTermination: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was missing the deploymentId existence check every sibling deployment-scoped op has; fixed"}
  BatchGetDeployments: {wire: ok, errors: n/a, state: ok, persist: ok, note: "same createTime/completeTime fix as GetDeployment"}
  BatchGetDeploymentInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: resolves real on-premises instances matched against the deployment group's tag filters (matchesOnPremisesTargeting), status derived from the deployment's own Status via targetStatusForDeployment instead of hardcoded Succeeded; unmatched/never-registered IDs silently omitted matching the codebase's Batch* convention"}
  BatchGetDeploymentTargets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: instanceTarget/ecsTarget/lambdaTarget union resolved from real deployment group config (on-prem tag matching / ECSServices list / single Lambda target); deploymentTargetType + nested member wire shape verified against types.DeploymentTarget deserializer"}
  GetDeploymentInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: real lookup against computed targets, InstanceDoesNotExistException for a real-but-non-participating or unknown instance instead of fabricating a match for any ID"}
  GetDeploymentTarget: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: real lookup against computed targets, new DeploymentTargetDoesNotExistException (404) sentinel for an unknown target ID instead of fabricating a match"}
  ListDeploymentInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: returns the real matched instanceTarget IDs for Server/Lambda platforms; empty for ECS (no per-instance concept) -- previously always empty regardless of real targets"}
  ListDeploymentTargets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: returns the real computed target IDs (sorted) -- previously always empty regardless of real targets"}
  PutLifecycleEventHookExecutionStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was missing the deploymentId existence check every sibling deployment-scoped op has; fixed"}
  CreateDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "createTime was UnixMilli int64, fixed to awstime.Epoch float64"}
  ListDeploymentConfigs: {wire: ok, errors: n/a, state: ok, persist: ok}
  DeleteDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterApplicationRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: persists to a real applicationRevisions store.Table keyed by (appName, canonical revision JSON); re-registering an already-known revision refreshes description, preserves original registerTime"}
  GetApplicationRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: reads the persisted revision, populates revisionInfo (GenericRevisionInfo: description/registerTime/firstUsedTime/lastUsedTime/deploymentGroups, field names+epoch-seconds verified against deserializers.go), new RevisionDoesNotExistException (404) for an unregistered revision instead of echoing the request back"}
  ListApplicationRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: returns real registered revisions for the application with deployed/s3Bucket/s3KeyPrefix/sortBy/sortOrder filtering; previously always empty since nothing was ever persisted"}
  BatchGetApplicationRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: genericRevisionInfo populated for revisions that are actually registered, omitted for ones that are not, instead of echoing the input unconditionally with no real lookup"}
  DeleteGitHubAccountToken: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGitHubAccountTokenNames: {wire: ok, errors: n/a, state: ok, persist: ok}
  RegisterOnPremisesInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterOnPremisesInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "ErrOnPremisesInstanceNotFound had the wrong error code (InstanceNameRequiredException) and no errorMappings entry at all, so it fell through to 500 ServiceException; fixed to InstanceDoesNotExistException + 404"}
  GetOnPremisesInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "same registerTime/deregisterTime epoch fix + errorMappings fix as DeregisterOnPremisesInstance"}
  ListOnPremisesInstances: {wire: ok, errors: n/a, state: ok, persist: ok}
  BatchGetOnPremisesInstances: {wire: ok, errors: n/a, state: ok, persist: ok, note: "same registerTime/deregisterTime epoch fix"}
  AddTagsToOnPremisesInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromOnPremisesInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcesByExternalId: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "no resource-by-external-id tracking exists anywhere in this backend (or any other gopherstack service); an idempotent no-op matches real AWS's own best-effort cleanup semantics"}
families:
  Application: {status: ok, note: "verified wire shapes (applicationId/applicationName/computePlatform/createTime), error codes, and persistence against aws-sdk-go-v2/service/codedeploy@v1.37.0 deserializers.go"}
  DeploymentGroup: {status: ok, note: "full field-by-field mapping (blue/green, alarms, triggers, ECS, tag sets, load balancer info) verified against dgToOutput/dgInputFromWire"}
  Deployment: {status: ok, note: "lifecycle is synchronous: CreateDeployment immediately sets status=Succeeded with completeTime = now+5s, which is a deliberate simplification (documented in Notes) rather than a bug"}
  DeploymentConfig: {status: ok, note: "9 built-in AWS default configs correctly seeded and protected from deletion (DeploymentConfigInUseException)"}
  Tags: {status: ok, note: "ARN-based dispatch to application/deploymentgroup tag stores verified; on-premises instance tagging is a separate, also-correct path"}
  OnPremisesInstance: {status: ok, note: "registerTime/deregisterTime epoch fix + error-code fix (earlier pass); this pass decomposed matchesTagFilters (banned gocognit nolint) and added matchesTagSetGroups/matchesOnPremisesTargeting, reused by the new deployment-target computation"}
  ApplicationRevision: {status: ok, note: "FIXED this pass: real applicationRevisions store.Table (composite key appName+canonical-revision-JSON, byApplication index), wired into backendSnapshot as a 'clean' table (no live tags.Tags field). RegisterApplicationRevision persists; CreateDeployment auto-registers an unseen revision and stamps FirstUsedTime/LastUsedTime/DeploymentGroups (touchApplicationRevisionForDeployment); DeleteApplication cascades deletes (deleteApplicationRevisions), UpdateApplication rename moves revisions to the new app name (renameApplicationRevisions) -- no ghost rows in either case"}
  DeploymentTarget: {status: ok, note: "FIXED this pass: GetDeploymentTarget/ListDeploymentTargets/BatchGetDeploymentTargets/GetDeploymentInstance/ListDeploymentInstances/BatchGetDeploymentInstances all resolve from deploymentTargets(), a real (not fabricated) computation over the deployment's owning deployment group: matched on-premises instances (Server), one target per configured ECS service (ECS), or the single Lambda target (Lambda) real AWS always has exactly one of for that platform. Target Status is mapped from the deployment's own current Status via targetStatusForDeployment instead of a hardcoded literal. KNOWN LIMITATION (documented, not silently fabricated): this backend has no live EC2 instance registry, so Ec2TagFilters/Ec2TagSet resolve zero instances -- only OnPremisesInstanceTagFilters/OnPremisesTagSet against b.onPremisesInstances are honored. A full EC2-instance-aware model would require cross-service coordination with services/ec2, out of scope for a single-service pass."}
  cross-service: {status: clean, note: "no shared pkgs/ or cli.go touches were needed; all fixes were internal to services/codedeploy. The DeploymentTarget EC2-instance limitation above is a documented boundary, not a cross-service touch."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ContinueDeployment does not model blue/green wait-state (DeploymentWaitType READY_WAIT/TERMINATION_WAIT); it only validates the deployment exists and no-ops. STILL OPEN after this pass -- see items_still_open in the audit receipt for the exact reason. Low-value to fix without the CreateDeployment lifecycle itself modeling a genuine in-progress/waiting state, since deployments complete synchronously today. (bd: unfiled)"
  - "DeploymentTarget/DeploymentInstance family's Server-platform resolution only covers on-premises instances (this service's own registry); Ec2TagFilters/Ec2TagSet on a deployment group match zero targets because this backend has no live EC2 instance registry to resolve them against. This is a real, documented boundary (not a fabricated/stubbed op) -- a full fix needs a dedicated instance-participation model coordinated with how services/ec2 exposes instances, which is cross-service scope this pass deliberately did not take on. (bd: unfiled)"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "ContinueDeployment blue/green wait-state modeling (see gaps) — deferred pending a genuine async deployment lifecycle, which is a larger rearchitecture than this pass's scope"
  - "Ec2TagFilters/Ec2TagSet resolution against real EC2 instances (see gaps) — deferred pending cross-service coordination with services/ec2"
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset/Snapshot/Restore all close tags.Tags handles correctly on the three dirty tables (applications, deploymentGroups, onPremisesInstances). The new applicationRevisions table carries no live handles (no tags.Tags field), so it needs no Close() calls -- registered as a 'clean' store.Table on b.registry like deployments/deploymentConfigs, reset via registry.ResetAll()."}
---

## Notes

- **Protocol**: awsjson1.1, single POST endpoint, `X-Amz-Target: CodeDeploy_20141006.<Op>`
  dispatch via `RouteMatcher`/`ExtractOperation` in handler.go. Verified every op in
  `GetSupportedOperations()` has a `dispatchTable()` entry and is reachable — no stub
  registrations, no ops silently dropped.

- **Epoch-seconds timestamp bug (the big one this pass)**: every `Timestamp` shape in
  CodeDeploy's model (`createTime`, `completeTime`, `registerTime`, `deregisterTime`, and the
  `createTimeRange.start`/`end` *request* filter) is serialized by the real SDK as an
  epoch-**seconds** JSON number (`smithytime.FormatEpochSeconds` / parsed with
  `smithytime.ParseEpochSeconds` — confirmed by reading
  `aws-sdk-go-v2/service/codedeploy@v1.37.0/deserializers.go` and `serializers.go` directly).
  The handler was using `time.Time.UnixMilli()` (epoch **milliseconds**, `int64`) for every
  response timestamp and `time.UnixMilli()` to parse the request-side range filter — a
  1000x wire-format mismatch in both directions. Fixed by switching every timestamp field to
  `float64` and using `pkgs/awstime.Epoch()` for output / a small `epochSecondsToTime` helper
  (mirrors the `secretsmanager` package's `time.Unix(0, int64(sec*float64(time.Second)))`
  pattern) for input. Proven with a real-SDK-client round-trip test
  (`handler_sdk_roundtrip_test.go`) rather than just unit-level JSON assertions, since a
  scale-wrong-but-well-typed number silently decodes to a garbage `time.Time` instead of
  erroring — exactly the kind of bug unit tests miss (see parity-principles.md rule 3).

- **StopDeploymentOutput.status is a *different* enum than Deployment.status**: real AWS's
  `StopStatus` enum is only ever `"Pending"` or `"Succeeded"` — it describes the outcome of
  the *stop request itself* (this backend performs it synchronously, so always
  `"Succeeded"`). The Deployment's own resulting lifecycle status (`"Stopped"`) is a
  completely separate field returned by `GetDeployment`. The handler was previously reusing
  `statusStopped` for both, which is not a valid `StopStatus` value and would fail real SDK
  unmarshaling into the `types.StopStatus` type in strict validation paths. This is a
  "looks-right-but-is-wrong" trap: don't re-flag `GetDeployment` returning `"Stopped"` after
  a stop — that part was and remains correct.

- **`ErrOnPremisesInstanceNotFound` sentinel had the wrong error code baked in**
  (`"InstanceNameRequiredException"`, which is actually a *different* real AWS exception for
  a missing/empty instance name) **and no `errorMappings` entry at all**, so any not-found
  lookup on an on-premises instance (`GetOnPremisesInstance`, `DeregisterOnPremisesInstance`)
  fell through to the generic 500 `ServiceException` branch regardless of the sentinel's own
  code — the exact "missing errCodeLookup entry" bug class called out in
  parity-principles.md rule 2. Fixed the sentinel's code to `InstanceDoesNotExistException`
  (confirmed against `types.InstanceDoesNotExistException` in the real SDK) and added the
  `errorMappings` row.

- **`SkipWaitTimeForInstanceTermination` and `PutLifecycleEventHookExecutionStatus` skipped
  the deployment-existence check** every sibling deployment-scoped op
  (`GetDeploymentInstance`, `GetDeploymentTarget`, `ListDeploymentInstances`,
  `ListDeploymentTargets`, `ContinueDeployment`, `StopDeployment`) performs via
  `h.Backend.GetDeployment(...)`. Both previously returned 200 OK for a nonexistent
  `deploymentId`. Fixed to match the sibling pattern; real AWS returns
  `DeploymentDoesNotExistException` in both cases.

- **Deliberate simplification, not a bug**: `CreateDeployment` sets `Status: "Succeeded"` and
  `CompleteTime: now + 5s` immediately at creation time rather than modeling a genuine
  `Created → Queued → InProgress → Succeeded` progression over time. `ListDeployments`'
  status filter and `StopDeployment`'s transition to `"Stopped"` both work correctly against
  this synchronous model. Don't re-flag this as a "stuck deployment" bug — it's the opposite
  problem (instant-complete, not stuck), and every consumer-visible field derived from it
  (`deploymentOverview`, status filters) is internally consistent.

- **`DeleteResourcesByExternalId` empty-envelope return is correct**, not a stub: this
  backend has no external-id-linked-resource tracking anywhere (nothing populates it), and
  the real AWS operation itself is a best-effort async cleanup with no required side effect
  visible to the caller synchronously. This is the "void-result op" pattern from
  parity-principles.md rule 4, not a disguised no-op.

- **ApplicationRevision family de-stubbed (this pass)**: `RegisterApplicationRevision`,
  `GetApplicationRevision`, `ListApplicationRevisions`, and `BatchGetApplicationRevisions`
  previously had no backing store at all — `RegisterApplicationRevision` validated the
  application existed and then discarded the revision, so `GetApplicationRevision` echoed the
  request straight back, `ListApplicationRevisions` always returned an empty list, and
  `BatchGetApplicationRevisions` echoed each requested revision with no real lookup. Added an
  `applicationRevisions *store.Table[ApplicationRevision]` keyed by
  `appName + "\x00" + canonical-JSON(RevisionLocation)` (`applicationRevisionKey` in
  store_setup.go — two `RevisionLocation` values with identical fields always produce the same
  key, matching how real CodeDeploy deduplicates revision registrations) with a `byApplication`
  index. Registered directly on `b.registry` as a "clean" table (no live `*tags.Tags` field, no
  DTO wrapper needed — see persistence.go's dirty-table doc comment). `CreateDeployment` now
  calls `touchApplicationRevisionForDeployment`, which auto-registers an unseen revision (real
  CodeDeploy auto-registers revisions supplied directly to `CreateDeployment`) and stamps
  `FirstUsedTime`/`LastUsedTime`/`DeploymentGroups` — the last of which also removes the
  deployment group from every *other* revision of the same application, since a deployment
  group targets exactly one revision at a time. `DeleteApplication`/`UpdateApplication` gained
  `deleteApplicationRevisions`/`renameApplicationRevisions` cascades so no ghost revision rows
  survive an app delete or outlive an app rename under the old name. Wire shapes
  (`genericRevisionInfo`/`revisionInfo`/`revisionLocation` field names, `GenericRevisionInfo`'s
  `registerTime`/`firstUsedTime`/`lastUsedTime` as epoch-seconds `float64`) verified against
  `aws-sdk-go-v2/service/codedeploy@v1.37.0/deserializers.go`'s
  `awsAwsjson11_deserializeDocumentGenericRevisionInfo`/`...RevisionInfo` and proven with a real
  SDK-client round trip (`Test_SDKRoundTrip_ApplicationRevision_EpochSeconds`). New sentinel
  `ErrRevisionNotFound` → `RevisionDoesNotExistException`, 404 (confirmed against
  `types.RevisionDoesNotExistException`).

- **Deployment-instance/target family de-stubbed (this pass)**: `GetDeploymentInstance`,
  `GetDeploymentTarget`, `ListDeploymentInstances`, `ListDeploymentTargets`,
  `BatchGetDeploymentInstances`, and `BatchGetDeploymentTargets` previously fabricated a
  `Succeeded` record for literally any requested ID (`Get*`/`Batch*`) or always returned an
  empty list (`List*`), regardless of whether that ID (or any target at all) actually existed.
  Added `(b *InMemoryBackend) deploymentTargets(d *Deployment) []DeploymentTargetRecord`
  (deployment_instances.go), computed on read from the deployment's owning deployment group's
  *real* configuration rather than persisted as a separate table (so it always reflects current
  on-premises instance/tag state, and needs no snapshot-version bump): for `ComputePlatform ==
  "Server"`, one `instanceTarget` per registered, non-deregistered on-premises instance whose
  tags satisfy the deployment group's `OnPremisesInstanceTagFilters`/`OnPremisesTagSet` (new
  `matchesOnPremisesTargeting`/`matchesTagSetGroups` helpers in on_premises_instances.go); for
  `"ECS"`, one `ecsTarget` per `ECSServices` entry (`clusterName/serviceName` as the deterministic
  `TargetID`); for `"Lambda"`, exactly one `lambdaTarget` keyed by the deployment ID itself
  (real CodeDeploy Lambda/ECS deployments always have exactly one and, respectively,
  `len(ECSServices)` targets — confirmed against
  `types.DeploymentTargetListSizeExceededException`'s doc comment). Every target's `Status` is
  derived from the deployment's own current `Status` via `targetStatusForDeployment`
  (`Succeeded`/`Failed`/`Skipped`/`InProgress`) instead of a hardcoded literal that ignored
  what actually happened to the deployment (e.g. a stopped deployment's targets now correctly
  report `Skipped`, proven by `TestDeploymentTargets_StatusTracksDeploymentStatus`). `Get*`/
  `GetDeploymentTarget` on an unresolvable ID now returns the new
  `ErrDeploymentTargetNotFound` → `DeploymentTargetDoesNotExistException` (404) sentinel instead
  of fabricating a match; `Batch*` silently omits unresolvable IDs, matching this codebase's
  established Batch* convention (`BatchGetApplications` etc.). `DeploymentTarget` union wire
  shape (`deploymentTargetType` PascalCase enum values `InstanceTarget`/`ECSTarget`/
  `LambdaTarget`, nested `instanceTarget`/`ecsTarget`/`lambdaTarget` member field names,
  `lastUpdatedAt` as epoch-seconds `float64`) verified against
  `awsAwsjson11_deserializeDocumentDeploymentTarget`/`...InstanceTarget`/`...ECSTarget`/
  `...LambdaTarget` and proven with a real SDK-client round trip
  (`Test_SDKRoundTrip_DeploymentTarget_EpochSeconds`). KNOWN, DOCUMENTED LIMITATION: this
  backend has no live EC2 instance registry, so `Ec2TagFilters`/`Ec2TagSet` resolve zero
  targets — only the on-premises side of `"Server"` targeting is modeled (see gaps).

- **Two banned `//nolint:gocognit,cyclop,funlen` removed via decomposition (this pass)**:
  `dgToOutput`/`dgInputFromWire` in handler_deployment_groups.go were single ~130-line
  functions doing wire-format conversion for every optional deployment-group sub-structure
  (load balancer info, blue/green config, alarms, auto-rollback, EC2/on-premises tag sets)
  inline. Split each into one `dgXToOutput`/`dgXFromWire` helper per sub-structure (7 helpers
  each direction); the top-level functions now just assemble the struct literal and loop over
  the flat slice fields, with zero behavior change (proven by the existing
  `deployment_groups_test.go` suite passing unmodified). One more banned
  `//nolint:gocognit` removed from `matchesTagFilters` in on_premises_instances.go by
  extracting the per-filter `KEY_ONLY`/`VALUE_ONLY`/default switch into `matchesOneTagFilter`
  (also fixed its comment, which incorrectly called the default case "EQUALS" — the real
  `TagFilterType` enum value is `KEY_AND_VALUE`, confirmed against `types.TagFilterType` in
  enums.go; the code was already correct, only the comment was wrong). All three were the
  full set of banned nolints flagged for this service — `grep -rnE
  'nolint:[a-z,]*(cyclop|gocyclo|gocognit|funlen)' services/codedeploy/` now returns empty.
