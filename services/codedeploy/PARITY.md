---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codedeploy
sdk_module: aws-sdk-go-v2/service/codedeploy@v1.37.0   # version audited against
last_audit_commit: 59ab8f6a                             # HEAD when this manifest was written
last_audit_date: 2026-07-12
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
  ContinueDeployment: {wire: ok, errors: ok, state: partial, note: "validates deployment exists and returns the correct empty envelope; does not model blue/green wait-state transitions (see gaps)"}
  SkipWaitTimeForInstanceTermination: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was missing the deploymentId existence check every sibling deployment-scoped op has; fixed"}
  BatchGetDeployments: {wire: ok, errors: n/a, state: ok, persist: ok, note: "same createTime/completeTime fix as GetDeployment"}
  BatchGetDeploymentInstances: {wire: ok, errors: ok, state: partial, note: "validates deployment exists; instance-level status is fabricated Succeeded (see gaps, no real instance/target simulation exists in this backend)"}
  BatchGetDeploymentTargets: {wire: ok, errors: ok, state: partial, note: "same fabricated-status caveat as BatchGetDeploymentInstances"}
  GetDeploymentInstance: {wire: ok, errors: ok, state: partial, note: "fabricated single Succeeded instance summary; see gaps"}
  GetDeploymentTarget: {wire: ok, errors: ok, state: partial, note: "fabricated single Succeeded target; see gaps"}
  ListDeploymentInstances: {wire: ok, errors: ok, state: partial, note: "always returns an empty list; see gaps"}
  ListDeploymentTargets: {wire: ok, errors: ok, state: partial, note: "always returns an empty list; see gaps"}
  PutLifecycleEventHookExecutionStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was missing the deploymentId existence check every sibling deployment-scoped op has; fixed"}
  CreateDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "createTime was UnixMilli int64, fixed to awstime.Epoch float64"}
  ListDeploymentConfigs: {wire: ok, errors: n/a, state: ok, persist: ok}
  DeleteDeploymentConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterApplicationRevision: {wire: partial, errors: ok, state: gap, note: "validates app exists but does not persist the revision; see gaps"}
  GetApplicationRevision: {wire: partial, errors: ok, state: gap, note: "echoes the input revision back verbatim instead of a stored one; missing optional revisionInfo field; see gaps"}
  ListApplicationRevisions: {wire: ok, errors: ok, state: gap, note: "always returns an empty list since revisions are never persisted; see gaps"}
  BatchGetApplicationRevisions: {wire: partial, errors: ok, state: gap, note: "validates app + batch-size limit but echoes input revisions rather than reading stored ones; see gaps"}
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
  OnPremisesInstance: {status: ok, note: "registerTime/deregisterTime epoch fix + error-code fix applied this pass"}
  ApplicationRevision: {status: gap, note: "no real revision storage backs Register/Get/List/BatchGet; see gaps below"}
  cross-service: {status: clean, note: "no shared pkgs/ or cli.go touches were needed; all fixes were internal to services/codedeploy"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ApplicationRevision family (RegisterApplicationRevision/GetApplicationRevision/ListApplicationRevisions/BatchGetApplicationRevisions) has no backing store: RegisterApplicationRevision only validates the app exists and discards the revision, so ListApplicationRevisions always returns an empty list and GetApplicationRevision just echoes the input back instead of a persisted record. Needs a revisions table (keyed by appName + revision identity) wired into backendSnapshot. (bd: unfiled)"
  - "Deployment-instance/target family (GetDeploymentInstance, GetDeploymentTarget, ListDeploymentInstances, ListDeploymentTargets, BatchGetDeploymentInstances, BatchGetDeploymentTargets) has no real instance/target participation model: Get* fabricate a single Succeeded record, List* always return empty, Batch* fabricate Succeeded for every requested ID regardless of whether that ID ever existed. This mirrors the deeper gap that CodeDeploy has no concept of EC2/on-premises instances actually executing a deployment (ec2TagFilters/onPremisesInstanceTagFilters on a DeploymentGroup are stored but never resolved against real instances). Fixing this exceeds this pass's ~2000 LOC budget; needs a dedicated instance-participation model, likely coordinated with how services/ec2 exposes instances. (bd: unfiled)"
  - "ContinueDeployment does not model blue/green wait-state (DeploymentWaitType READY_WAIT/TERMINATION_WAIT); it only validates the deployment exists and no-ops. Low-value to fix without the CreateDeployment lifecycle itself modeling a genuine in-progress/waiting state, since deployments complete synchronously today. (bd: unfiled)"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Deployment-instance/target family (see gaps) — deferred pending a real instance-participation model"
  - "ApplicationRevision storage (see gaps) — deferred pending a revisions table"
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset/Snapshot/Restore all close tags.Tags handles correctly on the three dirty tables (applications, deploymentGroups, onPremisesInstances)"}
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
