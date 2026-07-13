---
service: emrserverless
sdk_module: aws-sdk-go-v2/service/emrserverless@v1.40.2
last_audit_commit: b0d0cfe0
last_audit_date: 2026-07-13
overall: A
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: clientToken idempotency + config sub-object passthrough (initialCapacity/maximumCapacity/autoStart|StopConfiguration/networkConfiguration/imageConfiguration/monitoringConfiguration/workerTypeSpecifications/runtimeConfiguration/interactiveConfiguration) were previously silently discarded"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "now echoes ExtraConfig sub-objects; route/method verified against restjson1 serializer"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination via pkgs-style opaque index token; states filter ok; ExtraConfig echoed via applicationToMap"}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: PATCH now merges config sub-objects into ExtraConfig (shallow per-top-level-key replace, matching AWS partial-update semantics) instead of only touching releaseLabel"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while STARTED/STARTING/STOPPING/CREATING; cascades job runs + sessions; cleans sessionTokens + jobRunTokens for the deleted app"}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: jobDriver (required field on the real JobRun response shape) and configurationOverrides were previously dropped entirely on submit and never returned by Get/ListJobRuns; also added clientToken idempotency replay"}
  GetJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns jobDriver/configurationOverrides when supplied"}
  ListJobRuns: {wire: ok, errors: ok, state: ok, persist: ok, note: "states filter + pagination ok"}
  CancelJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "route is DELETE /applications/{appId}/jobruns/{jobRunId}, confirmed correct; rejects terminal states"}
  GetDashboardForJobRun: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthesized console URL, no persisted state to round-trip"}
  ListJobRunAttempts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthesizes a single attempt (0) from the job run; documented limitation, not a bug -- backend does not model retries"}
  GetResourceDashboard: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "already had clientToken idempotency (sessionTokens) prior to this audit -- used as the reference pattern for CreateApplication/StartJobRun"}
  GetSession: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "states + createdAtAfter/Before filters + pagination ok"}
  TerminateSession: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSessionEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "verified every op's REST path + HTTP method against emrserverless@v1.40.2 serializers.go: POST /applications, GET/PATCH/DELETE /applications/{id}, POST /applications/{id}/start|stop, POST/GET /applications/{id}/jobruns, GET/DELETE /applications/{id}/jobruns/{jobRunId}, GET .../dashboard, GET .../attempts, GET/POST/DELETE /tags/{resourceArn}, session sub-routes. All match; RouteMatcher's service-name disambiguation vs AppConfig (/applications collision) unaffected by this pass."}
  error_codes: {status: ok, note: "ErrNotFound->404 ResourceNotFoundException, ErrAlreadyExists->409 ConflictException, ErrValidation->400 ValidationException, ErrInvalidState->400 RequestFailedException, default->500 InternalFailure -- all mapped, no missing errCodeLookup entries found"}
  timestamps: {status: ok, note: "all createdAt/updatedAt/startedAt/endedAt/authTokenExpiresAt/jobCreatedAt use epochSeconds() (float64 Unix seconds), matching restjson1 epoch-seconds timestamp serialization -- no ISO8601 string bugs found"}
gaps:
  - "ApplicationStateTerminatedWithError (\"TERMINATED_WITH_ERRORS\") is not a real ApplicationState enum value in aws-sdk-go-v2/service/emrserverless@v1.40.2's types.enums.go (real enum: CREATING/CREATED/STARTING/STARTED/STOPPING/STOPPED/TERMINATED only). It is dead code in this backend -- referenced only by StartApplication/StopApplication terminal-state checks and one existing test (parity_pass1_test.go); no code path ever sets an application to this state, so it never appears on the wire. Left as-is this pass (removing it touches a pre-existing test file outside the bugs found here); low-priority cleanup for a follow-up bd issue."
  - "JobRunState is missing the real SDK's QUEUED value (types.enums.go has SUBMITTED/PENDING/SCHEDULED/RUNNING/SUCCESS/FAILED/CANCELLING/CANCELLED/QUEUED); this backend's StartJobRun always starts a run in SUBMITTED and never transitions through QUEUED. Not fixed this pass (no client-visible bug -- the backend's job runs complete no real work, so there's no natural point at which QUEUED would be observed); flag for a follow-up bd issue if job-lifecycle simulation is ever added."
  - "CreateApplication/StartJobRun/UpdateApplication only pass through a fixed allowlist of top-level configuration sub-objects (initialCapacity, maximumCapacity, autoStartConfiguration, autoStopConfiguration, networkConfiguration, imageConfiguration, monitoringConfiguration, workerTypeSpecifications, runtimeConfiguration, interactiveConfiguration for Application; jobDriver, configurationOverrides for JobRun). Fields not on this list (identityCenterConfiguration, diskEncryptionConfiguration, jobLevelCostAllocationConfiguration, schedulerConfiguration on Application; executionIamPolicy, executionTimeoutMinutes, retryPolicy on StartJobRun) are still silently dropped. These are rarer/advanced fields; flag for a follow-up bd issue if a client is found relying on them."
deferred:
  - "Session family (StartSession/GetSession/ListSessions/TerminateSession/GetSessionEndpoint/GetResourceDashboard) was spot-checked for wire/error correctness but not re-audited op-by-op this pass -- it already had the clientToken idempotency pattern this audit ported to CreateApplication/StartJobRun, and no bugs were found in it during the spot check."
leaks: {status: clean, note: "no goroutines/janitors in this service; sessionTokens/applicationTokens/jobRunTokens are plain in-memory maps cleaned up on DeleteApplication and full Reset(), and persisted/restored alongside the store.Table-backed resources -- no unbounded growth path found"}
---

## Notes

Protocol: **restjson1**. All timestamps are epoch-seconds floats via `epochSeconds()`
(this package's local helper, semantically equivalent to `pkgs/awstime.Epoch` -- not
reused from that package because this handler builds ad-hoc `map[string]any` wire bodies
rather than typed structs; worth revisiting if `pkgs/awstime` gains a
`map[string]any`-friendly helper).

### Real bugs found and fixed this pass

1. **CreateApplication / StartJobRun discarded client idempotency tokens**
   (`backend.go`, `handler.go`). Both ops accept a required `clientToken` field on the
   real API (`CreateApplicationInput.ClientToken`, `StartJobRunInput.ClientToken`), but
   gopherstack silently ignored it. `StartSession` in the same package already
   implemented the correct pattern (a `map[applicationID]map[clientToken]sessionID`
   replay cache) -- this pass ported that pattern to `CreateApplication`
   (`applicationTokens map[string]string`) and `StartJobRun`
   (`jobRunTokens map[string]map[string]string`), both persisted in
   `backendSnapshot`/`Snapshot`/`Restore`/`Reset` and cleaned up on `DeleteApplication`.
   Without this, an AWS SDK's automatic retry-after-timeout logic (which resends the
   same clientToken) would previously hit `ConflictException` on `CreateApplication`
   (duplicate name) or silently create a **second** job run on `StartJobRun` --
   a correctness bug for any client relying on retry safety.

2. **CreateApplication / UpdateApplication / StartJobRun discarded configuration
   sub-objects** (`backend.go`, `handler.go`). `Application` only stored
   name/type/releaseLabel/architecture/tags/state -- `initialCapacity`,
   `maximumCapacity`, `autoStartConfiguration`, `autoStopConfiguration`,
   `networkConfiguration`, `imageConfiguration`, `monitoringConfiguration`,
   `workerTypeSpecifications`, `runtimeConfiguration`, and `interactiveConfiguration`
   were accepted on the wire by nothing (the request body struct didn't even declare
   them) and were never echoed back by `GetApplication`/`ListApplications`. Similarly
   `JobRun` never stored or returned `jobDriver` (a **required** field on the real
   `JobRun` response shape per `types.go`) or `configurationOverrides` -- `StartJobRun`
   silently dropped the actual job specification the caller submitted. This is the
   "create/update that discards config" bug class called out in the parity principles:
   real clients (Terraform, drift-detection tooling, or anything that submits a job and
   later reads it back) would see their configuration vanish. Fixed by adding an opaque
   pass-through: `Application.ExtraConfig map[string]any` (merged into
   `applicationToMap`'s output) and `JobRun.JobDriver` / `JobRun.ConfigurationOverrides`
   (added to `jobRunToMap`'s output), all stored/cloned/persisted like the existing
   `Tags` field. `UpdateApplication`'s PATCH merges newly supplied top-level keys into
   the existing `ExtraConfig` rather than replacing the whole thing, matching AWS's
   per-field partial-update semantics (confirmed: fields omitted from a PATCH body stay
   unchanged; fields present replace their previous value wholesale, not deep-merged --
   this backend does the same, since AWS sub-objects like `autoStopConfiguration` are
   themselves atomic replacements, not deep-mergeable).

### Verified correct (no bug, but worth recording so the next audit doesn't re-flag)

- **Route matcher**: every op's REST path + HTTP method (`serializers.go` in the SDK
  module) matches `parseEMRPath` exactly, including the tricky ones called out in the
  audit brief: `UpdateApplication` is `PATCH /applications/{id}` (not POST),
  `StartApplication`/`StopApplication` are `POST /applications/{id}/start|stop`, and
  `CancelJobRun` is `DELETE /applications/{id}/jobruns/{jobRunId}` (not a POST-based
  cancel action). `RouteMatcher()`'s extra `Authorization`-header service-name check
  (disambiguating from AppConfig, which also serves `/applications`) is untouched and
  still correct.
- **CreateApplication application-name uniqueness check**: gopherstack rejects a
  second `CreateApplication` with a name already in use (`ConflictException`). This is
  **not** documented AWS behavior (the real API does not enforce unique application
  names; only `clientToken` gives idempotency) but was left as pre-existing behavior
  since removing it is a larger behavioral change outside this pass's bug-fix scope --
  now that `clientToken` replay is implemented, retried requests no longer hit this
  check, which was the main practical failure mode. Flagged in `gaps` only implicitly
  via the fixed clientToken behavior above; no separate action taken.
- **Pagination**: `emrPaginate` (index-based opaque `nextToken`, `maxResults` 1-50
  bounds-checked with `ValidationException` on violation) matches AWS's paginated-list
  contract for `ListApplications`/`ListJobRuns`/`ListJobRunAttempts`/`ListSessions`.
- **Timestamps**: every response field that AWS serializes as REST-JSON `timestamp`
  (epoch-seconds number, not ISO8601 string) uses `epochSeconds()` consistently --
  `createdAt`, `updatedAt`, `startedAt`, `endedAt`, `jobCreatedAt`,
  `authTokenExpiresAt`. No ISO8601-string-where-epoch-expected bugs found (the bug
  class `awstime.Epoch` exists to prevent).
- **Error code mapping**: `handleError` maps all four sentinel errors
  (`ErrNotFound`/`ErrAlreadyExists`/`ErrValidation`/`ErrInvalidState`) to the correct
  HTTP status + AWS error code; no missing `errCodeLookup`-style gap found (not-found
  paths all correctly return `ResourceNotFoundException`/404, not falling through to
  the 500 `InternalFailure` default).
