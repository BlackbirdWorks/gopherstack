---
service: emrserverless
sdk_module: aws-sdk-go-v2/service/emrserverless@v1.40.2
last_audit_commit: b0d0cfe0
last_audit_date: 2026-07-24
overall: A
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "config sub-object allowlist extended to cover every types.CreateApplicationInput sub-object (added identityCenterConfiguration/diskEncryptionConfiguration/jobLevelCostAllocationConfiguration/schedulerConfiguration -- previously silently dropped); clientToken idempotency retained from prior pass"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: stateDetails (a real, optional types.Application response field) was entirely absent from Application/applicationToMap -- now present-if-non-empty, matching the architecture field's convention; ExtraConfig sub-objects echoed"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination via pkgs-style opaque index token; states filter ok; ExtraConfig + stateDetails echoed via applicationToMap"}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH merges config sub-objects into ExtraConfig (shallow per-top-level-key replace, matching AWS partial-update semantics); now covers the same extended sub-object allowlist as CreateApplication"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while STARTED/STARTING/STOPPING/CREATING; cascades job runs + sessions; cleans sessionTokens + jobRunTokens for the deleted app"}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: state-machine switch no longer references the invented ApplicationStateTerminatedWithError sentinel (see gaps history -- deleted this pass, not a real ApplicationState enum value)"}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ApplicationStateTerminatedWithError cleanup as StartApplication"}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed real wire-shape bug: JobRun response (GetJobRun/ListJobRuns) was emitting the request-only field name \"executionRoleArn\" instead of the actual response field \"executionRole\" (confirmed against awsRestjson1_deserializeDocumentJobRun/JobRunSummary in the SDK's deserializers.go -- a real AWS SDK client parsing gopherstack's response would get a nil ExecutionRole). Also fixed: the required response field createdBy was entirely absent (now populated with the execution role ARN as a best-effort substitute, matching the convention already used by ListJobRunAttempts); executionIamPolicy/executionTimeoutMinutes/retryPolicy (real StartJobRunInput fields) were silently dropped -- now stored and echoed, with executionTimeoutMinutes defaulting to 720 per the documented AWS behavior when unset"}
  GetJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns executionRole (fixed key)/createdBy/executionTimeoutMinutes/jobDriver/configurationOverrides/executionIamPolicy/retryPolicy"}
  ListJobRuns: {wire: ok, errors: ok, state: ok, persist: ok, note: "states filter + pagination ok; JobRunSummary shares jobRunToMap so gets the same executionRole/createdBy fixes"}
  CancelJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "route is DELETE /applications/{appId}/jobruns/{jobRunId}, confirmed correct; rejects terminal states"}
  GetDashboardForJobRun: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthesized console URL, no persisted state to round-trip"}
  ListJobRunAttempts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthesizes a single attempt (0) from the job run; documented limitation, not a bug -- backend does not model retries"}
  GetResourceDashboard: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed this pass against types.StartSessionInput/Output -- clientToken/executionRoleArn/configurationOverrides/idleTimeoutMinutes/name/tags all match; response root applicationId/arn/sessionId matches StartSessionOutput exactly"}
  GetSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against awsRestjson1_deserializeDocumentSession: applicationId/arn/createdAt/createdBy/executionRoleArn (NOT executionRole -- Session uses the opposite field name from JobRun, confirmed via deserializers.go)/releaseLabel/sessionId/state/stateDetails/updatedAt (all required) plus startedAt/endedAt/idleTimeoutMinutes/configurationOverrides/tags all present and correctly keyed; sessionToMap needed no fix"}
  ListSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "states + createdAtAfter/Before filters + pagination ok; SessionSummary shares sessionToMap's field set, all required SessionSummary fields present"}
  TerminateSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "response shape (applicationId/sessionId) matches TerminateSessionOutput exactly"}
  GetSessionEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "response shape (applicationId/sessionId/endpoint/authToken/authTokenExpiresAt) matches GetSessionEndpointOutput exactly"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "verified every op's REST path + HTTP method against emrserverless@v1.40.2 serializers.go: POST /applications, GET/PATCH/DELETE /applications/{id}, POST /applications/{id}/start|stop, POST/GET /applications/{id}/jobruns, GET/DELETE /applications/{id}/jobruns/{jobRunId}, GET .../dashboard, GET .../attempts, GET/POST/DELETE /tags/{resourceArn}, session sub-routes. All match; RouteMatcher's service-name disambiguation vs AppConfig (/applications collision) unaffected by this pass."}
  error_codes: {status: ok, note: "ErrNotFound->404 ResourceNotFoundException, ErrAlreadyExists->409 ConflictException, ErrValidation->400 ValidationException, ErrInvalidState->400 RequestFailedException, default->500 InternalFailure -- all mapped, no missing errCodeLookup entries found"}
  timestamps: {status: ok, note: "all createdAt/updatedAt/startedAt/endedAt/authTokenExpiresAt/jobCreatedAt use epochSeconds() (float64 Unix seconds), matching restjson1 epoch-seconds timestamp serialization -- no ISO8601 string bugs found"}
  session_family: {status: ok, note: "fully field-diffed this pass (previously only spot-checked/deferred) against types.Session/SessionSummary and every session op's Input/Output shape in the SDK module -- no bugs found; optional resource-usage fields (billedResourceUtilization/totalResourceUtilization/totalExecutionDurationSeconds/idleSince/networkConfiguration) are intentionally omitted since this backend does not simulate real resource billing, matching the same documented omission already accepted for JobRun/Application"}
gaps:
  - "JobRunState is missing the real SDK's QUEUED value (types.enums.go has SUBMITTED/PENDING/SCHEDULED/RUNNING/SUCCESS/FAILED/CANCELLING/CANCELLED/QUEUED); this backend's StartJobRun always starts a run in SUBMITTED and never transitions through QUEUED. Not fixed this pass (no client-visible bug -- the backend's job runs complete no real work, so there's no natural point at which QUEUED would be observed); flag for a follow-up bd issue if job-lifecycle simulation is ever added."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; sessionTokens/applicationTokens/jobRunTokens are plain in-memory maps cleaned up on DeleteApplication and full Reset(), and persisted/restored alongside the store.Table-backed resources -- no unbounded growth path found. Re-verified this pass: no new goroutines/tickers were introduced by the field additions."}
---

## Notes

Protocol: **restjson1**. All timestamps are epoch-seconds floats via `epochSeconds()`
(this package's local helper, semantically equivalent to `pkgs/awstime.Epoch` -- not
reused from that package because this handler builds ad-hoc `map[string]any` wire bodies
rather than typed structs; worth revisiting if `pkgs/awstime` gains a
`map[string]any`-friendly helper).

### Real bugs found and fixed this pass

1. **JobRun response used the wrong wire field name for the execution role**
   (`handler.go`'s `jobRunToMap`). `GetJobRun`/`ListJobRuns` were emitting
   `"executionRoleArn": jr.ExecutionRoleArn`, but the real API's *response* field for
   both `types.JobRun` and `types.JobRunSummary` is `"executionRole"` --
   `"executionRoleArn"` is only the field name on the `StartJobRunInput` *request* body
   (confirmed by reading both `awsRestjson1_deserializeDocumentJobRun` and
   `awsRestjson1_deserializeDocumentJobRunSummary` in the SDK module's
   `deserializers.go`, and cross-checking that `Session`'s response, by contrast,
   genuinely does use `executionRoleArn` -- these two shapes use opposite field names on
   the same concept, a real AWS API inconsistency, not a gopherstack bug to "fix" on the
   Session side). A real AWS SDK client parsing gopherstack's previous `GetJobRun`
   response would silently get a `nil` `ExecutionRole` field. Fixed by changing the map
   key; the internal Go field name (`JobRun.ExecutionRoleArn`) was left unchanged since
   it is also legitimately used for the *request*-side parsing in `StartJobRun`.

2. **JobRun response was missing the required `createdBy` field entirely**
   (`models.go`, `handler.go`). `types.JobRun.CreatedBy` and
   `types.JobRunSummary.CreatedBy` are both marked required response members, but
   `JobRun` had no `CreatedBy` field at all and `jobRunToMap` never emitted the key. This
   backend does not model IAM principals, so (matching the pre-existing convention
   already used by `ListJobRunAttempts`' synthesized attempt) `StartJobRun` now sets
   `CreatedBy` to the execution role ARN as a best-effort substitute.

3. **`Application` had no `stateDetails` field** (`models.go`, `handler.go`).
   `types.Application.StateDetails` is a real (optional) response field that was
   entirely unmodeled -- `applicationToMap` never had a `stateDetails` key at all, so an
   application that legitimately reaches a state with details attached (e.g. a failure
   message) could never surface it. Added `Application.StateDetails` and wired it into
   `applicationToMap` as present-if-non-empty, matching the existing `architecture`
   field's convention.

4. **`executionIamPolicy`/`executionTimeoutMinutes`/`retryPolicy` silently dropped by
   `StartJobRun`** (`models.go`, `job_runs.go`, `handler.go`). All three are real
   `StartJobRunInput` fields (`types.JobRunExecutionIamPolicy`, `*int64`,
   `types.RetryPolicy`) that were accepted by nothing and never echoed back. Fixed with
   the same opaque-passthrough pattern already used for `jobDriver`/
   `configurationOverrides`: `JobRun.ExecutionIamPolicy`/`JobRun.RetryPolicy` (stored
   verbatim) and `JobRun.ExecutionTimeoutMinutes` (defaulted to 720 when unset, matching
   the real API's documented default: "If no timeout was specified, then it returns the
   default timeout of 720 minutes.").

5. **`CreateApplication`/`UpdateApplication` config-sub-object allowlist was missing
   four real fields** (`handler.go`'s `applicationConfigFields`):
   `identityCenterConfiguration`, `diskEncryptionConfiguration`,
   `jobLevelCostAllocationConfiguration`, `schedulerConfiguration`. All four are real
   `types.CreateApplicationInput`/`types.UpdateApplicationInput`/`types.Application`
   sub-objects that were silently dropped (the previous pass's `gaps` entry flagged
   these as known-missing; this pass closes that gap by extending the same generic
   opaque-passthrough mechanism already used for the other ten sub-objects). With this
   change, `applicationConfigFields` now covers every sub-object field on the real
   `CreateApplicationInput`/`UpdateApplicationInput` shapes -- no remaining allowlist
   gap.

6. **Deleted invented `ApplicationStateTerminatedWithError` ("TERMINATED_WITH_ERRORS")**
   (`models.go`, `applications.go`, `applications_test.go`). This was not a real
   `ApplicationState` enum value (`types/enums.go` only defines
   CREATING/CREATED/STARTING/STARTED/STOPPING/STOPPED/TERMINATED) -- it was dead code
   referenced only by `StartApplication`/`StopApplication`'s terminal-state `switch`
   statements and one test case, and no code path ever set an application to this state.
   A prior audit pass flagged but deliberately left this in place as low-priority
   cleanup; this pass deletes the constant, the two dead `switch` cases that referenced
   it, and the test case that exercised it, per the project's no-invented-enum-values
   rule.

### Verified correct (no bug, but worth recording so the next audit doesn't re-flag)

- **Session family, fully field-diffed this pass** (previously only spot-checked and
  listed under `deferred`): every op's request/response shape
  (`StartSession`/`GetSession`/`ListSessions`/`TerminateSession`/`GetSessionEndpoint`/
  `GetResourceDashboard`) was compared field-by-field against
  `types.Session`/`types.SessionSummary` and each op's generated `api_op_*.go`
  Input/Output struct. No bugs found. Notably, `Session`'s response field really is
  `executionRoleArn` (unlike `JobRun`'s `executionRole` -- see bug #1 above), so
  `sessionToMap` needed no change.
- **Route matcher**: every op's REST path + HTTP method (`serializers.go` in the SDK
  module) matches `parseEMRPath` exactly, including the tricky ones: `UpdateApplication`
  is `PATCH /applications/{id}` (not POST), `StartApplication`/`StopApplication` are
  `POST /applications/{id}/start|stop`, and `CancelJobRun` is
  `DELETE /applications/{id}/jobruns/{jobRunId}` (not a POST-based cancel action).
  `RouteMatcher()`'s extra `Authorization`-header service-name check (disambiguating
  from AppConfig, which also serves `/applications`) is untouched and still correct.
- **CreateApplication application-name uniqueness check**: gopherstack rejects a second
  `CreateApplication` with a name already in use (`ConflictException`). This is **not**
  documented AWS behavior (the real API does not enforce unique application names; only
  `clientToken` gives idempotency) but is left as pre-existing behavior -- `clientToken`
  replay (added in a prior pass) means retried requests no longer hit this check, which
  was the main practical failure mode.
- **Pagination**: `emrPaginate` (index-based opaque `nextToken`, `maxResults` 1-50
  bounds-checked with `ValidationException` on violation) matches AWS's paginated-list
  contract for `ListApplications`/`ListJobRuns`/`ListJobRunAttempts`/`ListSessions`.
- **Timestamps**: every response field that AWS serializes as REST-JSON `timestamp`
  (epoch-seconds number, not ISO8601 string) uses `epochSeconds()` consistently --
  `createdAt`, `updatedAt`, `startedAt`, `endedAt`, `jobCreatedAt`,
  `authTokenExpiresAt`. No ISO8601-string-where-epoch-expected bugs found.
- **Error code mapping**: `handleError` maps all four sentinel errors
  (`ErrNotFound`/`ErrAlreadyExists`/`ErrValidation`/`ErrInvalidState`) to the correct
  HTTP status + AWS error code; no missing gap found (not-found paths all correctly
  return `ResourceNotFoundException`/404, not falling through to the 500
  `InternalFailure` default).
- **`CreateApplicationInput.Name` is optional on the real API** (not marked
  `required` in `api_op_CreateApplication.go`), but gopherstack's `CreateApplication`
  rejects an empty name with `ValidationException`. This is a stricter-than-AWS
  defensive check, not an invented field/error (it doesn't reject anything a
  spec-compliant client would ever legitimately send in practice), so it was left as-is
  rather than loosened -- flagged here only for visibility, not as a gap.
