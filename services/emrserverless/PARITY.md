---
service: emrserverless
sdk_module: aws-sdk-go-v2/service/emrserverless@v1.44.4
last_audit_commit: b0d0cfe0  # this pass (2026-08-13, gopherstack-tuh5) fixed ListApplications/ListJobRuns/ListSessions Get-field leaks; commit hash not yet known at edit time
last_audit_date: 2026-08-13
overall: A
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "config sub-object allowlist extended to cover every types.CreateApplicationInput sub-object (added identityCenterConfiguration/diskEncryptionConfiguration/jobLevelCostAllocationConfiguration/schedulerConfiguration -- previously silently dropped); clientToken idempotency retained from prior pass"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: stateDetails (a real, optional types.Application response field) was entirely absent from Application/applicationToMap -- now present-if-non-empty, matching the architecture field's convention; ExtraConfig sub-objects echoed"}
  ListApplications: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-tuh5: was reusing applicationToMap (the full GetApplication converter) unscoped, leaking applicationId/tags plus every populated ExtraConfig sub-object (up to 14 keys -- maximumCapacity/networkConfiguration/autoStartConfiguration/etc, see applicationConfigFieldCount) that types.ApplicationSummary does not declare. The prior entry here verified only that ApplicationSummary's required fields were present and stopped there -- a one-direction check presented as a full wire verdict. Now emits types.ApplicationSummary (architecture/arn/createdAt/id/name/releaseLabel/state/stateDetails/type/updatedAt, confirmed against awsRestjson1_deserializeDocumentApplicationSummary) via a dedicated applicationSummaryToMap; pagination via pkgs-style opaque index token, states filter ok"}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH merges config sub-objects into ExtraConfig (shallow per-top-level-key replace, matching AWS partial-update semantics); now covers the same extended sub-object allowlist as CreateApplication"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while STARTED/STARTING/STOPPING/CREATING; cascades job runs + sessions; cleans sessionTokens + jobRunTokens for the deleted app"}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: state-machine switch no longer references the invented ApplicationStateTerminatedWithError sentinel (see gaps history -- deleted this pass, not a real ApplicationState enum value)"}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ApplicationStateTerminatedWithError cleanup as StartApplication"}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed real wire-shape bug: JobRun response (GetJobRun/ListJobRuns) was emitting the request-only field name \"executionRoleArn\" instead of the actual response field \"executionRole\" (confirmed against awsRestjson1_deserializeDocumentJobRun/JobRunSummary in the SDK's deserializers.go -- a real AWS SDK client parsing gopherstack's response would get a nil ExecutionRole). Also fixed: the required response field createdBy was entirely absent (now populated with the execution role ARN as a best-effort substitute, matching the convention already used by ListJobRunAttempts); executionIamPolicy/executionTimeoutMinutes/retryPolicy (real StartJobRunInput fields) were silently dropped -- now stored and echoed, with executionTimeoutMinutes defaulting to 720 per the documented AWS behavior when unset"}
  GetJobRun: {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "now returns executionRole (fixed key)/createdBy/executionTimeoutMinutes/jobDriver/configurationOverrides/executionIamPolicy/retryPolicy; 2026-08-21: required releaseLabel (types.JobRun) was dropped by an omitempty-style conditional whenever the owning application's own ReleaseLabel was an explicit empty string -- reachable, since CreateApplicationInput's validator only null-checks the ReleaseLabel pointer, never its content -- now always emitted; required jobDriver also now always emitted (fixed, not counted -- see gopherstack-r80d batch 20 note below for why no real client can observe the difference). See gopherstack-r80d batch 20 note below."}
  ListJobRuns: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-tuh5: was reusing jobRunToMap (the full GetJobRun converter) unscoped, leaking jobRunId/tags/executionTimeoutMinutes/jobDriver/configurationOverrides/executionIamPolicy/retryPolicy, none of which types.JobRunSummary declares. Now emits types.JobRunSummary (applicationId/arn/attempt/attemptCreatedAt/attemptUpdatedAt/createdAt/createdBy/executionRole/id/mode/name/releaseLabel/state/stateDetails/type/updatedAt, confirmed against awsRestjson1_deserializeDocumentJobRunSummary) via a dedicated jobRunSummaryToMap; states filter + pagination ok; 2026-08-21: same required-releaseLabel-dropped bug as GetJobRun, same fix -- see gopherstack-r80d batch 20 note below"}
  CancelJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "route is DELETE /applications/{appId}/jobruns/{jobRunId}, confirmed correct; rejects terminal states"}
  GetDashboardForJobRun: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthesized console URL, no persisted state to round-trip"}
  ListJobRunAttempts: {wire: ok (fixed), errors: ok, state: ok, persist: n/a, note: "synthesizes a single attempt (0) from the job run; documented limitation, not a bug -- backend does not model retries. 2026-08-21: the synthesized attempt's required releaseLabel/stateDetails (types.JobRunAttemptSummary) were hardcoded to empty string under a comment claiming neither was tracked by the backend -- false, both are already stored on the backing JobRun -- now mirrors jr.ReleaseLabel/jr.StateDetails. See gopherstack-r80d batch 20 note below."}
  GetResourceDashboard: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed this pass against types.StartSessionInput/Output -- clientToken/executionRoleArn/configurationOverrides/idleTimeoutMinutes/name/tags all match; response root applicationId/arn/sessionId matches StartSessionOutput exactly"}
  GetSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against awsRestjson1_deserializeDocumentSession: applicationId/arn/createdAt/createdBy/executionRoleArn (NOT executionRole -- Session uses the opposite field name from JobRun, confirmed via deserializers.go)/releaseLabel/sessionId/state/stateDetails/updatedAt (all required) plus startedAt/endedAt/idleTimeoutMinutes/configurationOverrides/tags all present and correctly keyed; sessionToMap needed no fix"}
  ListSessions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-tuh5: was reusing sessionToMap (the full GetSession converter) unscoped, leaking startedAt/endedAt/idleTimeoutMinutes/configurationOverrides/tags, none of which types.SessionSummary declares. The prior entry here verified only that SessionSummary's required fields were present and stopped there -- a one-direction check presented as a full wire verdict. Now emits types.SessionSummary (applicationId/arn/createdAt/createdBy/executionRoleArn/name/releaseLabel/sessionId/state/stateDetails/updatedAt, confirmed against awsRestjson1_deserializeDocumentSessionSummary) via a dedicated sessionSummaryToMap; states + createdAtAfter/Before filters + pagination ok"}
  TerminateSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "response shape (applicationId/sessionId) matches TerminateSessionOutput exactly"}
  GetSessionEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "response shape (applicationId/sessionId/endpoint/authToken/authTokenExpiresAt) matches GetSessionEndpointOutput exactly"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "verified every op's REST path + HTTP method against emrserverless@v1.40.2 serializers.go: POST /applications, GET/PATCH/DELETE /applications/{id}, POST /applications/{id}/start|stop, POST/GET /applications/{id}/jobruns, GET/DELETE /applications/{id}/jobruns/{jobRunId}, GET .../dashboard, GET .../attempts, GET/POST/DELETE /tags/{resourceArn}, session sub-routes. All match; RouteMatcher's service-name disambiguation vs AppConfig (/applications collision) unaffected by this pass."}
  error_codes: {status: ok, note: "ErrNotFound->404 ResourceNotFoundException, ErrAlreadyExists->409 ConflictException, ErrValidation->400 ValidationException, ErrInvalidState->400 RequestFailedException, default->500 InternalFailure -- all mapped, no missing errCodeLookup entries found"}
  timestamps: {status: ok, note: "all createdAt/updatedAt/startedAt/endedAt/authTokenExpiresAt/jobCreatedAt use epochSeconds() (float64 Unix seconds), matching restjson1 epoch-seconds timestamp serialization -- no ISO8601 string bugs found"}
  session_family: {status: fixed, note: "fully field-diffed against types.Session/SessionSummary and every session op's Input/Output shape in the SDK module; optional resource-usage fields (billedResourceUtilization/totalResourceUtilization/totalExecutionDurationSeconds/idleSince/networkConfiguration) are intentionally omitted since this backend does not simulate real resource billing, matching the same documented omission already accepted for JobRun/Application. This pass (gopherstack-tuh5): that field-diff covered presence of required fields but not absence of extras -- ListSessions was in fact leaking 5 Get-only members (see ops); a dedicated sessionSummaryToMap now scopes it correctly"}
  list_summary_shape: {status: fixed, note: "gopherstack-tuh5: ListApplications/ListJobRuns/ListSessions each reused their Get sibling's full converter (applicationToMap/jobRunToMap/sessionToMap) unscoped. Two prior audit entries (ListApplications, ListSessions) had verified only that each Summary type's required fields were present, and recorded wire: ok on that basis -- a correct check of one direction (presence) presented as a complete wire verdict; the other direction (absence of extras) was never checked, and gopherstack is a wire emulator seen by raw HTTP/non-SDK callers, not only SDK clients that happen to discard unrecognised keys. All three now have a dedicated *SummaryToMap converter built by reading that op's own types.*Summary struct and deserializer individually rather than assumed from a sibling; regression coverage in handler_list_summary_test.go asserts on the raw JSON body, not through an SDK client, which cannot observe this class of bug. codeartifact's sibling sweep in the same pass found a second bug class (a Summary member emitted under the wrong wire key, silently dropped by real deserializers) not present in emrserverless -- checked for here and not found: applicationSummaryToMap/jobRunSummaryToMap/sessionSummaryToMap key every field under the same name its own deserializer recognises."}
gaps:
  - "Fixed: JobRunState was missing the real SDK's QUEUED constant (types/enums.go:76-84 in aws-sdk-go-v2/service/emrserverless@v1.44.4, also emr-serverless/2021-07-13/service-2.json shapes.JobRunState, both list SUBMITTED/PENDING/SCHEDULED/RUNNING/SUCCESS/FAILED/CANCELLING/CANCELLED/QUEUED). Added JobRunStateQueued for enum completeness. The lifecycle itself is unaffected: StartJobRun still only ever produces SUBMITTED (or CANCELLED via explicit cancel) -- this backend does not model application capacity/scheduler configuration, which is the only real trigger for QUEUED (see JobRun.queuedDurationMilliseconds / SchedulerConfiguration.queueTimeoutMinutes in service-2.json), so nothing ever enters PENDING/SCHEDULED/RUNNING/SUCCESS/FAILED/CANCELLING/QUEUED either -- not just QUEUED. This is a self-consistent simplification (every client-polled field agrees the run stays SUBMITTED), not an instant-success bug; simulating job execution to make QUEUED observable is out of scope without job-lifecycle simulation (tracked separately if ever undertaken)."
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

### 2026-08-21: three required-output members dropped or hardcoded empty (gopherstack-r80d batch 20)

Verified as the largest remaining candidate after sagemaker (off-limits, an
unrelated conversion still uncommitted) via a fresh `cmd/requiredoutputfields`
run cross-checked against `services/_REQUIRED_OUTPUT_CANDIDATES.md` (both
agreed: emrserverless 25/22/14). The flat per-op scan (25 required fields
across 14 ops-with-required, 22 ops total) undercounts the real surface: an
AST-style walk of `types/types.go` (cross-checked three ways -- a
character-level brace matcher, a `go/parser` AST pass, and a raw
`grep -c "This member is required."`, all agreeing at 40 structs / 15 with
required members / 76 total required fields) finds `GetApplication`,
`UpdateApplication`, `GetJobRun`, `GetSession` each return a domain object
counted as **one** required field at the op level despite that object
itself carrying 7-11 required members, and `ListApplications`/`ListJobRuns`/
`ListJobRunAttempts`/`ListSessions` each return a list of such objects,
invisible to the per-op scan entirely -- the same "one wrapper key wraps a
whole domain object" class named by pinpoint/bedrockagent, compounded by
the "list of domain-struct elements" class named by omics/cleanrooms. The
gap (76 vs 25) is fully explained: 66 of the 76 domain-struct fields belong
to `Application`/`ApplicationSummary`/`JobRun`/`JobRunSummary`/
`JobRunAttemptSummary`/`Session`/`SessionSummary` (all reachable through
exactly the op-level wrapper fields above); the remaining 10
(`CloudWatchLoggingConfiguration`/`Configuration`/`Hive`/
`ImageConfiguration`/`InitialCapacityConfig`/`MaximumAllowedResources`/
`SparkSubmit`/`WorkerResourceConfig`) are all part of the
`applicationConfigFields`/`JobDriver`/`ConfigurationOverrides` opaque
echo-verbatim sub-objects this backend deliberately does not parse -- since
gopherstack never constructs these types itself (it stores and replays
whatever JSON the client sent), it cannot independently drop a required
member of one; if a client sends valid content, it survives untouched, and
if a client sends invalid/incomplete content that's a client bug the
backend has no way to detect or fabricate around. All 7 non-exempt domain
structs were read end to end against their handler's map-construction
function (`applicationToMap`/`applicationSummaryToMap`/`jobRunToMap`/
`jobRunSummaryToMap`/`sessionToMap`/`sessionSummaryToMap`/
`jobRunAttemptToMap` in `handler.go`/`session_handler.go`, plus
`job_run_attempts.go`'s attempt-construction), not grepped.

2 bugs counted, both `JobRun`/`JobRunSummary`/`JobRunAttemptSummary`'s
required `releaseLabel` dropped or fabricated-empty in a state a real
client can reach:

1. **`jobRunToMap`/`jobRunSummaryToMap` guarded `releaseLabel` behind
   `if jr.ReleaseLabel != ""`.** `JobRun.ReleaseLabel` is copied from the
   owning `Application.ReleaseLabel` at `StartJobRun` time
   (`job_runs.go`). `Application.ReleaseLabel` is itself copied verbatim
   from `CreateApplicationInput.ReleaseLabel`, whose real SDK
   `validateOpCreateApplicationInput` (validators.go) only checks
   `v.ReleaseLabel == nil` -- it never inspects the string's content, so a
   real client sending an explicit empty-string `ReleaseLabel` pointer
   passes client-side validation, reaches gopherstack (whose own
   `CreateApplication` only rejects an empty `name`/`type`, not an empty
   `releaseLabel`), and every job run started under that application then
   drops the required `releaseLabel` key entirely on `GetJobRun`/
   `ListJobRuns`. This is exactly the reachability wrinkle batch 19 named
   for cognitoidp: a validator's presence rules out nil, not content. Fixed
   by making both map builders always emit `releaseLabel` unconditionally
   (matching the convention `applicationToMap`/`sessionToMap` already used
   correctly for the same field). Proven via a real
   `aws-sdk-go-v2/service/emrserverless` client round trip
   (`TestGetJobRun_ReleaseLabelSurvivesEmptyApplicationReleaseLabel`,
   `wire_output_required_r80d_test.go`): `CreateApplication` with
   `ReleaseLabel: aws.String("")`, then `StartJobRun`/`GetJobRun`/
   `ListJobRuns`, asserting the typed `ReleaseLabel` field is non-nil
   (empty string, not omitted) on both the full and summary shapes.
   Hand-reverted (`handler.go` restored to `git show HEAD:...`), confirmed
   both assertions fail (`Expected value not to be nil`), restored,
   md5sum byte-identical.

2. **`ListJobRunAttempts`'s synthesized attempt hardcoded `releaseLabel`
   and `stateDetails` to `""`** (`job_run_attempts.go`), under a comment
   claiming neither field was "tracked by the backend, using sensible
   placeholders." Both claims were false: `JobRun.ReleaseLabel` and
   `JobRun.StateDetails` are both already stored on the backing `JobRun`
   this exact function reads six other fields from. Unlike bug #1, the
   wire key here was never dropped (the map literal always includes it) --
   this is a data-fidelity bug, not a dropped-key one, but it means real
   client-observable data was silently discarded for no reason on a
   required member. Fixed by reading `jr.ReleaseLabel`/`jr.StateDetails`
   directly, matching every other field in the same struct literal. Proven
   via a real SDK client round trip
   (`TestListJobRunAttempts_ReleaseLabelAndStateDetailsMirrorJobRun`):
   create an application with a real release label, start a job run,
   cancel it (which sets a real `StateDetails` message), then
   `ListJobRunAttempts` and assert the attempt's `ReleaseLabel`/
   `StateDetails` match the job run's, not empty. Hand-reverted,
   confirmed failing (`Not equal: expected "emr-6.6.0", actual ""`),
   restored, md5sum byte-identical.

**Fixed but NOT counted**: `jobRunToMap` also guarded `jobDriver` behind
`if jr.JobDriver != nil`. `JobDriver` is required on `types.JobRun` but
genuinely optional on `StartJobRunInput` (`validateOpStartJobRunInput` only
validates `JobDriver`'s content when non-nil, never requires it), so a real
client omitting it is a reachable state that dropped the required key --
the same class as bug #1. It was fixed (the key is now always present), but
unlike bug #1 this cannot be proven via any real client: reading
`awsRestjson1_deserializeDocumentJobDriver` shows its per-key `switch` over
the `"jobDriver"` object's own keys assigns nothing when that object is
empty, and the outer `awsRestjson1_deserializeDocumentJobRun`'s switch
skips the `"jobDriver"` case entirely when the key is absent from the
response body -- both paths leave the typed `JobDriver` field `nil` with no
observable difference. `TestGetJobRun_JobDriverKeyAlwaysPresent` documents
this (asserting the identical outcome under both configurations) rather
than asserting a provable regression, matching cognitoidp batch 19's
`AccountTakeoverActionType.Notify` precedent for a real-but-unprovable fix.

**Ruled out, not bugs**: `Application`/`ApplicationSummary`'s own
`releaseLabel` (unconditional in both `applicationToMap` and
`applicationSummaryToMap` already); `Session`/`SessionSummary`'s
`releaseLabel`/`stateDetails`/every other required member (`sessionToMap`/
`sessionSummaryToMap` build every required key unconditionally already,
confirmed by reading both functions in full); `JobRun`/`JobRunSummary`'s
own `stateDetails` (already unconditional in `jobRunToMap`/
`jobRunSummaryToMap`, unlike the neighboring `releaseLabel` bug -- read
carefully to confirm this wasn't the same bug twice); `GetSessionEndpoint`'s
5 required members (`applicationId`/`authToken`/`authTokenExpiresAt`/
`endpoint`/`sessionId`), all unconditional in `handleGetSessionEndpoint`;
`CancelJobRun`'s 2 required members, unconditional in
`handleCancelJobRun`; the 10 "echoed verbatim opaque sub-object" domain
structs named above, structurally exempt since gopherstack never
constructs their content itself. `JobRunAttemptSummary.Type` (not required
per the real SDK, confirmed against `types/types.go` -- gopherstack never
populates it, a data-completeness gap outside this cut's scope, not
flagged as a bug).

services/_REQUIRED_OUTPUT_CANDIDATES.md updated: emrserverless moved from
the ranked table into "Already examined" (settled-services count now 37,
2349 required output fields read end to end). networkmonitor (22,
ops=12/ops-with-required=7) is now the largest remaining candidate after
sagemaker (still off-limits this batch -- `git status` showed uncommitted
sagemaker changes both before and after this batch, from a concurrent
agent's in-flight conversion).
