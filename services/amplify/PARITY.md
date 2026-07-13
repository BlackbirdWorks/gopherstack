---
service: amplify
sdk_module: aws-sdk-go-v2/service/amplify@v1.40.0
last_audit_commit: c252f66
last_audit_date: 2026-07-13
overall: A            # real fixes found: stuck-state janitor, error-shape wire bug, missing jobArn field
ops:
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApp: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApps: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApp: {wire: partial, errors: ok, state: ok, persist: ok, note: "missing optional response fields (enableBranchAutoBuild, enableBasicAuth, environmentVariables, autoBranchCreationConfig, ...) -- see gaps"}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: was missing required jobArn field; job also used to stay RUNNING forever, now advanced to SUCCEED by janitor"}
  GetJob: {wire: partial, errors: ok, state: ok, persist: ok, note: "steps always [] (no build-step model); commitTime not modeled -- see gaps"}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: job created here was also missing jobArn"}
  CreateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: association used to stay PENDING_VERIFICATION forever, now advanced to AVAILABLE by janitor"}
  UpdateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  GetWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWebhooks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBackendEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateAccessLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "URL-only response, nothing to persist"}
  GetArtifactUrl: {wire: ok, errors: ok, state: ok, persist: ok}
  ListArtifacts: {wire: partial, errors: ok, state: ok, persist: ok, note: "always returns [] -- no artifact records are ever created by this backend (gap, see below)"}
families:
  routing: {status: ok, note: "every op's HTTP method + REST path verified 1:1 against aws-sdk-go-v2/service/amplify@v1.40.0 serializers.go SplitURI/request.Method calls (all 35 ops); no route-matcher bugs found -- POST-not-PUT for UpdateApp/UpdateBranch/UpdateDomainAssociation/UpdateWebhook already correct, tag ARN scoping (amplifyServiceIdentifier check) already correct"}
  errors: {status: fixed, note: "handleBackendError/amplifyErrorJSON now emit both the X-Amzn-Errortype header and a __type body field (see Notes) -- previously every error response (404/400/500) carried neither, so aws-sdk-go-v2 clients deserialized all Amplify errors as a generic UnknownError with empty code, breaking any errors.As(&types.NotFoundException{}) style handling"}
gaps:
  - "App response is missing several fields real Amplify always returns: enableBranchAutoBuild, enableBasicAuth, environmentVariables, autoBranchCreationConfig/Patterns, basicAuthCredentials, buildSpec, cacheConfig, customHeaders, customRules, iamServiceRoleArn, productionBranch, repositoryCloneMethod, wafConfiguration. None of these are modeled by the backend at all (CreateApp/UpdateApp inputs don't accept them either). Deliberately out of scope for this sweep (would require a much larger App model + input surface); noted for a future pass. (bd: TBD)"
  - "Branch model similarly omits enableBasicAuth, enablePerformanceMode, enablePullRequestPreview, buildSpec, customHeaders/rules, framework, ttl, associatedResources, backendEnvironmentArn, sourceBranch, totalNumberOfJobs, pullRequestEnvironmentName. (bd: TBD)"
  - "Stage enum (services/amplify/models.go) defines STAGING, which is not a real Amplify Stage value (real values: PRODUCTION, BETA, DEVELOPMENT, EXPERIMENTAL, PULL_REQUEST -- see types/enums.go). Stage is passed through as an unvalidated string end to end (no CreateBranch/UpdateBranch validation), so this doesn't cause a wire-shape bug today, but the constant should read BETA/PULL_REQUEST to match reality and CreateBranch/UpdateBranch should reject invalid stage values with a BadRequestException like real Amplify does. (bd: TBD)"
  - "JobSummary.commitTime (required field in the real SDK) is not modeled -- StartJobInput.commitTime is accepted by real Amplify but this backend's StartJob signature has no commitTime parameter, so it's silently dropped and the field is always omitted from responses. (bd: TBD)"
  - "GetJob's steps list is always empty ([]any{}); no build-step model exists. This is an intentional simplification (no real build pipeline behind the emulator), not a bug, but worth revisiting if a consumer depends on step-level detail. (bd: TBD)"
  - "ListArtifacts always returns an empty page because nothing in this backend ever creates an Artifact record (the artifacts table and ArtifactID-keyed GetArtifactUrl path exist, but there is no producer). Low priority: artifacts model actual build output, which this emulator does not produce. (bd: TBD)"
deferred:
  - "Full App/Branch field parity (see gaps above)"
  - "Server-side enum validation (Platform/Stage/JobType) returning BadRequestException for invalid values -- real Amplify validates these; this backend accepts any string"
leaks: {status: clean, note: "janitor.Run blocks on <-ctx.Done() and calls worker.Group.Stop() before returning, same lifecycle pattern as services/codebuild and services/batch; StartWorker only spawns the goroutine when a janitor was attached via WithJanitor (always true via provider.go), and it is bound to the process/JanitorCtx lifetime like every other service's janitor -- no per-request goroutines, no unbounded map growth (jobs/domains are only ever advanced in place, never leaked)"}
---

## Notes

Protocol: **restjson1**. Timestamps are Unix epoch-seconds `float64` (createTime/updateTime/startTime/endTime), not ISO8601 -- already correct throughout (toAppView/toBranchView/etc.).

### Real bugs fixed this sweep

1. **Jobs stuck RUNNING forever** (services/amplify/backend.go StartJob/StartDeployment). Nothing
   in the backend ever advanced a job's status past RUNNING -- StopJob/DeleteJob require an
   explicit caller action, and there was no equivalent of the async "build completes on its own"
   behavior real Amplify has. A client polling GetJob/ListJobs to wait for SUCCEED/FAILED would
   spin indefinitely. Fixed by adding a background Janitor (services/amplify/janitor.go, same
   pattern as services/codebuild and services/batch) that advances any non-terminal job to
   SUCCEED on each tick (default interval 5s -- much shorter than CodeBuild/Batch's 1-minute
   default since there's no per-service settings knob wired up for Amplify and polling loops in
   tests/clients shouldn't have to wait a full minute).

2. **Domain associations stuck PENDING_VERIFICATION forever** (services/amplify/backend.go
   CreateDomainAssociation). Same bug class: nothing ever advanced DomainStatus past
   PENDING_VERIFICATION, so GetDomainAssociation/ListDomainAssociations polling for AVAILABLE
   would spin indefinitely. Fixed by the same Janitor: advances any non-terminal domain
   association to AVAILABLE and marks every configured SubDomain Verified=true.

3. **Every error response was unclassifiable by aws-sdk-go-v2 clients**
   (services/amplify/handler.go amplifyError). The handler's error responses only ever set
   `{"message": "..."}` with no "X-Amzn-Errortype" header and no "__type"/"code" body field.
   aws-sdk-go-v2's generated restjson1 deserializer (see any
   `awsRestjson1_deserializeOpError*` func in the SDK's deserializers.go) resolves the response's
   exception type *only* from the header or a "code"/"__type" body field -- the HTTP status is
   only used to decide "this is an error", never to pick which typed exception to construct. With
   neither present, every gopherstack Amplify error (404 NotFoundException, 400
   BadRequestException, 500 InternalFailureException) deserialized client-side as a generic
   `*smithy.GenericAPIError{Code: "UnknownError"}`, breaking any caller that type-switches on a
   specific exception (a very common pattern, e.g. Terraform's delete-then-poll-for-404 waiters).
   Fixed by replacing `amplifyError(msg) map[string]any` with `amplifyErrorJSON(c, status, msg)`,
   which sets the `X-Amzn-Errortype` header and emits `{"__type": code, "message": msg}`, where
   `code` is derived from the HTTP status via `codeForStatus` (404->NotFoundException,
   400/405->BadRequestException, else->InternalFailureException). This mirrors the fix already
   applied to services/cleanrooms for the identical bug class.

4. **JobSummary was missing the required `jobArn` field** (services/amplify/backend.go
   StartJob/StartDeployment, services/amplify/models.go Job, services/amplify/handler_extended.go
   jobSummaryView/toJobSummaryView). Real Amplify's `types.JobSummary.JobArn` is a required
   response member; gopherstack's Job model never captured or returned one. Added `Job.JobARN`
   (built via `arn.Build` as `apps/{appId}/branches/{branchName}/jobs/{jobId}`, populated by both
   job-creating backend methods) and wired it through to the `jobArn` wire field.

### Verified clean (no bug, but worth recording so the next audit doesn't re-flag)

- **Routing**: every one of the 35 supported ops' HTTP method + REST path was diffed against
  `aws-sdk-go-v2/service/amplify@v1.40.0/serializers.go`'s `SplitURI(...)` / `request.Method =`
  pairs, 1:1. In particular UpdateApp/UpdateBranch/UpdateDomainAssociation/UpdateWebhook are all
  POST (not PUT) on the resource path, and the `/tags/{resourceArn}` handler already correctly
  scopes itself to ARNs containing `:amplify:` so it doesn't steal FIS's identically-prefixed
  `/tags/{arn}` requests. No route-matcher bugs found in this service.
- **Persistence**: Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore, which
  version-gate (`amplifySnapshotVersion`) and go through `store.Registry.SnapshotAll`/`RestoreAll`
  for every table (apps, branches, jobs, domains, webhooks, backendEnvironments, artifacts) --
  already correctly wired, nothing to fix.
- **enableAutoBuild vs enableBranchAutoBuild**: real Amplify uses two different field names for
  what looks like the same concept -- `enableBranchAutoBuild` on the App (default for new
  branches) vs `enableAutoBuild` on the Branch itself. gopherstack only models the Branch-level
  field (`enableAutoBuild`, correctly named); the App-level default isn't modeled at all (see
  gaps).
