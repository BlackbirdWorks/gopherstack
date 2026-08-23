---
service: amplify
sdk_module: aws-sdk-go-v2/service/amplify@v1.41.4
last_audit_commit: c807b481
last_audit_date: 2026-07-23
# 2026-08-21 gopherstack-r80d batch 14 (required-output cut): last_audit_commit
# left unchanged per this campaign's convention (the orchestrator, not this
# pass, creates the commit). 13 required-response-member bugs found and fixed
# at member granularity, grouped into 5 findings by root cause/wire struct --
# App.EnvironmentVariables/Description/Repository (3), Branch.ActiveJobId/
# CustomDomains/Description/Framework/EnvironmentVariables (5),
# DomainAssociation.StatusReason (1), Webhook.Description (1),
# JobSummary.CommitId/CommitMessage/CommitTime (3) -- see the dated 2026-08-21
# Notes entry below for the exact per-member breakdown and SDK file:line
# citations. All proven via real aws-sdk-go-v2/service/amplify client round
# trips (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/
# restored, md5sum-verified byte-identical. 1 additional finding
# (Branch.Stage) fixed but not counted -- see Notes for why no real-client
# test can distinguish it.
overall: A            # this sweep: full App/Branch field parity, Stage enum fix, commitTime,
                       # real build steps, real artifact producer + cascade delete, enum validation
ops:
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below. fixed 2026-08-21 (gopherstack-r80d batch 14): environmentVariables/description/repository are required response members that were tagged omitempty/omitzero and dropped whenever left unset -- a real client's typed field decoded nil instead of a present zero value; see Notes"}
  GetApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "same required-field presence fix as CreateApp (gopherstack-r80d batch 14)"}
  ListApps: {wire: ok, errors: ok, state: ok, persist: ok, note: "same required-field presence fix as CreateApp (gopherstack-r80d batch 14)"}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateApp, plus correct partial-update (nil-means-unchanged) semantics. Same required-field presence fix as CreateApp (gopherstack-r80d batch 14)"}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades jobs/artifacts/domains/webhooks/backendEnvironments, not just branches -- see leaks"}
  CreateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below. fixed 2026-08-21 (gopherstack-r80d batch 14): activeJobId/customDomains/description/framework/environmentVariables are required response members that were tagged omitempty and dropped whenever left unset/reachably-empty; see Notes"}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "same required-field presence fix as CreateBranch (gopherstack-r80d batch 14)"}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok, note: "same required-field presence fix as CreateBranch (gopherstack-r80d batch 14)"}
  UpdateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateBranch, plus correct partial-update semantics. Same required-field presence fix as CreateBranch (gopherstack-r80d batch 14)"}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades jobs/artifacts -- see leaks"}
last_audit_commit: 08bd3ef27
last_audit_date: 2026-08-19
overall: A            # 2026-08-19 wrapper-key/nested-shape sweep: DeleteApp/DeleteBranch now
                       # return the deleted resource (were bare 204s, dropping a required
                       # response member); GetArtifactUrl echoed the artifact TYPE under the
                       # "artifactId" key instead of the real ID; DomainAssociation and
                       # BackendEnvironment wire views each carried a fabricated "appId" key
                       # with no case in the real deserializer. See "Fixed this sweep
                       # (2026-08-19)" below. Prior sweep (2026-07-23): full App/Branch field
                       # parity, Stage enum fix, commitTime, real build steps, real artifact
                       # producer + cascade delete, enum validation.
ops:
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below"}
  GetApp: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApps: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateApp, plus correct partial-update (nil-means-unchanged) semantics"}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: response was a bare 204 No Content dropping DeleteAppOutput.App (a required member, api_op_DeleteApp.go:44) entirely -- a real client's out.App decoded nil; now returns {\"app\": <App>} of the app as it existed pre-delete. 2026-07-23: cascades jobs/artifacts/domains/webhooks/backendEnvironments, not just branches -- see leaks"}
  CreateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below"}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateBranch, plus correct partial-update semantics"}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: same bug as DeleteApp -- bare 204 dropped DeleteBranchOutput.Branch (required, api_op_DeleteBranch.go:44); now returns {\"branch\": <Branch>}. 2026-07-23: cascades jobs/artifacts -- see leaks"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: commitTime now modeled and round-trips; jobId+RETRY validated (BadRequestException if jobId absent, matches real StartJobInput) and inherits the retried job's commit metadata when the caller omits its own; jobType validated against the real JobType enum. fixed 2026-08-21 (gopherstack-r80d batch 14): commitId/commitMessage are required response members that were tagged omitempty and dropped when unset; commitTime -- also required -- was deliberately omitted whenever zero per this sweep's own design, which this batch reverses (falls back to the job's own StartTime instead of dropping the key); see Notes"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: steps now synthesizes one real BUILD step derived from the job's own status/timestamps (previously always []); commitTime now modeled -- see Notes for why one synthetic step, not a full per-stage model. Same commitId/commitMessage/commitTime presence fix as StartJob (gopherstack-r80d batch 14)"}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "same commitId/commitMessage/commitTime presence fix as StartJob (gopherstack-r80d batch 14)"}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades the job's own artifacts. Same commitId/commitMessage/commitTime presence fix as StartJob (gopherstack-r80d batch 14)"}
  StopJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same commitId/commitMessage/commitTime presence fix as StartJob (gopherstack-r80d batch 14)"}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same commitId/commitMessage/commitTime presence fix as StartJob (gopherstack-r80d batch 14)"}
  CreateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-21 (gopherstack-r80d batch 14): statusReason is a required response member that was tagged omitempty and dropped -- gopherstack never tracks a real reason (disclosed, honestly empty, not fabricated); see Notes"}
  UpdateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same statusReason presence fix as CreateDomainAssociation (gopherstack-r80d batch 14)"}
  DeleteDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same statusReason presence fix as CreateDomainAssociation (gopherstack-r80d batch 14)"}
  GetDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same statusReason presence fix as CreateDomainAssociation (gopherstack-r80d batch 14)"}
  ListDomainAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "same statusReason presence fix as CreateDomainAssociation (gopherstack-r80d batch 14)"}
  CreateWebhook: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-21 (gopherstack-r80d batch 14): description is a required response member that was tagged omitempty and dropped whenever the caller left it unset (CreateWebhookInput.Description is optional); see Notes"}
  UpdateWebhook: {wire: ok, errors: ok, state: ok, persist: ok, note: "same description presence fix as CreateWebhook (gopherstack-r80d batch 14)"}
  DeleteWebhook: {wire: ok, errors: ok, state: ok, persist: ok, note: "same description presence fix as CreateWebhook (gopherstack-r80d batch 14)"}
  GetWebhook: {wire: ok, errors: ok, state: ok, persist: ok, note: "same description presence fix as CreateWebhook (gopherstack-r80d batch 14)"}
  ListWebhooks: {wire: ok, errors: ok, state: ok, persist: ok, note: "same description presence fix as CreateWebhook (gopherstack-r80d batch 14)"}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: domainAssociationView carried a fabricated \"appId\" field with no case in the real deserializer -- types.DomainAssociation has no AppId member at all (types/types.go:542); removed. Applies to every op returning a DomainAssociation (Create/Update/Delete/Get/List)."}
  ListDomainAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  GetWebhook: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWebhooks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: backendEnvironmentView carried a fabricated \"appId\" field with no case in the real deserializer -- types.BackendEnvironment has no AppId member at all (types/types.go:230); removed. Applies to every op returning a BackendEnvironment (Create/Delete/Get/List)."}
  DeleteBackendEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBackendEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateAccessLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "URL-only response, nothing to persist"}
  GetArtifactUrl: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: the \"artifactId\" key (required string, api_op_GetArtifactUrl.go:39) carried InMemoryBackend.GetArtifactURL's first return value, which was artifact.ArtifactType (\"BUILD\") not the artifact's real ID -- same key, wrong value, no decode failure since both are strings. Now echoes artifact.ArtifactID."}
  ListArtifacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19: per-item Artifact wire view (artifactView) carried a fabricated \"artifactType\" field with no case at all in the real deserializer (types.Artifact has only ArtifactId/ArtifactFileName, types/types.go:157) -- removed. 2026-07-23: janitor.go now creates a real Artifact record (type BUILD, an internal-only bookkeeping field, never on the wire) for every job it advances to SUCCEED, indexed by job so ListArtifacts/GetArtifactUrl have real content -- see Notes"}
families:
  routing: {status: ok, note: "every op's HTTP method + REST path verified 1:1 against aws-sdk-go-v2/service/amplify@v1.40.0 serializers.go SplitURI/request.Method calls (all 35 ops); no route-matcher bugs found -- POST-not-PUT for UpdateApp/UpdateBranch/UpdateDomainAssociation/UpdateWebhook already correct, tag ARN scoping (amplifyServiceIdentifier check) already correct"}
  errors: {status: ok, note: "handleBackendError/amplifyErrorJSON emit both the X-Amzn-Errortype header and a __type body field; this sweep added a BadRequestException mapping for awserr.ErrInvalidParameter (the new Platform/Stage/JobType/RETRY-jobId validation errors) alongside the existing NotFoundException/AlreadyExists mappings"}
gaps:
  - "App: computeRoleArn, jobConfig, webhookCreateTime -- new optional response members added to types.App since the 2026-07-23 audit's v1.40.0 baseline (now v1.41.4); never emitted. Not required members, layer-3 (never-emitted), disclosed but not fixed this sweep per sweep scope."
  - "Branch: backend, computeRoleArn, destinationBranch, enableSkewProtection, thumbnailUrl -- same: new optional types.Branch members since v1.40.0, never emitted, layer-3, disclosed not fixed."
  - "JobSummary: sourceUrl, sourceUrlType -- optional members on types.JobSummary, never emitted (jobSummaryView), layer-3, disclosed not fixed."
  - "DomainAssociation: certificate, updateStatus, autoSubDomainCreationPatterns, autoSubDomainIAMRole -- optional types.DomainAssociation members, never emitted (domainAssociationView), layer-3, disclosed not fixed."
  # Every gap/deferred item from the 2026-07-23 audit was field-diffed against
  # aws-sdk-go-v2/service/amplify@v1.40.0/types and fixed for real that sweep.
  # The gaps above are new, surfaced by this sweep's field-diff against the
  # now-pinned v1.41.4 -- all are optional (non-required) response members
  # never emitted at all (layer 3), out of scope to fix per this sweep's
  # brief; none is a wrong key/shape/type bug.
deferred: []
  # "Full App/Branch field parity" and "server-side enum validation" (the two
  # prior deferred items) are both done this sweep -- see gaps history above.
leaks: {status: clean, note: "janitor.Run blocks on <-ctx.Done() and calls worker.Group.Stop() before returning, same lifecycle pattern as services/codebuild and services/batch; StartWorker only spawns the goroutine when a janitor was attached via WithJanitor (always true via provider.go), bound to the process/JanitorCtx lifetime. Fixed this sweep: DeleteApp previously cascaded only branches+tags, leaving jobs, domain associations, webhooks, and backend environments behind as ghost rows reachable by no legitimate path once the app 404s (an unbounded leak across create/delete churn in any long-running instance or test suite); DeleteBranch previously didn't cascade the branch's own jobs (or their artifacts) either. Both now cascade fully -- see InMemoryBackend.DeleteApp/deleteBranchLocked in apps.go and DeleteJob/DeleteBranch in jobs.go/branches.go. Every lock path remains defer-released; the new artifactsByJob store.Index (store_setup.go) adds no additional locking of its own, same invariant as every other index on this backend's single lockmetrics.RWMutex."}
---

## Notes

Protocol: **restjson1**. Timestamps are Unix epoch-seconds `float64` (createTime/updateTime/startTime/endTime/commitTime/lastDeployTime), not ISO8601 -- already correct throughout (toAppView/toBranchView/toJobSummaryView/toProductionBranchView/etc.), including every new timestamp field added this sweep.

### Fixed this sweep (2026-08-19)

Wrapper-key / nested-shape sweep against the pinned `aws-sdk-go-v2/service/amplify@v1.41.4`.
All 37 ops enumerated and re-verified (see method below); four real bugs found, each proven
by hand-revert (reintroduce the bug, confirm the new test reproduces the exact predicted
symptom, restore, confirm the source is byte-identical to before).

1. **DeleteApp/DeleteBranch dropped a required response member entirely** (services/amplify/
   handler_apps.go, handler_branches.go, apps.go, branches.go, interfaces.go). Real
   `DeleteAppOutput.App` and `DeleteBranchOutput.Branch` are both required members
   (api_op_DeleteApp.go:44, api_op_DeleteBranch.go:44) -- every Amplify Delete* op returns the
   deleted resource, not an empty body. gopherstack answered both with a bare
   `204 No Content`, so a real SDK client's `out.App`/`out.Branch` decoded as `nil` (the
   restjson1 deserializer tolerates an empty body via `io.EOF`, so the call itself doesn't
   error -- `err == nil`, silently wrong data, exactly the bug class this sweep hunts).
   `InMemoryBackend.DeleteApp`/`DeleteBranch` now return `(*App, error)`/`(*Branch, error)`
   (the view is computed via `appView`/`branchView` *before* the cascading delete, so it
   reflects the resource as it existed at deletion time); the handlers now answer
   `{"app": <App>}`/`{"branch": <Branch>}` at `200 OK`. `TestHandler_DeleteApp`/
   `TestHandler_DeleteBranch` previously asserted `http.StatusNoContent` -- a wrong-key test
   that locked in the bug -- corrected to assert `200` plus the wrapper key's presence. New
   tests: `TestDeleteApp_RoundTrip`, `TestDeleteBranch_RoundTrip` (wire_shape_test.go), driven
   through the real SDK client so the assertion is on the typed `out.App`/`out.Branch`, not a
   raw body. Hand-revert symptom: `out.App`/`out.Branch` nil (require.NotNil failure).

2. **GetArtifactUrl echoed the artifact TYPE under the "artifactId" key, not the artifact ID**
   (services/amplify/artifacts.go). `GetArtifactUrlOutput.ArtifactId` is a required string
   member that real Amplify echoes back (api_op_GetArtifactUrl.go:31,39).
   `InMemoryBackend.GetArtifactURL`'s first return value was `artifact.ArtifactType` (always
   `"BUILD"`) instead of `artifact.ArtifactID` -- the right JSON key, wrong value, no decode
   failure (both are plain strings, so a real client's `*out.ArtifactId` silently read
   `"BUILD"` for every artifact instead of its actual ID). Now returns `artifact.ArtifactID`.
   New test: `TestGetArtifactUrl_RoundTrip` (wire_shape_test.go), through the real SDK client.
   Hand-revert symptom: `out.ArtifactId == "BUILD"` instead of the real artifact ID.

3. **DomainAssociation and BackendEnvironment wire views each fabricated an "appId" key with
   no case at all in the real deserializer** (services/amplify/handler_domains.go,
   handler_environments.go). `types.DomainAssociation` (types/types.go:542) and
   `types.BackendEnvironment` (types/types.go:230) have no `AppId` member -- real Amplify
   never returns one for either resource type (it's implicit in the request path, not
   round-tripped in the body). `domainAssociationView`/`backendEnvironmentView` each carried
   an extra `AppID string \`json:"appId"\`` field with nothing corresponding to it in
   `awsRestjson1_deserializeDocumentDomainAssociation`/`...BackendEnvironment` -- the exact
   fabricated-member pattern this sweep's brief calls out (codepipeline `pipelineArn`, acm
   `KeyId`). Removed from both wire views (the internal `DomainAssociation.AppID`/
   `BackendEnvironment.AppID` Go struct fields are unaffected -- still used internally for
   backend indexing, just never serialized). Because the real SDK type has no field to
   observe this on, a typed-client assertion can't prove the fix; new tests
   `TestGetDomainAssociation_NoFabricatedAppID`/`TestGetBackendEnvironment_NoFabricatedAppID`
   (wire_shape_test.go) use a raw-body key-absence assertion instead, per the sweep's
   instrument-selection rule. Hand-revert symptom: `appId` key present in both response
   bodies.

**Method**: enumerated all 37 ops from `ls api_op_*.go` in the pinned SDK dir, matching 1:1
against `GetSupportedOperations`' 37-op list (`sdk_completeness_test.go` already enforces
this); confirmed protocol as `restjson1` from
`awsRestjson1_deserializeOp*` prefixes in `deserializers.go` (not from `_PROTOCOLS.md`, per
sweep brief); read every op's own `awsRestjson1_deserializeOpDocument<Op>Output` switch for
its top-level wrapper key, then every nested type's own `awsRestjson1_deserializeDocument<Type>`
switch for its full field/case list, diffed field-by-field against gopherstack's wire view
structs and `toXView` functions -- never generalized from a sibling op or sibling type.

### Fixed this sweep (2026-07-23)

1. **Full App field parity** (services/amplify/models.go App, apps.go, handler_apps.go). Field
   -diffed against `aws-sdk-go-v2/service/amplify@v1.40.0/types.App`. Added every field the prior
   audit flagged as missing except `wafConfiguration` (see "Verified correct as-is" below):
   `enableBranchAutoBuild` (defaults `true` on create, matching real Amplify), `enableBasicAuth`,
   `environmentVariables`, `autoBranchCreationConfig`/`autoBranchCreationPatterns`,
   `basicAuthCredentials`, `buildSpec`, `cacheConfig`, `customHeaders`, `customRules`,
   `iamServiceRoleArn`, `enableAutoBranchCreation`, `enableBranchAutoDeletion`. Also added two
   *computed*, never-persisted fields the real API always returns:
   `repositoryCloneMethod` (derived from whether `Repository` is set -- `TOKEN` or empty; real
   Amplify's SIGV4/SSH clone methods aren't modeled since this backend has no notion of repository
   provider) and `productionBranch` (the app's PRODUCTION-stage branch plus that branch's most
   recent job's status/start time, computed fresh on every GetApp/ListApps/CreateApp/UpdateApp by
   `InMemoryBackend.productionBranchFor` so it can never desync -- see leaks note on why it's
   deliberately not stored on the table record).
   CreateApp/UpdateApp's input surface grew to match: both now take an optional trailing
   `opts ...AppOptions` argument (see design note below) carrying every new field.

2. **Full Branch field parity** (services/amplify/models.go Branch, branches.go,
   handler_branches.go). Field-diffed against `types.Branch`. Added `enableBasicAuth` (was silently
   missing a *required* response member -- a real client dereferencing it as `*bool` would get a
   nil pointer instead of `false`), `enableNotification`, `enablePullRequestPreview`,
   `enablePerformanceMode`, `buildSpec`, `framework`, `ttl` (defaults `"5"`, matching real Amplify's
   5-minute default), `associatedResources`, `customDomains`, `backendEnvironmentArn`,
   `sourceBranch`, `pullRequestEnvironmentName`, `displayName` (defaults to the branch name). Also
   added two computed fields: `totalNumberOfJobs` (count of the branch's jobs) and `activeJobId`
   (its most-recently-started job), both computed fresh by `InMemoryBackend.branchView`. **Corrected
   a prior-audit error**: the earlier gap note claimed Branch was also missing `customHeaders`/
   `customRules` -- re-diffing `types.Branch` shows neither field exists on Branch at all (only on
   App); that note was simply wrong and has been dropped rather than "fixed" (adding them would
   have been inventing gopherstack-only fields).

3. **Stage enum had an invented value** (services/amplify/models.go). `StageStaging = "STAGING"`
   does not exist in real Amplify's `types.Stage` (`PRODUCTION, BETA, DEVELOPMENT, EXPERIMENTAL,
   PULL_REQUEST`). Renamed to `StageBeta = "BETA"` and added `StagePullRequest`. No other file in
   the repo referenced the old constant.

4. **Server-side enum validation** (deferred item, now done): CreateApp/UpdateApp validate
   `platform` against `isValidPlatform` (WEB/WEB_COMPUTE/WEB_DYNAMIC), CreateBranch/UpdateBranch
   validate `stage` against `isValidStage` (the corrected 5-value Stage enum), and StartJob
   validates `jobType` against `isValidJobType` (RELEASE/RETRY/MANUAL/WEB_HOOK) -- all three reject
   an unrecognized non-empty value with a 400 BadRequestException (`ErrValidation`, wired through
   `handleBackendError`'s existing `awserr.ErrInvalidParameter` branch), matching real Amplify. An
   empty string is still accepted everywhere as "caller didn't specify" and defaulted, matching the
   existing convention (e.g. Platform defaulting to WEB).

5. **StartJob was missing `commitTime` and RETRY support** (services/amplify/jobs.go,
   handler_jobs.go). `JobSummary.CommitTime` is a required response member in the real SDK;
   `StartJobInput.CommitTime` is now accepted (epoch-seconds in the request body, same as every
   other Amplify timestamp) and round-trips onto the created Job. Real Amplify also requires
   `jobId` when `jobType` is `RETRY` (`StartJobInput.JobId`, "required if jobType is RETRY" per the
   SDK doc comment) -- StartJob now validates this and, when the named prior job still exists,
   inherits its `commitId`/`commitMessage`/`commitTime` for any of those the caller left empty
   (matches "retry the same commit" semantics; a RETRY naming a job that's since been deleted still
   starts a fresh job rather than erroring, since gopherstack doesn't retain enough history to treat
   that as a hard failure).

6. **GetJob's `steps` was always `[]`** (services/amplify/handler_jobs.go). This backend has no
   real multi-stage build pipeline to model per-step (PROVISION/BUILD/DEPLOY/VERIFY) detail behind,
   so rather than fabricate stage data that doesn't correspond to anything real, `toStepViews` now
   synthesizes exactly one step (name `BUILD`) whose status and timestamps are derived directly from
   the job's own real state: `RUNNING` with `endTime == startTime` (a required response member, so
   an in-progress step still needs *a* value -- its own start time reads as "still going" rather
   than a fabricated zero) while the job runs, then the job's terminal status/EndTime once it
   completes. This is a deliberate, documented simplification (single-step build), not a stub: every
   value returned is real, not fabricated placeholder data.

7. **ListArtifacts had no producer** (services/amplify/artifacts.go, janitor.go, models.go
   Artifact, store_setup.go). Added `AppID`/`BranchName`/`JobID` to the `Artifact` model (needed to
   scope an artifact to the job that produced it -- previously absent, so there was no way to
   associate one even if created) and a `byJob` `store.Index` for the lookup. The janitor
   (`advanceJobs`) now creates one `BUILD`-type `Artifact` for every job it advances to `SUCCEED`,
   under the same write lock as the status transition. `ListArtifacts` now validates
   app/branch/job existence (previously only checked the app) and returns the real per-job list
   instead of an unconditional empty page; `GetArtifactUrl` was already correct and needed no
   change once real rows existed to look up.

8. **Leak: DeleteApp/DeleteBranch didn't cascade every child resource family** (see the `leaks`
   frontmatter entry above for the full description). DeleteApp now cascades jobs (and their
   artifacts), domain associations, webhooks, and backend environments in addition to the branches
   it already cascaded; DeleteBranch now cascades its own jobs (and their artifacts); DeleteJob now
   cascades its own artifacts. This is a genuine bug fix, not a gap-list item -- it was found while
   implementing the ListArtifacts producer above (a job/branch/app delete path that didn't clean up
   artifacts would otherwise immediately start leaking the new Artifact rows).

### Fixed 2026-08-21 (gopherstack-r80d batch 14): required response members dropped when empty

The 2026-07-23 sweep above field-diffed for *missing* fields (a required member with no struct
field at all, or never wired). It did not audit whether every field that *does* exist is ever
tagged `omitempty`/`omitzero` on a member the real SDK (`aws-sdk-go-v2/service/amplify@v1.41.4`)
marks `This member is required.` -- amplify's wire shape is almost entirely "one wrapper key = the
whole nested domain object" (`{"app": {...}}`, `{"branch": {...}}`, etc., the same class pinpoint/
bedrockagent/cleanrooms/inspector2 hit), so the real required surface lives in `types.go`'s
domain structs, not in the flat per-op `cmd/requiredoutputfields` count (35 fields / 37 ops, 33
ops-with-required per that tool). Read every domain struct in the pinned SDK's
`types/types.go` (App, Artifact, BackendEnvironment, Branch, DomainAssociation, Job, JobSummary,
Step, SubDomain, SubDomainSetting, Webhook -- 20 struct declarations, 63 required members total)
end to end against `handler_apps.go`/`handler_branches.go`/`handler_domains.go`/
`handler_jobs.go`/`handler_webhooks.go`'s wire-view structs, not grepped.

7 bugs found and fixed, each proven via a real `aws-sdk-go-v2/service/amplify` client round trip
(`wire_output_required_r80d_test.go`), hand-reverted/confirmed-failing/restored,
md5sum-verified byte-identical (`apps.go`/`branches.go` needed no changes and are confirmed
unchanged):

1. **`App.EnvironmentVariables`/`Description`/`Repository`** (`types/types.go:57,37,78`; wire keys
   confirmed against `deserializers.go:6250` `awsRestjson1_deserializeDocumentApp`'s
   `environmentVariables`/`description`/`repository` cases, all three deserializing into a
   pointer/map field with no zero-value fallback). All three are required on `App` but optional on
   `CreateAppInput` (`api_op_CreateApp.go:29`, only `Name` required) -- a real client creating an
   app without supplying them got a nil map/nil `*string` instead of a present empty value.
   `appView`'s tags dropped to plain (non-`omitempty`) in `handler_apps.go`; `toAppView` now
   nil-guards `EnvironmentVariables` to a non-nil `map[string]string{}` (needed for an app
   snapshotted before this map was tracked, per the "Verified correct as-is" persistence note
   below). `DefaultDomain` carried the same dead `omitempty` tag but is computed unconditionally
   non-empty at create time (`apps.go`'s `CreateApp`) and is never reachably empty through any real
   client path, so removing its tag is a harmless cleanup, not a counted bug (no test can
   distinguish the two states).
2. **`Branch.ActiveJobId`/`CustomDomains`/`Description`/`Framework`/`EnvironmentVariables`**
   (`types/types.go:270,290,295,330,325`; wire keys confirmed against
   `deserializers.go:7082` `awsRestjson1_deserializeDocumentBranch`'s
   `activeJobId`/`customDomains`/`description`/`framework`/`environmentVariables` cases). All
   required on `Branch` but optional/absent on `CreateBranchInput` (`api_op_CreateBranch.go:29`,
   only `AppId`/`BranchName` required) or, for `ActiveJobId`, computed by `branchView` and
   genuinely `""` for any branch with no jobs yet -- a fully reachable, unexceptional state, not an
   edge case. `CustomDomains` was never assigned anywhere in `branches.go` (always a nil slice).
   `branchView`'s tags dropped in `handler_branches.go`; `toBranchView` now nil-guards
   `EnvironmentVariables`/`CustomDomains` to non-nil empty values. `Stage` carried the same dead
   `omitempty` tag and was also dropped, but `Stage` is a non-pointer enum on the real SDK
   (`types.Stage`, not `*Stage`) -- a missing key and a present-but-empty key decode to the
   identical Go zero value (`""`) for any real client, so this is fixed (tag removed, harmless
   either way) but **not counted**: no real-client test can distinguish the two states. `TTL`/
   `DisplayName`/`TotalNumberOfJobs` carry the same dead tag but are never reachably empty (TTL
   defaults `"5"`, DisplayName defaults to the branch name which is itself required non-empty,
   TotalNumberOfJobs's zero state serializes as the non-empty string `"0"`) -- left as-is, no
   observable bug.
3. **`DomainAssociation.StatusReason`** (`types/types.go:568`; wire key confirmed against
   `deserializers.go`'s `awsRestjson1_deserializeDocumentDomainAssociation`'s `statusReason`
   case). Required on `DomainAssociation`, but `domains.go` never sets it anywhere (no
   certificate/DNS-propagation flow is modeled behind this emulator) -- always `""`, dropped
   entirely by `domainAssociationView`'s `omitempty` tag. This is the disclosed-non-fabrication-stub
   shape (an honest empty value beats a fabricated one): the fix removes the tag so the required
   key is always present, still carrying no invented content. `SubDomain.DnsRecord` carries the
   same dead tag but is computed unconditionally non-empty by `CreateDomainAssociation`/
   `UpdateDomainAssociation`, so it's never reachably empty -- not a bug.
4. **`Webhook.Description`** (`types/types.go:891`; wire key confirmed against
   `deserializers.go`'s `awsRestjson1_deserializeDocumentWebhook`'s `description` case). Required
   on `Webhook`, optional on `CreateWebhookInput` (`api_op_CreateWebhook.go:29`, only `AppId`/
   `BranchName` required) -- a webhook created without one got the key dropped by `webhookView`'s
   `omitempty` tag.
5. **`JobSummary.CommitId`/`CommitMessage`** (`types/types.go:692,697`; wire keys confirmed
   against `deserializers.go`'s `awsRestjson1_deserializeDocumentJobSummary`'s `commitId`/
   `commitMessage` cases). Required on `JobSummary`, but `StartJob` accepts both as optional
   strings with no default -- `jobSummaryView`'s `omitempty` tags dropped both whenever the
   caller supplied neither (e.g. a manually deployed app, `jobType: MANUAL`/`RELEASE` with no Git
   commit).
6. **`JobSummary.CommitTime`** (`types/types.go:702`; required). This one wasn't a missing-tag
   oversight -- the 2026-07-23 sweep's own item 5 *deliberately* omits `commitTime` whenever the
   job has no real commit timestamp, and says so in `jobs.go`'s `StartJob` doc comment. Per this
   campaign's established convention (a required-but-inapplicable member must be present-and-empty,
   not absent -- reversed for stepfunctions' `DescribeMapRun.ExecutionCounts` in an earlier batch),
   that decision is itself the bug: a real client's required `*time.Time` field came back `nil`.
   Fixed by falling back to the job's own `StartTime` when `CommitTime` is zero, the same "still
   needs *a* value" convention `toStepViews` already applies to a still-in-progress step's
   `EndTime` (see item 6 of the 2026-07-23 sweep above) -- not a fabricated date, the job's own real
   start time, on the same reasoning already established in this exact file.

All 5 test cases are in `wire_output_required_r80d_test.go`; existing tests needed no correction
(none asserted raw JSON with an expected-absent key for any of these members -- `go test
./services/amplify/...` passed unchanged both before and after these fixes were applied, confirming
no pre-existing test encoded the wrong shape).

### Design note: `opts ...AppOptions` / `opts ...BranchOptions`

CreateApp/UpdateApp/CreateBranch/UpdateBranch's existing positional-argument signatures
(`name, description, repository, platform string, tagMap map[string]string`, etc.) are called from
~90 sites across this package's test files plus `test/e2e/amplify_test.go`. Rather than thread every
new field through as additional positional parameters (forcing every call site to be rewritten) or
replace the signature with a single `xInput` struct (same problem), the new fields are carried by an
**optional trailing variadic** argument (`opts ...AppOptions`) that defaults to its zero value when
omitted. Every pre-existing call site keeps compiling unchanged; only the HTTP handlers (which need
every field) and the new tests that exercise the new fields pass a populated `opts` value. Every
`AppOptions`/`BranchOptions` field is a pointer/nil-able type so CreateApp/UpdateApp can distinguish
"not specified" (apply the create-time default, or leave unchanged on update) from an explicit zero
value -- see the type's doc comment in models.go for the exact convention.

### Verified correct as-is (not a gap)

- **`wafConfiguration` on App**: optional (not a required response member) in the real SDK, and
  there is no `AssociateWebAcl`-equivalent operation in the Amplify API surface at all -- real
  Amplify apps get Firewall/WAF association through the WAFv2 API directly against the app's ARN,
  not through any Amplify `CreateApp`/`UpdateApp` input field. gopherstack correctly leaves this
  `nil`/omitted for every app, identical to how real Amplify behaves for the (large majority of)
  apps that were never WAF-associated. The prior audit listed this under the same gap bullet as the
  fields above; re-diffing shows it doesn't belong there.
- **Branch has no `customHeaders`/`customRules`**: see item 2 above -- the prior audit's gap note
  was simply incorrect; these fields exist only on App in the real SDK.
- **Routing**: unchanged from the prior audit -- every one of the 35 supported ops' HTTP method +
  REST path was previously diffed 1:1 against
  `aws-sdk-go-v2/service/amplify@v1.40.0/serializers.go`'s `SplitURI(...)` / `request.Method =`
  pairs; nothing in this sweep touched routing, so it remains verified clean.
- **Persistence**: `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend.Snapshot`/`Restore`,
  which version-gate (`amplifySnapshotVersion`) and go through `store.Registry.SnapshotAll`/
  `RestoreAll` for every table (apps, branches, jobs, domains, webhooks, backendEnvironments,
  artifacts). The new App/Branch/Job/Artifact fields added this sweep are additive JSON fields on
  types already round-tripped this way, so no `amplifySnapshotVersion` bump was needed -- an older
  snapshot missing the new fields simply decodes them as their zero value, which is always a valid
  starting point (e.g. an app snapshotted before this sweep decodes with `EnvironmentVariables ==
  nil`, indistinguishable from "never set one").
