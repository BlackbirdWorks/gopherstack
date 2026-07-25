---
service: amplify
sdk_module: aws-sdk-go-v2/service/amplify@v1.40.0
last_audit_commit: c807b481
last_audit_date: 2026-07-23
overall: A            # this sweep: full App/Branch field parity, Stage enum fix, commitTime,
                       # real build steps, real artifact producer + cascade delete, enum validation
ops:
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below"}
  GetApp: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApps: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateApp, plus correct partial-update (nil-means-unchanged) semantics"}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades jobs/artifacts/domains/webhooks/backendEnvironments, not just branches -- see leaks"}
  CreateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: full field parity -- see gaps history below"}
  GetBranch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBranches: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: same field parity as CreateBranch, plus correct partial-update semantics"}
  DeleteBranch: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades jobs/artifacts -- see leaks"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: commitTime now modeled and round-trips; jobId+RETRY validated (BadRequestException if jobId absent, matches real StartJobInput) and inherits the retried job's commit metadata when the caller omits its own; jobType validated against the real JobType enum"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: steps now synthesizes one real BUILD step derived from the job's own status/timestamps (previously always []); commitTime now modeled -- see Notes for why one synthetic step, not a full per-stage model"}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: now cascades the job's own artifacts"}
  StopJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
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
  ListArtifacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep: janitor.go now creates a real Artifact record (type BUILD) for every job it advances to SUCCEED, indexed by job so ListArtifacts/GetArtifactUrl have real content -- see Notes"}
families:
  routing: {status: ok, note: "every op's HTTP method + REST path verified 1:1 against aws-sdk-go-v2/service/amplify@v1.40.0 serializers.go SplitURI/request.Method calls (all 35 ops); no route-matcher bugs found -- POST-not-PUT for UpdateApp/UpdateBranch/UpdateDomainAssociation/UpdateWebhook already correct, tag ARN scoping (amplifyServiceIdentifier check) already correct"}
  errors: {status: ok, note: "handleBackendError/amplifyErrorJSON emit both the X-Amzn-Errortype header and a __type body field; this sweep added a BadRequestException mapping for awserr.ErrInvalidParameter (the new Platform/Stage/JobType/RETRY-jobId validation errors) alongside the existing NotFoundException/AlreadyExists mappings"}
gaps: []
  # Every gap/deferred item from the prior audit (2026-07-13) was field-diffed
  # against aws-sdk-go-v2/service/amplify@v1.40.0/types and fixed for real this
  # sweep. See "Fixed this sweep" in Notes for what changed and why, and
  # "Verified correct as-is (not a gap)" for two items that were reclassified
  # after independently re-diffing them against the real SDK -- they don't
  # belong under gaps at all, prior-audit language notwithstanding.
deferred: []
  # "Full App/Branch field parity" and "server-side enum validation" (the two
  # prior deferred items) are both done this sweep -- see gaps history above.
leaks: {status: clean, note: "janitor.Run blocks on <-ctx.Done() and calls worker.Group.Stop() before returning, same lifecycle pattern as services/codebuild and services/batch; StartWorker only spawns the goroutine when a janitor was attached via WithJanitor (always true via provider.go), bound to the process/JanitorCtx lifetime. Fixed this sweep: DeleteApp previously cascaded only branches+tags, leaving jobs, domain associations, webhooks, and backend environments behind as ghost rows reachable by no legitimate path once the app 404s (an unbounded leak across create/delete churn in any long-running instance or test suite); DeleteBranch previously didn't cascade the branch's own jobs (or their artifacts) either. Both now cascade fully -- see InMemoryBackend.DeleteApp/deleteBranchLocked in apps.go and DeleteJob/DeleteBranch in jobs.go/branches.go. Every lock path remains defer-released; the new artifactsByJob store.Index (store_setup.go) adds no additional locking of its own, same invariant as every other index on this backend's single lockmetrics.RWMutex."}
---

## Notes

Protocol: **restjson1**. Timestamps are Unix epoch-seconds `float64` (createTime/updateTime/startTime/endTime/commitTime/lastDeployTime), not ISO8601 -- already correct throughout (toAppView/toBranchView/toJobSummaryView/toProductionBranchView/etc.), including every new timestamp field added this sweep.

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
