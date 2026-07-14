---
service: codebuild
sdk_module: aws-sdk-go-v2/service/codebuild@v1.68.11   # version audited against
last_audit_commit: 0627d5d3                             # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: B                # already-accurate after 3 prior parity sweeps; this pass found and
                           # fixed one genuine "disguised no-op" class bug (builds stuck
                           # IN_PROGRESS forever) plus one real missing-join gap (webhook not
                           # mirrored onto Project)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateProject:   {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProject:   {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades build deletion via buildsByProject index"}
  BatchGetProjects: {wire: ok, errors: ok, state: ok, persist: ok, note: "now includes webhook field, see gaps fixed below"}
  ListProjects:    {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortBy/sortOrder — always returns full unpaginated list, see deferred"}
  StartBuild:      {wire: ok, errors: ok, state: ok, persist: ok, note: "env var override uses correct AWS replace-by-name-else-append merge semantics"}
  StopBuild:       {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetBuilds:  {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts both build ID and ARN via buildsByARN index"}
  ListBuilds:      {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortOrder, see deferred"}
  ListBuildsForProject: {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortOrder, see deferred"}
  RetryBuild:      {wire: ok, errors: ok, state: ok, persist: ok, note: "inherits env/source/artifacts/role/timeouts from original build, matching AWS"}
  BatchDeleteBuilds: {wire: ok, errors: ok, state: ok, persist: ok}
  StartBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: was stuck IN_PROGRESS forever, see gaps fixed below"}
  StopBuildBatch:  {wire: ok, errors: ok, state: ok, persist: ok}
  RetryBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetBuildBatches: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetReportGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name"}
  ListReportGroups: {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortBy/sortOrder, see deferred"}
  BatchGetReports: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReport:    {wire: ok, errors: ok, state: ok, persist: ok}
  ListReports:     {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortOrder, see deferred"}
  ListReportsForReportGroup: {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortOrder, see deferred"}
  GetReportGroupTrend: {wire: partial, errors: ok, state: ok, persist: n/a, note: "returns empty stats map (no report-execution data modeled), acceptable stub-free no-op since no reports carry numeric stats"}
  DescribeCodeCoverages: {wire: partial, errors: ok, state: ok, persist: n/a, note: "always empty list — no coverage data modeled, see deferred"}
  DescribeTestCases: {wire: partial, errors: ok, state: ok, persist: n/a, note: "always empty list — no test-case data modeled, see deferred"}
  CreateFleet:     {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFleet:     {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFleet:     {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name"}
  BatchGetFleets:  {wire: ok, errors: ok, state: ok, persist: ok}
  ListFleets:      {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken/sortBy/sortOrder, see deferred"}
  CreateWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: webhook now mirrored onto Project.Webhook, see gaps fixed below"}
  UpdateWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as CreateWebhook"}
  DeleteWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: clears Project.Webhook"}
  ImportSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent, matches AWS"}
  UpdateProjectVisibility: {wire: ok, errors: ok, state: ok, persist: ok, note: "generates/clears publicProjectAlias correctly on PUBLIC_READ toggle"}
  InvalidateProjectCache: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly a real no-op (cache not modeled) once project existence is validated"}
  TagResource:     {wire: ok, errors: ok, state: ok, persist: ok, note: "covers project/build/fleet/reportGroup ARN namespaces via *ByARN indexes"}
  UntagResource:   {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  StartSandbox:    {wire: ok, errors: ok, state: ok, persist: ok}
  StopSandbox:     {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetSandboxes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSandboxes:   {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken, see deferred"}
  ListSandboxesForProject: {wire: partial, errors: ok, state: ok, persist: ok, note: "no nextToken, see deferred"}
  StartSandboxConnection: {wire: partial, errors: ok, state: ok, persist: n/a, note: "returns a synthesized wss:// endpoint; real interactive terminal not modeled, acceptable for an emulator"}
  StartCommandExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetCommandExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCommandExecutionsForSandbox: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly returns full CommandExecution objects, not just IDs"}
  ListCuratedEnvironmentImages: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "hardcoded minimal image catalog, acceptable (AWS's own catalog is also effectively static reference data)"}
  ListSharedProjects: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "correctly empty — no cross-account project sharing modeled"}
  ListSharedReportGroups: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "correctly empty, same reasoning"}
families:
  errors: {status: ok, note: "handleError maps ErrNotFound/ErrAlreadyExists/ErrValidation to ResourceNotFoundException/ResourceAlreadyExistsException/InvalidInputException at 400, matching real AWS; all backend ErrNotFound paths reach errCodeLookup correctly"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore, versioned (codebuildSnapshotVersion), backed by store.Registry across all store.Table-based resource maps plus a plain resourcePolicies map"}
  janitor: {status: ok, note: "FIXED this pass: janitor.tick now runs sweepCompletedBuilds (TTL eviction) then advanceInProgressBuilds (status advancement) every tick, see gaps fixed below"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "List* operations (ListProjects, ListBuilds, ListBuildsForProject, ListBuildBatches, ListBuildBatchesForProject, ListFleets, ListReportGroups, ListReports, ListReportsForReportGroup, ListSandboxes, ListSandboxesForProject) accept but ignore nextToken/sortBy/sortOrder and always return the full unpaginated result set. Real AWS caps most of these at 100 items/page. Low risk for an emulator at typical test-fixture scale, but a client that relies on SDK paginators terminating on empty nextToken (rather than result size) would still work correctly since NextToken is always omitted/empty."
  - "Project is missing the top-level sourceVersion field (distinct from secondarySourceVersions, which IS modeled) present on real AWS's Project shape. CreateProject/UpdateProject inputs don't expose a top-level sourceVersion either, so nothing currently threads it through — additive fix, not a state-mutation bug."
  - "Webhook is missing status/statusMessage/manualCreation/lastModifiedSecret/secret/pullRequestBuildPolicy/scopeConfiguration fields present on real AWS's Webhook shape (GitHub Enterprise / newer webhook features). Additive, no client observed depending on these in this emulator's test/integration suite."
  - "DescribeCodeCoverages/DescribeTestCases/GetReportGroupTrend always return empty results because no report actually populates coverage/test-case/trend data anywhere in the backend (reports are seed-only via AddReportInternal test helper). Real report content ingestion (from build artifacts) is out of scope for this pass."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full request-shape field coverage for CreateWebhook/UpdateWebhook (manualCreation, scopeConfiguration) if a future consumer needs GitHub Enterprise webhook parity"
  - "Pagination (nextToken) implementation for List* ops via pkgs/page, should upstream 100-item-page AWS behavior be needed for load-testing scenarios"
leaks: {status: clean, note: "janitor.Run selects on ctx.Done() and calls worker.Group.Stop(); TestCodeBuildJanitor_RunContext passes under -race. No new goroutines introduced by advanceInProgressBuilds (runs synchronously inside the existing ticker callback)."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CodeBuild_20161006.<Op>`).
Route matcher (`RouteMatcher`) is a simple `X-Amz-Target` prefix check — verified every
op in `GetSupportedOperations()` round-trips through `doRequest`'s header-based dispatch
in the existing handler_test.go/codebuild_ops_test.go/aws_accuracy_test.go suites, not just
via `Handler()` direct calls that could bypass the matcher.

Timestamps (`created`, `lastModified`, `startTime`, `endTime`, etc.) are plain `float64`
Unix-seconds fields that marshal as bare JSON numbers — this is wire-compatible with the
real deserializer's `case "created": ... jtv.(json.Number) -> smithytime.ParseEpochSeconds`
path (confirmed by reading `codebuild@v1.68.11/deserializers.go`), even though the field
isn't typed via `pkgs/awstime.Epoch`. Not a bug; noted so a future auditor doesn't "fix" it.

### Bugs fixed this pass

1. **Builds/build batches stuck at IN_PROGRESS forever** (`services/codebuild/janitor.go`).
   `StartBuild`/`StartBuildBatch` set `BuildStatus`/`BuildBatchStatus` to `IN_PROGRESS` and
   nothing ever advanced it — the only other writer was the explicit `StopBuild`/
   `StopBuildBatch` caller action. A real client polling `BatchGetBuilds` (or
   `BatchGetBuildBatches`) to wait for build completion, exactly the way the AWS CLI's
   `codebuild wait build-complete`-style scripts and most CI wrapper tooling do, would spin
   forever against this emulator. Fixed by adding `Janitor.advanceInProgressBuilds`,
   following the same pattern already used by `services/batch/janitor.go`'s `advanceJobs`:
   every janitor tick, any build/build batch still `IN_PROGRESS` is advanced to `SUCCEEDED`
   (`EndTime` set, `CurrentPhase = "COMPLETED"`, `BuildComplete = true` for builds). Already-
   terminal builds (e.g. explicitly `STOPPED`) are left untouched. Wired into both
   `Janitor.Run` (via the existing ticker) and `Janitor.SweepOnce` (used by tests) through a
   shared `tick` method.

2. **Project.Webhook not populated after CreateWebhook** (`services/codebuild/backend.go`).
   Real AWS's `Project` shape has a `webhook` field that `BatchGetProjects`/`GetProject`
   populate once a webhook exists for that project (confirmed via
   `codebuild@v1.68.11/deserializers.go`'s `awsAwsjson11_deserializeDocumentProject`, case
   `"webhook"`). This backend stored `Webhook` in its own `store.Table` but never joined it
   back onto the `Project` record, so any client relying on `BatchGetProjects` (or drift
   detection / IaC read-back) to discover whether a project has a webhook configured would
   incorrectly see none. Fixed by mirroring the webhook onto `Project.Webhook` in
   `CreateWebhook`/`UpdateWebhook` (set) and `DeleteWebhook` (clear).

Both fixes are covered by new table-driven tests: `TestJanitor_AdvanceInProgressBuilds` /
`TestJanitor_AdvanceInProgressBuilds_LeavesTerminalBuildsAlone` in `janitor_test.go`, and
`TestParity_CreateWebhook_MirroredOnProject` in `parity_a_test.go`.
