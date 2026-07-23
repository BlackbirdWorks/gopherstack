---
service: codebuild
sdk_module: aws-sdk-go-v2/service/codebuild@v1.68.11   # version audited against
last_audit_commit: 0627d5d3                             # HEAD when the PRIOR manifest was written;
                                                          # this pass ran under the "no git" constraint
                                                          # and could not read/update this hash
last_audit_date: 2026-07-23
overall: A-               # this pass: deleted 3 gopherstack-invented ops (TagResource/
                           # UntagResource/ListTagsForResource — confirmed absent from the real
                           # aws-sdk-go-v2/service/codebuild Client method set), implemented
                           # nextToken/sortBy/sortOrder/maxResults pagination + filter.status for
                           # every List* op, added the missing top-level Project.sourceVersion
                           # field, and added the missing Webhook status/secret/manualCreation/
                           # scopeConfiguration/pullRequestBuildPolicy/rotateSecret fields.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now threads top-level sourceVersion, see gaps fixed below"}
  UpdateProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as CreateProject"}
  DeleteProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades build deletion via buildsByProject index"}
  BatchGetProjects: {wire: ok, errors: ok, state: ok, persist: ok, note: "includes webhook and sourceVersion fields"}
  ListProjects:    {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortBy(NAME|CREATED_TIME|LAST_MODIFIED_TIME)/sortOrder all implemented via ListProjectsSortedBy + paginateIDs, 100-item default page matching real AWS"}
  StartBuild:      {wire: ok, errors: ok, state: ok, persist: ok, note: "env var override uses correct AWS replace-by-name-else-append merge semantics"}
  StopBuild:       {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetBuilds:  {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts both build ID and ARN via buildsByARN index"}
  ListBuilds:      {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortOrder via paginateIDs (ListBuilds has no sortBy/maxResults in the real request shape)"}
  ListBuildsForProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortOrder via paginateIDs"}
  RetryBuild:      {wire: ok, errors: ok, state: ok, persist: ok, note: "inherits env/source/artifacts/role/timeouts from original build, matching AWS"}
  BatchDeleteBuilds: {wire: ok, errors: ok, state: ok, persist: ok}
  StartBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  StopBuildBatch:  {wire: ok, errors: ok, state: ok, persist: ok}
  RetryBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetBuildBatches: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBuildBatches: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: filter.status/nextToken/sortOrder/maxResults implemented, and the op is now documented here (it was already routed/tested pre-pass, just missing from this manifest)"}
  ListBuildBatchesForProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as ListBuildBatches; also newly documented here"}
  CreateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetReportGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name"}
  ListReportGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortBy(NAME|CREATED_TIME|LAST_MODIFIED_TIME)/sortOrder/maxResults via ListReportGroupsSortedBy + paginateIDs"}
  BatchGetReports: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReport:    {wire: ok, errors: ok, state: ok, persist: ok}
  ListReports:     {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: filter.status/nextToken/sortOrder/maxResults implemented"}
  ListReportsForReportGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as ListReports"}
  GetReportGroupTrend: {wire: partial, errors: ok, state: ok, persist: n/a, note: "returns empty stats map (no report-execution data modeled), acceptable stub-free no-op since no reports carry numeric stats; see items_still_open"}
  DescribeCodeCoverages: {wire: partial, errors: ok, state: ok, persist: n/a, note: "always empty list — no coverage data modeled; see items_still_open"}
  DescribeTestCases: {wire: partial, errors: ok, state: ok, persist: n/a, note: "always empty list — no test-case data modeled; see items_still_open"}
  CreateFleet:     {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFleet:     {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFleet:     {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name"}
  BatchGetFleets:  {wire: ok, errors: ok, state: ok, persist: ok}
  ListFleets:      {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortBy(NAME|CREATED_TIME|LAST_MODIFIED_TIME)/sortOrder/maxResults via ListFleetsSortedBy + paginateIDs; also fixed default ordering to be NAME-ascending (was ARN-string-ascending, an internal artifact with no real-AWS basis)"}
  CreateWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: adds manualCreation/scopeConfiguration/pullRequestBuildPolicy request fields and status/secret/lastModifiedSecret/statusMessage response fields, see gaps fixed below"}
  UpdateWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: adds pullRequestBuildPolicy/rotateSecret request fields (rotateSecret regenerates secret+lastModifiedSecret)"}
  DeleteWebhook:   {wire: ok, errors: ok, state: ok, persist: ok, note: "clears Project.Webhook"}
  ImportSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSourceCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent, matches AWS"}
  UpdateProjectVisibility: {wire: ok, errors: ok, state: ok, persist: ok, note: "generates/clears publicProjectAlias correctly on PUBLIC_READ toggle"}
  InvalidateProjectCache: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly a real no-op (cache not modeled) once project existence is validated"}
  StartSandbox:    {wire: ok, errors: ok, state: ok, persist: ok}
  StopSandbox:     {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetSandboxes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSandboxes:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortOrder/maxResults via paginateIDs"}
  ListSandboxesForProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as ListSandboxes"}
  StartSandboxConnection: {wire: partial, errors: ok, state: ok, persist: n/a, note: "returns a synthesized wss:// endpoint; real interactive terminal not modeled, acceptable for an emulator"}
  StartCommandExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetCommandExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCommandExecutionsForSandbox: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly returns full CommandExecution objects, not just IDs"}
  ListCuratedEnvironmentImages: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "hardcoded minimal image catalog, acceptable (AWS's own catalog is also effectively static reference data)"}
  ListSharedProjects: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "correctly empty — no cross-account project sharing modeled"}
  ListSharedReportGroups: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "correctly empty, same reasoning"}
families:
  errors: {status: ok, note: "handleError maps ErrNotFound/ErrAlreadyExists/ErrValidation to ResourceNotFoundException/ResourceAlreadyExistsException/InvalidInputException at 400, matching real AWS; all backend ErrNotFound paths reach errCodeLookup correctly; invalid nextToken now also maps to InvalidInputException via ErrValidation"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore, versioned (codebuildSnapshotVersion), backed by store.Registry across all store.Table-based resource maps plus a plain resourcePolicies map"}
  janitor: {status: ok, note: "janitor.tick runs sweepCompletedBuilds (TTL eviction) then advanceInProgressBuilds (status advancement) every tick"}
  tags: {status: ok, note: "REMOVED this pass: TagResource/UntagResource/ListTagsForResource were gopherstack-invented operations with no counterpart on the real aws-sdk-go-v2/service/codebuild Client (verified: the SDK module has no api_op_TagResource.go/api_op_UntagResource.go/api_op_ListTagsForResource.go, and Client's exported method set — grepped directly from api_op_*.go — has no such methods). Real AWS CodeBuild only supports tagging inline via the `tags` field on CreateProject/CreateReportGroup/CreateFleet/UpdateProject (already implemented and unaffected). Deleted services/codebuild/tags.go, handler_tags.go, tags_test.go; removed the 3 ops from GetSupportedOperations()/dispatchTable(); TestHandler_GetSupportedOperations now asserts their absence."}
items_still_open:            # genuinely unfinished — do not mark ok
  - "DescribeCodeCoverages/DescribeTestCases/GetReportGroupTrend always return empty results because no report actually populates coverage/test-case/trend data anywhere in the backend (reports are seed-only via the AddReportInternal test helper — there is no real CodeBuild API to push test-case/coverage content; on real AWS it's ingested by the managed build agent parsing buildspec `reports` sections and artifact files, which this emulator's build execution does not model). Implementing this for real would require modeling report-content ingestion from build artifacts, which is out of scope for this pass."
gaps: []                  # known divergences NOT fixed — link bd issue ids; none remaining after this pass
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Report-content ingestion (DescribeCodeCoverages/DescribeTestCases/GetReportGroupTrend real data) — see items_still_open above for why this is a substantially larger feature (build artifact parsing), not a quick fix."
leaks: {status: clean, note: "janitor.Run selects on ctx.Done() and calls worker.Group.Stop(); TestCodeBuildJanitor_RunContext passes under -race. paginateIDs/ListProjectsSortedBy/ListFleetsSortedBy/ListReportGroupsSortedBy are pure functions under the existing RLock scope — no new goroutines, no new lock paths, all backend locks remain defer-released."}
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

Pagination: `services/codebuild/pagination.go`'s `paginateIDs` applies nextToken/maxResults/
sortOrder uniformly across every List* op, using `pkgs/page.New` for the opaque-offset token
(matching the pattern already used by e.g. `services/acm`). `defaultListPageSize = 100` matches
real AWS's documented page cap. `ListProjects`/`ListFleets`/`ListReportGroups` additionally
support `sortBy` (NAME|CREATED_TIME|LAST_MODIFIED_TIME) via dedicated
`List*SortedBy(sortBy string)` backend methods, since real AWS's request shape for exactly
these three ops includes a `sortBy` field (confirmed by field-diffing
`api_op_ListProjects.go`/`api_op_ListFleets.go`/`api_op_ListReportGroups.go`
against `api_op_ListBuilds.go` et al, which have no `sortBy`). `ListBuildBatches(ForProject)`/
`ListReports(ForReportGroup)` additionally support `filter.status`, matching real AWS's
`BuildBatchFilter`/`ReportFilter` request shapes (each a single optional `status` field).

### Invented-ops deletion this pass

**TagResource / UntagResource / ListTagsForResource were gopherstack inventions, not real AWS
CodeBuild operations.** Field-diffed against `aws-sdk-go-v2/service/codebuild@v1.68.11`: the
module directory has no `api_op_TagResource.go`, `api_op_UntagResource.go`, or
`api_op_ListTagsForResource.go`, and grepping every `func (c *Client) ...` across all
`api_op_*.go` files in the module yields exactly 59 operations — none of which are these three.
Real CodeBuild only exposes tagging inline via the `tags` field on `CreateProject`/
`UpdateProject`/`CreateReportGroup`/`CreateFleet` (cross-service ARN-based tag discovery is the
job of the separate `resourcegroupstaggingapi` service on real AWS, which CodeBuild does not
duplicate). Deleted `tags.go` (backend `TagResource`/`UntagResource`/`ListTagsForResource`
methods), `handler_tags.go` (the three op handlers), and `tags_test.go` (12 tests exercising the
invented API). Removed the three names from `GetSupportedOperations()`/`dispatchTable()` in
`handler.go`. `TestHandler_GetSupportedOperations` now asserts these three are absent, so a
future accidental reintroduction gets caught immediately. `janitor_test.go`'s
`TestJanitor_SweepCleansARNIndex` (which used `TagResource` on an evicted build's ARN purely as
a convenient "does an ARN-based op see this as gone" probe) was rewritten to use `BatchGetBuilds`
instead — same assertion (no ghost row after eviction), real op.

### Bugs fixed this pass

1. **Missing pagination wire shape on every List\* operation** (`services/codebuild/pagination.go`,
   `handler_projects.go`, `handler_builds.go`, `handler_build_batches.go`, `handler_reports.go`,
   `handler_fleets.go`, `handler_sandboxes.go`). Every List* op previously accepted but silently
   ignored `nextToken`/`sortBy`/`sortOrder`/`maxResults`, always returning the full unpaginated
   result set with `nextToken` always omitted. A real client relying on the SDK paginator's
   `HasMorePages()` (which checks the returned `nextToken`, not result-set size) would still
   terminate correctly, but a client testing "at least 100 items on this page then a token"
   load-testing/pagination-contract scenario, or a client using `sortOrder=DESCENDING` to see
   newest-first results, would silently get wrong data. Fixed by adding a shared
   `paginateIDs(all []string, nextToken, sortOrder string, maxResults int32) (page.Page[string], error)`
   helper (using `pkgs/page`, matching the pattern in `services/acm`) and wiring it into every
   List* handler. `ListProjects`/`ListFleets`/`ListReportGroups` (the three ops whose real
   request shape has `sortBy`) got new `List*SortedBy(sortBy string)` backend methods;
   `ListFleets`'s default order was also corrected from ARN-string-ascending (an internal
   implementation artifact) to NAME-ascending, consistent with `ListProjects`/`ListReportGroups`
   and with `sortBy=NAME`.

2. **`Project` missing the top-level `sourceVersion` field** (`services/codebuild/models.go`,
   `projects.go`, `handler_projects.go`). Real AWS's `Project` shape has a `sourceVersion` field
   distinct from `secondarySourceVersions` (confirmed via `codebuild@v1.68.11/types/types.go`);
   `CreateProjectInput`/`UpdateProjectInput` also carry it. Nothing threaded it through before —
   a client setting `sourceVersion` on `CreateProject` would silently have it dropped. Fixed by
   adding `Project.SourceVersion`, `ProjectConfig.SourceVersion`, and wiring it through
   `CreateProject`/`UpdateProject`/`applyProjectOptionalFields` (update semantics: non-empty
   value overwrites, matching every other optional string field on this resource) and the
   `createProjectInput`/`updateProjectInput` wire structs.

3. **`Webhook` missing `status`/`statusMessage`/`manualCreation`/`lastModifiedSecret`/`secret`/
   `pullRequestBuildPolicy`/`scopeConfiguration`** (`services/codebuild/models.go`,
   `webhooks.go`, `handler_webhooks.go`). Field-diffed against
   `codebuild@v1.68.11/types/types.go`'s `Webhook` struct. Since this emulator never performs a
   real GitHub/GitLab/Bitbucket round-trip, `CreateWebhook` now synthesizes a terminal
   `status: "ACTIVE"` immediately (the state a client would eventually observe on real AWS after
   webhook provisioning completes) plus a generated `secret` and `lastModifiedSecret` timestamp;
   `manualCreation`/`scopeConfiguration` are accepted on `CreateWebhook` and echoed back;
   `pullRequestBuildPolicy` is accepted on both `CreateWebhook` and `UpdateWebhook`; `UpdateWebhook`
   also gained `rotateSecret` (regenerates `secret` + bumps `lastModifiedSecret` when true, leaves
   both untouched otherwise, matching real AWS's `UpdateWebhookInput.rotateSecret` semantics).

Covered by new table-driven tests: `TestHandler_ListProjects_SortOrderDescending`,
`TestHandler_ListProjects_InvalidNextToken`, `TestHandler_ListProjects_SortByCreatedTime`,
`TestHandler_ListFleets_MaxResultsPagination`, `TestHandler_ListBuildBatches_FilterByStatus`,
`TestHandler_ListReports_FilterByStatus` (`pagination_test.go`); `TestHandler_Project_SourceVersion`
(`projects_test.go`); `TestHandler_CreateWebhook_ExtendedFields`,
`TestHandler_UpdateWebhook_RotateSecret` (`webhooks_test.go`).

Prior-pass fixes (builds/build batches stuck IN_PROGRESS forever; `Project.Webhook` not mirrored
after `CreateWebhook`) remain in place and are covered by `TestJanitor_AdvanceInProgressBuilds` /
`TestJanitor_AdvanceInProgressBuilds_LeavesTerminalBuildsAlone` (`janitor_test.go`) and
`TestHandler_Webhook_MirroredOnProject` (`webhooks_test.go`).
