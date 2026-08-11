---
service: codebuild
sdk_module: aws-sdk-go-v2/service/codebuild@v1.72.4   # version audited against
last_audit_commit: 0627d5d3                             # HEAD when the PRIOR manifest was written;
                                                          # this pass ran under the "no git" constraint
                                                          # and could not read/update this hash
last_audit_date: 2026-08-11
overall: A                # 2026-07-23 pass: deleted 3 invented ops, implemented pagination,
                           # sourceVersion, extended Webhook fields (see below). 2026-07-25 pass #1:
                           # field-diffed Fleet against real types.Fleet -- found+fixed a real gap
                           # (id/overflowBehavior/imageId/fleetServiceRole silently unsupported on
                           # Create/UpdateFleet); ComputeConfiguration/ProxyConfiguration/VpcConfig/
                           # ScalingConfiguration were left genuinely unmodeled, holding this at A-.
                           # 2026-07-25 pass #2: implemented all four nested Fleet configuration
                           # objects end to end (request parsing, backend state, response wire
                           # shape, persistence via the existing store.Table JSON round trip) --
                           # gaps: is now empty. DescribeCodeCoverages/DescribeTestCases/
                           # GetReportGroupTrend's empty report-content data remains a genuinely
                           # out-of-scope items_still_open entry (not a gaps: blocker -- see below),
                           # so this reaches A.
                           # 2026-08-11 pass (gopherstack-3y6x follow-up): field-diffed
                           # DescribeCodeCoverages/DescribeTestCases/GetReportGroupTrend's
                           # error sets against botocore -- confirmed the empty-content verdict
                           # is still correct (no report-execution pipeline exists to source real
                           # numbers from) but found the *validation* half was two more bugs:
                           # DescribeTestCases/GetReportGroupTrend accepted a nonexistent
                           # reportArn/reportGroupArn and returned success (real AWS declares
                           # ResourceNotFoundException for both; DescribeCodeCoverages correctly
                           # does not); GetReportGroupTrend's trendField was parsed and never
                           # validated against its 9-value enum. Also field-diffed
                           # CodeCoverage/TestCase against real types -- both had invented field
                           # names. Swept Delete* ops repo-wide for the same existence-check
                           # pattern and found the inverse bug in 5 places: DeleteProject/
                           # DeleteBuildBatch/DeleteReport/DeleteReportGroup/DeleteFleet all
                           # rejected a nonexistent resource with ResourceNotFoundException, but
                           # real AWS declares no such exception for any of the five (idempotent
                           # delete) -- fixed all five. ListReportsForReportGroup was missing the
                           # same reportGroupArn existence check as GetReportGroupTrend/
                           # DescribeTestCases -- fixed.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now threads top-level sourceVersion, see gaps fixed below"}
  UpdateProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as CreateProject"}
  DeleteProject:   {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades build deletion via buildsByProject index. FIXED this pass: now idempotent on a nonexistent name -- real AWS declares no ResourceNotFoundException for this op, gopherstack previously invented one"}
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
  DeleteBuildBatch: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now idempotent on a nonexistent id, same real-AWS error-contract fix as DeleteProject"}
  ListBuildBatches: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: filter.status/nextToken/sortOrder/maxResults implemented, and the op is now documented here (it was already routed/tested pre-pass, just missing from this manifest)"}
  ListBuildBatchesForProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass, same as ListBuildBatches; also newly documented here"}
  CreateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateReportGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReportGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now idempotent on a nonexistent arn, same real-AWS error-contract fix as DeleteProject"}
  BatchGetReportGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name"}
  ListReportGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: nextToken/sortBy(NAME|CREATED_TIME|LAST_MODIFIED_TIME)/sortOrder/maxResults via ListReportGroupsSortedBy + paginateIDs"}
  BatchGetReports: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReport:    {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now idempotent on a nonexistent arn, same real-AWS error-contract fix as DeleteProject"}
  ListReports:     {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: filter.status/nextToken/sortOrder/maxResults implemented"}
  ListReportsForReportGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: pagination (as before), plus a real gap found this audit -- a nonexistent reportGroupArn returned an empty list instead of ResourceNotFoundException (real AWS declares that exception for this op, unlike ListReports/ListReportGroups)"}
  GetReportGroupTrend: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: reportGroupArn existence and trendField enum (9 real values) are now validated (both were previously accepted-then-ignored -- ResourceNotFoundException/InvalidInputException respectively); rawData (a real response field, previously missing entirely) is now present as an empty list. Content itself (stats map) remains empty -- no report-execution data is modeled, and none can be fabricated; see items_still_open"}
  DescribeCodeCoverages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: reportArn is now required (was unchecked); CodeCoverage's field names were fixed to match the real type (id/reportARN/lineCoveragePercentage/branchCoveragePercentage/linesCovered/linesMissed/branchesCovered/branchesMissed/expired -- previously invented filePath/branchCoverage/lineCoverage). Confirmed (not changed) that a nonexistent reportArn correctly still returns an empty list rather than ResourceNotFoundException -- real AWS declares no such exception for this op, unlike DescribeTestCases/GetReportGroupTrend. Content itself remains empty; see items_still_open"}
  DescribeTestCases: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass: reportArn is now required and validated to exist (real AWS declares ResourceNotFoundException for this op; previously accepted any ARN and returned an empty list); TestCase's field names were fixed to match the real type (reportArn/testRawDataPath/prefix/name/status/durationInNanoSeconds/message/testSuiteName/expired -- previously invented duration). Content itself remains empty; see items_still_open"}
  CreateFleet:     {wire: ok, errors: ok, state: ok, persist: ok, note: "id (Fleet had no separate id field at all -- now uuid-generated), overflowBehavior, imageId, fleetServiceRole fixed in the earlier 2026-07-25 pass. Second 2026-07-25 pass: computeConfiguration/proxyConfiguration/vpcConfig/scalingConfiguration now also accepted, stored, and echoed back (scalingConfiguration's desiredCapacity is populated from baseCapacity, matching AWS's no-scaling-event-yet behavior -- see fleets.go's outputScalingConfiguration doc comment)"}
  UpdateFleet:     {wire: ok, errors: ok, state: ok, persist: ok, note: "computeType/environmentType/overflowBehavior/imageId/fleetServiceRole fixed in the earlier 2026-07-25 pass. Second 2026-07-25 pass: computeConfiguration/proxyConfiguration/vpcConfig/scalingConfiguration now also updatable (nil pointer leaves the existing value unchanged, non-nil overwrites -- matches real UpdateFleetInput's partial-update semantics)"}
  DeleteFleet:     {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts ARN or bare name. FIXED this pass: now idempotent on a nonexistent name/arn, same real-AWS error-contract fix as DeleteProject"}
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
  - "DescribeCodeCoverages/DescribeTestCases/GetReportGroupTrend always return empty content (codeCoverages/testCases/stats) because no report actually populates coverage/test-case/trend data anywhere in the backend (reports are seed-only via the AddReportInternal test helper — there is no real CodeBuild API to push test-case/coverage content; on real AWS it's ingested by the managed build agent parsing buildspec `reports` sections and artifact files, which this emulator's build execution does not model). This remains genuinely correct to leave empty rather than fabricate numbers a client cannot distinguish from real data. Implementing this for real would require modeling report-content ingestion from build artifacts, which is out of scope for this pass. NOTE: as of the 2026-08-11 pass, this is now *only* a content gap -- the request validation these three ops perform (required fields, ARN existence where real AWS declares it, trendField enum) is complete and correct; see ops: above."
gaps: []                  # known divergences NOT fixed — link bd issue ids. Fleet's
                           # ComputeConfiguration/ProxyConfiguration/VpcConfig/ScalingConfiguration
                           # (found genuinely unmodeled in the first 2026-07-25 pass) were
                           # implemented end to end in the second 2026-07-25 pass -- see Notes.
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

### 2026-07-25 pass: Fleet field-diff

Field-diffed `Fleet`/`CreateFleetInput`/`UpdateFleetInput` against
`aws-sdk-go-v2/service/codebuild@v1.68.11/types/types.go` and
`awsAwsjson11_deserializeDocumentFleet` directly (not against gopherstack's own
output, per parity-principles.md rule 2). Found and fixed a real, previously-unflagged
gap: `Fleet` had no `id` field at all (a real, separate field from `name`/`arn` on
`types.Fleet`), and `CreateFleetInput`/`UpdateFleetInput`'s wire structs had no
`overflowBehavior`/`imageId`/`fleetServiceRole` members, so a real client setting any
of these had them silently dropped on create and had **no way at all** to change them
(or `computeType`/`environmentType`) after creation via `UpdateFleet`, which previously
only ever touched `baseCapacity`. Fixed by adding `Fleet.ID`/`Fleet.ImageID`, generating
a UUID `id` at `CreateFleet` time (mirroring how other resources in this service
generate IDs), and refactoring `CreateFleet`/`UpdateFleet`'s backend signatures to take
`CreateFleetOptions`/`UpdateFleetOptions` structs (the growing flat-positional-parameter
lists were becoming unwieldy) wired through from new `createFleetInput`/
`updateFleetInput` JSON fields. `UpdateFleet`'s "empty string leaves field unchanged"
semantics mirror the existing `applyProjectOptionalFields` convention for optional
string-field updates on this service.

**Also found, NOT fixed in this pass**: `Fleet.ComputeConfiguration`/`ProxyConfiguration`/
`VpcConfig`/`ScalingConfiguration` (all real fields on `types.Fleet`) remained entirely
unmodeled -- these are nested objects (attribute-based-compute vCPU/memory/disk specs,
subnet/security-group VPC config, scaling-type semantics) that would require real design
work, not a wire-shape passthrough fix. Documented as a new `gaps:` entry rather than
silently left unflagged like the id/overflowBehavior/imageId/fleetServiceRole gap was
before this pass. **Closed in the second 2026-07-25 pass immediately below.**

Covered by new tests: `TestHandler_CreateFleet_ExtendedFields`,
`TestHandler_UpdateFleet_ExtendedFields` (`fleets_test.go`).

### 2026-07-25 pass #2: Fleet nested configuration objects (closes the gap above)

Implemented `ComputeConfiguration`/`ProxyConfiguration`/`VpcConfig`/`ScalingConfiguration`
end to end, field-diffed against `aws-sdk-go-v2/service/codebuild@v1.68.11`'s
`types.ComputeConfiguration`/`types.ProxyConfiguration`/`types.VpcConfig`/
`types.FleetProxyRule`/`types.TargetTrackingScalingConfiguration`/
`types.ScalingConfigurationInput`/`types.ScalingConfigurationOutput` directly (`go doc`
plus reading `serializers.go`'s `awsAwsjson11_serializeOpDocumentCreateFleetInput`/
`UpdateFleetInput` for exact request field names, and `deserializers.go`'s
`awsAwsjson11_deserializeDocumentFleet` for the response). Added new model types
(`models.go`): `ComputeConfiguration`, `ProxyConfiguration`, `FleetProxyRule`,
`TargetTrackingScalingConfig`; extended the existing (previously dead-field) `ScalingConfiguration`
type with `TargetTrackingScalingConfigs`. `Fleet.VpcConfig` deliberately reuses the
existing `VpcConfig` type already defined for `Project` -- the real
`aws-sdk-go-v2/service/codebuild/types.VpcConfig` used by both `Fleet` and `Project` has
the identical shape (`securityGroupIds`/`subnets`/`vpcId`), so a second, duplicate type
would have been pure duplication.

Wired through `CreateFleetOptions`/`UpdateFleetOptions` (fleets.go) -- extending the
existing options structs from the prior pass rather than inventing a parallel path, per
this pass's instructions -- and the `createFleetInput`/`updateFleetInput` JSON wire
structs (handler_fleets.go). `UpdateFleet`'s nested-object semantics: a non-nil pointer
overwrites, `nil` (absent from the request) leaves the existing value unchanged, matching
real `UpdateFleetInput`'s partial-update contract (distinct from the string fields' "empty
string leaves unchanged" convention, since a nested object has no equivalent "empty"
sentinel).

`ScalingConfiguration.DesiredCapacity` is response-only on real AWS (`types.
ScalingConfigurationOutput` has it; `types.ScalingConfigurationInput`, the request shape,
does not) -- confirmed by diffing the two types separately in `types/types.go`. Since this
emulator does not model live auto-scaling telemetry, `outputScalingConfiguration`
(`fleets.go`) populates it with the fleet's `baseCapacity` on every `Create`/`UpdateFleet`
response, matching real AWS's own value immediately after a create/update, before any
scaling event has occurred -- not a fabricated number, the literal correct value for that
moment.

**Disguised-stub pattern found and fixed while doing this**: `Fleet.ScalingConfiguration`
already existed as a model field (with a real JSON tag) before this pass, but nothing
anywhere ever set it -- a stray leftover from an earlier partial attempt, matching the
"field exists, no write-sites" bug class documented in `parity-principles.md`. It is now
genuinely wired end to end.

Covered by a new table test, `TestHandler_Fleet_NestedConfiguration` (`fleets_test.go`,
cases: `create_computeConfiguration`, `create_proxyConfiguration`, `create_vpcConfig`,
`create_scalingConfiguration_desiredCapacityMatchesBase`,
`update_overwrites_nested_configuration`,
`update_without_nested_configuration_leaves_it_unchanged`).

### 2026-08-11 pass: report-content follow-up (gopherstack-3y6x)

Re-examined the `items_still_open` premise that `DescribeCodeCoverages`/`DescribeTestCases`/
`GetReportGroupTrend` "always return empty, blamed on no report-content ingestion pipeline."
Confirmed the content verdict is still correct (no build-artifact/report-content pipeline
exists to source real numbers from, and fabricating them would be worse than an empty
response per `parity-principles.md`) -- but the *validation* half of each op was checked
against `aws-sdk-go-v2/service/codebuild@v1.72.4`'s botocore source
(`codebuild/2016-10-06/service-2.json`'s per-operation `errors` list, which is the
authoritative declared-exception contract each op's real deserializer is generated from)
and two of the three had a real bug:

- **`DescribeTestCases`/`GetReportGroupTrend` accepted a nonexistent `reportArn`/
  `reportGroupArn` and returned 200 with empty content.** Real AWS declares
  `ResourceNotFoundException` for both ops. Fixed: both now look up the resource first and
  return `ErrNotFound` if absent (`reports.go`'s `DescribeTestCases`/`GetReportGroupTrend`).
  **`DescribeCodeCoverages` does *not* declare `ResourceNotFoundException`** (only
  `InvalidInputException`) -- confirmed by reading the same errors list -- so its identical
  "accept anything, return empty" behavior was already correct and was left unchanged; a new
  test (`describe_code_coverages_nonexistent_report_still_returns_empty`) documents this
  asymmetry so a future pass doesn't "fix" it into a regression.
- **`GetReportGroupTrend`'s `trendField` was parsed and never validated.** Real AWS's
  `ReportGroupTrendFieldType` is a 9-value enum (`types/enums.go:895-926`); any string,
  including garbage, was silently accepted. Fixed: `handler_reports.go` now checks
  `trendField` against the real enum via `slices.Contains`, rejecting with
  `InvalidInputException` otherwise.
- **`reportArn`/`reportGroupArn` were never checked for presence** on any of the three ops
  (nor was `GetReportGroupTrend`'s `reportGroupArn`) -- all are `required` members on the real
  input shapes. Fixed: added the standard `in.X == ""` → `errInvalidRequest` check already
  used throughout this file (see `handleDeleteReport` etc.) to all three.
- **`GetReportGroupTrend`'s response was missing `rawData`**, a real member of
  `GetReportGroupTrendOutput` (`api_op_GetReportGroupTrend.go`) — the handler only ever
  returned `stats`. Fixed: `rawData` is now present as an empty list (structurally correct,
  not fabricated — there is no report data to populate it with).
- **`CodeCoverage`/`TestCase` had invented field names**, not matching
  `aws-sdk-go-v2/service/codebuild@v1.72.4/types.CodeCoverage`/`types.TestCase` (verified via
  `deserializers.go`'s `awsAwsjson11_deserializeDocumentCodeCoverage`). `CodeCoverage` had
  only `filePath`/`branchCoverage`/`lineCoverage`; real AWS has 10 fields including
  `id`/`reportARN`/`lineCoveragePercentage`/`branchCoveragePercentage`/`linesCovered`/
  `linesMissed`/`branchesCovered`/`branchesMissed`/`expired`. `TestCase` had only
  `name`/`status`/`duration`; real AWS has 9 fields including `reportArn`/`testRawDataPath`/
  `prefix`/`durationInNanoSeconds`/`message`/`testSuiteName`/`expired` (no `duration` field
  exists on the real type at all). Fixed both models to match; `handler_reports.go` now
  marshals the real slices directly instead of hand-building `map[string]any` with the wrong
  keys. Since both lists remain always-empty (per the content verdict above), this has no
  observable effect today, but is now correct for whenever report-content ingestion is
  eventually implemented.

**Deliberately not implemented**: `DescribeCodeCoverages`'s `sortBy`/`sortOrder`/`maxResults`/
`nextToken`/`minLineCoveragePercentage`/`maxLineCoveragePercentage` and `DescribeTestCases`'s
`filter`/`maxResults`/`nextToken` request fields. Unlike the pagination/filter parameters
fixed on `ListReports`/`ListBuildBatches`/etc in the 2026-07-23 pass (which silently returned
wrong results against *real, non-empty* data), these parameters would have zero observable
effect: the result set they'd sort/filter/paginate is provably always empty (see above), so
accepting-and-ignoring them is behaviorally identical to not accepting them at all. Revisit
this decision if/when report-content ingestion is ever implemented.

**Swept the rest of the service for the same two bug classes** (nonexistent-resource
accepted; hand-written checks vs the real error contract) and found:

- **The inverse bug in five `Delete*` ops.** `DeleteProject`/`DeleteBuildBatch`/`DeleteReport`/
  `DeleteReportGroup`/`DeleteFleet` all rejected a nonexistent resource with
  `ResourceNotFoundException` (400) — but real AWS declares no such exception for *any* of the
  five (`DeleteProject.errors`/`DeleteBuildBatch.errors`/`DeleteReport.errors`/
  `DeleteReportGroup.errors`/`DeleteFleet.errors`: all just `["InvalidInputException"]`),
  meaning all five are idempotent deletes on real AWS. This matches the precedent already
  established (and already correctly implemented) by `DeleteResourcePolicy`
  ("idempotent, matches AWS" in `ops:` above). Fixed all five to no-op on a missing resource
  instead of erroring. Cross-checked the two `Delete*` ops that legitimately keep erroring —
  `DeleteWebhook`/`DeleteSourceCredentials` — both *do* declare `ResourceNotFoundException`
  for real, so they were correctly left unchanged.
- **`ListReportsForReportGroup` had the same missing-existence-check bug** as
  `DescribeTestCases`/`GetReportGroupTrend` above: a nonexistent `reportGroupArn` returned an
  empty list instead of `ResourceNotFoundException` (real AWS declares it for this op,
  confirmed via the same botocore errors list — unlike `ListReports`/`ListReportGroups`, which
  correctly don't and were correctly left alone). Fixed the same way: existence check before
  listing, `ListReportsForReportGroup`'s backend signature gained an `error` return (no
  external call sites outside `services/codebuild`, confirmed by repo-wide grep; `go vet .`
  clean at the repository root).

New/updated tests: `reports_test.go` (`TestCodeBuild_ReportExtras`,
`TestCodeBuild_Reports`, `TestCodeBuild_ReportGroups`), `fleets_test.go`
(`delete_missing_fleet_is_idempotent`), `build_batches_test.go`
(`delete_missing_is_idempotent`), `projects_test.go` (`not_found_is_idempotent`),
`handler_test.go` (`TestHandler_ErrorTypeMapping`'s `delete_project_missing_is_idempotent`/
`delete_fleet_missing_is_idempotent`), `pagination_test.go`/`persistence_test.go` (updated to
create real report groups instead of hand-constructing ARNs that were never registered, which
the new `ListReportsForReportGroup` existence check would otherwise correctly reject).
