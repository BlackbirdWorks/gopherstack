---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: omics
sdk_module: aws-sdk-go-v2/service/omics@v1.49.5
last_audit_commit: pending (uncommitted this pass -- see git log at merge time)
last_audit_date: 2026-08-07
overall: A            # 2026-08-07 (gopherstack-hnhk): RunBatch's real body shape is now modeled.
                       # StartRunBatch takes real BatchRunSettings (inlineSettings, field-diffed
                       # against awsRestjson1_serializeDocumentBatchRunSettings/InlineSetting) +
                       # DefaultRunSetting (subset -- see the RunBatch family note for what's not
                       # modeled) instead of a flat {workflowId,roleArn,name} shape a real client
                       # never sends, and now actually creates the batch's constituent Run records
                       # synchronously (previously it created zero runs regardless of what a
                       # caller sent). GetBatch now returns real runSummary/submissionSummary/
                       # totalRuns/uuid/submittedTime/processedTime, with runSummary computed live
                       # from surviving Run rows rather than fabricated. ListRunsInBatch's
                       # runSettingId filter is now real (previously accepted-but-ignored). See the
                       # RunBatch family note below for what's still not modeled (s3UriSettings,
                       # most optional DefaultRunSetting fields, RequestId idempotency). This pass
                       # did NOT re-walk the rest of hnhk's stated scope (ListAnnotationStores/
                       # VariantStores/ShareVersions/Shares filters; ReferenceMetadata/
                       # ReadSetMetadata optional sub-objects) -- left open, see gaps below.
                       #
                       # --- prior (2026-07-23) history, kept for context ---
                       # this pass: closed all 3 tracked gaps (jxc5/x7qq/fedo), killed all 6 banned
                       # nolints via a table-based route/dispatch refactor, and field-diffing the
                       # request/response shapes turned up and fixed 4 more real wire bugs: a wrong
                       # JSON key on Run's batch association (runBatchId -> batchId), an invented
                       # field name on ReadSetMetadata/MultipartReadSetUpload (sequenceType, which
                       # appears nowhere in the real API -> fileType/sourceFileType), an invented
                       # "status" field on MultipartReadSetUpload (no such field in the real API,
                       # removed) with two real required fields (sampleId/subjectId) that were
                       # missing entirely, and a wrong JSON key on S3AccessPolicy's policy document
                       # (policy -> s3AccessPolicy, the key real SDK clients actually read).
families:
  ReferenceStore: {status: ok, note: "CRUD + List; pagination now reads maxResults/nextToken from the query string (was body)"}
  Reference: {status: ok, note: "Get/List/Delete + GetReferenceBytes/GetReferenceMetadata; pagination fixed same as ReferenceStore"}
  ReferenceImportJob: {status: ok, note: "completes synchronously (Status=COMPLETED at creation) -- no waiter-hang risk since Get never needs to transition; pagination fixed; ListReferenceImportJobs now applies its status body filter (was gap jxc5)"}
  SequenceStore: {status: ok, note: "CRUD + List; created ACTIVE immediately (no CREATING phase in the real API for this resource); pagination fixed. FIXED (gopherstack-5wj0): CreateSequenceStore accepted no eTagAlgorithmFamily/s3AccessConfig fields at all even though the SequenceStore struct already reserved ETagAlgorithm/S3Access fields for them -- both were always zero-valued. eTagAlgorithmFamily now defaults to MD5up (real API default) when omitted and is stored as given otherwise; s3AccessConfig.accessLogLocation is echoed into the response's s3Access object. s3Access.s3Uri/s3AccessPointArn (server-synthesized, no honest source in this in-memory backend) remain absent rather than fabricated"}
  ReadSet: {status: ok, note: "Get/List/BatchDelete/GetReadSetBytes; pagination fixed; ListReadSets already filtered by name/status. FIXED wire bug: ReadSetMetadata's file-type field was serialized as the invented key \"sequenceType\" (appears nowhere in GetReadSetMetadataOutput/ReadSetListItem) -- renamed to \"fileType\", the real key confirmed against the SDK deserializer. Files/CreationJobId/CreationType/Etag/SequenceInformation sub-objects remain unpopulated (deferred, optional/pointer-safe)"}
  ReadSetActivationJob: {status: ok, note: "completes synchronously; pagination fixed"}
  ReadSetExportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  ReadSetImportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  MultipartReadSetUpload: {status: ok, note: "FIXED (field-diffed against CreateMultipartReadSetUploadInput/Output and MultipartReadSetUploadListItem): the file-type field was serialized as the invented key \"sequenceType\" -- renamed to the real key \"sourceFileType\"; SampleID/SubjectID are real required fields that were missing entirely -- added and threaded through CreateMultipartReadSetUpload's signature; there is no real \"status\" field on this resource at all -- the invented one was deleted. GeneratedFrom/ReferenceARN/Description (real optional fields) also added"}
  RunGroup: {status: ok, note: "CRUD + List; already used correct maxResults+startingToken query params; ListRunGroups now applies its name query filter (bonus find alongside gap jxc5, real AWS ListRunGroupsInput has a \"name\" query param the backend previously ignored)"}
  Run: {status: ok, note: "FIXED: GetRun advances PENDING->RUNNING->COMPLETED across polls (waiter-hang fix, prior pass). This pass: (1) ListRuns now applies its name/runGroupId/batchId/status query filters (gap jxc5); (2) the run's batch association was serialized under the invented JSON key \"runBatchId\" -- real GetRunOutput/RunListItem use \"batchId\" (confirmed against the SDK deserializer) -- renamed; (3) added the real (previously entirely absent) RunGroupID field, threaded through StartRun so ListRuns' runGroupId filter has something real to match against; (4) StartRun/GetRun responses now include the optional uuid/networkingMode/runOutputUri/configuration fields real StartRunOutput/GetRunOutput have (gap fedo) -- networkingMode/outputUri are accepted from the request body (real StartRunInput field names, note outputUri on input vs runOutputUri on output)"}
  RunTask: {status: ok, note: "FIXED: GetRunTask advances PENDING->RUNNING->COMPLETED across polls, same waiter-hang fix as Run. This pass: ListRunTasks now applies its status query filter (gap jxc5)"}
  Workflow: {status: ok, note: "FIXED: GetWorkflow advances CREATING->ACTIVE on first poll (waiter-hang fix, prior pass). This pass: (1) ListWorkflows now applies its name/type query filters (gap jxc5); (2) CreateWorkflow's response now includes the optional uuid field real CreateWorkflowOutput has (gap fedo)"}
  WorkflowVersion: {status: ok, note: "FIXED: GetWorkflowVersion advances CREATING->ACTIVE on first poll (waiter-hang fix, prior pass); pagination already correct. This pass: ListWorkflowVersions now applies its type query filter (gap jxc5)"}
  AnnotationStore: {status: ok, note: "FIXED: GetAnnotationStore advances CREATING->ACTIVE on first poll (real AnnotationStoreCreatedWaiter previously hung forever); pagination fixed to query maxResults+nextToken. ListAnnotationStores' own status/ids filter still not applied (see deferred)"}
  AnnotationStoreVersion: {status: ok, note: "created ACTIVE immediately (no waiter-hang risk); pagination fixed. ListAnnotationStoreVersions' own status filter still not applied (see deferred)"}
  AnnotationImportJob: {status: ok, note: "completes synchronously; pagination fixed. This pass: ListAnnotationImportJobs now applies its status/storeName body filter and explicit ids list (gap jxc5)"}
  VariantStore: {status: ok, note: "FIXED: GetVariantStore advances CREATING->ACTIVE on first poll (real VariantStoreCreatedWaiter previously hung forever); pagination fixed. ListVariantStores' own status/ids filter still not applied (see deferred)"}
  VariantImportJob: {status: ok, note: "completes synchronously; pagination fixed. This pass: ListVariantImportJobs now applies its status/storeName body filter and explicit ids list (gap jxc5)"}
  Share: {status: ok, note: "Create/Accept/Delete/Get/List; ACCEPTING/DELETED transient statuses returned synchronously, unchanged this pass; pagination fixed. ListShares' own resourceArns/status/resourceTypes filter still not applied (see deferred)"}
  RunCache: {status: ok, note: "CRUD + List; already used correct query params"}
  RunBatch: {status: ok, note: "2026-08-07 (gopherstack-hnhk): body-shape re-architecture. StartRunBatch's real wire shape ({requestId, batchName, batchRunSettings:{inlineSettings|s3UriSettings}, defaultRunSetting:{roleArn,workflowId,...}, tags} -- field-diffed against awsRestjson1_serializeOpDocumentStartRunBatchInput/DefaultRunSetting/BatchRunSettings/InlineSetting) replaces the old flat {workflowId,roleArn,name} shape a real client never sends. Each inlineSettings entry (merged with defaultRunSetting per the documented per-run-override semantics) now creates a real constituent Run via the new startRunLocked helper shared with StartRun -- previously StartRunBatch created zero runs regardless of what a caller sent. GetBatch's real response shape (arn/creationTime/defaultRunSetting/id/name/runSummary/status/submissionSummary/submittedTime/processedTime/tags/totalRuns/uuid -- field-diffed against awsRestjson1_deserializeOpDocumentGetBatchOutput) is now built by a dedicated handler response, separate from ListBatch's smaller BatchListItem shape (arn/createdAt/id/name/status/totalRuns/workflowId) which was previously (and remains, now correctly) served by marshaling the same struct -- a latent leak risk this pass closed by giving each its own wire type instead of widening the shared one. runSummary's pending/running/completed/cancelled/failed counts are computed LIVE from surviving Run rows (summarizeRunBatchLocked) rather than stored, since this backend creates/completes runs synchronously and a stored counter would drift; deletedRunCount and submissionSummary's success/failure counts ARE stored, since DeleteRunsInBatch actually removes the Run rows they'd otherwise be computed from. ListRunsInBatch's runSettingId filter is now real (previously accepted-but-ignored; SubmissionStatus remains accepted-but-ignored -- this backend has no async submission-status state machine, batches complete synchronously). NOT modeled, see gaps: s3UriSettings (rejected with a clear ValidationException rather than silently creating zero runs -- reading real S3 object content synchronously is not something this backend can honestly simulate), most optional DefaultRunSetting fields (cacheBehavior/cacheId/configurationName/engineSettings/logLevel/networkingMode/outputBucketOwnerId/parameters/retentionMode/scratchStorageMode/storageCapacity/storageType/workflowOwnerId), and RequestId idempotency (accepted and required, matching the real API, but not deduplicated against retries)."}
  Configuration: {status: ok, note: "CRUD + List; query params already correct"}
  S3AccessPolicy: {status: ok, note: "FIXED (field-diffed against PutS3AccessPolicyInput/Output and GetS3AccessPolicyOutput, closing the prior deferred item): the policy document was serialized under the invented key \"policy\" -- real GetS3AccessPolicyOutput uses \"s3AccessPolicy\" (confirmed against the SDK deserializer) -- renamed; PutS3AccessPolicy's response now echoes s3AccessPointArn (was an empty {}); added StoreID/StoreType/UpdateTime fields to the model (StoreID/StoreType left empty -- this backend has no S3-access-point-to-store association to derive them from, but they're optional/pointer-safe on the wire)"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource; RouteMatcher correctly scopes /tags/{arn} to arn containing \":omics:\" so FIS's /tags/{arn} isn't stolen"}
gaps:
  - "CLOSED 2026-08-07 (gopherstack-hnhk): RunBatch's real body shape is now modeled and StartRunBatch creates its constituent runs -- see the RunBatch family note above for the full accounting, including what's still not modeled (s3UriSettings, most optional DefaultRunSetting fields, RequestId idempotency dedup)."
  - "ListAnnotationStores/ListVariantStores (status + ids), ListAnnotationStoreVersions (status), and ListShares (resourceArns/status/resourceTypes) still don't apply their own real AWS filter/ids body fields -- same silent-no-op gap class as jxc5 but these 4 ops weren't in that ticket's named list. NOT attempted this pass (hnhk's stated scope also named this and ReferenceMetadata/ReadSetMetadata optional sub-objects; prioritized the RunBatch re-architecture instead given the time available). Needs a new bd issue."
  - "RunBatchFilter.RunGroupID (ListBatch) is accepted from the query string for wire compatibility but not applied -- this backend has no run-group-of-a-batch's-runs association. RunsInBatchFilter.SubmissionStatus (ListRunsInBatch) is likewise accepted but not applied -- this backend has no async submission-status state machine (batches complete submission synchronously). RunSettingID IS now applied (fixed this pass, see RunBatch family note)."
deferred:
  - "Field-by-field diff of ReferenceMetadata/ReadSetMetadata optional sub-object fields (Files/ReferenceFiles, CreationJobId, CreationType, Etag, SequenceInformation) against the SDK model -- MD5/fileType (top-level scalars) are now confirmed correct; the sub-objects remain unpopulated but are optional/pointer-safe on the wire"
leaks: {status: clean, note: "pure synchronous in-memory backend -- no goroutines, tickers, or janitors; nothing to leak (reconfirmed this pass)"}
---

## Notes

**2026-08-13 (gopherstack-jqh2 pass 2):** re-extracted all 107 ops' real
method+path directly from `omics@v1.49.5` serializers.go and drove them
through `ExtractOperation` via `handler_sdk_route_table_test.go`
(`TestExtractOperation_SDKRouteTable`, one subtest per op). All 107 resolved
correctly — the route/dispatch fixes from the 2026-08-07 pass below held, and
no new drift was found. This test is now the permanent regression guard for
route-table drift, replacing ad hoc re-verification on future audits.

Protocol: restjson1. Every op path/method was cross-checked op-by-op against
`aws-sdk-go-v2/service/omics@v1.45.0`'s generated `serializers.go` (both the
`awsRestjson1_serializeOpHttpBindings*Input` — method/URI/query — and
`awsRestjson1_serializeOpDocument*Input` — JSON body — functions for every
op), not against this handler's own output. That direct cross-check is what
surfaced the three route/wire-shape bugs below; `go test ./services/omics/...`
was green throughout because the pre-existing unit tests drive the handler via
`h.Handler()(c)` directly (bypassing `RouteMatcher`) and, worse, used the
*wrong* HTTP method for `ListRunsInBatch` to begin with — the bug and its test
coverage were both wrong in the same direction. This is the same trap class
that hit services/backup, eks, and s3control.

**Waiter-hang state-machine bug (5 resources).** HealthOmics's SDK ships
generated waiters (`WorkflowActiveWaiter`, `WorkflowVersionActiveWaiter`,
`AnnotationStoreCreatedWaiter`, `VariantStoreCreatedWaiter`,
`RunRunningWaiter`/`RunCompletedWaiter`, `TaskRunningWaiter`/
`TaskCompletedWaiter`) that poll `GetWorkflow`/`GetAnnotationStore`/
`GetVariantStore`/`GetWorkflowVersion`/`GetRun`/`GetRunTask` until `Status`
leaves its initial transient value (`CREATING` or `PENDING`). Before this
pass, `Workflow`, `WorkflowVersion`, `AnnotationStore`, `VariantStore` were
created with `Status: CREATING` and nothing ever mutated it, and `Run`/
`RunTask` were created `PENDING` and only ever moved to a terminal state via
explicit `CancelRun`. Any script/test using the real SDK's own waiters (the
idiomatic way to wait for `CreateWorkflow`/`StartRun` to be usable) would
therefore poll forever until the waiter's own `maxWaitDur` elapsed. Fixed by
adding an unexported `pollCount int` field to each of these five structs
(intentionally not JSON-tagged, so it isn't part of the wire shape and isn't
persisted across a snapshot/restore — the exact same pattern
`services/kafka`'s `Cluster.pollCount` / `DescribeCluster` already uses) and
advancing `Status` by one step each time the corresponding `Get*` op is
called: `CREATING`→`ACTIVE` on the first poll for the four store/workflow
resources, `PENDING`→`RUNNING`→`COMPLETED` across the first two polls for
`Run`/`RunTask`. `List*` ops deliberately do **not** advance status (matching
kafka's `ListClusters` precedent) — only the real waiters' polling target
(`Get*`) does.

**Pagination wire-shape bug (16 List ops, all with a `filter` body field).**
For `ListReferenceStores`, `ListReferences`, `ListReferenceImportJobs`,
`ListSequenceStores`, `ListReadSets`, `ListReadSetActivationJobs`,
`ListReadSetExportJobs`, `ListReadSetImportJobs`,
`ListMultipartReadSetUploads`, `ListReadSetUploadParts`,
`ListAnnotationImportJobs`, `ListVariantImportJobs`,
`ListAnnotationStoreVersions`, `ListShares`, `ListAnnotationStores`,
`ListVariantStores`, the real SDK always sends `maxResults`/`nextToken` as
**query-string** parameters — only the optional `filter` (plus, for a few
ops, `ids`/`resourceOwner`) travels in the JSON body. This handler previously
read `maxResults`/`nextToken` from the JSON body for all of these, which real
SDK clients never populate there, so pagination was silently broken: a
client's `nextToken` was ignored (every page restarted from the top) and
`maxResults` defaulted to the 100-item cap regardless of what the client
asked for. Fixed via a new `listQueryParams(c)` helper (reads `maxResults` +
`nextToken` from the query string) used by all 16 handlers; the `filter` body
struct is kept as-is where the backend already supports it.

The RunGroup/Run/RunTask/Workflow/WorkflowVersion/RunCache/Configuration
family instead uses `maxResults` + **`startingToken`** (not `nextToken`) —
those already read correctly via the pre-existing `paginationQueryParams`
helper and were not touched. The RunBatch family (`ListBatch`/
`ListRunsInBatch`) uses **`maxItems`** (not `maxResults`) + `startingToken`;
a new `batchQueryParams(c)` helper was added for those two.

**RunBatch family: three compounding bugs, all in the same neighborhood.**
1. `ListRunsInBatch` (real wire shape: `GET /runBatch/{batchId}/run`) was
   classified under `classifyPOST` instead of `classifyGET`, so it was
   entirely unreachable by a real SDK client (which always sends GET); a
   POST to that path fell through to `opUnknown` → 501. The two pre-existing
   `parity_test.go` tests for this op used `http.MethodPost`, which is why
   green tests didn't catch it.
2. Real `DeleteBatch` (`DELETE /runBatch/{batchId}`) deletes the batch
   *resource*; real `DeleteRunBatch` (`POST /runBatch/delete`, body
   `{"batchId": "<single id>"}`) deletes the *runs* belonging to a batch and
   leaves the batch resource intact — confirmed against `DeleteBatchInput`/
   `DeleteRunBatchInput` and their serializers, which both take a single
   `BatchId *string`, not an array, and `DeleteRunBatchOutput` is empty (no
   error list). This codebase had the two operations' wire paths bound to the
   opposite handler bodies: `DELETE /runBatch/{id}` ran a single-ID
   batch-record delete under the *`DeleteRunBatch`* op name, and
   `POST /runBatch/delete` expected a `{"batchIds": [...]}` array and
   bulk-deleted batch *records* (not runs) under the *`DeleteBatch`* op name.
   Fixed by swapping which op constant `classifyDELETE`/`classifyPOST` return
   for each path, swapping the two handler function bodies to match, and
   replacing `InMemoryBackend.DeleteRunBatches([]string)` with
   `DeleteRunsInBatch(batchID string)` (cascades to the run's `RunTask`s, not
   the `RunBatch` record).
3. `ListBatch`/`ListRunsInBatch` pagination used the query key `maxResults`;
   real AWS uses `maxItems` for these two ops specifically (see the
   pagination note above).

**Trap for the next auditor:** `RunBatch`/`ReadSetActivationJob`/
`ReadSetExportJob`/`ReadSetImportJob`/`AnnotationImportJob`/
`VariantImportJob`/`ReferenceImportJob`/`AnnotationStoreVersion` are all
created with a terminal `Status` (`COMPLETED`/`ACTIVE`) synchronously — this
looks superficially like the same "never transitions" bug class as
Workflow/AnnotationStore/VariantStore/Run/RunTask, but it is **not** a bug:
these resources have no real-AWS waiter that ever expects to observe a
transient state first (`RunBatch` in particular starts `COMPLETED` because
this emulator has no actual async batch-run orchestration to model), so
returning the terminal state immediately is correct and matches how a fast
real backend would eventually settle. Confirm by checking whether the SDK
ships a `*Waiter` for the corresponding `Get*`/`Describe*` op before flagging
this pattern again.
