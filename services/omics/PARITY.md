---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: omics
sdk_module: aws-sdk-go-v2/service/omics@v1.45.0
last_audit_commit: 42cff5ce
last_audit_date: 2026-07-12
overall: A            # genuine fixes found: waiter-hang state bugs, service-wide pagination
                       # wire-shape bug, a route-matcher reachability bug, and a swapped
                       # operation-semantics bug in the RunBatch family
families:
  ReferenceStore: {status: ok, note: "CRUD + List; pagination now reads maxResults/nextToken from the query string (was body)"}
  Reference: {status: ok, note: "Get/List/Delete + GetReferenceBytes/GetReferenceMetadata; pagination fixed same as ReferenceStore"}
  ReferenceImportJob: {status: ok, note: "completes synchronously (Status=COMPLETED at creation) -- no waiter-hang risk since Get never needs to transition; pagination fixed"}
  SequenceStore: {status: ok, note: "CRUD + List; created ACTIVE immediately (no CREATING phase in the real API for this resource); pagination fixed"}
  ReadSet: {status: ok, note: "Get/List/BatchDelete/GetReadSetBytes; pagination fixed"}
  ReadSetActivationJob: {status: ok, note: "completes synchronously; pagination fixed"}
  ReadSetExportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  ReadSetImportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  MultipartReadSetUpload: {status: ok, note: "Create/Abort/Complete/List/ListParts/UploadPart; SHA256 checksum on UploadReadSetPart matches real behavior; pagination fixed"}
  RunGroup: {status: ok, note: "CRUD + List; already used correct maxResults+startingToken query params"}
  Run: {status: ok, note: "FIXED: GetRun now advances PENDING->RUNNING->COMPLETED across polls (real RunRunningWaiter/RunCompletedWaiter poll GetRun expecting exactly this transition; previously runs stayed PENDING forever and any waiter-based client would time out)"}
  RunTask: {status: ok, note: "FIXED: GetRunTask advances PENDING->RUNNING->COMPLETED across polls, same waiter-hang fix as Run"}
  Workflow: {status: ok, note: "FIXED: GetWorkflow advances CREATING->ACTIVE on first poll (real WorkflowActiveWaiter previously hung forever); CreateWorkflow still correctly returns CREATING + partial {arn,id,status,tags} envelope"}
  WorkflowVersion: {status: ok, note: "FIXED: GetWorkflowVersion advances CREATING->ACTIVE on first poll (real WorkflowVersionActiveWaiter previously hung forever); pagination for ListWorkflowVersions already correct (query maxResults+startingToken)"}
  AnnotationStore: {status: ok, note: "FIXED: GetAnnotationStore advances CREATING->ACTIVE on first poll (real AnnotationStoreCreatedWaiter previously hung forever); pagination fixed to query maxResults+nextToken"}
  AnnotationStoreVersion: {status: ok, note: "created ACTIVE immediately (no waiter-hang risk); pagination fixed"}
  AnnotationImportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  VariantStore: {status: ok, note: "FIXED: GetVariantStore advances CREATING->ACTIVE on first poll (real VariantStoreCreatedWaiter previously hung forever); pagination fixed"}
  VariantImportJob: {status: ok, note: "completes synchronously; pagination fixed"}
  Share: {status: ok, note: "Create/Accept/Delete/Get/List; ACCEPTING/DELETED transient statuses returned synchronously, unchanged this pass; pagination fixed"}
  RunCache: {status: ok, note: "CRUD + List; already used correct query params"}
  RunBatch: {status: ok, note: "FIXED (3 bugs): (1) ListRunsInBatch was routed as POST, real AWS sends GET /runBatch/{batchId}/run -- completely unreachable by a real SDK client; (2) DeleteBatch (DELETE /runBatch/{batchId}, deletes the batch resource) and DeleteRunBatch (POST /runBatch/delete, body {batchId}, deletes the runs in a batch) had their wire-path<->semantics swapped -- DELETE /runBatch/{id} ran DeleteRunBatch's old body-array bulk-delete logic and POST /runBatch/delete expected a nonexistent {batchIds:[...]} array; (3) ListBatch/ListRunsInBatch pagination used query key maxResults, real AWS uses maxItems"}
  Configuration: {status: ok, note: "CRUD + List; query params already correct"}
  S3AccessPolicy: {status: ok, note: "Put/Get/Delete by ARN; body shape not diffed field-by-field against SDK (deferred)"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource; RouteMatcher correctly scopes /tags/{arn} to arn containing \":omics:\" so FIS's /tags/{arn} isn't stolen"}
gaps:
  - "List* ops (ListRuns, ListWorkflows, ListRunTasks, ListWorkflowVersions, ListBatch, ListRunsInBatch, *ImportJobs) accept optional filter/name/status/type/ids query or body params on real AWS; InMemoryBackend signatures don't take them so filtering silently no-ops (bd: gopherstack-jxc5)"
  - "DeleteBatch has no terminal-state precondition (real AWS requires PROCESSED/FAILED/CANCELLED/RUNS_DELETED before allowing the batch resource to be deleted) (bd: gopherstack-x7qq)"
  - "CreateWorkflow/StartRun response envelopes omit optional uuid/configuration/networkingMode/runOutputUri fields (wire-safe since they're pointer-optional on the SDK side, just lower fidelity) (bd: gopherstack-fedo)"
deferred:
  - "Field-by-field diff of S3AccessPolicy body shape against the SDK model"
  - "Field-by-field diff of ReferenceMetadata/ReadSetMetadata optional fields (Md5, file-type sub-objects) against the SDK model"
leaks: {status: clean, note: "pure synchronous in-memory backend -- no goroutines, tickers, or janitors; nothing to leak"}
---

## Notes

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
