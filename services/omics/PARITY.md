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
  MultipartReadSetUpload: {status: ok, note: "FIXED (field-diffed against CreateMultipartReadSetUploadInput/Output and MultipartReadSetUploadListItem): the file-type field was serialized as the invented key \"sequenceType\" -- renamed to the real key \"sourceFileType\"; SampleID/SubjectID are real required fields that were missing entirely -- added and threaded through CreateMultipartReadSetUpload's signature; there is no real \"status\" field on this resource at all -- the invented one was deleted. GeneratedFrom/ReferenceARN/Description (real optional fields) also added. 2026-08-14 (gopherstack-7185, mutating-op sweep): CompleteMultipartReadSetUploadOutput's real (and only) member is \"readSetId\" (deserializers.go's awsRestjson1_deserializeOpDocumentCompleteMultipartReadSetUploadOutput) -- a different key from GetReadSetMetadataOutput's \"id\" for the same resource, same split-response class as the AnnotationImportJob/VariantImportJob start-vs-get bugs fixed elsewhere in this file. The handler previously marshaled the full ReadSetMetadata struct (tagged \"id\") as the Complete response, so a real client's ReadSetId was always nil -- the next call in the natural create-then-GetReadSetMetadata chain would silently receive a zero-value ID. Fixed to emit the dedicated {\"readSetId\": ...} shape."}
  RunGroup: {status: ok, note: "CRUD + List; already used correct maxResults+startingToken query params; ListRunGroups now applies its name query filter (bonus find alongside gap jxc5, real AWS ListRunGroupsInput has a \"name\" query param the backend previously ignored)"}
  Run: {status: ok, note: "FIXED: GetRun advances PENDING->RUNNING->COMPLETED across polls (waiter-hang fix, prior pass). This pass: (1) ListRuns now applies its name/runGroupId/batchId/status query filters (gap jxc5); (2) the run's batch association was serialized under the invented JSON key \"runBatchId\" -- real GetRunOutput/RunListItem use \"batchId\" (confirmed against the SDK deserializer) -- renamed; (3) added the real (previously entirely absent) RunGroupID field, threaded through StartRun so ListRuns' runGroupId filter has something real to match against; (4) StartRun/GetRun responses now include the optional uuid/networkingMode/runOutputUri/configuration fields real StartRunOutput/GetRunOutput have (gap fedo) -- networkingMode/outputUri are accepted from the request body (real StartRunInput field names, note outputUri on input vs runOutputUri on output)"}
  RunTask: {status: ok, note: "FIXED: GetRunTask advances PENDING->RUNNING->COMPLETED across polls, same waiter-hang fix as Run. This pass: ListRunTasks now applies its status query filter (gap jxc5)"}
  Workflow: {status: ok, note: "FIXED: GetWorkflow advances CREATING->ACTIVE on first poll (waiter-hang fix, prior pass). This pass: (1) ListWorkflows now applies its name/type query filters (gap jxc5); (2) CreateWorkflow's response now includes the optional uuid field real CreateWorkflowOutput has (gap fedo)"}
  WorkflowVersion: {status: ok, note: "FIXED: GetWorkflowVersion advances CREATING->ACTIVE on first poll (waiter-hang fix, prior pass); pagination already correct. This pass: ListWorkflowVersions now applies its type query filter (gap jxc5)"}
  AnnotationStore: {status: ok, note: "FIXED: GetAnnotationStore advances CREATING->ACTIVE on first poll (real AnnotationStoreCreatedWaiter previously hung forever); pagination fixed to query maxResults+nextToken. ListAnnotationStores' own status/ids filter still not applied (see deferred). 2026-08-13 (gopherstack-lx5h/kb66): the ARN was tagged json:\"arn\" -- real GetAnnotationStoreOutput/AnnotationStoreItem wire key is \"storeArn\" (deserializers.go:6266) -- renamed to StoreArn/storeArn. Added NumVersions (real required \"numVersions\", deserializers.go:6225), computed live from annotationVersionsByStore at Get/List/Update time rather than stored, since a stored counter would drift as versions are added/deleted. Added StoreSizeBytes (real required \"storeSizeBytes\", deserializers.go:6289) -- this backend does not track actual stored bytes, so it is always 0 (modeled honestly, not fabricated) rather than omitted, since the field is required on the real wire. 2026-08-13 (gopherstack-7s8r): added StatusMessage (real required GetAnnotationStoreOutput field, deserializers.go) -- always empty, no error state tracked. Separately found and NOT fixed this pass: ListAnnotationStores marshals this same struct, leaking NumVersions/Tags/StoreOptions, which the real List element (AnnotationStoreItem, types.go:152-211) lacks -- needs a follow-up bd issue, same class of bug as the import-job List/Get split fixed below"}
  AnnotationStoreVersion: {status: ok, note: "created ACTIVE immediately (no waiter-hang risk); pagination fixed. ListAnnotationStoreVersions' own status filter still not applied (see deferred). 2026-08-13 (gopherstack-lx5h/kb66): the ARN was tagged json:\"arn\" -- real GetAnnotationStoreVersionOutput/AnnotationStoreVersionItem wire key is \"versionArn\" (deserializers.go:6564) -- renamed to VersionArn/versionArn. Added VersionSizeBytes (real required \"versionSizeBytes\", deserializers.go:6587) -- always 0, same not-tracked rationale as AnnotationStore.StoreSizeBytes. 2026-08-13 (gopherstack-7s8r): added StatusMessage (real required GetAnnotationStoreVersionOutput field) -- always empty, no error state tracked"}
  AnnotationImportJob: {status: ok, note: "completes synchronously; pagination fixed. This pass: ListAnnotationImportJobs now applies its status/storeName body filter and explicit ids list (gap jxc5). 2026-08-13 (gopherstack-lx5h/kb66): StartAnnotationImportJob's response was built by marshaling this same domain struct with its ID field tagged json:\"id\" -- correct for GetAnnotationImportJobOutput/AnnotationImportJobItem (deserializers.go:5954/21500s) but WRONG for StartAnnotationImportJobOutput, whose only member is \"jobId\" (deserializers.go:17434). The two ops don't share a response shape in the real API, so this needed splitting rather than a rename: the start handler now builds its own {\"jobId\": ...} response and leaves the shared struct's \"id\" tag alone. Also added FormatOptions/RunLeftNormalization/VersionName/StatusMessage/UpdateTime/AnnotationFields -- real GetAnnotationImportJobOutput required members (deserializers.go:5949-6015) and real StartAnnotationImportJobInput optional members (serializers.go:7892-7935) that were entirely absent from this struct before -- a schema gap, not a dropped key, on both the request and response sides. FormatOptions is modeled as a passthrough map (same convention as Reference/SseConfig/StoreOptions elsewhere in this service); StatusMessage is always empty (no error state to describe -- this backend completes synchronously). 2026-08-13 (gopherstack-7s8r): fixed the deferred item-level JobStatus gap -- Items is now []AnnotationImportItemDetail (real GetAnnotationImportJobOutput.Items shape, JobStatus+Source, types.go:75-89) instead of reusing the Start-request-only ItemSource shape (Source only, types.go:91-99, still what AnnotationImportItem models and StartAnnotationImportJobInput.Items correctly uses). JobStatus is stamped once from the job's own Status at Start time, since this backend completes synchronously in one step so that is each item's true final state. The originating issue also assumed ListAnnotationImportJobs returns ItemDetail-shaped Items; verified false against the pinned SDK -- the real List element (AnnotationImportJobItem, types.go:102-146) has no items/formatOptions/statusMessage member at all, narrower than Get, so this backend's prior habit of marshaling the Get-shaped struct for List leaked all three. List now builds a dedicated AnnotationImportJobSummary"}
  VariantStore: {status: ok, note: "FIXED: GetVariantStore advances CREATING->ACTIVE on first poll (real VariantStoreCreatedWaiter previously hung forever); pagination fixed. ListVariantStores' own status/ids filter still not applied (see deferred). 2026-08-13 (gopherstack-lx5h/kb66): the ARN was tagged json:\"arn\" -- real GetVariantStoreOutput/VariantStoreItem wire key is \"storeArn\" (deserializers.go:11673) -- renamed to StoreArn/storeArn. Added StoreSizeBytes (real required \"storeSizeBytes\", deserializers.go:11682) -- always 0, not tracked (see AnnotationStore note). VariantStore has no NumVersions concept in the real API (confirmed: GetVariantStoreOutput/VariantStoreItem have no such field) -- correctly not added. 2026-08-13 (gopherstack-7s8r): added StatusMessage (real required GetVariantStoreOutput field) -- always empty, no error state tracked. Same unfixed ListVariantStores over-share as AnnotationStore (see its note)"}
  VariantImportJob: {status: ok, note: "completes synchronously; pagination fixed. This pass: ListVariantImportJobs now applies its status/storeName body filter and explicit ids list (gap jxc5). 2026-08-13 (gopherstack-lx5h/kb66): same StartVariantImportJobOutput \"jobId\" (deserializers.go:18893) vs GetVariantImportJobOutput \"id\" (deserializers.go:11383) split-response bug as AnnotationImportJob above -- found by reading the whole Start/Get operation pair, not itemized in either originating bd issue, fixed the same way (dedicated {\"jobId\": ...} start response). Added RunLeftNormalization/StatusMessage/UpdateTime/AnnotationFields (real GetVariantImportJobOutput required members, deserializers.go:11406-11444, and StartVariantImportJobInput optional members, serializers.go:8737-8767) -- previously absent entirely. Unlike AnnotationImportJob, variant import jobs have NO FormatOptions or VersionName field anywhere in the real API (confirmed against both StartVariantImportJobInput and GetVariantImportJobOutput) -- correctly not added, verified rather than assumed from the annotation sibling. 2026-08-13 (gopherstack-7s8r): fixed the deferred item-level JobStatus gap, same treatment as AnnotationImportJob -- Items is now []VariantImportItemDetail (JobStatus+Source+optional StatusMessage, types.go:2060-2071); StartVariantImportJobInput.Items keeps using VariantImportItemSource (Source only, types.go:2079-2087) via the unchanged VariantImportItem type. ListVariantImportJobs also over-shared Items/StatusMessage vs the real narrower List element (VariantImportJobItem, types.go:2090-2132) -- same false List==Get premise as AnnotationImportJob, fixed the same way with a dedicated VariantImportJobSummary"}
  Share: {status: ok, note: "Create/Accept/Delete/Get/List; ACCEPTING/DELETED transient statuses returned synchronously, unchanged this pass; pagination fixed. ListShares' own resourceArns/status/resourceTypes filter still not applied (see deferred). 2026-08-14 (gopherstack-7185, mutating-op sweep): the shared Share model's Name field was tagged json:\"name\" -- real CreateShareOutput and ShareDetails (used by Get/List) both use the wire key \"shareName\" (deserializers.go:3062 and :26670) -- confirmed against the request side of the same handler, which already read the input as \"shareName\". A real client's ShareName was always empty on every op that returns a Share. Fixed by retagging the field; CreateShareOutput/AcceptShareOutput/DeleteShareOutput are each narrower than the full Share struct on the real wire (e.g. AcceptShareOutput/DeleteShareOutput carry only \"status\") but the extra fields this backend still emits are harmless since unknown keys are silently dropped, not a correctness bug."}
  RunCache: {status: ok, note: "CRUD + List; already used correct query params. 2026-08-14 (gopherstack-7185, mutating-op sweep): RunCache.CacheS3Location was tagged json:\"cacheS3Location\" -- that key is real only for CreateRunCacheInput's request body (serializers.go:1334); every RESPONSE shape (CreateRunCacheOutput, GetRunCacheOutput, ListRunCaches' element) uses the different key \"cacheS3Uri\" (deserializers.go:9853). A real client's CacheS3Uri was always nil on every read of a run cache. Fixed by retagging the model field (the handler's separate request-parsing struct already correctly used \"cacheS3Location\" and was untouched)."}
  RunBatch: {status: ok, note: "2026-08-07 (gopherstack-hnhk): body-shape re-architecture. StartRunBatch's real wire shape ({requestId, batchName, batchRunSettings:{inlineSettings|s3UriSettings}, defaultRunSetting:{roleArn,workflowId,...}, tags} -- field-diffed against awsRestjson1_serializeOpDocumentStartRunBatchInput/DefaultRunSetting/BatchRunSettings/InlineSetting) replaces the old flat {workflowId,roleArn,name} shape a real client never sends. Each inlineSettings entry (merged with defaultRunSetting per the documented per-run-override semantics) now creates a real constituent Run via the new startRunLocked helper shared with StartRun -- previously StartRunBatch created zero runs regardless of what a caller sent. GetBatch's real response shape (arn/creationTime/defaultRunSetting/id/name/runSummary/status/submissionSummary/submittedTime/processedTime/tags/totalRuns/uuid -- field-diffed against awsRestjson1_deserializeOpDocumentGetBatchOutput) is now built by a dedicated handler response, separate from ListBatch's smaller BatchListItem shape (arn/createdAt/id/name/status/totalRuns/workflowId) which was previously (and remains, now correctly) served by marshaling the same struct -- a latent leak risk this pass closed by giving each its own wire type instead of widening the shared one. runSummary's pending/running/completed/cancelled/failed counts are computed LIVE from surviving Run rows (summarizeRunBatchLocked) rather than stored, since this backend creates/completes runs synchronously and a stored counter would drift; deletedRunCount and submissionSummary's success/failure counts ARE stored, since DeleteRunsInBatch actually removes the Run rows they'd otherwise be computed from. ListRunsInBatch's runSettingId filter is now real (previously accepted-but-ignored; SubmissionStatus remains accepted-but-ignored -- this backend has no async submission-status state machine, batches complete synchronously). NOT modeled, see gaps: s3UriSettings (rejected with a clear ValidationException rather than silently creating zero runs -- reading real S3 object content synchronously is not something this backend can honestly simulate), most optional DefaultRunSetting fields (cacheBehavior/cacheId/configurationName/engineSettings/logLevel/networkingMode/outputBucketOwnerId/parameters/retentionMode/scratchStorageMode/storageCapacity/storageType/workflowOwnerId), and RequestId idempotency (accepted and required, matching the real API, but not deduplicated against retries)."}
  Configuration: {status: fixed, note: "gopherstack-4ggy: CreateConfiguration's RunConfigurations (a required CreateConfigurationInput member, api_op_CreateConfiguration.go:30-55) was dropped entirely, and the response was a near-total fabrication -- Configuration previously had only {creationTime,name,description,value}, where \"value\" is not a real field anywhere in the API at all (invented) and Arn/Status/Tags/Uuid/RunConfigurations (all real CreateConfigurationOutput/GetConfigurationOutput members) were simply absent. Rebuilt to the real shape: RunConfigurations now required and validated, ARN synthesized via pkgs/arn (arn:aws:omics:<region>:<account>:configuration/<uuid>, matching this service's existing workflow/run-group ARN convention), Status set to ACTIVE immediately (this resource has no async provisioning to model), Tags stored and echoed, Uuid populated. RunConfigurations.VpcConfig models SecurityGroupIds/SubnetIds; the response-only computed VpcId (types.VpcConfigResponse) is left empty rather than fabricated -- this backend does no real VPC/subnet resolution. RequestId (also client-side-required, but auto-filled by the SDK's IdempotencyTokenAutoFill middleware before validation runs, so a real client never omits it) is accepted but not enforced or deduplicated server-side -- out of scope for this fix, same category as RunBatch's RequestId gap noted below."}
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

**2026-08-13 (gopherstack-lx5h/gopherstack-kb66):** fixed the omics items
these two bd issues deferred from the required-response-member sweep (the
other 7 services across both issues were fixed elsewhere; omics was held by
another agent at the time). All four premises verified against the pinned
`omics@v1.49.5` `deserializers.go`/`serializers.go` (path resolved from
`go.mod`, read only that module-cache copy, not a stale sibling version):

- `GetAnnotationStore`/`GetVariantStore` ARN tagged `json:"arn"` → real key
  `storeArn` (deserializers.go:6266/11673).
- `GetAnnotationStoreVersion` ARN tagged `json:"arn"` → real key
  `versionArn` (deserializers.go:6564).
- `StartAnnotationImportJob` job id tagged `json:"id"` → real key `jobId`
  (deserializers.go:17434) — but `GetAnnotationImportJob`/
  `ListAnnotationImportJobs` genuinely use `id` (deserializers.go:5954), so
  this was NOT a blanket rename: `AnnotationImportJob.ID` keeps its `id` tag
  (correct for Get/List) and `handleStartAnnotationImportJob` now builds a
  dedicated `{"jobId": ...}` response instead of marshaling the domain
  struct. Reading the whole Start/Get operation pair (not just the one op
  gopherstack-lx5h named) found the *identical* bug on
  `StartVariantImportJob` (real key `jobId`, deserializers.go:18893, vs
  `GetVariantImportJob`'s `id`, deserializers.go:11383) — fixed the same way,
  reported here since neither originating issue named it.
- `NumVersions`/`StoreSizeBytes` (AnnotationStore, VariantStore) and
  `VersionSizeBytes` (AnnotationStoreVersion) had no model field at all.
  `NumVersions` is derived live from `annotationVersionsByStore` (the backend
  already tracks per-store version rows via that index) rather than stored,
  to avoid drift. The two size fields are honestly modeled but always `0`:
  nothing in this in-memory backend measures real stored bytes, and a
  required wire field can't be omitted the way an optional one can, so `0`
  (not a fabricated plausible-looking number) is what a client receives.
- `FormatOptions`/`RunLeftNormalization` were missing from
  `GetAnnotationImportJob`/`GetVariantImportJob` on both the request
  (`StartAnnotationImportJob`/`StartVariantImportJob` silently dropped
  caller-supplied values — the handler never even read them into its request
  struct) and response sides — a schema gap, not a dropped key, per the
  issue's framing. Field-diffing the full `GetAnnotationImportJobOutput`/
  `GetVariantImportJobOutput` shapes (not just the two named fields) while
  already inside these structs turned up the same class of gap on
  `VersionName` (annotation only — confirmed variant import jobs have no
  such field anywhere in the real API), `StatusMessage`, `UpdateTime`, and
  `AnnotationFields`; all four are also real required (or accepted-but-
  dropped optional) members of the same ops, so they were closed in the same
  pass rather than left half-fixed next to the two the issue named.

Two further gaps were found but NOT fixed this pass, to keep scope to what
the two structs actually needed for their named ops (both worth a follow-up
bd issue): `StatusMessage` (real required `GetAnnotationStoreOutput`/
`GetVariantStoreOutput`/`GetAnnotationStoreVersionOutput` field,
deserializers.go:6257 etc.) is absent from `AnnotationStore`/`VariantStore`/
`AnnotationStoreVersion` entirely — a wider version of the same StatusMessage
gap closed on the import-job structs above, but touching three more
Create/Get/Update/List families was out of scope here; and per-item
`JobStatus` (real required `AnnotationImportItemDetail`/
`VariantImportItemDetail` field, types.go:75-88/2060-2076) is still missing
from `AnnotationImportJob.Items`/`VariantImportJob.Items`, which only carry
`Source` — `StartAnnotationImportJob`/`StartVariantImportJob` take
`ItemSource` (Source only) but `Get`/`List` return `ItemDetail` (Source +
JobStatus), two genuinely different real shapes this service currently
conflates into one.

Proof: `wire_field_additions_test.go` drives the real `aws-sdk-go-v2/service/
omics` client against an `httptest` server (same pattern as
`services/acm/wire_field_additions_test.go`) for all of the above — a raw-
JSON assertion against the wrong key would have passed against the bug, so
only round-tripping through the genuine SDK deserializer proves the fix.
Every case was hand-verified to fail against the pre-fix code (tag/field/
handler-arg reverted, test re-run, re-applied) before being counted as
proof, not just written and trusted: 8 tests, 7 of 8 fix categories directly
reverted and re-confirmed failing (the eighth, `VersionSizeBytes`, shares its
model-diffing proof with `StoreSizeBytes`/`NumVersions`).

The annotation/variant-store family of operations routes to a real
`analytics-<region>...` endpoint-host-prefix (see e.g.
`endpointPrefix_opGetAnnotationStoreMiddleware` in the generated SDK); the
test helper disables it via `smithyhttp.DisableEndpointHostPrefix` on an
Initialize-step middleware so the SDK talks to the local `httptest` server
instead of trying to resolve a nonexistent `analytics-127.0.0.1` host.

**2026-08-13 (gopherstack-7s8r):** closed the two gaps the prior pass
deferred, both verified against the pinned `omics@v1.49.5`
`deserializers.go`/`types/types.go`:

- `StatusMessage` is a real required member of `GetAnnotationStoreOutput`,
  `GetVariantStoreOutput` and `GetAnnotationStoreVersionOutput` and was
  absent from `AnnotationStore`/`VariantStore`/`AnnotationStoreVersion`
  entirely (the equivalent gap on the import-job structs was already closed
  in c41d36cb6). Added to all three, always empty: none of these ops track
  an error state to describe.
- The `Items` conflation: the issue's stated premise was that Start returns
  `ItemSource` (Source only) while Get *and List* return `ItemDetail`
  (Source + required `JobStatus`). Verified half true, half false against
  the SDK: Get does return `ItemDetail` — confirmed for both
  `AnnotationImportItemDetail` (types.go:75-89) and
  `VariantImportItemDetail` (types.go:2060-2071, which also carries an
  optional `StatusMessage` the annotation variant lacks) — but List does
  not return `Items` at all. The real List element types
  (`AnnotationImportJobItem`, types.go:102-146;
  `VariantImportJobItem`, types.go:2090-2132) have no `items`,
  `formatOptions` or `statusMessage` member whatsoever — a narrower shape
  than Get, the same class of gap `c41d36cb6` already found and split for
  Start's `jobId`-only response. `AnnotationImportJob`/`VariantImportJob`
  (used for Get) now carry `Items []AnnotationImportItemDetail`/
  `[]VariantImportItemDetail`, populated once at Start time by stamping
  every source item with the job's own `Status` — honest because this
  backend always completes import jobs synchronously in one step, so that
  status is each item's genuine final state, not a guess partway through an
  async pipeline that doesn't exist here. `AnnotationImportItem`/
  `VariantImportItem` (the pre-existing Source-only structs) are unchanged
  and now documented as exactly `ItemSource`, still correct for
  `StartAnnotationImportJobInput.Items`/`StartVariantImportJobInput.Items`.
  New `AnnotationImportJobSummary`/`VariantImportJobSummary` types back the
  List responses instead of the Get-shaped domain structs.

Five new tests in `wire_field_additions_test.go`, all driving the real
`aws-sdk-go-v2/service/omics` client (or, for the two List-narrowing tests,
a raw HTTP body inspection — the SDK's `ListAnnotationImportJobsOutput`/
`ListVariantImportJobsOutput` deserializers silently discard unrecognized
keys, so an SDK round trip cannot detect an over-wide response the way it
can detect a missing field; only inspecting the raw wire body proves the
extra keys are gone). All five were hand-verified to fail against the
pre-fix code (files reverted to `HEAD`, tests re-run, fix re-applied) before
being counted as proof: `Test_SDKRoundTrip_StatusMessage`,
`Test_SDKRoundTrip_AnnotationImportJob_ItemDetail`,
`Test_SDKRoundTrip_VariantImportJob_ItemDetail`,
`TestListAnnotationImportJobs_OmitsGetOnlyFields`,
`TestListVariantImportJobs_OmitsGetOnlyFields`.

Found while reading the whole operations but NOT fixed this pass (separate,
narrower scope than the two named findings; worth its own bd issue):
`ListAnnotationStores`/`ListVariantStores`/`ListAnnotationStoreVersions`
have the identical List-narrower-than-Get defect just fixed for import
jobs — the real List element types (`AnnotationStoreItem`, types.go:152-211;
similarly for variant stores and store versions) lack `NumVersions`/`Tags`/
`StoreOptions` that `Get*StoreOutput` requires, but this backend still
marshals the full Get-shaped store struct for List, leaking those fields.

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
