---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: glacier
sdk_module: aws-sdk-go-v2/service/glacier@v1.32.4
last_audit_commit: f8ae77eb7c84189d9fca29cce357a9cfaf72fd9c
last_audit_date: 2026-07-24
overall: A            # both deferred resource families (Select jobs, range inventory retrieval) now implemented for real + field-diffed; 1 pre-existing leak fixed
ops:
  CreateVault:            {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVault:          {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVault:            {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes jobs/uploads/lock; blocks on non-empty vault; this pass fixed a leak where cascade-deleting a vault's multipart uploads dropped the store.Table row but orphaned the raw multipartParts map entry (see Notes)"}
  ListVaults:             {wire: ok, errors: ok, state: ok, persist: ok, note: "marker/limit pagination verified vs SDK Marker/VaultList shape"}
  UploadArchive:          {wire: ok, errors: ok, state: ok, persist: ok, note: "ArchiveId/Checksum/Location are header-only on real wire (confirmed via awsRestjson1_deserializeOpHttpBindingsUploadArchiveOutput); gopherstack sets all three headers correctly, body is a harmless bonus"}
  DeleteArchive:          {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateJob:            {wire: ok, errors: ok, state: ok, persist: ok, note: "response is header-only (X-Amz-Job-Id/x-amz-job-output-path/Location) on real wire; verified. This pass added real support for JobParameters.Type=select (SelectParameters/OutputLocation, full field validation, MissingParameterValueException vs InvalidParameterValueException distinguished) and JobParameters.InventoryRetrievalParameters (range inventory retrieval: StartDate/EndDate/Limit/Marker, validated) -- see Notes"}
  DescribeJob:            {wire: ok, errors: ok, state: ok, persist: ok, note: "GlacierJobDescription now also carries JobOutputPath/OutputLocation/SelectParameters (select jobs) and a proper nested InventoryRetrievalParameters object (range inventory retrieval jobs) -- see Notes for the invented top-level Format field this replaced"}
  ListJobs:               {wire: ok, errors: ok, state: ok, persist: ok, note: "same describeJobResponse DTO as DescribeJob, same coverage applies"}
  GetJobOutput:           {wire: ok, errors: ok, state: ok, persist: ok, note: "archive-retrieval/inventory-retrieval unchanged; select jobs now execute their SQL Expression for real against the stored archive (see select_jobs family note) instead of erroring/stubbing"}
  SetVaultNotifications:      {wire: ok, errors: ok, state: ok, persist: ok}
  GetVaultNotifications:      {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVaultNotifications:   {wire: ok, errors: ok, state: ok, persist: ok}
  SetVaultAccessPolicy:       {wire: ok, errors: ok, state: ok, persist: ok}
  GetVaultAccessPolicy:       {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVaultAccessPolicy:    {wire: ok, errors: ok, state: ok, persist: ok}
  AddTagsToVault:         {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForVault:       {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromVault:    {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateVaultLock:      {wire: ok, errors: ok, state: ok, persist: ok}
  AbortVaultLock:         {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteVaultLock:      {wire: ok, errors: ok, state: ok, persist: ok}
  GetVaultLock:           {wire: ok, errors: ok, state: ok, persist: ok, note: "24h InProgress expiry verified"}
  GetDataRetrievalPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FreeTier default matches AWS"}
  SetDataRetrievalPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateMultipartUpload:   {wire: ok, errors: ok, state: ok, persist: ok, note: "response header-only (Location/x-amz-multipart-upload-id) confirmed"}
  UploadMultipartPart:       {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteMultipartUpload:   {wire: ok, errors: ok, state: ok, persist: ok, note: "response header-only (ArchiveId/Checksum/Location) confirmed, same as UploadArchive"}
  AbortMultipartUpload:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListMultipartUploads:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListParts:                 {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisionedCapacity:      {wire: ok, errors: ok, state: ok, persist: ok}
  PurchaseProvisionedCapacity:  {wire: ok, errors: ok, state: ok, persist: ok, note: "2-unit cap + monthly expiry verified"}
families:
  route_matching: {status: ok, note: "RouteMatcher + parseGlacierPath path/method table cross-checked against every literal opPath in serializers.go (SplitURI calls) -- all 32 ops match prefix+method; no unreachable-op bug found"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go); registered snapshot version-guarded (glacierSnapshotVersion); cli.go wiring not touched/verified this pass (out of scope), but Handler exposes the exact Snapshot(ctx)[]byte / Restore(ctx,[]byte)error signature setupPersistence expects. This pass verified the new Job fields (SelectParameters/OutputLocation/JobOutputPath, InventoryRetrieval* range fields) round-trip through Snapshot/Restore (TestPersistenceRoundTrip_SelectAndRangeInventoryJobs) -- additive fields on an already-JSON-round-trippable struct, no snapshot version bump needed"}
  select_jobs: {status: ok, note: "IMPLEMENTED this pass (was deferred). InitiateJob Type=select is fully validated (ArchiveId existence, SelectParameters.Expression/ExpressionType=SQL/InputSerialization.Csv/OutputSerialization.Csv all required with MissingParameterValueException vs InvalidParameterValueException distinguished per-field, OutputLocation.S3.BucketName required, Expression syntax-checked) and the SQL query is REALLY executed against the stored archive bytes (select.go/select_sql.go: hand-rolled tokenizer+recursive-descent parser+evaluator for a documented SQL subset -- SELECT */columns [AS alias] FROM table [alias] [WHERE pred (AND|OR pred)*] [LIMIT n], positional _N or header-name column refs, numeric-or-lexical comparison). DEVIATION (documented, not a wire bug): real AWS never serves select results via GetJobOutput (they go to an S3 OutputLocation); gopherstack has no cross-service S3 write-back so it serves the real computed result via GetJobOutput instead of silently discarding the query -- InitiateJob/DescribeJob still report the correct (synthetic) JobOutputPath/OutputLocation wire shape. See select.go's package doc."}
  range_inventory_retrieval: {status: ok, note: "IMPLEMENTED this pass (was deferred). InventoryRetrievalParameters (StartDate/EndDate/Limit/Marker) on InitiateJob is validated (ISO-8601 dates, positive-integer Limit) and echoed back correctly nested under InventoryRetrievalParameters on DescribeJob/ListJobs (inventory_retrieval.go). GetJobOutput's inventory listing is actually filtered by the stored parameters: StartDate inclusive / EndDate exclusive bound on Archive.CreationDate, Marker resumes strictly after the named ArchiveId, Limit caps the count -- filterArchivesForInventory, covered by TestGetJobOutput_InventoryRetrieval_{DateRangeFilters,Limit,Marker}."}
gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; retrievalDelay promotion is read-triggered (promoteJobIfReady), not a background timer. FIXED this pass: DeleteVault's multipart-upload cascade deleted the store.Table row but never the corresponding raw-map multipartParts[uploadKey] row (AbortMultipartUpload/CompleteMultipartUpload already did this correctly; DeleteVault's cascade loop did not) -- every vault deleted with an in-progress multipart upload left an orphaned parts row forever. Fixed in vaults.go's DeleteVault; regression test TestDeleteVault_CascadeCleansMultipartParts (leak_test.go)."}
---

## Notes

Protocol: **restjson1** (AWS restJson1, not query-XML). Response bodies are JSON;
request/response IDs and checksums are carried in **headers**, not JSON body, for
UploadArchive / CompleteMultipartUpload / InitiateJob / InitiateMultipartUpload
(confirmed via `awsRestjson1_deserializeOpHttpBindings*Output` functions in the
real SDK's `deserializers.go` — these ops use header-only output shapes). Timestamps
are ISO-8601 strings (`2006-01-02T15:04:05.000Z`), never epoch numbers — confirmed
correct throughout (`formatDate` in models.go).

### Bugs fixed this pass

1. **`DescribeJob`/`ListJobs` missing `ArchiveSHA256TreeHash` wire field.**
   The real Glacier `GlacierJobDescription` shape has **two distinct** checksum
   fields: `ArchiveSHA256TreeHash` (checksum of the *entire archive*, archive
   metadata available as soon as the job exists) and `SHA256TreeHash` (checksum
   of the *retrieved range*, null while the job is `InProgress`, confirmed via
   the real deserializer's `case "ArchiveSHA256TreeHash":` / `case
   "SHA256TreeHash":` switch arms in `deserializers.go`). gopherstack's
   `describeJobResponse` only had `SHA256TreeHash` and set it eagerly at
   `InitiateJob` time regardless of completion state — so every real SDK client
   calling `DescribeJob`/`ListJobs` for a completed `ArchiveRetrieval` job got a
   **nil `ArchiveSHA256TreeHash`**, permanently losing the documented way to
   verify the full-archive checksum via `DescribeJob` (see the SDK's own
   `GetJobOutput` doc comment, which tells callers to cross-check downloaded
   chunks against `DescribeJob`'s archive checksum). Fixed: `Job` now carries
   `ArchiveSHA256TreeHash` (set immediately at `InitiateJob`, from archive
   metadata) separately from `SHA256TreeHash` (set only once
   `promoteJobIfReady` transitions the job to `Succeeded`), and
   `describeJobResponse` serializes both under their correct AWS field names.

2. **`GetJobOutput` missing `X-Amz-Archive-Description` response header.**
   For archive-retrieval jobs, real AWS returns the archive's description via
   the `x-amz-archive-description` response header (confirmed via
   `awsRestjson1_deserializeOpHttpBindingsGetJobOutputOutput`, which populates
   `GetJobOutputOutput.ArchiveDescription` purely from that header — there is
   no JSON-body equivalent). `handleArchiveJobOutput` never set this header, so
   `output.ArchiveDescription` was always nil for every archive download.
   Fixed: `Job` now carries `ArchiveDescription` (copied from the `Archive` at
   `InitiateJob` time — internal field, not part of the `DescribeJob` DTO,
   since AWS has no such field there), and `handleArchiveJobOutput` sets the
   header when non-empty.

### Bugs/gaps fixed this pass (2026-07-24)

3. **Select jobs (`Type=select`) were entirely unimplemented** — `InitiateJob`
   only recognized `archive-retrieval`/`inventory-retrieval`, so any real SDK
   client requesting a select job got a generic `InvalidParameterValueException`
   for an unrecognized `Type` instead of a working job. Implemented for real:
   full request-shape validation (`SelectParameters`/`OutputLocation` field-by-
   field against the real `JobParameters`/`SelectParameters`/`OutputLocation`/
   `S3Location`/`CSVInput`/`CSVOutput` types), a real SQL query engine
   (`select.go`, `select_sql.go`) that actually executes the `Expression`
   against the archive's CSV bytes, and correct `GlacierJobDescription` echo of
   `JobOutputPath`/`OutputLocation`/`SelectParameters`. See the `select_jobs`
   family note above for the one documented AWS-behavior deviation (GetJobOutput
   delivery in lieu of cross-service S3 write-back).

4. **Range inventory retrieval (`InventoryRetrievalParameters`) was entirely
   unimplemented** — the request field was silently dropped, so inventory jobs
   always returned the full vault inventory regardless of any
   `StartDate`/`EndDate`/`Limit`/`Marker` the caller specified, with no
   validation error to warn them. Implemented for real: validated parsing,
   correct nested-object echo on `DescribeJob`/`ListJobs` (see bug 5 below),
   and actual `CreationDate`-range/marker/limit filtering of the inventory
   returned by `GetJobOutput` (`inventory_retrieval.go`).

5. **`describeJobResponse.InventoryFormat` (`json:"Format"`) was a
   gopherstack-invented top-level field** — the real `GlacierJobDescription`
   type has **no top-level `Format` field** at all; `Format` only ever exists
   nested under `InventoryRetrievalParameters`. Per this campaign's "delete
   gopherstack-invented fields" rule, the top-level field is now gone,
   replaced by a real `InventoryRetrievalParameters` nested object (which also
   now carries `StartDate`/`EndDate`/`Limit`/`Marker`, previously entirely
   absent — see bug 4). (Previously this was logged as a "harmless, do not
   fix" trap because removing it without also implementing the real nested
   object would have been a net regression; it is safe now that the real
   field exists.)

6. **`DeleteVault` leaked `multipartParts` rows** (leak, not a wire bug) — see
   the `leaks` field above for detail; fixed in `vaults.go`.

### Traps for the next auditor

- `UploadArchive` / `CompleteMultipartUpload` / `InitiateJob` /
  `InitiateMultipartUpload` responses carry a JSON body in gopherstack
  (`uploadArchiveResponse`, `completeMultipartUploadResponse`,
  `initiateJobResponse`, `initiateMultipartUploadResponse`) even though real
  AWS returns an **empty body** for these ops (all data is in headers). This is
  intentional and harmless: the real SDK's `awsRestjson1_deserializeOp*`
  handlers for these ops never call the JSON-body document deserializer, only
  the HTTP-bindings (header) one, so the body is simply never parsed by a real
  client. Do not flag the body-in-a-header-only-op pattern as a bug.
- `ErrResourceInUse` → `ResourceInUseException` and `ErrVaultNotEmpty` /
  `ErrLockConflict` / `ErrLockAlreadyLocked` → `ConflictException` /
  `InvalidParameterValueException` are **not** modeled exception types in
  `aws-sdk-go-v2/service/glacier/types/errors.go` (the SDK only models
  `InsufficientCapacityException`, `InvalidParameterValueException`,
  `LimitExceededException`, `MissingParameterValueException`,
  `NoLongerSupportedException`, `PolicyEnforcedException`,
  `RequestTimeoutException`, `ResourceNotFoundException`,
  `ServiceUnavailableException`). Real clients still get a working
  `smithy.GenericAPIError` with the correct `Code`/`Message`/HTTP status
  (unmodeled codes fall through to the generic-error `default:` branch in every
  `awsRestjson1_deserializeOpError*` function) — this is NOT a bug, just an
  SDK modeling gap on AWS's side that gopherstack correctly works around.
- Route matching (`RouteMatcher` + `parseGlacierPath`) was cross-checked
  against every literal `httpbinding.SplitURI(...)` path string in the real
  SDK's `serializers.go` (32 matches, one per op) plus HTTP verb per branch —
  no unreachable-op bug found this pass, unlike several other services hit by
  that bug class.
- The Select job SQL engine (`select_sql.go`) intentionally supports a
  **documented subset** of SQL, not full ANSI SQL: no parenthesized/nested
  boolean expressions (`WHERE` is a flat OR-of-AND-groups only), no `CAST`,
  no aggregate functions, no joins. This mirrors the real Glacier/S3 Select
  feature's own documented SQL-subset scope, not an emulator shortcut passed
  off as complete — do not treat a rejected complex expression as a bug
  without checking whether real Glacier Select supports it either. See
  `select.go`'s package doc comment for the exact grammar.
- Select job results are served via `GetJobOutput` in gopherstack, which real
  AWS does **not** do (real results only ever land in the `OutputLocation` S3
  bucket, never retrievable via `GetJobOutput`). This is a deliberate,
  documented deviation forced by the lack of cross-service S3 write-back in
  this codebase (see the `select_jobs` family note above) — `InitiateJob` and
  `DescribeJob` still report the correct real-wire `JobOutputPath`/
  `OutputLocation` fields regardless. Do not "fix" `GetJobOutput` by making it
  reject select jobs; that would turn a real implementation back into a stub.
