---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: glacier
sdk_module: aws-sdk-go-v2/service/glacier@v1.32.4
last_audit_commit: 2b6e7cfbeda75dd7c0cf87e417157275792ac5e3
last_audit_date: 2026-07-12
overall: A            # 2 genuine wire-shape fixes found; rest of surface already accurate
ops:
  CreateVault:            {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVault:          {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVault:            {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes jobs/uploads/lock; blocks on non-empty vault"}
  ListVaults:             {wire: ok, errors: ok, state: ok, persist: ok, note: "marker/limit pagination verified vs SDK Marker/VaultList shape"}
  UploadArchive:          {wire: ok, errors: ok, state: ok, persist: ok, note: "ArchiveId/Checksum/Location are header-only on real wire (confirmed via awsRestjson1_deserializeOpHttpBindingsUploadArchiveOutput); gopherstack sets all three headers correctly, body is a harmless bonus"}
  DeleteArchive:          {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateJob:            {wire: ok, errors: ok, state: ok, persist: ok, note: "response is header-only (X-Amz-Job-Id/Location) on real wire; verified"}
  DescribeJob:            {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing ArchiveSHA256TreeHash wire field entirely (see Notes)"}
  ListJobs:               {wire: fixed, errors: ok, state: ok, persist: ok, note: "same describeJobResponse DTO as DescribeJob, same fix applies"}
  GetJobOutput:           {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing X-Amz-Archive-Description response header for archive-retrieval jobs (see Notes)"}
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
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go); registered snapshot version-guarded (glacierSnapshotVersion); cli.go wiring not touched/verified this pass (out of scope), but Handler exposes the exact Snapshot(ctx)[]byte / Restore(ctx,[]byte)error signature setupPersistence expects"}
  select_jobs: {status: deferred, note: "Select-type jobs (Type=select, SelectParameters/OutputLocation/CSVInput/CSVOutput) are not implemented -- InitiateJob only recognizes archive-retrieval/inventory-retrieval. Low priority: Select was a rarely-used, since-deprecated Athena-style query-in-place feature."}
  range_inventory_retrieval: {status: deferred, note: "InventoryRetrievalParameters (StartDate/EndDate/Marker/Limit sub-object on InitiateJob request, echoed back on DescribeJob) is not parsed or stored. Inventory retrieval always returns the full vault inventory. Edge feature, not core traffic."}
gaps:
  - Select-job type unsupported (bd: file if range/Select support requested)
  - InventoryRetrievalParameters range-filtering unsupported (bd: file if requested)
deferred:
  - Select jobs (SelectParameters/OutputLocation/CSVInput/CSVOutput)
  - InventoryRetrievalParameters range inventory retrieval
leaks: {status: clean, note: "no goroutines/janitors in this service; retrievalDelay promotion is read-triggered (promoteJobIfReady), not a background timer"}
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

### Traps for the next auditor

- `describeJobResponse.InventoryFormat` (`json:"Format"`) is sent by
  gopherstack at the top level of the `DescribeJob` response, but the real
  `GlacierJobDescription` type has **no top-level `Format` field** at all (only
  nested under the unimplemented `InventoryRetrievalParameters`). This is
  **harmless** — restjson1 deserializers silently ignore unknown JSON keys
  (`default: _, _ = key, value` in the generated switch) — so it is not a wire
  bug, just a field the real SDK will never read. Do not "fix" this by
  removing it without checking whether any test depends on it.
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
