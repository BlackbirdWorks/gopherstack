---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: fsx
sdk_module: aws-sdk-go-v2/service/fsx@v1.66.2   # version audited against
last_audit_commit: d7ff080e08875e33a7abf232387b6a9ae99408f4
last_audit_date: 2026-07-12
overall: A            # genuine wire-format + error-code bugs found and fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  FileSystem: {wire: partial, errors: ok, state: ok, persist: ok, note: "Create/Describe/Update/Delete/CreateFileSystemFromBackup all mutate the real store.Table and round-trip through backendSnapshot. CreationTime already used epochTime pre-audit (correct). Gap: only LustreConfiguration is modeled on the response and only createLustreConfiguration on the request -- WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration are never populated or accepted (see gaps)."}
  Backup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Create/Describe/Delete/Copy + CreateFileSystemFromBackup verified against real BackupId/FileSystemId shapes. CreationTime already epochTime pre-audit."}
  FileSystemAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "Associate/Disassociate/Describe verified; insertion-order preserved via plain map+slice (documented in store_setup.go), matches DescribeFileSystemAliases pagination expectations."}
  DataRepositoryAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass (was time.Time -> RFC3339 string; now epochTime -> epoch-seconds number, matching the real deserializer). Tag storage + arnExists coverage were already fixed in a prior sweep (parity_b_test.go)."}
  DataRepositoryTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. Cancel/Create/Describe verified; Lifecycle EXECUTING/CANCELING matches real enum values."}
  FileCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. Create/Delete/Describe/Update verified against FileCacheId/FileCacheType shapes."}
  Snapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. Create/Delete/Describe/Update/CopySnapshotAndUpdateVolume verified; CopySnapshotAndUpdateVolume and RestoreVolumeFromSnapshot correctly validate volume+snapshot existence before returning (real read+validate, not a disguised no-op)."}
  StorageVirtualMachine: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. Create requires FileSystemId (matches real required-parameter behavior); Subtype/RootVolumeSecurityStyle round-trip."}
  Volume: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. Create/CreateFromBackup/Delete/Describe/RestoreFromSnapshot/Update verified. CreateVolumeFromBackup's VolumeType input field is a local convenience (defaults to ONTAP) -- harmless since the real CreateVolumeFromBackup wire shape has no VolumeType member at all (ONTAP-only operation), so no real client ever sends it."}
  S3AccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed this pass. CreateAndAttach/DetachAndDelete/DescribeAttachments verified."}
  SharedVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "Describe/Update verified; single scalar field, not a collection, so untouched by the store.Table refactor per store_setup.go."}
  Misc: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ReleaseFileSystemNfsV3Locks and StartMisconfiguredStateRecovery both validate FileSystemId existence against real state (not disguised no-ops) and echo the file system back; neither op has persisted side effects in real AWS beyond a transient Lifecycle flicker, which this synchronous emulator does not model (consistent with the immediate-AVAILABLE pattern used for every other resource in this service)."}
  Tags: {wire: ok, errors: ok, state: ok, persist: ok, note: "TagResource/UntagResource/ListTagsForResource error code fixed this pass: unrecognized ARNs now return the generic ResourceNotFound exception (matches real FSx, which uses one generic not-found exception across all resource types for the Tag family) instead of the file-system-specific FileSystemNotFound. ListTagsForResource already returned [] not null for empty tag sets (fixed in a prior sweep)."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "FileSystem responses never populate WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration (only LustreConfiguration is modeled), and CreateFileSystem/UpdateFileSystem accept no corresponding nested config input (ThroughputCapacity, PreferredSubnetId, DeploymentType, etc. for Windows/ONTAP/OpenZFS). The existing toFileSystem() doc comment already documents that terraform-provider-aws treats a nil LustreConfiguration as an empty read result for Lustre; the same failure mode likely affects Windows/ONTAP/OpenZFS file systems, which get no config block at all. Out of scope for this pass (requires new request/response surface + per-type required-field validation, not a narrow bug fix) — needs a bd issue for a follow-up pass."
  - "Delete*Output shapes (DeleteFileSystem, DeleteVolume) do not include the optional WindowsResponse/LustreResponse/OpenZFSConfiguration finalizer sub-objects (e.g. FinalBackupTags) that real AWS returns when a final backup is requested at delete time. Low traffic; not fixed this pass."
  - "No per-file-system-type required-field validation on CreateFileSystem beyond StorageCapacity minimums (e.g. real AWS requires SubnetIds, and requires ThroughputCapacity for Windows/ONTAP/OpenZFS) -- requests missing these are accepted here rather than rejected with ValidationException. Tied to the config-block gap above."
deferred: []              # consciously not audited this pass (scope) — next pass targets
leaks: {status: clean, note: "Single InMemoryBackend with no goroutines, timers, or janitors; Reset()/Snapshot()/Restore() all go through the coarse lockmetrics.RWMutex and store.Registry -- no ephemeral state outside the registered tables/maps."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: AWSSimbaAPIService_v20180301.<Op>`).
Route matcher (`RouteMatcher`) does a prefix match on the target header, which is correct
and matches the real client's request shape; `GetSupportedOperations()` / `buildOps()` stay
in sync (verified no orphan registrations, no missing dispatch entries).

**Bug fixed this pass — CreationTime wire format (high-confidence, high-impact):**
Verified directly against `aws-sdk-go-v2/service/fsx@v1.66.2`'s `deserializers.go`: every
single `CreationTime` field across all 10 FSx response shapes (FileSystem, Backup,
DataRepositoryAssociation, DataRepositoryTask, FileCache, Snapshot,
StorageVirtualMachine, Volume, S3AccessPoint, and one more) is deserialized as
`case json.Number: ... smithytime.ParseEpochSeconds(f64)`, with an explicit
`default: return fmt.Errorf("expected CreationTime to be a JSON Number, got %T instead", value)`.
Pre-audit, `FileSystem` and `Backup` already used the local `epochTime` marshaler (defined in
interfaces.go) to emit an epoch-seconds JSON number, but the other 7 resource types
(`DataRepositoryAssociation`, `DataRepositoryTask`, `FileCache`, `Snapshot`,
`StorageVirtualMachine`, `Volume`, `S3AccessPoint`) declared `CreationTime time.Time`, which
Go's `encoding/json` renders as an RFC3339 string (e.g. `"2024-01-01T00:00:00Z"`). Any real
`aws-sdk-go-v2` client calling `DescribeDataRepositoryTasks`, `DescribeSnapshots`,
`DescribeStorageVirtualMachines`, `DescribeVolumes`, `DescribeS3AccessPointAttachments`,
`DescribeFileCaches`, or `DescribeDataRepositoryAssociations` would hard-fail parsing the
response. Fixed by switching those 7 struct fields to `epochTime` and wrapping the
corresponding `toPublic()` conversions in `backend_resources.go`. No unit test previously
asserted on `CreationTime`'s JSON type (confirmed via grep), which is why this survived
three prior parity sweeps undetected — regression test added
(`Test_CreationTime_IsEpochSecondsNumber` in `parity_c_test.go`, one subtest per resource
type).

**Bug fixed this pass — ResourceNotFound error code for Tag family:**
Real FSx defines a generic `ResourceNotFound` exception (`types/errors.go`, with a
`ResourceARN` field) distinct from the resource-type-specific `FileSystemNotFound`,
`BackupNotFound`, etc. `TagResource`/`UntagResource`/`ListTagsForResource` are generic
across every FSx resource type (not file-system-specific), so real AWS returns
`ResourceNotFound` for an unrecognized ARN regardless of what kind of resource ARN was
expected. The emulator's `arnExists()` check previously returned `ErrFileSystemNotFound`
unconditionally, mislabeling e.g. an unknown *backup* or *volume* ARN as
`FileSystemNotFound`. Fixed by adding `ErrResourceNotFound` and using it in all three
tag ops; regression test added (`Test_TagOps_UnknownARN_ReturnsResourceNotFound`).
`handleError`'s cyclomatic complexity was split into `notFoundErrorCode()` to add this case
without a `//nolint:cyclop` (per repo convention: no complexity-suppression comments).

**Traps for the next auditor:**
- The `toFileSystem()` special-case for `fileSystemTypeLustre` (always populating
  `LustreConfiguration` even when the create request didn't send one) is *intentional*,
  not a bug — see its doc comment re: terraform-provider-aws treating a nil config block as
  an empty read. Don't "simplify" it away.
- `CreateVolumeFromBackup`'s local `VolumeType` input field with an `"ONTAP"` default looks
  unusual but is harmless: the real `CreateVolumeFromBackupInput` wire shape has no
  `VolumeType` member at all (the operation is ONTAP-only), so no real client will ever send
  a conflicting value.
- Every resource here transitions straight to `AVAILABLE`/deletes straight away rather than
  sitting in `CREATING`/`DELETING` for a poll cycle. This is a deliberate, service-wide
  synchronous-emulation choice (matches every other resource type in this file), not a
  disguised no-op — don't flag individual ops for this without also flagging the whole
  service's design.
