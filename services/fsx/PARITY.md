---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: fsx
sdk_module: aws-sdk-go-v2/service/fsx@v1.68.4   # version audited against
last_audit_commit: 3f66c846bf7d76db6a4cc4dccd4d56face616885
last_audit_date: 2026-07-24
overall: A            # genuine wire-format + error-code bugs found and fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  FileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "Fixed this pass: CreateFileSystem/UpdateFileSystem now accept and CreateFileSystem/DescribeFileSystems/UpdateFileSystem/CreateFileSystemFromBackup now return real WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration blocks (previously only LustreConfiguration was ever modeled). Windows requires WindowsConfiguration.ThroughputCapacity; ONTAP requires OntapConfiguration.DeploymentType + (ThroughputCapacity or ThroughputCapacityPerHAPair); OpenZFS requires OpenZFSConfiguration.DeploymentType + ThroughputCapacity -- an absent config block on these three types returns MissingFileSystemConfiguration, a present-but-incomplete block returns BadRequest, matching real AWS's required-member validation (field-diffed against CreateFileSystemWindowsConfiguration/CreateFileSystemOntapConfiguration/CreateFileSystemOpenZFSConfiguration in types/types.go). OpenZFS file systems now get a real, describable RootVolumeId (a genuine storedVolume row is created, not a disguised placeholder string) matching AWS auto-creating a root volume per OpenZFS file system. CreateFileSystemFromBackup now carries the source file system's type-specific config fields (ThroughputCapacity, DeploymentType, etc.) onto the restored file system instead of returning an all-zero-valued config block, and now sets DNSName (previously left empty). DeleteFileSystem now cascades to child StorageVirtualMachines/Volumes/Snapshots/DataRepositoryAssociations (see leaks note) -- previously only removed the file system + its own tags. UpdateFileSystem applies WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration update sub-blocks (ThroughputCapacity, backup schedule fields, HAPairs) with real AWS's 'only overwrites non-null values' semantics. CreationTime already used epochTime pre-audit (correct). FIXED THIS PASS (gopherstack-wjjl): CreateFileSystem now implements real ClientRequestToken idempotency -- createFileSystemInput gained the field (previously entirely absent, so a retried create silently made a second resource); a repeat call with the same token and identical parameters now returns the ORIGINAL FileSystem (verified via FileSystemId/ResourceARN/CreationTime equality + unchanged resource count, not merely 'no error'); a repeat call with the same token but different parameters returns IncompatibleParameterError (new sentinel, field-diffed against types/errors.go), matching real AWS's documented CreateFileSystem contract verbatim. Dedup state (createFileSystemTokens) is a plain map guarded by the same coarse b.mu as the fileSystems table (the token check-then-set must be atomic with the resource write) and is now part of backendSnapshot (fsxSnapshotVersion bumped 1->2), so it survives Snapshot/Restore -- proven in TestInMemoryBackend_SnapshotRestore_FullState. ALSO FIXED THIS PASS: SubnetIds/SecurityGroupIds, when supplied to CreateFileSystem, are now format-validated against the real ID patterns from the API reference (subnet-[0-9a-f]{8,} / sg-[0-9a-f]{8,}) and rejected with the real InvalidNetworkSettings exception if malformed. This is real-format validation only, not existence/topology validation -- see gaps below for what's still not covered (SubnetIds required-ness, AZ-count-per-deployment-type rules)."}
  Backup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Create/Describe/Delete/Copy + CreateFileSystemFromBackup verified against real BackupId/FileSystemId shapes. CreationTime already epochTime pre-audit. Confirmed this pass: DeleteFileSystem does NOT cascade-delete backups, matching real AWS (backups persist independently of their source file system)."}
  FileSystemAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "Associate/Disassociate/Describe verified; insertion-order preserved via plain map+slice (documented in store_setup.go), matches DescribeFileSystemAliases pagination expectations. DeleteFileSystem now clears aliases[fileSystemID] on delete (fixed this pass; see leaks note)."}
  DataRepositoryAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Tag storage + arnExists coverage fixed in a prior sweep. Fixed this pass: DeleteFileSystem now cascade-deletes DRAs belonging to the deleted file system (previously left as ghost rows; see leaks note)."}
  DataRepositoryTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Cancel/Create/Describe verified; Lifecycle EXECUTING/CANCELING matches real enum values. Intentionally NOT cascade-deleted on DeleteFileSystem: DataRepositoryTasks are historical execution records in real AWS, not live child resources."}
  FileCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/Delete/Describe/Update verified against FileCacheId/FileCacheType shapes. errValidation's wire code fixed this pass (see Misc/global note below) -- FileCache's own ErrValidation-based rejections (missing FileCacheType) now correctly return BadRequest instead of the non-existent 'ValidationError'."}
  Snapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/Delete/Describe/Update/CopySnapshotAndUpdateVolume verified; CopySnapshotAndUpdateVolume and RestoreVolumeFromSnapshot correctly validate volume+snapshot existence before returning (real read+validate, not a disguised no-op). Fixed this pass: DeleteVolume and DeleteStorageVirtualMachine (transitively) now cascade-delete a volume's snapshots (previously left as ghost rows pointing at a deleted VolumeId; see leaks note). errValidation's wire code fixed this pass (see Misc/global note)."}
  StorageVirtualMachine: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create requires FileSystemId (matches real required-parameter behavior); Subtype/RootVolumeSecurityStyle round-trip. Fixed this pass: DeleteStorageVirtualMachine now cascade-deletes the volumes hosted on that SVM (and, transitively, those volumes' snapshots); DeleteFileSystem now cascade-deletes SVMs belonging to the deleted file system. errValidation's wire code fixed this pass (see Misc/global note)."}
  Volume: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/CreateFromBackup/Delete/Describe/RestoreFromSnapshot/Update verified. CreateVolumeFromBackup's VolumeType input field is a local convenience (defaults to ONTAP) -- harmless since the real CreateVolumeFromBackup wire shape has no VolumeType member at all (ONTAP-only operation), so no real client ever sends it. Fixed this pass: DeleteVolume now cascade-deletes that volume's snapshots; DeleteFileSystem/DeleteStorageVirtualMachine now cascade-delete volumes belonging to the deleted file system/SVM. errValidation's wire code fixed this pass (see Misc/global note)."}
  S3AccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. CreateAndAttach/DetachAndDelete/DescribeAttachments verified. errValidation's wire code fixed this pass (see Misc/global note)."}
  SharedVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "Describe/Update verified; single scalar field, not a collection, so untouched by the store.Table refactor per store_setup.go."}
  Misc: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ReleaseFileSystemNfsV3Locks and StartMisconfiguredStateRecovery both validate FileSystemId existence against real state (not disguised no-ops) and echo the file system back; neither op has persisted side effects in real AWS beyond a transient Lifecycle flicker, which this synchronous emulator does not model (consistent with the immediate-AVAILABLE pattern used for every other resource in this service). GLOBAL FIX this pass: errValidation's wire code was 'ValidationError', which is not a real FSx exception (field-diffed against types/errors.go -- FSx's generic client-error type is BadRequest; there is no ValidationError type at all). Every op across every family that returns ErrValidation (CreateFileSystem, CreateSnapshot, CreateStorageVirtualMachine, CreateVolume, CreateAndAttachS3AccessPoint, CreateFileCache) now correctly returns BadRequest. Added ErrMissingFileSystemConfiguration (wire code MissingFileSystemConfiguration) for CreateFileSystem's new required-config-block validation."}
  Tags: {wire: ok, errors: ok, state: ok, persist: ok, note: "TagResource/UntagResource/ListTagsForResource error code fixed in a prior pass: unrecognized ARNs return the generic ResourceNotFound exception. ListTagsForResource already returned [] not null for empty tag sets."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Delete*Output shapes (DeleteFileSystem, DeleteVolume) do not include the optional WindowsResponse/LustreResponse/OpenZFSConfiguration finalizer sub-objects (e.g. FinalBackupTags) that real AWS returns when a final backup is requested at delete time. Low traffic; not fixed this pass (gopherstack-wjjl was scoped to idempotency + network validation, not this)."
  - "CreateFileSystem still does not REQUIRE SubnetIds (real AWS: Required: Yes, and exactly two for Windows/ONTAP MULTI_AZ_1 deployments). Re-confirmed this pass (gopherstack-wjjl) against the live API reference (docs.aws.amazon.com/fsx/latest/APIReference/API_CreateFileSystem.html): SubnetIds is genuinely required. Still not enforced: grep confirms zero test fixtures across the entire fsx package (5 test files, 28+ CreateFileSystem call sites) ever populate SubnetIds, so flipping it to required would be a wholesale fixture migration, not a small fix, and this emulator still does not model Availability Zone topology needed for the exactly-one-vs-exactly-two-subnets MULTI_AZ_1 rule. What WAS fixed this pass: SubnetIds/SecurityGroupIds, when supplied, are now format-validated against the real ID patterns (subnet-[0-9a-f]{8,} / sg-[0-9a-f]{8,}) and rejected with InvalidNetworkSettings if malformed -- see families note below."
  - "ActiveDirectoryError (AD-join failures for WINDOWS/ONTAP file systems joining a directory) is not modeled: ActiveDirectoryId is accepted and echoed back but never validated against a real Directory Service resource (gopherstack's ds package). Not fixed this pass -- cross-service validation, out of scope for a single-service parity pass."
deferred: []              # consciously not audited this pass (scope) — next pass targets
leaks: {status: clean, note: "Single InMemoryBackend with no goroutines, timers, or janitors; Reset()/Snapshot()/Restore() all go through the coarse lockmetrics.RWMutex and store.Registry -- no ephemeral state outside the registered tables/maps. FIXED THIS PASS (previously leaky): DeleteFileSystem only removed the file system + its own tags, leaving ghost StorageVirtualMachine/Volume/Snapshot/DataRepositoryAssociation rows (and a stale aliases[fileSystemID] map entry) referencing a FileSystemId that no longer existed. DeleteVolume and DeleteStorageVirtualMachine had the same gap one level down (a deleted volume's snapshots, and a deleted SVM's volumes, were never cleaned up). All four Delete ops now cascade correctly (deleteVolumeLocked / deleteStorageVirtualMachineLocked / cascadeDeleteFileSystemChildrenLocked in file_systems.go, volumes.go, storage_virtual_machines.go), while intentionally leaving Backups and DataRepositoryTasks alone (real AWS retains both independently of the file system they reference). Regression tests added in cascade_delete_test.go."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: AWSSimbaAPIService_v20180301.<Op>`).
Route matcher (`RouteMatcher`) does a prefix match on the target header, which is correct
and matches the real client's request shape; `GetSupportedOperations()` / `buildOps()` stay
in sync (verified no orphan registrations, no missing dispatch entries).

**Fixed this pass — the FileSystem config-block gap (the headline item from the prior
pass's `gaps` list):**
Field-diffed against `aws-sdk-go-v2/service/fsx@v1.66.2`'s `types/types.go`
(`CreateFileSystemWindowsConfiguration`, `CreateFileSystemOntapConfiguration`,
`CreateFileSystemOpenZFSConfiguration`, `WindowsFileSystemConfiguration`,
`OntapFileSystemConfiguration`, `OpenZFSFileSystemConfiguration`,
`UpdateFileSystemWindowsConfiguration`, `UpdateFileSystemOntapConfiguration`,
`UpdateFileSystemOpenZFSConfiguration`). Added `WindowsConfiguration`, `OntapConfiguration`,
`OpenZFSConfiguration`, `FileSystemEndpoints`, `FileSystemEndpoint` to interfaces.go, and
matching `createWindowsConfiguration`/`createOntapConfiguration`/`createOpenZFSConfiguration`
request types and `updateWindowsConfiguration`/`updateOntapConfiguration`/
`updateOpenZFSConfiguration` request types to file_systems.go. Required-member validation
(`applyWindowsConfig`/`applyOntapConfig`/`applyOpenZFSConfig` in file_systems.go) matches real
AWS: an absent config block on WINDOWS/ONTAP/OPENZFS returns `MissingFileSystemConfiguration`;
a present block missing `ThroughputCapacity` (Windows/OpenZFS) or `DeploymentType`+
(`ThroughputCapacity` or `ThroughputCapacityPerHAPair`) (ONTAP) returns `BadRequest`. Lustre is
untouched (LustreConfiguration remains genuinely optional with a SCRATCH_1 default, matching
real AWS). OpenZFS file systems now get a real backing root `Volume` row (Name `"fsx"`,
VolumeType `OPENZFS`) so `RootVolumeId` in the response is a genuine, describable ID rather than
a disguised placeholder string -- verified via `DescribeVolumes` in
`TestFSx_FileSystem_OpenZFSConfiguration`.

This required updating three shared test fixtures that previously created WINDOWS/ONTAP/OPENZFS
file systems with no config block at all (which is now correctly rejected): the shared
`createFS` helper in handler_test.go (now routes through `fileSystemCreateBody()`, which builds
a minimal valid config per type) and the two `CreateFileSystem` request bodies in
`TestCreateFileSystem_FileSystemTypeValidation`/`TestCreateFileSystem_StorageCapacityMinimum` in
handler_file_systems_test.go.

**Fixed this pass — errValidation's wire code (`"ValidationError"` is not a real FSx
exception):**
Field-diffed against `types/errors.go` in the SDK: FSx's generic client-error exception is
`BadRequest`; there is no `ValidationError` exception type anywhere in the FSx API. This was a
pre-existing bug (not introduced this pass) affecting every op that returns `ErrValidation`
across every family -- `handleError` in handler.go had a hardcoded `"ValidationError"` string
for the generic `awserr.ErrInvalidParameter` case instead of deriving the code from the error's
own message, which happened to mask the same bug in the `errValidation` constant. Fixed both:
`errValidation` is now `"BadRequest"`, and `handleError`'s generic case now emits `"BadRequest"`
directly. Added a `ErrMissingFileSystemConfiguration` case just above it for the new
`MissingFileSystemConfiguration` code introduced this pass.

**Fixed this pass — DeleteFileSystem/DeleteVolume/DeleteStorageVirtualMachine cascade
deletes (see `leaks` above):** previously these three ops only removed the target resource
itself, leaving ghost rows in every child table. Now: `DeleteStorageVirtualMachine` cascades to
its volumes (which cascades to those volumes' snapshots); `DeleteVolume` cascades to its
snapshots; `DeleteFileSystem` cascades to its SVMs (transitively volumes/snapshots), its
directly-attached volumes (e.g. an OpenZFS root volume), its DataRepositoryAssociations, and its
DNS aliases. Backups and DataRepositoryTasks are intentionally left alone (real AWS retains both
independently of the file system they reference).

**Traps for the next auditor:**
- The `toFileSystem()` special-case for `fileSystemTypeLustre` (always populating
  `LustreConfiguration` even when the create request didn't send one) is *intentional*,
  not a bug — see its doc comment re: terraform-provider-aws treating a nil config block as
  an empty read. Don't "simplify" it away. The same pattern now applies to
  `toWindowsConfiguration()`/`toOntapConfiguration()`/`toOpenZFSConfiguration()`.
- `CreateVolumeFromBackup`'s local `VolumeType` input field with an `"ONTAP"` default looks
  unusual but is harmless: the real `CreateVolumeFromBackupInput` wire shape has no
  `VolumeType` member at all (the operation is ONTAP-only), so no real client will ever send
  a conflicting value.
- Every resource here transitions straight to `AVAILABLE`/deletes straight away rather than
  sitting in `CREATING`/`DELETING` for a poll cycle. This is a deliberate, service-wide
  synchronous-emulation choice (matches every other resource type in this file), not a
  disguised no-op — don't flag individual ops for this without also flagging the whole
  service's design.
- `storedFileSystem`'s per-type fields (`ThroughputCapacity`, `DeploymentType`,
  `PreferredSubnetID`, etc.) are shared across Lustre/Windows/ONTAP/OpenZFS rather than each
  type getting its own nested struct -- this is intentional (see the doc comment on
  `storedFileSystem`): the real wire shape happens to reuse the same concept name
  (`DeploymentType`) across all four `*Configuration` blocks, and `toFileSystem()`'s switch on
  `FileSystemType` picks which public `*Configuration` block to populate from those shared
  fields. Don't "fix" this into four separate nested stored structs without a concrete reason.
- `WindowsConfiguration.Aliases` is deliberately never populated by `toWindowsConfiguration()`;
  the source of truth for DNS aliases is `DescribeFileSystemAliases`
  (`AssociateFileSystemAliases`/`DisassociateFileSystemAliases` in file_systems.go). See the doc
  comment on `WindowsConfiguration` in interfaces.go before "fixing" this.
