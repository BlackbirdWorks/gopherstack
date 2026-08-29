---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: fsx
sdk_module: aws-sdk-go-v2/service/fsx@v1.68.4   # version audited against
last_audit_commit: 8d4556e7938635cdf7c945d46cea23d9dbe03cb9
last_audit_date: 2026-08-29
overall: A            # genuine wire-format + error-code bugs found and fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  FileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "Fixed this pass: CreateFileSystem/UpdateFileSystem now accept and CreateFileSystem/DescribeFileSystems/UpdateFileSystem/CreateFileSystemFromBackup now return real WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration blocks (previously only LustreConfiguration was ever modeled). Windows requires WindowsConfiguration.ThroughputCapacity; ONTAP requires OntapConfiguration.DeploymentType + (ThroughputCapacity or ThroughputCapacityPerHAPair); OpenZFS requires OpenZFSConfiguration.DeploymentType + ThroughputCapacity -- an absent config block on these three types returns MissingFileSystemConfiguration, a present-but-incomplete block returns BadRequest, matching real AWS's required-member validation (field-diffed against CreateFileSystemWindowsConfiguration/CreateFileSystemOntapConfiguration/CreateFileSystemOpenZFSConfiguration in types/types.go). OpenZFS file systems now get a real, describable RootVolumeId (a genuine storedVolume row is created, not a disguised placeholder string) matching AWS auto-creating a root volume per OpenZFS file system. CreateFileSystemFromBackup now carries the source file system's type-specific config fields (ThroughputCapacity, DeploymentType, etc.) onto the restored file system instead of returning an all-zero-valued config block, and now sets DNSName (previously left empty). DeleteFileSystem now cascades to child StorageVirtualMachines/Volumes/Snapshots/DataRepositoryAssociations (see leaks note) -- previously only removed the file system + its own tags. UpdateFileSystem applies WindowsConfiguration/OntapConfiguration/OpenZFSConfiguration update sub-blocks (ThroughputCapacity, backup schedule fields, HAPairs) with real AWS's 'only overwrites non-null values' semantics. CreationTime already used epochTime pre-audit (correct). FIXED THIS PASS (gopherstack-wjjl): CreateFileSystem now implements real ClientRequestToken idempotency -- createFileSystemInput gained the field (previously entirely absent, so a retried create silently made a second resource); a repeat call with the same token and identical parameters now returns the ORIGINAL FileSystem (verified via FileSystemId/ResourceARN/CreationTime equality + unchanged resource count, not merely 'no error'); a repeat call with the same token but different parameters returns IncompatibleParameterError (new sentinel, field-diffed against types/errors.go), matching real AWS's documented CreateFileSystem contract verbatim. Dedup state (createFileSystemTokens) is a plain map guarded by the same coarse b.mu as the fileSystems table (the token check-then-set must be atomic with the resource write) and is now part of backendSnapshot (fsxSnapshotVersion bumped 1->2), so it survives Snapshot/Restore -- proven in TestInMemoryBackend_SnapshotRestore_FullState. ALSO FIXED THIS PASS: SubnetIds/SecurityGroupIds, when supplied to CreateFileSystem, are now format-validated against the real ID patterns from the API reference (subnet-[0-9a-f]{8,} / sg-[0-9a-f]{8,}) and rejected with the real InvalidNetworkSettings exception if malformed. This is real-format validation only, not existence/topology validation -- see gaps below for what's still not covered (SubnetIds required-ness, AZ-count-per-deployment-type rules). FIXED (gopherstack-cgq3) — CreateFileSystemFromBackup was missing the real optional FileSystemTypeVersion field (*string, the Lustre engine-version override; per api_op_CreateFileSystemFromBackup.go, real AWS lets a restore specify a newer Lustre version than the backup's own setting, defaulting to the backup's if omitted). Now modeled on FileSystem/storedFileSystem and threaded through: an explicit request value wins, otherwise it falls back to the source file system's own FileSystemTypeVersion. Note CreateFileSystem (the non-backup create path) still has no way to set FileSystemTypeVersion at all, so that fallback is currently always empty in practice — a related, distinct gap (real CreateFileSystemInput also has this field) left unfixed since it's out of this op's scope; see gaps: below."}
  Backup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Create/Describe/Delete/Copy + CreateFileSystemFromBackup verified against real BackupId/FileSystemId shapes. CreationTime already epochTime pre-audit. Confirmed this pass: DeleteFileSystem does NOT cascade-delete backups, matching real AWS (backups persist independently of their source file system)."}
  FileSystemAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "Associate/Disassociate/Describe verified; insertion-order preserved via plain map+slice (documented in store_setup.go), matches DescribeFileSystemAliases pagination expectations. DeleteFileSystem now clears aliases[fileSystemID] on delete (fixed this pass; see leaks note)."}
  DataRepositoryAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Tag storage + arnExists coverage fixed in a prior sweep. Fixed this pass: DeleteFileSystem now cascade-deletes DRAs belonging to the deleted file system (previously left as ghost rows; see leaks note)."}
  DataRepositoryTask: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Cancel/Create/Describe verified; Lifecycle EXECUTING/CANCELING matches real enum values. Intentionally NOT cascade-deleted on DeleteFileSystem: DataRepositoryTasks are historical execution records in real AWS, not live child resources. FIXED this pass (gopherstack-4ggy): Report (a required CreateDataRepositoryTaskInput member, api_op_CreateDataRepositoryTask.go:49-129, whose own Enabled member is required per validateCompletionReport) was dropped entirely -- the request read only FileSystemId/Type/Paths/Tags. Now required, validated, stored, and echoed back on DataRepositoryTask.Report (the real DescribeDataRepositoryTasks/CreateDataRepositoryTask response member); Format/Path/Scope accepted but not enforced, matching the SDK's own client-side validator (only Enabled is checked there, despite the doc comment saying the other three are 'required if Enabled is true')."}
  FileCache: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/Delete/Describe/Update verified against FileCacheId/FileCacheType shapes. errValidation's wire code fixed this pass (see Misc/global note below) -- FileCache's own ErrValidation-based rejections (missing FileCacheType) now correctly return BadRequest instead of the non-existent 'ValidationError'. FIXED 2026-08-11 -- CreateFileCache's request/response StorageCapacity field was wire-tagged StorageCapacityGiB; the real CreateFileCacheRequest/FileCache field is StorageCapacity, so every real client's capacity value was silently discarded (created caches always got 0 GiB). UpdateFileCache's StorageCapacityGiB acceptance is untouched -- the real UpdateFileCacheRequest has no storage-capacity field at all (out of scope, pre-existing invented field, not a rename target). FIXED this pass (gopherstack-4ggy): FileCacheTypeVersion (named in the issue) AND SubnetIds (also a required CreateFileCacheInput member, api_op_CreateFileCache.go:48-124, equally absent -- floor confirmed) were both dropped entirely; StorageCapacity was wired but never required-checked (also fixed, same required set). All three now validated and echoed back on FileCache.FileCacheTypeVersion/SubnetIds (types.FileCacheCreating, types.go:2349). FIXED 2026-08-20 (wrapper-key sweep): a single FileCache Go type, WITH a Tags field, was reused for CreateFileCache/DescribeFileCaches/UpdateFileCache responses alike. Real AWS splits these into two distinct wire types -- types.FileCacheCreating (types/types.go:2349, HAS Tags; deserializers.go:9984 case \"Tags\") for CreateFileCacheOutput.FileCache only, vs types.FileCache (types/types.go:2264, NO Tags at all; deserializers.go:9818 has no case \"Tags\") for DescribeFileCachesOutput.FileCaches/UpdateFileCacheOutput.FileCache -- so gopherstack emitting a Tags key on Describe/Update responses was a fabricated member with no case in the live deserializer, silently dropped by a real client (harmlessly, since the real Go type has no field to hold it, but still wire-inaccurate). Split into FileCacheCreating (interfaces.go, Tags) and FileCache (interfaces.go, no Tags); CreateFileCache's backend method now returns *FileCacheCreating via toPublicCreating(), Describe/UpdateFileCache keep *FileCache via toPublic(). Proven by services/fsx/file_cache_wire_test.go (TestFileCache_TagsWireShape), hand-revert confirmed the exact predicted symptom (Tags key present on Describe/Update)."}
  Snapshot: {wire: ok, errors: ok, state: fixed, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/Delete/Describe/Update verified; RestoreVolumeFromSnapshot correctly validates volume+snapshot existence before returning (real read+validate, not a disguised no-op). Fixed this pass: DeleteVolume and DeleteStorageVirtualMachine (transitively) now cascade-delete a volume's snapshots (previously left as ghost rows pointing at a deleted VolumeId; see leaks note). errValidation's wire code fixed this pass (see Misc/global note). FIXED 2026-08-20 (wrapper-key sweep, critical): CopySnapshotAndUpdateVolume's response was wrapped under a fabricated \"Volume\" key ({Volume: *Volume}). Real AWS's CopySnapshotAndUpdateVolumeOutput (api_op_CopySnapshotAndUpdateVolume.go:87) has NO Volume member at all -- it wraps under root-level \"Lifecycle\"/\"VolumeId\" plus \"AdministrativeActions\" (a list of the new AdministrativeAction type, TargetVolumeValues nested), confirmed via deserializers.go:15903's live per-op switch (no case \"Volume\"). A real client got a completely empty CopySnapshotAndUpdateVolumeOutput back (VolumeId/Lifecycle empty, AdministrativeActions nil) -- total data loss, not a dropped field. See Volume family for the paired RestoreVolumeFromSnapshot bug (identical pattern, shared fix). Proven by services/fsx/administrative_action_wire_test.go; two PRE-EXISTING tests that had encoded the wrong key as correct (handler_snapshots_test.go asserting out[\"Volume\"], handler_volumes_test.go same) were corrected to assert the real shape. FIXED 2026-08-29 (write-only-state sweep): the 08-20 note above claimed CopySnapshotAndUpdateVolume 'correctly validates volume+snapshot existence' -- this pass's write-only-state method (primary method: what's accepted from a request and never read?) found that claim was WRONG for the snapshot half. SourceSnapshotARN (a required real CopySnapshotAndUpdateVolumeInput member, api_op_CopySnapshotAndUpdateVolume.go) was decoded off the wire into copySnapshotAndUpdateVolumeInput.SourceSnapshotID but never referenced anywhere else in the package (grep-confirmed zero other reads) -- any ARN, including one naming a nonexistent or malformed snapshot, silently 'succeeded'. Fixed: extracts the snapshot ID from the ARN's trailing snapshot/<id> segment (matching the format snapshotARN itself builds) and existence-checks it, returning SnapshotNotFound like the sibling RestoreVolumeFromSnapshot op already correctly did for its own (non-ARN) SnapshotId parameter. Proven by wire_field_fixes_test.go's TestCopySnapshotAndUpdateVolume_SourceSnapshotARNValidated (real client, hand-reverted, confirmed failing pre-fix, restored md5sum-identical)."}
  StorageVirtualMachine: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create requires FileSystemId (matches real required-parameter behavior); Subtype/RootVolumeSecurityStyle round-trip. Fixed this pass: DeleteStorageVirtualMachine now cascade-deletes the volumes hosted on that SVM (and, transitively, those volumes' snapshots); DeleteFileSystem now cascade-deletes SVMs belonging to the deleted file system. errValidation's wire code fixed this pass (see Misc/global note). ActiveDirectoryConfiguration/Endpoints (SvmEndpoints/SvmEndpoint) are genuine real SDK members never emitted at all (types/types.go, deserializers.go:14651 case list confirmed) -- Layer 3 gap, out of scope this pass (not hunted, not fixed)."}
  Volume: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "CreationTime wire bug fixed in a prior pass. Create/CreateFromBackup/Delete/Describe/Update verified. Fixed this pass: DeleteVolume now cascade-deletes that volume's snapshots; DeleteFileSystem/DeleteStorageVirtualMachine now cascade-delete volumes belonging to the deleted file system/SVM. errValidation's wire code fixed this pass (see Misc/global note). FIXED 2026-08-20 (wrapper-key sweep, critical): RestoreVolumeFromSnapshot's response was wrapped under a fabricated \"Volume\" key, exactly mirroring CopySnapshotAndUpdateVolume's bug (see Snapshot family for full citation) -- real RestoreVolumeFromSnapshotOutput (api_op_RestoreVolumeFromSnapshot.go) also has no Volume member, only root-level Lifecycle/VolumeId + AdministrativeActions (deserializers.go:17381 live switch, no case \"Volume\"). Added AdministrativeAction (interfaces.go) reusing the existing Volume type for TargetVolumeValues (matches real types.AdministrativeAction.TargetVolumeValues *Volume, types/types.go:185) so no backend logic changed, only the handler's response wrapping. AdministrativeActionType values used (VOLUME_RESTORE for Restore, VOLUME_UPDATE_WITH_SNAPSHOT for Copy) are exact matches against types/enums.go, Status COMPLETED likewise. FIXED 2026-08-23 (gopherstack batch8, request-side): CreateVolume's INPUT was reading gopherstack-invented top-level FileSystemId/StorageVirtualMachineId fields real CreateVolumeInput has never had at all (api_op_CreateVolume.go) -- a real client's SVM/parent-volume reference was silently ignored, producing a volume with an empty FileSystemId and no real StorageVirtualMachine association, no error either way. Now reads the real nested OntapConfiguration.StorageVirtualMachineId (ONTAP, existence-checked, FileSystemId derived from the resolved SVM) / OpenZFSConfiguration.ParentVolumeId (OPENZFS, existence-checked, FileSystemId derived from the resolved parent volume), and rejects a VolumeType=ONTAP/OPENZFS request with no matching config block as MissingVolumeConfiguration (types.MissingVolumeConfiguration, real wire code, fsx@v1.68.4 types/errors.go) -- mirrors CreateFileSystem's already-established per-type-config-block-required pattern (see FileSystem family). See Notes for proof/hand-revert. FIXED 2026-08-29 (write-only-state sweep, response-side): the 2026-08-20/08-23 passes both explicitly disclosed 'Volume has no OntapVolumeConfiguration at all' as a Layer-3 gap and left it there -- but re-reading the live deserializer (deserializers.go:15307's Volume case switch) this pass found gopherstack was NOT simply omitting the SVM: it was emitting StorageVirtualMachineId as a FABRICATED TOP-LEVEL key with no counterpart on real types.Volume at all (the real member is OntapConfiguration.StorageVirtualMachineId, deserializers.go:12447). A real typed client silently drops the top-level key and gets nil OntapConfiguration -- so even after 08-23 fixed CreateVolume's *request*-side SVM resolution, the resolved SVM remained completely unreadable through every op returning a Volume (CreateVolume, CreateVolumeFromBackup, DescribeVolumes, UpdateVolume, and the AdministrativeAction.TargetVolumeValues nested Volume on RestoreVolumeFromSnapshot/CopySnapshotAndUpdateVolume). Added OntapVolumeConfiguration{StorageVirtualMachineId} (interfaces.go, only this one real member modeled, matching this fix's scope); storedVolume.toPublic() now nests it under Volume.OntapConfiguration for ONTAP volumes (OpenZFS volumes correctly get no OntapConfiguration -- OpenZFS has no SVM concept). Also fixed CreateVolumeFromBackup (same sweep): its request struct had a flat top-level StorageVirtualMachineId, exactly the same accept-and-drop bug the 08-23 pass fixed on CreateVolume itself -- real CreateVolumeFromBackupInput (api_op_CreateVolumeFromBackup.go) has no top-level VolumeType or StorageVirtualMachineId at all, only nested OntapConfiguration.StorageVirtualMachineId (types.CreateOntapVolumeConfiguration); no real client's SVM assignment could ever have reached this backend. Now resolves the same createOntapVolumeConfigInput type CreateVolume already uses, existence-checks the SVM, derives FileSystemId from it, and rejects a request with no OntapConfiguration as MissingVolumeConfiguration. Proven by wire_field_fixes_test.go's TestVolume_StorageVirtualMachineIdWireShape and TestCreateVolumeFromBackup_StorageVirtualMachineIdRoundTrip (real aws-sdk-go-v2 client round trips); both hand-reverted (git checkout -- the touched files, confirmed all four new round-trip tests fail with the predicted symptom, restored, md5sum byte-identical)."}
  S3AccessPoint: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "CreationTime wire bug fixed in a prior pass. errValidation's wire code fixed this pass (see Misc/global note). FIXED 2026-08-20 (wrapper-key sweep, most severe bug this pass): the ENTIRE S3AccessPoint feature modeled the wrong AWS type on both request and response. gopherstack's old flat S3AccessPoint{Name,FileSystemID,VolumeID,Lifecycle,ResourceARN,Tags,CreationTime} does not correspond to any real FSx wire shape -- CreateAndAttachS3AccessPointOutput/DescribeS3AccessPointAttachmentsOutput actually wrap under \"S3AccessPointAttachment\"/\"S3AccessPointAttachments\" (types.S3AccessPointAttachment, types/types.go:3898; deserializers.go:15957/16995 live switches confirm no case \"S3AccessPoint\"/\"S3AccessPoints\" exists), whose real case list is CreationTime/Lifecycle/LifecycleTransitionReason/Name/OntapConfiguration/OpenZFSConfiguration/S3AccessPoint/Type -- NO top-level FileSystemId, VolumeId, ResourceARN, or Tags at all. The attached VolumeId lives nested under whichever of OntapConfiguration/OpenZFSConfiguration (types/types.go:3956/3970) matches Type, and ResourceARN/Alias live under a DIFFERENT nested type, types.S3AccessPoint (deserializers.go:13775, case list Alias/ResourceARN/VpcConfiguration only). The real request side is equally different: CreateAndAttachS3AccessPointInput has no FileSystemId member at all (api_op_CreateAndAttachS3AccessPoint.go:52) -- Name+Type+OntapConfiguration.VolumeId|OpenZFSConfiguration.VolumeId is the real contract -- and DetachAndDeleteS3AccessPointInput has no FileSystemId either (api_op_DetachAndDeleteS3AccessPoint.go:36, Name alone). Before this fix a real typed SDK client's CreateAndAttachS3AccessPoint call sent the real (Name/Type/OntapConfiguration) shape and gopherstack's old handler, which required FileSystemId, rejected it outright with 400 BadRequest -- the op was non-functional against a real client. Rebuilt: S3AccessPointAttachment/S3AccessPointOntapConfiguration/S3AccessPointOpenZFSConfiguration/S3AccessPoint (interfaces.go), createAndAttachS3AccessPointInput now parses Type+nested VolumeId (s3_access_points.go), DetachAndDeleteS3AccessPoint(name string) dropped the fileSystemID parameter, Tags support removed from Create input (real AWS has none there -- also independently confirmed by the pre-existing exclusion note in handler_create_tags_test.go). A synthetic Alias is generated (generateS3AccessPointAlias) since AWS's real alias-hashing algorithm is undocumented -- a plausible stand-in, not a byte-exact reproduction. Proven by services/fsx/s3_access_point_wire_test.go via a real typed SDK client round-trip; hand-revert reproduced a nil S3AccessPointAttachment. Three PRE-EXISTING tests that had encoded the wrong contract as correct (handler_s3_access_points_test.go x2, handler_test.go's Test_CreationTime_IsEpochSecondsNumber/S3AccessPoint case, persistence_test.go's createS3AP helper) were corrected."}
  SharedVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "Describe/Update verified; single scalar field, not a collection, so untouched by the store.Table refactor per store_setup.go."}
  Misc: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ReleaseFileSystemNfsV3Locks and StartMisconfiguredStateRecovery both validate FileSystemId existence against real state (not disguised no-ops) and echo the file system back; neither op has persisted side effects in real AWS beyond a transient Lifecycle flicker, which this synchronous emulator does not model (consistent with the immediate-AVAILABLE pattern used for every other resource in this service). GLOBAL FIX this pass: errValidation's wire code was 'ValidationError', which is not a real FSx exception (field-diffed against types/errors.go -- FSx's generic client-error type is BadRequest; there is no ValidationError type at all). Every op across every family that returns ErrValidation (CreateFileSystem, CreateSnapshot, CreateStorageVirtualMachine, CreateVolume, CreateAndAttachS3AccessPoint, CreateFileCache) now correctly returns BadRequest. Added ErrMissingFileSystemConfiguration (wire code MissingFileSystemConfiguration) for CreateFileSystem's new required-config-block validation."}
  Tags: {wire: ok, errors: ok, state: ok, persist: ok, note: "TagResource/UntagResource/ListTagsForResource error code fixed in a prior pass: unrecognized ARNs return the generic ResourceNotFound exception. ListTagsForResource already returned [] not null for empty tag sets."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "FIXED 2026-08-29 (write-only-state sweep): CreateFileSystemFromBackup had no SubnetIds field at all -- SubnetIds is a required real CreateFileSystemFromBackupInput member (api_op_CreateFileSystemFromBackup.go) that every real client's SDK-side validator forces it to send, and it round-trips onto FileSystem.SubnetIds on every other file-system create path (CreateFileSystem already accepts/echoes it). It was being silently discarded: the restored file system always came back with empty SubnetIds/NetworkInterfaceIds regardless of what was requested. Fixed: accepted, format-validated (same subnet-[0-9a-f]{8,} pattern as CreateFileSystem), stored, and echoed, plus SecurityGroupIds accepted-and-validated for consistency (matches real AWS: 'This value isn't returned in later DescribeFileSystem requests', so, like CreateFileSystem, intentionally not stored/echoed). Not made required-and-rejecting-when-absent, matching the existing precedent immediately below (CreateFileSystem's own SubnetIds gap) and to avoid breaking the existing test fixtures that predate SubnetIds support on this op. Proven by wire_field_fixes_test.go's TestCreateFileSystemFromBackup_SubnetIdsRoundTrip (real client, hand-reverted, confirmed failing pre-fix, restored md5sum-identical)."
  - "Delete*Output shapes (DeleteFileSystem, DeleteVolume) do not include the optional WindowsResponse/LustreResponse/OpenZFSConfiguration finalizer sub-objects (e.g. FinalBackupTags) that real AWS returns when a final backup is requested at delete time. Low traffic; not fixed this pass (gopherstack-wjjl was scoped to idempotency + network validation, not this)."
  - "CreateFileSystem still does not REQUIRE SubnetIds (real AWS: Required: Yes, and exactly two for Windows/ONTAP MULTI_AZ_1 deployments). Re-confirmed this pass (gopherstack-wjjl) against the live API reference (docs.aws.amazon.com/fsx/latest/APIReference/API_CreateFileSystem.html): SubnetIds is genuinely required. Still not enforced: grep confirms zero test fixtures across the entire fsx package (5 test files, 28+ CreateFileSystem call sites) ever populate SubnetIds, so flipping it to required would be a wholesale fixture migration, not a small fix, and this emulator still does not model Availability Zone topology needed for the exactly-one-vs-exactly-two-subnets MULTI_AZ_1 rule. What WAS fixed this pass: SubnetIds/SecurityGroupIds, when supplied, are now format-validated against the real ID patterns (subnet-[0-9a-f]{8,} / sg-[0-9a-f]{8,}) and rejected with InvalidNetworkSettings if malformed -- see families note below."
  - "ActiveDirectoryError (AD-join failures for WINDOWS/ONTAP file systems joining a directory) is not modeled: ActiveDirectoryId is accepted and echoed back but never validated against a real Directory Service resource (gopherstack's ds package). Not fixed this pass -- cross-service validation, out of scope for a single-service parity pass."
  - "CreateFileSystem (the non-backup create path) does not accept FileSystemTypeVersion, unlike CreateFileSystemFromBackup which gained it this pass (gopherstack-cgq3). Real CreateFileSystemInput has this field too (api_op_CreateFileSystem.go:118), so a Lustre file system created directly (not restored from a backup) can never have a non-empty FileSystemTypeVersion in this emulator, and CreateFileSystemFromBackup's own \"inherit from source file system\" fallback is therefore currently always empty in practice unless the caller supplies an explicit override. Not fixed this pass -- out of the single-op scope that found it."
  - "FIXED 2026-08-23: CreateVolume's input-shape gap (see the Volume family note and Notes section) -- real CreateVolumeInput has no top-level FileSystemId/StorageVirtualMachineId; the anchor is OntapConfiguration.StorageVirtualMachineId (ONTAP) / OpenZFSConfiguration.ParentVolumeId (OPENZFS). Response-side OntapVolumeConfiguration/OpenZFSVolumeConfiguration on Volume remain unmodeled (Layer 3, unchanged, see the Volume family note above)."
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

**2026-08-22 (gopherstack-r80d, batch 34 -- required-OUTPUT-member sweep, wrapped-type-shape
candidate):** every fsx op's `<Op>Output` declares zero required members at its own top level
(confirmed via `cmd/requiredoutputfields`), so this service was invisible to every ranking this
campaign used through batch 33. Selected as one of the two candidates named by batch 33's
"ops with zero required fields wrapping richly-required domain types" mechanism test (`services/
_REQUIRED_OUTPUT_CANDIDATES.md`'s batch-33 section) and given the full hand audit that batch left
undone.

Every non-slice field of every `<Op>Output` was walked one hop into its own type
(`aws-sdk-go-v2/service/fsx@v1.68.4/types/types.go`); only two wrapped types declare >=2
required members of their own: `Backup` (`BackupId`, `CreationTime`, `FileSystem`, `Lifecycle`,
`Type` -- types.go, `type Backup struct`) and `DataRepositoryTask` (`CreationTime`, `Lifecycle`,
`TaskId`, `Type` -- types.go, `type DataRepositoryTask struct`), reached via
`CreateBackup`/`DescribeBackups`/`CopyBackup` and `CreateDataRepositoryTask`/
`DescribeDataRepositoryTasks` respectively.

**1 bug found and fixed:** `Backup.FileSystem` (`*types.FileSystem`, required, a pointer and
therefore provable) was dropped whenever the backup's source file system had since been
deleted. `Backup.FileSystem`'s own doc comment states this metadata "is persisted even if the
file system is deleted" -- and `DeleteFileSystem` intentionally does not cascade-delete backups
(already covered by `TestFSx_DeleteFileSystem_DoesNotCascadeToBackups`), so this is a genuinely
reachable, unexceptional state, not an edge case. `backups.go`'s `toBackup` derived `FileSystem`
from a live lookup in the `fileSystems` table at read time (`CreateBackup`/`DescribeBackups`/
`CopyBackup` all did this); once the file system was deleted, the lookup missed, and the
`omitempty`-tagged `FileSystem` wire key vanished entirely, decoding to `nil` on any real client
even though real FSx keeps serving it.

Fixed by snapshotting the source file system's metadata onto `storedBackup` at
`CreateBackup`/`CopyBackup` time (a new `FileSystem *storedFileSystem` field, deep-copied via a
new `cloneStoredFileSystem` helper so the snapshot never aliases the live, mutable
`fileSystems` table entry -- `store.Table.Get` returns the live pointer and `UpdateFileSystem`
mutates it in place) instead of re-deriving it from live state on every read; `toBackup` now
prefers the stored snapshot and falls back to a live lookup only for pre-existing
snapshot-restored backups that predate this fix. `CopyBackup` propagates the *source backup's*
own snapshot (not a fresh live lookup), matching that a copied backup's metadata should reflect
the source backup's own recorded state.

This is a purely additive field on `storedBackup` (part of `backendSnapshot`'s `backups` table);
`fsxSnapshotVersion` was correctly **not** bumped -- `pkgs/persistence`'s
`TestSnapshotVersionGuard` enforces this, and the checked-in golden
(`pkgs/persistence/testdata/snapshot_inventory.json`) was regenerated with `-update` to add the
new field.

Proven via a real `aws-sdk-go-v2/service/fsx` client round trip
(`wire_output_required_r80d_test.go`): create a file system, create a backup, delete the file
system, then `DescribeBackups`/`CopyBackup` and assert `Backup.FileSystem` is still non-nil with
the original `FileSystemId`. Hand-reverted (`backups.go` restored to `git show HEAD:services/
fsx/backups.go`), confirmed the test fails with "Backup.FileSystem is required and must survive
deletion..." on the pre-fix code, restored, `md5sum` byte-identical.

**Reviewed and ruled clean, not a bug:** `DataRepositoryTask`'s four required members
(`CreationTime`, `Lifecycle`, `TaskId`, `Type`) are all set unconditionally in `toPublic()` for
every task, with no live-lookup dependency of the `Backup.FileSystem` kind. `TaskId`/
`CreationTime` are the only provable (pointer) members among the four and both are always
populated; `Lifecycle`/`Type` are non-pointer enums in the real SDK type, so per this campaign's
standing rule they are not provable regardless and were not further pursued.

**Wrapped-type-shape hypothesis verdict for fsx: held.** The bug above sat two levels below
`DescribeBackupsOutput` (Output -> `Backup` -> `FileSystem`), invisible to every ranking this
campaign has used (required-field count, op count) since `Backup` and `FileSystem` are not
op-level fields at all. See `services/_REQUIRED_OUTPUT_CANDIDATES.md`'s batch-34 section for the
cross-service verdict (fsx found a bug this way; the paired candidate, codebuild, did not).
## 2026-08-20 — wrapper-key / nested-shape wire-parity sweep

Protocol re-confirmed independently this pass (don't trust `services/_PROTOCOLS.md` per the
sweep brief): `awsAwsjson11_*` prefix in both serializers.go/deserializers.go, `X-Amz-Target:
AWSSimbaAPIService_v20180301.<Op>` header — JSON-RPC 1.1. All 48 `awsAwsjson11_deserializeOpDocument<Op>Output`
helpers are both defined AND called from their op's own `HandleDeserialize` (grep-verified), so
the restjson "flat body, dead OpDocument helper" false-positive trap from the sweep brief does
not apply to this service.

Enumerated all 48 SDK ops (`ls api_op_*.go`) against `GetSupportedOperations()` /
`TestSDKCompleteness`'s empty `notImplemented` list — full 1:1 coverage, no orphans either
direction (`opDescribeDataRepositoryAssocs`'s Go identifier maps to the real op name
`DescribeDataRepositoryAssociations`, not a gap).

Three real bugs found and fixed, in order of severity: the S3AccessPoint feature modeling the
wrong AWS type entirely (wrong on both request and response, non-functional against a real
typed client), RestoreVolumeFromSnapshot/CopySnapshotAndUpdateVolume wrapping their response
under a fabricated "Volume" key that doesn't exist on either op's real Output struct (both
opened with an empty response for a real client), and FileCache's Describe/Update paths leaking
a Tags key that only the sibling Create response type actually has. Full citations and fix
descriptions are inline in the `families:` block above, per bug. All three were proven by a
real aws-sdk-go-v2 client round-trip (`services/fsx/s3_access_point_wire_test.go`,
`services/fsx/administrative_action_wire_test.go`, `services/fsx/file_cache_wire_test.go`), and
each fix was hand-reverted to confirm the exact predicted symptom before being restored.

Near-identical type families checked this pass, one line each:
- `FileCache` vs `FileCacheCreating` — was a single shared Go type with a fabricated `Tags`
  field bleeding across both real wire shapes; now split, matching the two real SDK types 1:1.
- The four per-flavour `*FileSystemConfiguration` blocks (Lustre/Windows/Ontap/OpenZFS) — each
  has its own distinct Go type with a case-by-case key check against its own live deserializer
  function; all clean, no cross-contamination.
- The `Target*Values` trio implied by `AdministrativeAction` — only `TargetVolumeValues` is
  populated (reusing the existing `Volume` type, matching the real SDK's own reuse);
  `TargetFileSystemValues`/`TargetSnapshotValues` are never emitted (Layer 3, out of scope).
- `S3AccessPointAttachment` vs the nested `S3AccessPoint` — two genuinely distinct real SDK
  types with the same-ish name; gopherstack previously conflated them into one flat type that
  matched neither. Now split into `S3AccessPointAttachment` (top-level) + `S3AccessPoint`
  (nested details, `Alias`/`ResourceARN` only) + per-flavour `S3AccessPointOntapConfiguration`/
  `S3AccessPointOpenZFSConfiguration`.

Families hand-verified clean (own deserializer's case list read and diffed against gopherstack's
emitted keys, no fabricated members found): `FileSystem` (including all four per-flavour
configs + `FileSystemEndpoints`/`FileSystemEndpoint`), `Backup` (single `types.Backup` shape
shared correctly across Create/Describe/Copy — confirmed via each op's own Output struct field
type, not assumed from the name), `DataRepositoryAssociation`, `DataRepositoryTask` (+
`CompletionReport`), `Snapshot`, `StorageVirtualMachine`, `Volume` (top-level fields only —
see gaps for the nested `OntapVolumeConfiguration`/`OpenZFSVolumeConfiguration` members never
emitted), `SharedVpcConfiguration`, `Tag`. Enum spellings spot-checked against `types/enums.go`
for every value gopherstack emits (`FileSystemType`, `StorageType`, `VolumeType`,
`BackupType`/`BackupLifecycle`, every resource's `*Lifecycle`, `DataRepositoryTaskType`,
`AdministrativeActionType`, `S3AccessPointAttachmentType`) — all exact matches, zero
case-mismatches or fabricated enum values found this pass.

Genuinely NOT reached this pass, disclosed rather than hunted: `ActiveDirectoryConfiguration`/
`SvmEndpoints`/`SvmEndpoint` on `StorageVirtualMachine`, and `OntapVolumeConfiguration`/
`OpenZFSVolumeConfiguration`/`TieringPolicy`/`SnaplockConfiguration`/`AutocommitPeriod`/
`RetentionPeriod` on `Volume` — all real SDK members never emitted at all by this backend
(Layer 3, "never emitted" is out of scope for a hunt per this sweep's brief; noted in the
`Volume`/`StorageVirtualMachine` family rows above rather than silently skipped).

`last_audit_commit` provenance: the prior manifest cited `3f66c846b...`, dated 2026-07-24 in
its own commit metadata (`git show -s --format=%ad`) — which matches `last_audit_date:
2026-07-24` exactly, even though that sha is not an ancestor of current `HEAD` (repo-wide
history-rewrite artifact per `gopherstack-z31a`, not a sign this manifest's prior audit was
fabricated or stale). Verdict: the prior audit's provenance checks out; updated to this
session's `HEAD` (`8d4556e79386...`, 2026-08-20) above.

## 2026-08-23 (gopherstack batch8) — CreateVolume request-side accept-and-drop fixed

**Bug**: `CreateVolume`'s decode struct (`createVolumeInput`, `volumes.go`) had
top-level `FileSystemId`/`StorageVirtualMachineId` fields. Real
`CreateVolumeInput` (`fsx@v1.68.4 api_op_CreateVolume.go`) has neither — the
only real anchor for an ONTAP volume's parent is
`OntapConfiguration.StorageVirtualMachineId` (required within that block per
`validators.go:1122`'s `validateCreateOntapVolumeConfiguration`), and for
OpenZFS it's `OpenZFSConfiguration.ParentVolumeId` (required per
`validators.go:1160`). Since gopherstack's struct had no `OntapConfiguration`/
`OpenZFSConfiguration` fields at all, a real client's nested SVM/parent-volume
reference was silently dropped by `encoding/json` (unknown key, no error) —
`CreateVolume` always succeeded with `FileSystemId`/`StorageVirtualMachineId`
both empty, no existence check performed, no error surfaced either way. This
is the "request-side accept-and-drop" bug class (see the S3AccessPoint/
RestoreVolumeFromSnapshot fixes above for the paired response-side version)
that this campaign's batches have repeatedly found to be real.

**Fix** (`volumes.go`): added `createOntapVolumeConfigInput{StorageVirtualMachineID}`
and `createOpenZFSVolumeConfigInput{ParentVolumeID}` (the sole required member
of each real config block; the rest of each block's fields are out of this
fix's scope since `Volume`'s response has no matching
`OntapVolumeConfiguration`/`OpenZFSVolumeConfiguration` to round-trip them
into at all — a separate, disclosed, pre-existing Layer-3 gap, unchanged). New
`resolveVolumeParentLocked` mirrors `CreateFileSystem`'s existing
`applyOntapConfig`/`applyOpenZFSConfig` precedent: `VolumeType=ONTAP` without
`OntapConfiguration.StorageVirtualMachineId` (or `OPENZFS` without
`OpenZFSConfiguration.ParentVolumeId`) now returns `MissingVolumeConfiguration`
(new sentinel `ErrMissingVolumeConfiguration`, `errors.go`; real wire code
confirmed via `types.MissingVolumeConfiguration`, `types/errors.go:769`, "A
volume configuration is required for this operation."); a named-but-unknown
`StorageVirtualMachineId`/`ParentVolumeId` returns
`StorageVirtualMachineNotFound`/`VolumeNotFound` (both pre-existing
sentinels); a resolved SVM/parent volume's `FileSystemId` is copied onto the
new volume for real, closing the FileSystemId-always-empty symptom.

**Proof**: `TestCreateVolume_RealRequestShape` (`handler_volumes_test.go`, 4
subtests) drives the real `aws-sdk-go-v2/service/fsx` client end to end
(`CreateFileSystem` -> `CreateStorageVirtualMachine` -> `CreateVolume` for
ONTAP; `CreateFileSystem` -> `CreateVolume` against the auto-created OpenZFS
root volume) and asserts `Volume.FileSystemId` matches the real parent, plus
the two negative cases (missing config block, unknown SVM). Hand-reverted
`volumes.go` to `git show HEAD:services/fsx/volumes.go` (the pre-fix flat
top-level-fields version) and confirmed all 4 subtests fail exactly as
predicted: the two positive cases assert `Volume.FileSystemId` equals the
real file system's ID and get back `""` instead (no error — the exact silent
accept-and-drop symptom), and the two negative cases assert an error and get
`nil` (no error — the exact fabricated-success symptom). Restored;
`md5sum` byte-identical to the pre-revert file.

Five pre-existing test call sites relied on the old (invalid) flat wire shape
and were updated to build a real SVM/OpenZFS-root-volume anchor first,
matching what a real client must now do:
`administrative_action_wire_test.go` (2 sites, via a new shared
`createTestOntapVolume` helper in `handler_create_tags_test.go`),
`s3_access_point_wire_test.go` (1 site, same helper),
`handler_create_tags_test.go`'s own "snapshot"/"volume" tag-reachability
cases (2 sites), `cascade_delete_test.go` (3 sites, ONTAP + OPENZFS),
`handler_volumes_test.go`'s `TestFSx_Volume` table test and the shared
`createVolume(t, h, fsID, volType, name)` helper in `handler_test.go` (now
resolves/creates a real SVM or OpenZFS root volume internally instead of
sending the old flat shape), and `Test_CreationTime_IsEpochSecondsNumber`'s
"Snapshot"/"Volume" cases in `handler_test.go`. None of these were testing
anything about the flat shape itself — they were incidentally relying on the
bug being present to get away with an unauthenticated SVM reference.

**Not fixed, still disclosed** (unchanged from the prior gap entry, narrowed):
`Volume`'s response has no `OntapVolumeConfiguration`/`OpenZFSVolumeConfiguration`
at all (Layer 3, pre-existing, see the Volume family note above) — this fix is
request-side only; the resolved `StorageVirtualMachineId` is stored on
`storedVolume` (used internally, e.g. by `deleteStorageVirtualMachineLocked`'s
cascade-delete) but has nowhere real on the wire to be echoed back nested,
same as before this pass. `OntapConfiguration`'s other real fields
(`JunctionPath`, `SizeInBytes`, `SecurityStyle`, `OntapVolumeType`,
`SnaplockConfiguration`, ...) and `OpenZFSConfiguration`'s (`DataCompressionType`,
`NfsExports`, `OriginSnapshot`, `UserAndGroupQuotas`, ...) remain unmodeled —
only each block's sole required member was added, matching this fix's scope
(the bug named in the prior gap entry was specifically about the missing
anchor, not full per-type field coverage).

Gates: `go build ./...`, `go vet ./services/fsx/...`, `gofmt -l`/`golines -l`
(clean), `go test -race -count=1 ./services/fsx/...` (pass),
`golangci-lint run ./services/fsx/...` (0 issues). No persisted struct's wire
shape changed (`storedVolume`'s `StorageVirtualMachineID` field already
existed; only how it's populated changed) — `fsxSnapshotVersion` correctly
left unbumped.
