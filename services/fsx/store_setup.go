package fsx

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/waf. See pkgs/store's package doc for the underlying primitive.
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- storedFileSystem.FileSystemID,
// storedBackup.BackupID, storedDataRepositoryAssoc.AssociationID,
// storedDataRepositoryTask.TaskID, storedFileCache.FileCacheID,
// storedSnapshot.SnapshotID, storedStorageVirtualMachine.StorageVirtualMachineID,
// storedVolume.VolumeID, storedS3AccessPoint.Name -- so all nine are "clean"
// tables registered directly on b.registry, with no secondary index needed:
// every Describe* op here is either a direct-ID lookup or a full-table scan
// (sorted by the same ID for pagination), never a parent-scoped lookup that
// would benefit from a store.Index.
//
// Left as plain maps (not store.Table-backed):
//   - tags (map[string]map[string]string): value is a bare tag map, not *T.
//   - aliases (map[string][]string): slice-valued and order-sensitive (alias
//     names keep their AssociateFileSystemAliases append order, which
//     DescribeFileSystemAliases pagination relies on), so it does not fit
//     store.Table's keyed-by-identity-value shape and must not be routed
//     through a store.Index (which does not preserve insertion order).
//
// sharedVpcEnabled is a single string, not a collection, so it is untouched
// by this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func fileSystemKeyFn(v *storedFileSystem) string { return v.FileSystemID }

func backupKeyFn(v *storedBackup) string { return v.BackupID }

func dataRepositoryAssocKeyFn(v *storedDataRepositoryAssoc) string { return v.AssociationID }

func dataRepositoryTaskKeyFn(v *storedDataRepositoryTask) string { return v.TaskID }

func fileCacheKeyFn(v *storedFileCache) string { return v.FileCacheID }

func snapshotKeyFn(v *storedSnapshot) string { return v.SnapshotID }

func storageVirtualMachineKeyFn(v *storedStorageVirtualMachine) string {
	return v.StorageVirtualMachineID
}

func volumeKeyFn(v *storedVolume) string { return v.VolumeID }

func s3AccessPointKeyFn(v *storedS3AccessPoint) string { return v.Name }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.fileSystems = store.Register(b.registry, "fileSystems", store.New(fileSystemKeyFn))
	b.backups = store.Register(b.registry, "backups", store.New(backupKeyFn))
	b.dataRepositoryAssocs = store.Register(
		b.registry, "dataRepositoryAssocs", store.New(dataRepositoryAssocKeyFn),
	)
	b.dataRepositoryTasks = store.Register(
		b.registry, "dataRepositoryTasks", store.New(dataRepositoryTaskKeyFn),
	)
	b.fileCaches = store.Register(b.registry, "fileCaches", store.New(fileCacheKeyFn))
	b.snapshots = store.Register(b.registry, "snapshots", store.New(snapshotKeyFn))
	b.storageVirtualMachines = store.Register(
		b.registry, "storageVirtualMachines", store.New(storageVirtualMachineKeyFn),
	)
	b.volumes = store.Register(b.registry, "volumes", store.New(volumeKeyFn))
	b.s3AccessPoints = store.Register(b.registry, "s3AccessPoints", store.New(s3AccessPointKeyFn))
}
