package fsx

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	lifecycleAvailable      = "AVAILABLE"
	lifecycleDeleting       = "DELETING"
	lifecycleDeleted        = "DELETED"
	backupTypeUserInitiated = "USER_INITIATED"

	// sharedVpcDisabled is the default/reset value of sharedVpcEnabled.
	sharedVpcDisabled = "false"

	fileSystemTypeLustre            = "LUSTRE"
	fileSystemTypeWindows           = "WINDOWS"
	fileSystemTypeONTAP             = "ONTAP"
	fileSystemTypeOpenZFS           = "OPENZFS"
	dataRepositoryLifecycleDisabled = "DISABLED"
	lustreDeploymentTypeScratch1    = "SCRATCH_1"
	windowsDeploymentTypeSingleAZ1  = "SINGLE_AZ_1"
	lustreMountNameLen              = 8

	// defaultAutomaticBackupRetentionDays is the real-AWS default backup
	// retention for Windows/ONTAP/OpenZFS file systems when the create
	// request doesn't set AutomaticBackupRetentionDays.
	defaultAutomaticBackupRetentionDays = 30
	// defaultHAPairs is the real-AWS default HAPairs for ONTAP file systems.
	defaultHAPairs = 1
	// openZFSRootVolumeName is the fixed name AWS assigns to the
	// auto-created root volume of every FSx for OpenZFS file system.
	openZFSRootVolumeName = "fsx"

	// Minimum StorageCapacity (GiB) enforced by real AWS FSx per file system type.
	minStorageCapacityLustre  = 1200
	minStorageCapacityWindows = 32
	minStorageCapacityONTAP   = 1024
	minStorageCapacityOpenZFS = 64

	maxResultsDefault  = 2147483647
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	maxTagsPerResource = 50
)

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu *lockmetrics.RWMutex
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment for why every table here is
	// "clean" (registered directly, no DTO-registry needed).
	registry               *store.Registry
	fileSystems            *store.Table[storedFileSystem]            // fileSystemID → fs
	backups                *store.Table[storedBackup]                // backupID → backup
	tags                   map[string]map[string]string              // resourceARN → tags
	aliases                map[string][]string                       // fileSystemID → []alias name
	dataRepositoryAssocs   *store.Table[storedDataRepositoryAssoc]   // associationID → assoc
	dataRepositoryTasks    *store.Table[storedDataRepositoryTask]    // taskID → task
	fileCaches             *store.Table[storedFileCache]             // fileCacheID → cache
	snapshots              *store.Table[storedSnapshot]              // snapshotID → snapshot
	storageVirtualMachines *store.Table[storedStorageVirtualMachine] // svmID → svm
	volumes                *store.Table[storedVolume]                // volumeID → volume
	s3AccessPoints         *store.Table[storedS3AccessPoint]         // name → access point
	sharedVpcEnabled       string
	accountID              string
	region                 string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:               lockmetrics.New("fsx"),
		registry:         store.NewRegistry(),
		tags:             make(map[string]map[string]string),
		aliases:          make(map[string][]string),
		sharedVpcEnabled: sharedVpcDisabled,
		accountID:        accountID,
		region:           region,
	}

	registerAllTables(b)

	return b
}

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.aliases = make(map[string][]string)
	b.sharedVpcEnabled = sharedVpcDisabled
}

// hasResourceARN reports whether any value in t has the given ResourceARN, as
// extracted by arnOf. It backs arnExists's per-table checks.
func hasResourceARN[V any](t *store.Table[V], arnOf func(*V) string, resourceARN string) bool {
	found := false

	t.Range(func(v *V) bool {
		if arnOf(v) == resourceARN {
			found = true

			return false
		}

		return true
	})

	return found
}

// arnExists checks whether a resource ARN belongs to any known FSx resource.
func (b *InMemoryBackend) arnExists(resourceARN string) bool {
	if hasResourceARN(b.fileSystems, func(v *storedFileSystem) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(b.backups, func(v *storedBackup) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(b.fileCaches, func(v *storedFileCache) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(b.snapshots, func(v *storedSnapshot) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(
		b.storageVirtualMachines, func(v *storedStorageVirtualMachine) string { return v.ResourceARN }, resourceARN,
	) {
		return true
	}

	if hasResourceARN(b.volumes, func(v *storedVolume) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(
		b.dataRepositoryAssocs, func(v *storedDataRepositoryAssoc) string { return v.ResourceARN }, resourceARN,
	) {
		return true
	}

	if hasResourceARN(b.s3AccessPoints, func(v *storedS3AccessPoint) string { return v.ResourceARN }, resourceARN) {
		return true
	}

	if hasResourceARN(
		b.dataRepositoryTasks, func(v *storedDataRepositoryTask) string { return v.ResourceARN }, resourceARN,
	) {
		return true
	}

	return false
}

// paginate returns (start, end, nextToken) for a sorted slice of length n.
// keyFn maps index → token string.
func paginate(n, maxResults int, nextToken string, keyFn func(int) string) (int, int, string) {
	start := 0
	if nextToken != "" {
		for i := range n {
			if keyFn(i) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+maxResults, n)

	var next string
	if end < n {
		next = keyFn(end)
	}

	return start, end, next
}
