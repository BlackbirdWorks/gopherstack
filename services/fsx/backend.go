package fsx

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errFileSystemNotFound = "FileSystemNotFound"
	errBackupNotFound     = "BackupNotFound"
	errValidation         = "ValidationError"

	lifecycleAvailable      = "AVAILABLE"
	lifecycleDeleting       = "DELETING"
	lifecycleDeleted        = "DELETED"
	backupTypeUserInitiated = "USER_INITIATED"

	fileSystemTypeLustre            = "LUSTRE"
	dataRepositoryLifecycleDisabled = "DISABLED"
	lustreDeploymentTypeScratch1    = "SCRATCH_1"
	lustreMountNameLen              = 8

	maxResultsDefault  = 2147483647
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	maxTagsPerResource = 50
)

var (
	// ErrFileSystemNotFound is returned when a file system does not exist.
	ErrFileSystemNotFound = awserr.New(errFileSystemNotFound, awserr.ErrNotFound)
	// ErrBackupNotFound is returned when a backup does not exist.
	ErrBackupNotFound = awserr.New(errBackupNotFound, awserr.ErrConflict)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrTagInvalid is returned when a tag key or value fails validation.
	ErrTagInvalid = awserr.New("BadRequest", awserr.ErrInvalidParameter)
	// ErrTagLimitExceeded is returned when the 50-tag-per-resource limit is exceeded.
	ErrTagLimitExceeded = awserr.New("ServiceLimitExceeded", awserr.ErrInvalidParameter)

	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = awserr.New("SnapshotNotFound", awserr.ErrNotFound)
	// ErrStorageVirtualMachineNotFound is returned when an SVM does not exist.
	ErrStorageVirtualMachineNotFound = awserr.New("StorageVirtualMachineNotFound", awserr.ErrNotFound)
	// ErrVolumeNotFound is returned when a volume does not exist.
	ErrVolumeNotFound = awserr.New("VolumeNotFound", awserr.ErrNotFound)
	// ErrFileCacheNotFound is returned when a file cache does not exist.
	ErrFileCacheNotFound = awserr.New("FileCacheNotFound", awserr.ErrNotFound)
	// ErrDataRepositoryAssociationNotFound is returned when a DRA does not exist.
	ErrDataRepositoryAssociationNotFound = awserr.New("DataRepositoryAssociationNotFound", awserr.ErrNotFound)
	// ErrDataRepositoryTaskNotFound is returned when a DRT does not exist.
	ErrDataRepositoryTaskNotFound = awserr.New("DataRepositoryTaskNotFound", awserr.ErrNotFound)
	// ErrS3AccessPointNotFound is returned when an S3 access point does not exist.
	ErrS3AccessPointNotFound = awserr.New("InvalidRequest", awserr.ErrNotFound)
)

// storedFileSystem is the persisted form of a FileSystem.
// time.Time is first: non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedFileSystem struct {
	CreationTime        time.Time         `json:"creationTime"`
	Tags                map[string]string `json:"tags"`
	FileSystemID        string            `json:"fileSystemId"`
	FileSystemType      string            `json:"fileSystemType"`
	Lifecycle           string            `json:"lifecycle"`
	ResourceARN         string            `json:"resourceArn"`
	DNSName             string            `json:"dnsName,omitempty"`
	StorageType         string            `json:"storageType,omitempty"`
	VpcID               string            `json:"vpcId,omitempty"`
	OwnerID             string            `json:"ownerId,omitempty"`
	DeploymentType      string            `json:"deploymentType,omitempty"`
	MountName           string            `json:"mountName,omitempty"`
	SubnetIDs           []string          `json:"subnetIds,omitempty"`
	NetworkInterfaceIDs []string          `json:"networkInterfaceIds,omitempty"`
	StorageCapacityGiB  int32             `json:"storageCapacity,omitempty"`
}

func (s *storedFileSystem) toFileSystem() *FileSystem {
	fs := &FileSystem{
		CreationTime:        epochTime(s.CreationTime),
		Tags:                tagsMapToSlice(s.Tags),
		FileSystemID:        s.FileSystemID,
		FileSystemType:      s.FileSystemType,
		Lifecycle:           s.Lifecycle,
		ResourceARN:         s.ResourceARN,
		DNSName:             s.DNSName,
		StorageCapacityGiB:  s.StorageCapacityGiB,
		StorageType:         s.StorageType,
		VpcID:               s.VpcID,
		OwnersID:            s.OwnerID,
		SubnetIDs:           s.SubnetIDs,
		NetworkInterfaceIDs: s.NetworkInterfaceIDs,
	}

	// AWS always returns a LustreConfiguration block for Lustre file systems.
	// The terraform-provider-aws Read path treats a nil LustreConfiguration as
	// an empty result, so a Lustre file system must echo this back.
	if s.FileSystemType == fileSystemTypeLustre {
		fs.LustreConfiguration = &LustreConfiguration{
			DeploymentType: s.DeploymentType,
			MountName:      s.MountName,
			DataRepositoryConfiguration: &DataRepositoryConfiguration{
				Lifecycle: dataRepositoryLifecycleDisabled,
			},
		}
	}

	return fs
}

// storedBackup is the persisted form of a Backup.
// time.Time is first: non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedBackup struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	FileSystemID string            `json:"fileSystemId"`
	BackupID     string            `json:"backupId"`
	BackupType   string            `json:"backupType"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
}

func (b *storedBackup) toBackup(fs *storedFileSystem) *Backup {
	bk := &Backup{
		BackupID:     b.BackupID,
		BackupType:   b.BackupType,
		CreationTime: epochTime(b.CreationTime),
		Lifecycle:    b.Lifecycle,
		ResourceARN:  b.ResourceARN,
		Tags:         tagsMapToSlice(b.Tags),
	}
	if fs != nil {
		bk.FileSystem = fs.toFileSystem()
	}

	return bk
}

// snapshot holds serializable backend state.
type snapshot struct {
	FileSystems            map[string]*storedFileSystem            `json:"fileSystems"`
	Backups                map[string]*storedBackup                `json:"backups"`
	Tags                   map[string]map[string]string            `json:"tags"`
	Aliases                map[string][]string                     `json:"aliases"`
	DataRepositoryAssocs   map[string]*storedDataRepositoryAssoc   `json:"dataRepositoryAssocs"`
	DataRepositoryTasks    map[string]*storedDataRepositoryTask    `json:"dataRepositoryTasks"`
	FileCaches             map[string]*storedFileCache             `json:"fileCaches"`
	Snapshots              map[string]*storedSnapshot              `json:"snapshots"`
	StorageVirtualMachines map[string]*storedStorageVirtualMachine `json:"storageVirtualMachines"`
	Volumes                map[string]*storedVolume                `json:"volumes"`
	S3AccessPoints         map[string]*storedS3AccessPoint         `json:"s3AccessPoints"`
	SharedVpcEnabled       string                                  `json:"sharedVpcEnabled"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu                     *lockmetrics.RWMutex
	fileSystems            map[string]*storedFileSystem            // fileSystemID → fs
	backups                map[string]*storedBackup                // backupID → backup
	tags                   map[string]map[string]string            // resourceARN → tags
	aliases                map[string][]string                     // fileSystemID → []alias name
	dataRepositoryAssocs   map[string]*storedDataRepositoryAssoc   // associationID → assoc
	dataRepositoryTasks    map[string]*storedDataRepositoryTask    // taskID → task
	fileCaches             map[string]*storedFileCache             // fileCacheID → cache
	snapshots              map[string]*storedSnapshot              // snapshotID → snapshot
	storageVirtualMachines map[string]*storedStorageVirtualMachine // svmID → svm
	volumes                map[string]*storedVolume                // volumeID → volume
	s3AccessPoints         map[string]*storedS3AccessPoint         // name → access point
	sharedVpcEnabled       string
	accountID              string
	region                 string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                     lockmetrics.New("fsx"),
		fileSystems:            make(map[string]*storedFileSystem),
		backups:                make(map[string]*storedBackup),
		tags:                   make(map[string]map[string]string),
		aliases:                make(map[string][]string),
		dataRepositoryAssocs:   make(map[string]*storedDataRepositoryAssoc),
		dataRepositoryTasks:    make(map[string]*storedDataRepositoryTask),
		fileCaches:             make(map[string]*storedFileCache),
		snapshots:              make(map[string]*storedSnapshot),
		storageVirtualMachines: make(map[string]*storedStorageVirtualMachine),
		volumes:                make(map[string]*storedVolume),
		s3AccessPoints:         make(map[string]*storedS3AccessPoint),
		sharedVpcEnabled:       "false",
		accountID:              accountID,
		region:                 region,
	}
}

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.fileSystems = make(map[string]*storedFileSystem)
	b.backups = make(map[string]*storedBackup)
	b.tags = make(map[string]map[string]string)
	b.aliases = make(map[string][]string)
	b.dataRepositoryAssocs = make(map[string]*storedDataRepositoryAssoc)
	b.dataRepositoryTasks = make(map[string]*storedDataRepositoryTask)
	b.fileCaches = make(map[string]*storedFileCache)
	b.snapshots = make(map[string]*storedSnapshot)
	b.storageVirtualMachines = make(map[string]*storedStorageVirtualMachine)
	b.volumes = make(map[string]*storedVolume)
	b.s3AccessPoints = make(map[string]*storedS3AccessPoint)
	b.sharedVpcEnabled = "false"
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		FileSystems:            b.fileSystems,
		Backups:                b.backups,
		Tags:                   b.tags,
		Aliases:                b.aliases,
		DataRepositoryAssocs:   b.dataRepositoryAssocs,
		DataRepositoryTasks:    b.dataRepositoryTasks,
		FileCaches:             b.fileCaches,
		Snapshots:              b.snapshots,
		StorageVirtualMachines: b.storageVirtualMachines,
		Volumes:                b.volumes,
		S3AccessPoints:         b.s3AccessPoints,
		SharedVpcEnabled:       b.sharedVpcEnabled,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.fileSystems = s.FileSystems
	b.backups = s.Backups
	b.tags = s.Tags
	b.aliases = s.Aliases
	b.dataRepositoryAssocs = s.DataRepositoryAssocs
	b.dataRepositoryTasks = s.DataRepositoryTasks
	b.fileCaches = s.FileCaches
	b.snapshots = s.Snapshots
	b.storageVirtualMachines = s.StorageVirtualMachines
	b.volumes = s.Volumes
	b.s3AccessPoints = s.S3AccessPoints
	b.sharedVpcEnabled = s.SharedVpcEnabled

	return nil
}

// createFileSystemInput holds parameters for CreateFileSystem.
type createFileSystemInput struct {
	LustreConfiguration *createLustreConfiguration `json:"LustreConfiguration,omitempty"`
	FileSystemType      string                     `json:"FileSystemType"`
	StorageType         string                     `json:"StorageType,omitempty"`
	VpcID               string                     `json:"VpcId,omitempty"`
	Tags                []Tag                      `json:"Tags,omitempty"`
	SubnetIDs           []string                   `json:"SubnetIds,omitempty"`
	StorageCapacityGiB  int32                      `json:"StorageCapacity,omitempty"`
}

// createLustreConfiguration mirrors the CreateFileSystemLustreConfiguration
// block sent by the AWS provider for Lustre file systems.
type createLustreConfiguration struct {
	DeploymentType string `json:"DeploymentType,omitempty"`
}

// CreateFileSystem creates a new file system.
func (b *InMemoryBackend) CreateFileSystem(input *createFileSystemInput) (*FileSystem, error) {
	if input.FileSystemType == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	id := "fs-" + uuid.New().String()[:17]
	arn := b.fsARN(id)
	now := time.Now().UTC()

	tags := tagsSliceToMap(input.Tags)

	fs := &storedFileSystem{
		CreationTime:        now,
		Tags:                tags,
		FileSystemID:        id,
		FileSystemType:      input.FileSystemType,
		Lifecycle:           lifecycleAvailable,
		ResourceARN:         arn,
		DNSName:             fmt.Sprintf("%s.fsx.%s.amazonaws.com", id, b.region),
		StorageCapacityGiB:  input.StorageCapacityGiB,
		StorageType:         input.StorageType,
		VpcID:               input.VpcID,
		OwnerID:             b.accountID,
		SubnetIDs:           input.SubnetIDs,
		NetworkInterfaceIDs: networkInterfaceIDsForSubnets(input.SubnetIDs),
	}

	if input.FileSystemType == fileSystemTypeLustre {
		fs.MountName = generateLustreMountName()
		if input.LustreConfiguration != nil {
			fs.DeploymentType = input.LustreConfiguration.DeploymentType
		}

		if fs.DeploymentType == "" {
			fs.DeploymentType = lustreDeploymentTypeScratch1
		}
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	b.fileSystems[id] = fs
	b.tags[arn] = tags

	return fs.toFileSystem(), nil
}

// generateLustreMountName returns a short, lowercase alphanumeric mount name in
// the style AWS assigns to Lustre file systems (e.g. "abcd1234").
func generateLustreMountName() string {
	raw := strings.ReplaceAll(uuid.New().String(), "-", "")
	if len(raw) > lustreMountNameLen {
		raw = raw[:lustreMountNameLen]
	}

	return raw
}

// networkInterfaceIDsForSubnets returns one synthetic ENI ID per subnet, as AWS
// attaches an elastic network interface to the file system in each subnet.
func networkInterfaceIDsForSubnets(subnetIDs []string) []string {
	if len(subnetIDs) == 0 {
		return nil
	}

	enis := make([]string, 0, len(subnetIDs))
	for range subnetIDs {
		enis = append(enis, "eni-"+strings.ReplaceAll(uuid.New().String(), "-", "")[:17])
	}

	return enis
}

// DescribeFileSystems returns file systems, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeFileSystems(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*FileSystem, string, error) {
	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedFileSystem

	if len(ids) > 0 {
		for _, id := range ids {
			fs, ok := b.fileSystems[id]
			if !ok {
				return nil, "", ErrFileSystemNotFound
			}

			all = append(all, fs)
		}
	} else {
		for _, fs := range b.fileSystems {
			all = append(all, fs)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].FileSystemID < all[j].FileSystemID })
	}

	start := 0
	if nextToken != "" {
		for i, fs := range all {
			if fs.FileSystemID == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+int(maxResults), len(all))
	page := all[start:end]

	var next string
	if end < len(all) {
		next = all[end].FileSystemID
	}

	result := make([]*FileSystem, len(page))
	for i, fs := range page {
		result[i] = fs.toFileSystem()
	}

	return result, next, nil
}

// DeleteFileSystem removes a file system.
func (b *InMemoryBackend) DeleteFileSystem(fileSystemID string) error {
	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return ErrFileSystemNotFound
	}

	delete(b.fileSystems, fileSystemID)
	delete(b.tags, fs.ResourceARN)

	return nil
}

// updateFileSystemInput holds parameters for UpdateFileSystem.
type updateFileSystemInput struct {
	FileSystemID       string `json:"FileSystemId"`
	StorageCapacityGiB int32  `json:"StorageCapacity,omitempty"`
}

// UpdateFileSystem updates a file system's configuration.
func (b *InMemoryBackend) UpdateFileSystem(input *updateFileSystemInput) (*FileSystem, error) {
	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[input.FileSystemID]
	if !ok {
		return nil, ErrFileSystemNotFound
	}

	if input.StorageCapacityGiB > 0 {
		fs.StorageCapacityGiB = input.StorageCapacityGiB
	}

	return fs.toFileSystem(), nil
}

// createBackupInput holds parameters for CreateBackup.
type createBackupInput struct {
	FileSystemID string `json:"FileSystemId"`
	Tags         []Tag  `json:"Tags,omitempty"`
}

// CreateBackup creates a backup of the specified file system.
func (b *InMemoryBackend) CreateBackup(input *createBackupInput) (*Backup, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateBackup")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[input.FileSystemID]
	if !ok {
		return nil, ErrFileSystemNotFound
	}

	id := "backup-" + uuid.New().String()[:17]
	arn := b.backupARN(id)
	now := time.Now().UTC()

	tags := tagsSliceToMap(input.Tags)

	bk := &storedBackup{
		BackupID:     id,
		BackupType:   backupTypeUserInitiated,
		CreationTime: now,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
		Tags:         tags,
		FileSystemID: input.FileSystemID,
	}

	b.backups[id] = bk
	b.tags[arn] = tags

	return bk.toBackup(fs), nil
}

// DescribeBackups returns backups, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeBackups(
	backupIDs []string,
	maxResults int32,
	nextToken string,
) ([]*Backup, string, error) {
	b.mu.RLock("DescribeBackups")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedBackup

	if len(backupIDs) > 0 {
		for _, id := range backupIDs {
			bk, ok := b.backups[id]
			if !ok {
				return nil, "", ErrBackupNotFound
			}

			all = append(all, bk)
		}
	} else {
		for _, bk := range b.backups {
			all = append(all, bk)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].BackupID < all[j].BackupID })
	}

	start := 0
	if nextToken != "" {
		for i, bk := range all {
			if bk.BackupID == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+int(maxResults), len(all))
	page := all[start:end]

	var next string
	if end < len(all) {
		next = all[end].BackupID
	}

	result := make([]*Backup, len(page))
	for i, bk := range page {
		var fs *storedFileSystem
		if bk.FileSystemID != "" {
			fs = b.fileSystems[bk.FileSystemID]
		}

		result[i] = bk.toBackup(fs)
	}

	return result, next, nil
}

// DeleteBackup removes a backup.
func (b *InMemoryBackend) DeleteBackup(backupID string) error {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	bk, ok := b.backups[backupID]
	if !ok {
		return ErrBackupNotFound
	}

	delete(b.backups, backupID)
	delete(b.tags, bk.ResourceARN)

	return nil
}

// createFileSystemFromBackupInput holds parameters for CreateFileSystemFromBackup.
type createFileSystemFromBackupInput struct {
	BackupID           string `json:"BackupId"`
	FileSystemType     string `json:"FileSystemType,omitempty"`
	StorageType        string `json:"StorageType,omitempty"`
	VpcID              string `json:"VpcId,omitempty"`
	Tags               []Tag  `json:"Tags,omitempty"`
	StorageCapacityGiB int32  `json:"StorageCapacity,omitempty"`
}

// CreateFileSystemFromBackup creates a new file system from an existing backup.
func (b *InMemoryBackend) CreateFileSystemFromBackup(input *createFileSystemFromBackupInput) (*FileSystem, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileSystemFromBackup")
	defer b.mu.Unlock()

	src, ok := b.backups[input.BackupID]
	if !ok {
		return nil, ErrBackupNotFound
	}

	srcFS := b.fileSystems[src.FileSystemID]

	fsType := input.FileSystemType
	if fsType == "" && srcFS != nil {
		fsType = srcFS.FileSystemType
	}

	id := "fs-" + uuid.New().String()[:17]
	arn := b.fsARN(id)
	now := time.Now().UTC()

	capacity := input.StorageCapacityGiB
	if capacity == 0 && srcFS != nil {
		capacity = srcFS.StorageCapacityGiB
	}

	storageType := input.StorageType
	if storageType == "" && srcFS != nil {
		storageType = srcFS.StorageType
	}

	tags := tagsSliceToMap(input.Tags)

	fs := &storedFileSystem{
		CreationTime:       now,
		Tags:               tags,
		FileSystemID:       id,
		FileSystemType:     fsType,
		Lifecycle:          lifecycleAvailable,
		ResourceARN:        arn,
		StorageCapacityGiB: capacity,
		StorageType:        storageType,
		VpcID:              input.VpcID,
		OwnerID:            b.accountID,
	}

	b.fileSystems[id] = fs
	b.tags[arn] = tags

	return fs.toFileSystem(), nil
}

// copyBackupInput holds parameters for CopyBackup.
type copyBackupInput struct {
	SourceBackupID string `json:"SourceBackupId"`
	Tags           []Tag  `json:"Tags,omitempty"`
}

// CopyBackup creates a copy of an existing backup.
func (b *InMemoryBackend) CopyBackup(input *copyBackupInput) (*Backup, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CopyBackup")
	defer b.mu.Unlock()

	src, ok := b.backups[input.SourceBackupID]
	if !ok {
		return nil, ErrBackupNotFound
	}

	id := "backup-" + uuid.New().String()[:17]
	arn := b.backupARN(id)
	now := time.Now().UTC()

	tags := tagsSliceToMap(input.Tags)

	bk := &storedBackup{
		BackupID:     id,
		BackupType:   src.BackupType,
		CreationTime: now,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
		Tags:         tags,
		FileSystemID: src.FileSystemID,
	}

	b.backups[id] = bk
	b.tags[arn] = tags

	var fs *storedFileSystem
	if src.FileSystemID != "" {
		fs = b.fileSystems[src.FileSystemID]
	}

	return bk.toBackup(fs), nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrFileSystemNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	existing := b.tags[resourceARN]
	newKeys := 0
	for _, t := range tags {
		if _, ok := existing[t.Key]; !ok {
			newKeys++
		}
	}

	if len(existing)+newKeys > maxTagsPerResource {
		return fmt.Errorf("%w: adding %d tag(s) would exceed the %d-tag limit",
			ErrTagLimitExceeded, newKeys, maxTagsPerResource)
	}

	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrFileSystemNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags on a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, ErrFileSystemNotFound
	}

	return tagsMapToSlice(b.tags[resourceARN]), nil
}

// arnExists checks whether a resource ARN belongs to any known FSx resource.
func (b *InMemoryBackend) arnExists(resourceARN string) bool { //nolint:gocognit,cyclop // existing issue.
	for _, fs := range b.fileSystems {
		if fs.ResourceARN == resourceARN {
			return true
		}
	}

	for _, bk := range b.backups {
		if bk.ResourceARN == resourceARN {
			return true
		}
	}

	for _, fc := range b.fileCaches {
		if fc.ResourceARN == resourceARN {
			return true
		}
	}

	for _, sn := range b.snapshots {
		if sn.ResourceARN == resourceARN {
			return true
		}
	}

	for _, svm := range b.storageVirtualMachines {
		if svm.ResourceARN == resourceARN {
			return true
		}
	}

	for _, v := range b.volumes {
		if v.ResourceARN == resourceARN {
			return true
		}
	}

	for _, a := range b.dataRepositoryAssocs {
		if a.ResourceARN == resourceARN {
			return true
		}
	}

	for _, ap := range b.s3AccessPoints {
		if ap.ResourceARN == resourceARN {
			return true
		}
	}

	return false
}

// validateTags returns ErrTagInvalid if any tag key or value violates FSx constraints:
// key must be 1–128 chars and must not start with "aws:"; value must be 0–256 chars.
func validateTags(tags []Tag) error {
	for _, t := range tags {
		klen := utf8.RuneCountInString(t.Key)
		if klen == 0 || klen > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1–%d chars, got %d", ErrTagInvalid, maxTagKeyLen, klen)
		}

		if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return fmt.Errorf("%w: tag key must not start with reserved prefix \"aws:\"", ErrTagInvalid)
		}

		if vlen := utf8.RuneCountInString(t.Value); vlen > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0–%d chars, got %d", ErrTagInvalid, maxTagValueLen, vlen)
		}
	}

	return nil
}

func (b *InMemoryBackend) fsARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:file-system/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) backupARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:backup/%s", b.region, b.accountID, id)
}

func tagsSliceToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func tagsMapToSlice(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	tags := make([]Tag, len(keys))
	for i, k := range keys {
		tags[i] = Tag{Key: k, Value: m[k]}
	}

	return tags
}
