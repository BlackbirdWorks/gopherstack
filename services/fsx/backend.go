package fsx

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errFileSystemNotFound = "FileSystemNotFound"
	errBackupNotFound     = "BackupNotFound"
	errValidation         = "ValidationError"

	lifecycleAvailable      = "AVAILABLE"
	backupTypeUserInitiated = "USER_INITIATED"

	maxResultsDefault = 2147483647
)

var (
	// ErrFileSystemNotFound is returned when a file system does not exist.
	ErrFileSystemNotFound = awserr.New(errFileSystemNotFound, awserr.ErrNotFound)
	// ErrBackupNotFound is returned when a backup does not exist.
	ErrBackupNotFound = awserr.New(errBackupNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)

// storedFileSystem is the persisted form of a FileSystem.
// time.Time is first: non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedFileSystem struct {
	CreationTime       time.Time         `json:"creationTime"`
	Tags               map[string]string `json:"tags"`
	FileSystemID       string            `json:"fileSystemId"`
	FileSystemType     string            `json:"fileSystemType"`
	Lifecycle          string            `json:"lifecycle"`
	ResourceARN        string            `json:"resourceArn"`
	StorageType        string            `json:"storageType,omitempty"`
	VpcID              string            `json:"vpcId,omitempty"`
	OwnerID            string            `json:"ownerId,omitempty"`
	StorageCapacityGiB int32             `json:"storageCapacity,omitempty"`
}

func (s *storedFileSystem) toFileSystem() *FileSystem {
	return &FileSystem{
		CreationTime:       s.CreationTime,
		Tags:               tagsMapToSlice(s.Tags),
		FileSystemID:       s.FileSystemID,
		FileSystemType:     s.FileSystemType,
		Lifecycle:          s.Lifecycle,
		ResourceARN:        s.ResourceARN,
		StorageCapacityGiB: s.StorageCapacityGiB,
		StorageType:        s.StorageType,
		VpcID:              s.VpcID,
		OwnersID:           s.OwnerID,
	}
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
		CreationTime: b.CreationTime,
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
	FileSystems map[string]*storedFileSystem `json:"fileSystems"`
	Backups     map[string]*storedBackup     `json:"backups"`
	Tags        map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	fileSystems map[string]*storedFileSystem // fileSystemID → fs
	backups     map[string]*storedBackup     // backupID → backup
	tags        map[string]map[string]string // resourceARN → tags
	accountID   string
	region      string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:          lockmetrics.New("fsx"),
		fileSystems: make(map[string]*storedFileSystem),
		backups:     make(map[string]*storedBackup),
		tags:        make(map[string]map[string]string),
		accountID:   accountID,
		region:      region,
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
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		FileSystems: b.fileSystems,
		Backups:     b.backups,
		Tags:        b.tags,
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

	return nil
}

// createFileSystemInput holds parameters for CreateFileSystem.
type createFileSystemInput struct {
	FileSystemType     string `json:"FileSystemType"`
	StorageType        string `json:"StorageType,omitempty"`
	VpcID              string `json:"VpcId,omitempty"`
	Tags               []Tag  `json:"Tags,omitempty"`
	StorageCapacityGiB int32  `json:"StorageCapacity,omitempty"`
}

// CreateFileSystem creates a new file system.
func (b *InMemoryBackend) CreateFileSystem(input *createFileSystemInput) (*FileSystem, error) {
	if input.FileSystemType == "" {
		return nil, ErrValidation
	}

	id := "fs-" + uuid.New().String()[:17]
	arn := b.fsARN(id)
	now := time.Now().UTC()

	tags := tagsSliceToMap(input.Tags)

	fs := &storedFileSystem{
		CreationTime:       now,
		Tags:               tags,
		FileSystemID:       id,
		FileSystemType:     input.FileSystemType,
		Lifecycle:          lifecycleAvailable,
		ResourceARN:        arn,
		StorageCapacityGiB: input.StorageCapacityGiB,
		StorageType:        input.StorageType,
		VpcID:              input.VpcID,
		OwnerID:            b.accountID,
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	b.fileSystems[id] = fs
	b.tags[arn] = tags

	return fs.toFileSystem(), nil
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

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrFileSystemNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	for _, t := range tags {
		b.tags[resourceARN][t.Key] = t.Value
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

// arnExists checks whether a resource ARN belongs to a known file system or backup.
func (b *InMemoryBackend) arnExists(resourceARN string) bool {
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

	return false
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
