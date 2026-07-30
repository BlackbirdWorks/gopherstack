package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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

	fs, ok := b.fileSystems.Get(input.FileSystemID)
	if !ok {
		return nil, ErrFileSystemNotFound
	}

	id := newFSxBackupID()
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

	b.backups.Put(bk)
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
			bk, ok := b.backups.Get(id)
			if !ok {
				return nil, "", ErrBackupNotFound
			}

			all = append(all, bk)
		}
	} else {
		all = b.backups.All()

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
			fs, _ = b.fileSystems.Get(bk.FileSystemID)
		}

		result[i] = bk.toBackup(fs)
	}

	return result, next, nil
}

// DeleteBackup removes a backup.
func (b *InMemoryBackend) DeleteBackup(backupID string) error {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	bk, ok := b.backups.Get(backupID)
	if !ok {
		return ErrBackupNotFound
	}

	b.backups.Delete(backupID)
	delete(b.tags, bk.ResourceARN)

	return nil
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

	src, ok := b.backups.Get(input.SourceBackupID)
	if !ok {
		return nil, ErrBackupNotFound
	}

	id := newFSxBackupID()
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

	b.backups.Put(bk)
	b.tags[arn] = tags

	var fs *storedFileSystem
	if src.FileSystemID != "" {
		fs, _ = b.fileSystems.Get(src.FileSystemID)
	}

	return bk.toBackup(fs), nil
}

func (b *InMemoryBackend) backupARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("backup/%s", id))
}
