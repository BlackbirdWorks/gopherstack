package fsx

import "time"

// StorageBackend is the interface for FSx storage operations.
type StorageBackend interface {
	CreateFileSystem(input *createFileSystemInput) (*FileSystem, error)
	DescribeFileSystems(ids []string, maxResults int32, nextToken string) ([]*FileSystem, string, error)
	DeleteFileSystem(fileSystemID string) error
	UpdateFileSystem(input *updateFileSystemInput) (*FileSystem, error)

	CreateBackup(input *createBackupInput) (*Backup, error)
	DescribeBackups(backupIDs []string, maxResults int32, nextToken string) ([]*Backup, string, error)
	DeleteBackup(backupID string) error

	CreateFileSystemFromBackup(input *createFileSystemFromBackupInput) (*FileSystem, error)

	TagResource(resourceARN string, tags []Tag) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) ([]Tag, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// FileSystem represents an Amazon FSx file system.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
type FileSystem struct {
	CreationTime       time.Time `json:"CreationTime"`
	FileSystemID       string    `json:"FileSystemId"`
	FileSystemType     string    `json:"FileSystemType"`
	Lifecycle          string    `json:"Lifecycle"`
	ResourceARN        string    `json:"ResourceARN"`
	StorageType        string    `json:"StorageType,omitempty"`
	VpcID              string    `json:"VpcId,omitempty"`
	OwnersID           string    `json:"OwnerId,omitempty"`
	Tags               []Tag     `json:"Tags,omitempty"`
	StorageCapacityGiB int32     `json:"StorageCapacity,omitempty"`
}

// Backup represents an Amazon FSx backup.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
type Backup struct {
	CreationTime time.Time   `json:"CreationTime"`
	FileSystem   *FileSystem `json:"FileSystem,omitempty"`
	BackupID     string      `json:"BackupId"`
	BackupType   string      `json:"Type"`
	Lifecycle    string      `json:"Lifecycle"`
	ResourceARN  string      `json:"ResourceARN"`
	Tags         []Tag       `json:"Tags,omitempty"`
}

// Tag is a key-value pair attached to an FSx resource.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

var _ StorageBackend = (*InMemoryBackend)(nil)
