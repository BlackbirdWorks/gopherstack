package directoryservice

import "time"

// StorageBackend is the interface for DirectoryService storage operations.
type StorageBackend interface {
	CreateDirectory(name, shortName, description, password string, size DirectorySize, tags []Tag) (*Directory, error)
	CreateMicrosoftAD(
		name, shortName, description, password string,
		edition DirectoryEdition,
		tags []Tag,
	) (*Directory, error)
	DeleteDirectory(directoryID string) error
	DescribeDirectories(directoryIDs []string, limit int32, nextToken string) ([]*Directory, string, error)
	CreateAlias(directoryID, alias string) error
	EnableSso(directoryID string) error
	DisableSso(directoryID string) error
	GetDirectoryLimits() *DirectoryLimits

	CreateSnapshot(directoryID, name string) (*Snapshot, error)
	DeleteSnapshot(snapshotID string) error
	DescribeSnapshots(
		directoryID string,
		snapshotIDs []string,
		limit int32,
		nextToken string,
	) ([]*Snapshot, string, error)
	GetSnapshotLimits(directoryID string) (*SnapshotLimits, error)
	RestoreFromSnapshot(snapshotID string) error

	AddTagsToResource(resourceID string, tags []Tag) error
	RemoveTagsFromResource(resourceID string, tagKeys []string) error
	ListTagsForResource(resourceID string, limit int32, nextToken string) ([]Tag, string, error)

	AccountID() string
	Region() string
	Reset()
	BackendSnapshot() []byte
	Restore(data []byte) error
}

// DirectorySize matches the AWS DirectorySize enum.
type DirectorySize string

const (
	DirectorySizeSmall DirectorySize = "Small"
	DirectorySizeLarge DirectorySize = "Large"
)

// DirectoryEdition matches the AWS DirectoryEdition enum.
type DirectoryEdition string

const (
	DirectoryEditionEnterprise DirectoryEdition = "Enterprise"
	DirectoryEditionStandard   DirectoryEdition = "Standard"
)

// DirectoryType matches the AWS DirectoryType enum.
type DirectoryType string

const (
	DirectoryTypeSimpleAD    DirectoryType = "SimpleAD"
	DirectoryTypeMicrosoftAD DirectoryType = "MicrosoftAD"
)

// DirectoryStage matches the AWS DirectoryStage enum.
type DirectoryStage string

const (
	DirectoryStageActive  DirectoryStage = "Active"
	DirectoryStageDeleted DirectoryStage = "Deleted"
)

// SnapshotStatus matches the AWS SnapshotStatus enum.
type SnapshotStatus string

const (
	SnapshotStatusCompleted SnapshotStatus = "Completed"
)

// SnapshotType matches the AWS SnapshotType enum.
type SnapshotType string

const (
	SnapshotTypeManual SnapshotType = "Manual"
)

// Directory represents an AWS Directory Service directory.
// LaunchTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Directory struct {
	LaunchTime  time.Time
	DirectoryID string
	Name        string
	ShortName   string
	Description string
	Alias       string
	AccessURL   string
	Type        DirectoryType
	Stage       DirectoryStage
	Size        DirectorySize
	Edition     DirectoryEdition
	SsoEnabled  bool
}

// Snapshot represents an AWS Directory Service snapshot.
// StartTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Snapshot struct {
	StartTime   time.Time
	SnapshotID  string
	DirectoryID string
	Name        string
	Status      SnapshotStatus
	Type        SnapshotType
}

// Tag is a key-value pair.
type Tag struct {
	Key   string
	Value string
}

// DirectoryLimits contains directory limit information.
type DirectoryLimits struct {
	CloudOnlyDirectoriesCurrentCount int32
	CloudOnlyDirectoriesLimit        int32
	CloudOnlyMicrosoftADCurrentCount int32
	CloudOnlyMicrosoftADLimit        int32
	ConnectedDirectoriesCurrentCount int32
	ConnectedDirectoriesLimit        int32
	CloudOnlyDirectoriesLimitReached bool
	CloudOnlyMicrosoftADLimitReached bool
	ConnectedDirectoriesLimitReached bool
}

// SnapshotLimits contains snapshot limit information.
type SnapshotLimits struct {
	ManualSnapshotsCurrentCount int32
	ManualSnapshotsLimit        int32
	ManualSnapshotsLimitReached bool
}

var _ StorageBackend = (*InMemoryBackend)(nil)
