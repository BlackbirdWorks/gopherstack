package workspaces

import "time"

// StorageBackend is the interface for WorkSpaces storage operations.
type StorageBackend interface {
	CreateWorkspace(userID, directoryID, bundleID string, tags map[string]string) (*Workspace, error)
	DescribeWorkspaces(
		workspaceIDs, directoryID, userID, bundleID []string,
		limit int32, nextToken string,
	) ([]*Workspace, string, error)
	GetWorkspacesConnectionStatus(workspaceIDs []string) ([]*WorkspaceConnectionStatus, error)
	ModifyWorkspaceProperties(workspaceID string, props WorkspaceProperties) error
	ModifyWorkspaceState(workspaceID, state string) error
	RebootWorkspaces(workspaceIDs []string) ([]FailedRequest, error)
	RebuildWorkspaces(workspaceIDs []string) ([]FailedRequest, error)
	StartWorkspaces(workspaceIDs []string) ([]FailedRequest, error)
	StopWorkspaces(workspaceIDs []string) ([]FailedRequest, error)
	TerminateWorkspaces(workspaceIDs []string) ([]FailedRequest, error)

	CreateTags(resourceID string, tags map[string]string) error
	DeleteTags(resourceID string, tagKeys []string) error
	DescribeTags(resourceID string) (map[string]string, error)

	DescribeWorkspaceBundles(bundleIDs []string, owner string, nextToken string) ([]*WorkspaceBundle, string, error)
	DescribeWorkspaceDirectories(directoryIDs []string, nextToken string) ([]*WorkspaceDirectory, string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Workspace holds full WorkSpace details.
type Workspace struct {
	Properties   *WorkspaceProperties
	Tags         map[string]string
	WorkspaceID  string
	DirectoryID  string
	UserName     string
	BundleID     string
	State        string
	ComputerName string
	SubnetID     string
	ErrorCode    string
	ErrorMessage string
}

// WorkspaceConnectionStatus holds connection status for a WorkSpace.
type WorkspaceConnectionStatus struct {
	LastKnownUserTime time.Time
	WorkspaceID       string
	ConnectionState   string
}

// WorkspaceProperties holds mutable WorkSpace properties.
type WorkspaceProperties struct {
	ComputeTypeName                     string
	RunningMode                         string
	RootVolumeSizeGib                   int32
	RunningModeAutoStopTimeoutInMinutes int32
	UserVolumeSizeGib                   int32
}

// FailedRequest holds error information for a failed workspace bulk operation.
type FailedRequest struct {
	WorkspaceID  string
	ErrorCode    string
	ErrorMessage string
}

// WorkspaceBundle holds WorkSpace bundle details.
type WorkspaceBundle struct {
	BundleID    string
	Name        string
	Owner       string
	Description string
}

// WorkspaceDirectory holds WorkSpace directory details.
type WorkspaceDirectory struct {
	DirectoryID   string
	DirectoryName string
	DirectoryType string
	Alias         string
	State         string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
