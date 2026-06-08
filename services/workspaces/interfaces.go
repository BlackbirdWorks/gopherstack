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

	// IP Groups
	CreateIpGroup(groupName, groupDesc string, userRules []ipRuleItem, tags map[string]string) (string, error)
	DescribeIpGroups(groupIDs []string, maxResults int32, nextToken string) ([]*storedIpGroup, string, error)
	DeleteIpGroup(groupID string) error
	AuthorizeIpRules(groupID string, rules []ipRuleItem) error
	RevokeIpRules(groupID string, ipRules []string) error
	UpdateRulesOfIpGroup(groupID string, rules []ipRuleItem) error
	AssociateIpGroups(directoryID string, groupIDs []string) error
	DisassociateIpGroups(directoryID string, groupIDs []string) error

	// Connection Aliases
	CreateConnectionAlias(connectionString string, tags map[string]string) (string, error)
	DescribeConnectionAliases(
		aliasIDs []string, resourceID string, limit int32, nextToken string,
	) ([]*storedConnAlias, string, error)
	DeleteConnectionAlias(aliasID string) error
	AssociateConnectionAlias(aliasID, resourceID string) (string, error)
	DisassociateConnectionAlias(aliasID string) error
	DescribeConnectionAliasPermissions(
		aliasID string, maxResults int32, nextToken string,
	) (string, []connAliasPermission, string, error)
	UpdateConnectionAliasPermission(aliasID, accountID string, allowAssociation bool) error

	// Custom Bundles
	CreateWorkspaceBundle(
		name, description, imageID, computeType string,
		tags map[string]string,
	) (*storedCustomBundle, error)
	DeleteWorkspaceBundle(bundleID string) error
	UpdateWorkspaceBundle(bundleID, imageID string) error

	// Images
	CopyWorkspaceImage(name, sourceImageID, sourceRegion, description string, tags map[string]string) (string, error)
	CreateWorkspaceImage(name, description, workspaceID string, tags map[string]string) (*storedImage, error)
	DeleteWorkspaceImage(imageID string) error
	ImportWorkspaceImage(ec2ImageID, name, description string, tags map[string]string) (string, error)
	ImportCustomWorkspaceImage(name, description string) (*storedImage, error)
	CreateUpdatedWorkspaceImage(sourceImageID, name, description string, tags map[string]string) (string, error)
	DescribeWorkspaceImages(
		imageIDs []string,
		imageType string,
		maxResults int32,
		nextToken string,
	) ([]*storedImage, string, error)
	DescribeWorkspaceImagePermissions(imageID string) (string, map[string]bool, error)
	UpdateWorkspaceImagePermission(imageID, sharedAccountID string, allowCopy bool) error
	DescribeCustomWorkspaceImageImport(imageID string) (*storedImage, error)

	// Pools
	CreateWorkspacesPool(
		poolName, bundleID, directoryID, description string,
		tags map[string]string,
	) (*storedPool, error)
	DescribeWorkspacesPools(poolIDs []string, limit int32, nextToken string) ([]*storedPool, string, error)
	StartWorkspacesPool(poolID string) error
	StopWorkspacesPool(poolID string) error
	TerminateWorkspacesPool(poolID string) error
	UpdateWorkspacesPool(poolID, description, bundleID, directoryID string) (*storedPool, error)
	DescribeWorkspacesPoolSessions(
		poolID, userID string,
		limit int32,
		nextToken string,
	) ([]*storedPoolSession, string, error)
	TerminateWorkspacesPoolSession(sessionID string) error

	// Directories
	RegisterWorkspaceDirectory(directoryID string, subnetIDs []string) error
	DeregisterWorkspaceDirectory(directoryID string) error

	// Account
	DescribeAccount() storedAccountConfig
	ModifyAccount(dedicatedTenancyCidr, dedicatedTenancySupport string) error
	ModifyEndpointEncryptionMode(directoryID, mode string) error

	// Connect Client Add-Ins
	CreateConnectClientAddIn(name, resourceID, url string) (string, error)
	DeleteConnectClientAddIn(addInID, resourceID string) error
	DescribeConnectClientAddIns(
		resourceID string,
		maxResults int32,
		nextToken string,
	) ([]*storedConnectAddIn, string, error)
	UpdateConnectClientAddIn(addInID, resourceID, name, url string) error

	// Client Branding
	ImportClientBranding(resourceID string, platforms map[string]map[string]interface{}) error
	DescribeClientBranding(resourceID string) (map[string]map[string]interface{}, error)
	DeleteClientBranding(resourceID string, platforms []string) error

	// Client Properties
	DescribeClientProperties(resourceIDs []string) (map[string]storedClientProps, error)
	ModifyClientProperties(resourceID, reconnectEnabled string) error

	// Directory modify ops
	ModifyCertificateBasedAuthProperties(directoryID string, props map[string]string) error
	ModifySamlProperties(directoryID string, props map[string]string) error
	ModifySelfservicePermissions(directoryID string, props map[string]string) error
	ModifyStreamingProperties(directoryID string, props map[string]string) error
	ModifyWorkspaceAccessProperties(directoryID string, props map[string]string) error
	ModifyWorkspaceCreationProperties(directoryID string, props map[string]string) error

	// Account Links
	CreateAccountLinkInvitation(targetAccountID string) (*storedAccountLink, error)
	AcceptAccountLinkInvitation(linkID string) (*storedAccountLink, error)
	RejectAccountLinkInvitation(linkID string) (*storedAccountLink, error)
	DeleteAccountLinkInvitation(linkID string) (*storedAccountLink, error)
	GetAccountLink(linkID string) (*storedAccountLink, error)
	ListAccountLinks(statusFilter string, maxResults int32, nextToken string) ([]*storedAccountLink, string, error)

	// Applications
	AssociateWorkspaceApplication(workspaceID, applicationID string) error
	DisassociateWorkspaceApplication(workspaceID, applicationID string) error
	DeployWorkspaceApplications(workspaceID string, force bool) ([]map[string]string, error)
	DescribeWorkspaceAssociations(workspaceID string, associatedResourceTypes []string) ([]map[string]string, error)
	DescribeApplicationAssociations(
		applicationID string, associatedResourceTypes []string, maxResults int32, nextToken string,
	) ([]map[string]string, string, error)
	DescribeApplications(appIDs []string, maxResults int32, nextToken string) ([]*storedApplication, string, error)

	// Workspace-level ops
	MigrateWorkspace(sourceWorkspaceID, bundleID string) (sourceID, targetID string, err error)
	RestoreWorkspace(workspaceID string) error
	CreateStandbyWorkspaces(
		primaryRegion string,
		standby []map[string]string,
	) ([]map[string]string, []map[string]string, error)

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
	ComputeTypeName                     string `json:"ComputeTypeName,omitempty"`
	RunningMode                         string `json:"RunningMode,omitempty"`
	RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib,omitempty"`
	RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes,omitempty"`
	UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib,omitempty"`
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
