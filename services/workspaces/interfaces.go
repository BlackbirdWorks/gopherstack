package workspaces

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// WorkspaceCreationSpec holds all fields for creating a workspace.
// WorkspaceName mirrors WorkspaceRequest.WorkspaceName
// (aws-sdk-go-v2/service/workspaces@v1.73.1/types/types.go:1874-1879):
// "not applicable if UserName is specified for user-assigned WorkSpaces",
// so an empty value here means the field genuinely stays unset, not a
// fallback to derive from UserName.
type WorkspaceCreationSpec struct {
	Properties                  *WorkspaceProperties
	Tags                        map[string]string
	UserName                    string
	WorkspaceName               string
	DirectoryID                 string
	BundleID                    string
	SubnetID                    string
	VolumeEncryptionKey         string
	UserVolumeEncryptionEnabled bool
	RootVolumeEncryptionEnabled bool
}

// StorageBackend is the interface for WorkSpaces storage operations.
type StorageBackend interface {
	CreateWorkspace(ctx context.Context, spec *WorkspaceCreationSpec) (*Workspace, error)
	DescribeWorkspaces(
		ctx context.Context,
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

	DescribeWorkspaceBundles(
		ctx context.Context,
		bundleIDs []string,
		owner string,
		nextToken string,
	) ([]*WorkspaceBundle, string, error)
	DescribeWorkspaceDirectories(
		ctx context.Context,
		directoryIDs []string,
		nextToken string,
	) ([]*WorkspaceDirectory, string, error)

	// IP Groups
	CreateIpGroup(
		groupName, groupDesc string,
		userRules []ipRuleItem,
		tags map[string]string,
	) (string, error)
	DescribeIpGroups(
		groupIDs []string,
		maxResults int32,
		nextToken string,
	) ([]*storedIpGroup, string, error)
	DeleteIPGroup(groupID string) error
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
	DescribeBundleAssociations(
		bundleID string, resourceTypes []string,
	) ([]BundleResourceAssociation, error)

	// Images
	CopyWorkspaceImage(
		name, sourceImageID, sourceRegion, description string,
		tags map[string]string,
	) (string, error)
	CreateWorkspaceImage(
		name, description, workspaceID string,
		tags map[string]string,
	) (*storedImage, error)
	DeleteWorkspaceImage(imageID string) error
	ImportWorkspaceImage(
		ec2ImageID, name, description, ingestionProcess string,
		tags map[string]string,
	) (string, error)
	ImportCustomWorkspaceImage(
		name, description string, spec customWorkspaceImageImportSpec,
	) (*storedImage, error)
	CreateUpdatedWorkspaceImage(
		sourceImageID, name, description string,
		tags map[string]string,
	) (string, error)
	DescribeWorkspaceImages(
		imageIDs []string,
		imageType string,
		maxResults int32,
		nextToken string,
	) ([]*storedImage, string, error)
	DescribeWorkspaceImagePermissions(
		imageID, nextToken string, maxResults int,
	) (string, page.Page[ImagePermission], error)
	UpdateWorkspaceImagePermission(imageID, sharedAccountID string, allowCopy bool) error
	DescribeCustomWorkspaceImageImport(imageID string) (*storedImage, error)
	DescribeImageAssociations(
		imageID string, resourceTypes []string,
	) ([]ImageResourceAssociation, error)

	// Pools
	CreateWorkspacesPool(
		poolName, bundleID, directoryID, description, runningMode string,
		desiredUserSessions int32,
		tags map[string]string,
	) (*storedPool, error)
	DescribeWorkspacesPools(
		poolIDs []string,
		limit int32,
		nextToken string,
	) ([]*storedPool, string, error)
	StartWorkspacesPool(poolID string) error
	StopWorkspacesPool(poolID string) error
	TerminateWorkspacesPool(poolID string) error
	UpdateWorkspacesPool(
		poolID, description, bundleID, directoryID, runningMode string,
		desiredUserSessions int32,
	) (*storedPool, error)
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
	DescribeAccountModifications(
		nextToken string,
	) ([]AccountModification, string, error)
	ListAvailableManagementCidrRanges(
		constraint string, maxResults int32, nextToken string,
	) ([]string, string, error)

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
	ImportClientBranding(resourceID string, platforms map[string]map[string]any) error
	DescribeClientBranding(resourceID string) (map[string]map[string]any, error)
	DeleteClientBranding(resourceID string, platforms []string) error

	// Client Properties
	DescribeClientProperties(resourceIDs []string) (map[string]storedClientProps, error)
	ModifyClientProperties(resourceID string, clientExperiencePolicy, logUploadEnabled, reconnectEnabled *string) error

	// Directory modify ops
	ModifyCertificateBasedAuthProperties(directoryID string, props map[string]string, propertiesToDelete []string) error
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
	ListAccountLinks(
		statusFilter string,
		maxResults int32,
		nextToken string,
	) ([]*storedAccountLink, string, error)

	// Applications
	AssociateWorkspaceApplication(
		workspaceID, applicationID string,
	) (WorkspaceResourceAssociation, error)
	DisassociateWorkspaceApplication(
		workspaceID, applicationID string,
	) (WorkspaceResourceAssociation, error)
	DeployWorkspaceApplications(
		workspaceID string, force bool,
	) ([]WorkspaceResourceAssociation, error)
	DescribeWorkspaceAssociations(
		workspaceID string,
		associatedResourceTypes []string,
	) ([]WorkspaceResourceAssociation, error)
	DescribeApplicationAssociations(
		applicationID string, associatedResourceTypes []string, maxResults int32, nextToken string,
	) ([]ApplicationResourceAssociation, string, error)
	DescribeApplications(
		appIDs []string,
		maxResults int32,
		nextToken string,
	) ([]*storedApplication, string, error)

	// Workspace-level ops
	MigrateWorkspace(sourceWorkspaceID, bundleID string) (sourceID, targetID string, err error)
	RestoreWorkspace(workspaceID string) error
	CreateStandbyWorkspace(
		ctx context.Context, spec StandbyWorkspaceSpec,
	) (*PendingStandbyWorkspace, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// DataReplicationSettings holds data replication settings.
type DataReplicationSettings struct {
	RecoverySnapshotTime *time.Time `json:"RecoverySnapshotTime,omitempty"`
	DataReplication      string     `json:"DataReplication,omitempty"`
}

// StandbyWorkspaceProperties holds standby workspace properties.
type StandbyWorkspaceProperties struct {
	RecoverySnapshotTime *time.Time `json:"RecoverySnapshotTime,omitempty"`
	StandbyWorkspaceID   string     `json:"StandbyWorkspaceId,omitempty"`
	DataReplication      string     `json:"DataReplication,omitempty"`
}

// RelatedWorkspace holds related workspace information.
type RelatedWorkspace struct {
	Region      string `json:"Region,omitempty"`
	State       string `json:"State,omitempty"`
	Type        string `json:"Type,omitempty"`
	WorkspaceID string `json:"WorkspaceId,omitempty"`
}

// ModificationState holds modification state of a workspace.
type ModificationState struct {
	Resource string `json:"Resource,omitempty"`
	State    string `json:"State,omitempty"`
}

// Workspace holds full WorkSpace details.
type Workspace struct {
	Properties                  *WorkspaceProperties
	Tags                        map[string]string
	DataReplicationSettings     *DataReplicationSettings
	VolumeEncryptionKey         string
	State                       string
	ErrorMessage                string
	WorkspaceID                 string
	WorkspaceName               string
	DirectoryID                 string
	UserName                    string
	IPAddress                   string
	BundleID                    string
	ErrorCode                   string
	ComputerName                string
	SubnetID                    string
	StandbyWorkspacesProperties []StandbyWorkspaceProperties
	RelatedWorkspaces           []RelatedWorkspace
	ModificationStates          []ModificationState
	UserVolumeEncryptionEnabled bool
	RootVolumeEncryptionEnabled bool
}

// WorkspaceConnectionStatus holds connection status for a WorkSpace, matching
// the real WorkspaceConnectionStatus SDK type's four fields (ConnectionState,
// ConnectionStateCheckTimestamp, LastKnownUserConnectionTimestamp,
// WorkspaceId) -- LastKnownUserConnectionTimestamp stays the zero time since
// this backend models no actual client connection activity.
type WorkspaceConnectionStatus struct {
	ConnectionStateCheckTimestamp    time.Time
	LastKnownUserConnectionTimestamp time.Time
	WorkspaceID                      string
	ConnectionState                  string
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

// BundleComputeType holds the compute type name for a bundle.
type BundleComputeType struct {
	Name string
}

// BundleStorage holds storage capacity for a bundle.
type BundleStorage struct {
	Capacity int32
}

// WorkspaceBundle holds WorkSpace bundle details.
type WorkspaceBundle struct {
	ComputeType BundleComputeType
	BundleID    string
	Name        string
	Owner       string
	Description string
	ImageID     string
	UserStorage BundleStorage
	RootStorage BundleStorage
}

// StandbyWorkspaceSpec holds the fields for creating a single standby
// WorkSpace, matching the real StandbyWorkspace request shape (DirectoryId,
// PrimaryWorkspaceId, DataReplication, Tags, VolumeEncryptionKey -- no
// UserName/BundleId, unlike WorkspaceCreationSpec).
type StandbyWorkspaceSpec struct {
	Tags                map[string]string
	DirectoryID         string
	PrimaryWorkspaceID  string
	DataReplication     string
	VolumeEncryptionKey string
}

// PendingStandbyWorkspace holds the identity of a newly created standby
// WorkSpace, matching the real PendingCreateStandbyWorkspacesRequest shape.
type PendingStandbyWorkspace struct {
	WorkspaceID string
	DirectoryID string
	State       string
}

// WorkspaceResourceAssociation describes a workspace<->application
// association, matching the real WorkspaceResourceAssociation SDK type
// (field-diffed against deserializers.go's
// awsAwsjson11_deserializeDocumentWorkspaceResourceAssociation: the wire key
// is "State" with AssociationState enum values, not "AssociationStatus" /
// "INSTALLED", which don't exist on the real type). This backend applies
// Associate/Deploy synchronously, so State is always the terminal
// COMPLETED/REMOVED value -- there is no pending/installing window to model.
type WorkspaceResourceAssociation struct {
	Created                time.Time
	LastUpdatedTime        time.Time
	AssociatedResourceID   string
	AssociatedResourceType string
	State                  string
	WorkspaceID            string
}

// ApplicationResourceAssociation describes an association between an
// application and the resource it is associated with (a WorkSpace), matching
// the real ApplicationResourceAssociation SDK type. Unlike
// WorkspaceResourceAssociation, the identifying field here is ApplicationId,
// not WorkspaceId -- the wire shape describes the operation "from the
// application's side".
type ApplicationResourceAssociation struct {
	Created                time.Time
	LastUpdatedTime        time.Time
	AssociatedResourceID   string
	AssociatedResourceType string
	ApplicationID          string
	State                  string
}

// ImageResourceAssociation describes an application association for an image,
// matching the real ImageResourceAssociation SDK type. A freshly emulated
// account always returns an empty list from DescribeImageAssociations since
// there is no public API to create this kind of association (see
// DescribeImageAssociations doc comment in images.go) -- this type exists so
// the wire shape is correct if that ever changes.
type ImageResourceAssociation struct {
	Created                 time.Time
	LastUpdatedTime         time.Time
	AssociatedResourceID    string
	AssociatedResourceType  string
	ImageID                 string
	State                   string
	StateReasonErrorCode    string
	StateReasonErrorMessage string
}

// BundleResourceAssociation describes an application association for a
// bundle, matching the real BundleResourceAssociation SDK type. See
// ImageResourceAssociation -- same "always empty in this emulator" rationale.
type BundleResourceAssociation struct {
	Created                 time.Time
	LastUpdatedTime         time.Time
	AssociatedResourceID    string
	AssociatedResourceType  string
	BundleID                string
	State                   string
	StateReasonErrorCode    string
	StateReasonErrorMessage string
}

// AccountModification records one completed change to the account's BYOL
// (Bring Your Own License) configuration, matching the real AccountModification
// SDK type (ErrorCode/ErrorMessage are omitted here: ModifyAccount in this
// backend always succeeds once it validates, so no failure path populates
// them).
type AccountModification struct {
	StartTime                           time.Time
	ModificationState                   string
	DedicatedTenancySupport             string
	DedicatedTenancyManagementCidrRange string
}

// WorkspaceDirectory holds WorkSpace directory details.
//
// IPGroupIDs / EndpointEncryptionMode / CertificateBasedAuthProperties /
// SamlProperties / SelfservicePermissions / WorkspaceAccessProperties /
// WorkspaceCreationProperties are all real members of the wire type
// (aws-sdk-go-v2/service/workspaces@v1.73.1/types.WorkspaceDirectory) that
// real AWS's DescribeWorkspaceDirectories echoes back -- there is no
// separate Describe op for any of these settings, only Modify* ops, so
// DescribeWorkspaceDirectories is the only place a real client ever reads
// them. Pointer sub-structs are nil (omitted) when the directory was never
// touched by the corresponding Modify op, matching this backend's honest
// no-default-simulated posture elsewhere.
type WorkspaceDirectory struct {
	CertificateBasedAuthProperties *CertificateBasedAuthProperties
	SamlProperties                 *SamlProperties
	SelfservicePermissions         *SelfservicePermissions
	WorkspaceAccessProperties      *WorkspaceAccessProperties
	WorkspaceCreationProperties    *WorkspaceCreationProperties
	DirectoryID                    string
	DirectoryName                  string
	DirectoryType                  string
	Alias                          string
	State                          string
	EndpointEncryptionMode         string
	SubnetIDs                      []string
	IPGroupIDs                     []string
}

// CertificateBasedAuthProperties mirrors types.CertificateBasedAuthProperties.
type CertificateBasedAuthProperties struct {
	Status                  string
	CertificateAuthorityArn string
}

// SamlProperties mirrors types.SamlProperties.
type SamlProperties struct {
	Status                  string
	UserAccessUrl           string //nolint:revive,staticcheck // matches real SDK field name
	RelayStateParameterName string
}

// SelfservicePermissions mirrors types.SelfservicePermissions.
type SelfservicePermissions struct {
	RestartWorkspace   string
	IncreaseVolumeSize string
	ChangeComputeType  string
	SwitchRunningMode  string
	RebuildWorkspace   string
}

// WorkspaceAccessProperties mirrors types.WorkspaceAccessProperties (device
// type members only -- AccessEndpointConfig is not modeled by
// ModifyWorkspaceAccessProperties's handler and stays omitted).
type WorkspaceAccessProperties struct {
	DeviceTypeWindows    string
	DeviceTypeOsx        string
	DeviceTypeWeb        string
	DeviceTypeIos        string
	DeviceTypeAndroid    string
	DeviceTypeChromeOs   string
	DeviceTypeZeroClient string
	DeviceTypeLinux      string
}

// WorkspaceCreationProperties mirrors the two fields
// ModifyWorkspaceCreationProperties' handler actually threads through
// (types.DefaultWorkspaceCreationProperties has more real members --
// EnableInternetAccess, EnableMaintenanceMode, EnableWorkDocs,
// UserEnabledAsLocalAdministrator -- that this backend never accepted as
// input either, so they stay genuinely omitted rather than fabricated).
type WorkspaceCreationProperties struct {
	DefaultOu             string
	CustomSecurityGroupId string //nolint:revive,staticcheck // matches real SDK field name
}

var _ StorageBackend = (*InMemoryBackend)(nil)
