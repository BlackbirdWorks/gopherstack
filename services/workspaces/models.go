package workspaces

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// storedWorkspace holds a workspace with all persisted fields.
type storedWorkspace struct {
	Properties                  *WorkspaceProperties `json:"properties,omitempty"`
	Tags                        map[string]string    `json:"tags"`
	WorkspaceID                 string               `json:"workspaceId"`
	DirectoryID                 string               `json:"directoryId"`
	UserName                    string               `json:"userName"`
	BundleID                    string               `json:"bundleId"`
	State                       string               `json:"state"`
	ComputerName                string               `json:"computerName"`
	SubnetID                    string               `json:"subnetId"`
	VolumeEncryptionKey         string               `json:"volumeEncryptionKey,omitempty"`
	ErrorCode                   string               `json:"errorCode"`
	ErrorMessage                string               `json:"errorMessage"`
	Region                      string               `json:"region"`
	UserVolumeEncryptionEnabled bool                 `json:"userVolumeEncryptionEnabled"`
	RootVolumeEncryptionEnabled bool                 `json:"rootVolumeEncryptionEnabled"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Phase 3.3: every map[string]*T resource field is a *store.Table[T]
// registered on b.registry (see store_setup.go); registry.SnapshotAll /
// RestoreAll / ResetAll now drive Reset/Snapshot/Restore below (see
// persistence.go) instead of per-map hand-written boilerplate. tags,
// directoryIpGroups, imagePermissions, clientProperties, and appAssociations
// are left as plain maps -- their values are not *T (a map, a scalar, or a
// set), so there is no identity for store.Table to key on; see the field
// comments below and persistence.go's doc comment for which of these were
// persisted before this refactor and remain so.
type InMemoryBackend struct {
	applications         *store.Table[storedApplication]
	images               *store.Table[storedImage]
	workspaces           *store.Table[storedWorkspace]
	ipGroups             *store.Table[storedIpGroup]
	connAliases          *store.Table[storedConnAlias]
	customBundles        *store.Table[storedCustomBundle]
	accountLinks         *store.Table[storedAccountLink]
	pools                *store.Table[storedPool]
	poolSessions         *store.Table[storedPoolSession]
	connectAddIns        *store.Table[storedConnectAddIn]
	registry             *store.Registry
	clientBranding       *store.Table[storedClientBranding]
	imagePermissions     map[string]map[string]bool
	dirSettings          *store.Table[storedDirSettings]
	tags                 map[string]map[string]string
	directoryIpGroups    map[string]map[string]struct{} //nolint:revive,staticcheck // existing issue.
	mu                   *lockmetrics.RWMutex
	clientProperties     map[string]storedClientProps
	appAssociations      map[string]map[string]*storedAppAssociation
	accountConfig        storedAccountConfig
	accountID            string
	region               string
	accountModifications []AccountModification
	counter              int
}

// ---------------------------------------------------------------------------
// IP Groups
// ---------------------------------------------------------------------------

type ipRuleItem struct {
	IpRule   string `json:"ipRule"` //nolint:revive,staticcheck // existing issue.
	RuleDesc string `json:"ruleDesc"`
}

type storedIpGroup struct { //nolint:revive,staticcheck // existing issue.
	Tags      map[string]string `json:"tags"`
	GroupID   string            `json:"groupId"`
	GroupName string            `json:"groupName"`
	GroupDesc string            `json:"groupDesc"`
	UserRules []ipRuleItem      `json:"userRules"`
}

// ---------------------------------------------------------------------------
// Connection Aliases
// ---------------------------------------------------------------------------

type connAliasPermission struct {
	AccountID        string `json:"accountId"`
	AllowAssociation bool   `json:"allowAssociation"`
}

type storedConnAlias struct {
	Tags                 map[string]string     `json:"tags"`
	AliasID              string                `json:"aliasId"`
	ConnectionString     string                `json:"connectionString"`
	State                string                `json:"state"`
	OwnerAccountID       string                `json:"ownerAccountId"`
	AssociatedResource   string                `json:"associatedResource"`
	ConnectionIdentifier string                `json:"connectionIdentifier"`
	SharedAccounts       []connAliasPermission `json:"sharedAccounts"`
}

// ---------------------------------------------------------------------------
// Bundles
// ---------------------------------------------------------------------------

type storedCustomBundle struct {
	Tags        map[string]string `json:"tags"`
	BundleID    string            `json:"bundleId"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	ImageID     string            `json:"imageId"`
	ComputeType string            `json:"computeType"`
}

// ---------------------------------------------------------------------------
// Images
// ---------------------------------------------------------------------------

type storedImage struct {
	Created       time.Time         `json:"created"`
	Tags          map[string]string `json:"tags"`
	ImageID       string            `json:"imageId"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	State         string            `json:"state"`
	SourceImageID string            `json:"sourceImageId"`
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

type storedPool struct {
	CreatedAt           time.Time         `json:"createdAt"`
	Tags                map[string]string `json:"tags"`
	PoolID              string            `json:"poolId"`
	PoolArn             string            `json:"poolArn"`
	PoolName            string            `json:"poolName"`
	BundleID            string            `json:"bundleId"`
	DirectoryID         string            `json:"directoryId"`
	Description         string            `json:"description"`
	State               string            `json:"state"`
	RunningMode         string            `json:"runningMode"`
	DesiredUserSessions int32             `json:"desiredUserSessions"`
}

type storedPoolSession struct {
	SessionID string `json:"sessionId"`
	PoolID    string `json:"poolId"`
	UserID    string `json:"userId"`
}

// ---------------------------------------------------------------------------
// Connect Client Add-Ins
// ---------------------------------------------------------------------------

type storedConnectAddIn struct {
	AddInID    string `json:"addInId"`
	Name       string `json:"name"`
	ResourceID string `json:"resourceId"`
	URL        string `json:"url"`
}

// ---------------------------------------------------------------------------
// Client Branding / Client Properties
// ---------------------------------------------------------------------------

type storedClientBranding struct {
	Platforms  map[string]map[string]any `json:"platforms"`
	ResourceID string                    `json:"resourceId"`
}

type storedClientProps struct {
	ReconnectEnabled string `json:"reconnectEnabled"`
}

// ---------------------------------------------------------------------------
// Account Links
// ---------------------------------------------------------------------------

type storedAccountLink struct {
	LinkID          string `json:"linkId"`
	Status          string `json:"status"`
	SourceAccountID string `json:"sourceAccountId"`
	TargetAccountID string `json:"targetAccountId"`
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

type storedAccountConfig struct {
	DedicatedTenancySupport string `json:"dedicatedTenancySupport"`
	ManagementCidrRange     string `json:"managementCidrRange"`
}

// ---------------------------------------------------------------------------
// Applications
// ---------------------------------------------------------------------------

type storedApplication struct {
	AppID string `json:"appId"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
	State string `json:"state"`
}

// storedAppAssociation tracks a workspace<->application association.
// appAssociations is ephemeral (not part of backendSnapshot), so this
// carries no json tags -- see persistence.go.
type storedAppAssociation struct {
	CreatedAt       time.Time
	LastUpdatedTime time.Time
	State           string
}

// ---------------------------------------------------------------------------
// Directories
// ---------------------------------------------------------------------------

type storedDirSettings struct {
	Properties  map[string]string `json:"properties"`
	DirectoryID string            `json:"directoryId"`
}
