package codestarconnections

import "time"

// Connection status values.
const (
	ConnectionStatusAvailable = "AVAILABLE"
	ConnectionStatusPending   = "PENDING"
	ConnectionStatusError     = "ERROR"
)

// Host status values.
const (
	HostStatusAvailable           = "AVAILABLE"
	HostStatusPending             = "PENDING"
	HostStatusVPCConfigDeleting   = "VPC_CONFIG_DELETING"
	HostStatusVPCConfigFailed     = "VPC_CONFIG_FAILED"
	HostStatusVPCConfigInProgress = "VPC_CONFIG_IN_PROGRESS"
)

// VpcConfiguration holds the VPC connectivity settings for a host.
type VpcConfiguration struct {
	VpcID            string   `json:"VpcId"`
	TLSCertificate   string   `json:"TlsCertificate,omitempty"`
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

// Sync status values.
const (
	SyncStatusSucceeded  = "SUCCEEDED"
	SyncStatusFailed     = "FAILED"
	SyncStatusInProgress = "IN_PROGRESS"
	SyncStatusQueued     = "QUEUED"
)

// SyncBlocker status values.
const (
	SyncBlockerStatusActive   = "ACTIVE"
	SyncBlockerStatusResolved = "RESOLVED"
)

// SyncBlocker type values.
const (
	SyncBlockerTypeAutomated = "AUTOMATED"
	SyncBlockerTypeManual    = "MANUAL"
)

// Connection represents an in-memory AWS CodeStar connection.
//
// ConnectionArn already embeds its own region (arn:partition:service:region:
// account:resource, see regionFromARN), so Connection needs no hidden region
// field: store_setup.go's connections table is keyed directly by
// ConnectionArn and its byRegion/byName indexes derive region from the ARN.
type Connection struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ConnectionName   string            `json:"connectionName"`
	ConnectionArn    string            `json:"connectionArn"`
	ConnectionStatus string            `json:"connectionStatus"`
	OwnerAccountID   string            `json:"ownerAccountId"`
	ProviderType     string            `json:"providerType"`
	HostArn          string            `json:"hostArn,omitempty"`
}

// Host represents an in-memory AWS CodeStar host.
//
// Like Connection, HostArn already embeds its own region, so Host needs no
// hidden region field either.
type Host struct {
	Tags             map[string]string `json:"tags,omitempty"`
	VpcConfiguration *VpcConfiguration `json:"vpcConfiguration,omitempty"`
	Name             string            `json:"name"`
	HostArn          string            `json:"hostArn"`
	ProviderType     string            `json:"providerType"`
	ProviderEndpoint string            `json:"providerEndpoint"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
}

// RepositoryLink represents an in-memory AWS CodeStar Connections repository link.
type RepositoryLink struct {
	CreatedAt         time.Time `json:"createdAt"`
	ConnectionArn     string    `json:"connectionArn"`
	OwnerID           string    `json:"ownerID"`
	RepositoryName    string    `json:"repositoryName"`
	RepositoryLinkID  string    `json:"repositoryLinkID"`
	RepositoryLinkArn string    `json:"repositoryLinkArn"`
	ProviderType      string    `json:"providerType"`
	EncryptionKeyArn  string    `json:"encryptionKeyArn,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey and
	// store_setup.go); it is unexported so a plain json.Marshal(RepositoryLink)
	// never sees it and is instead carried through persistence via a
	// regionalDTO (see persistence.go). Unlike Connection/Host,
	// GetRepositoryLink/DeleteRepositoryLink/UpdateRepositoryLink resolve
	// their region from the caller's context rather than from any ARN (a
	// bare RepositoryLinkID carries no region of its own), so region must be
	// captured explicitly at creation time and used for every subsequent
	// lookup by ID.
	region string
}

// SyncConfiguration represents an in-memory AWS CodeStar Connections sync configuration.
type SyncConfiguration struct {
	CreatedAt               time.Time `json:"createdAt"`
	Branch                  string    `json:"branch"`
	ConfigFile              string    `json:"configFile"`
	RepositoryLinkID        string    `json:"repositoryLinkID"`
	ResourceName            string    `json:"resourceName"`
	RoleArn                 string    `json:"roleArn"`
	SyncType                string    `json:"syncType"`
	OwnerID                 string    `json:"ownerID"`
	ProviderType            string    `json:"providerType"`
	RepositoryName          string    `json:"repositoryName"`
	PublishDeploymentStatus string    `json:"publishDeploymentStatus,omitempty"`
	TriggerResourceUpdateOn string    `json:"triggerResourceUpdateOn,omitempty"`
	// region is the store.Table composite-key qualifier: ResourceName+SyncType
	// carries no region of its own and every lookup is scoped by the
	// caller's context region, exactly like RepositoryLink.region above. See
	// persistence.go for how it survives Snapshot/Restore.
	region string
}

// SyncEvent is a single event in a sync attempt.
type SyncEvent struct {
	Time       time.Time
	Event      string
	Type       string
	ExternalID string
}

// RepositorySyncStatus holds the latest sync attempt information for a repository link.
type RepositorySyncStatus struct {
	StartedAt time.Time
	Status    string
	// region/repositoryLinkID/branch/syncType are the store.Table composite-
	// key components: RepositorySyncStatus carries no identity field of its
	// own (it is looked up purely via repositorySyncStatusKey), so each is
	// captured explicitly at write time and carried through persistence via
	// a dedicated DTO (see persistence.go).
	region           string
	repositoryLinkID string
	branch           string
	syncType         string
	Events           []SyncEvent
}

// ResourceSyncStatus holds the latest sync attempt for an AWS resource.
type ResourceSyncStatus struct {
	StartedAt time.Time
	Status    string
	// region/resourceName/syncType are the store.Table composite-key
	// components: ResourceSyncStatus carries no identity field of its own,
	// exactly like RepositorySyncStatus above.
	region       string
	resourceName string
	syncType     string
	Events       []SyncEvent
}

// SyncBlockerSummary is a summary of sync blockers for a resource.
type SyncBlockerSummary struct {
	ResourceName       string
	ParentResourceName string
	LatestBlockers     []SyncBlocker
}

// SyncBlocker represents a single sync blocker entry.
type SyncBlocker struct {
	ID             string
	Type           string
	Status         string
	CreatedAt      time.Time
	CreatedReason  string
	ResolvedAt     *time.Time
	ResolvedReason string
	ResourceName   string
	SyncType       string
	// region qualifies the byResource secondary index (see
	// syncBlockerResourceIndexKeyFn in store_setup.go): ResourceName+SyncType
	// alone is not region-unique, and UpdateSyncBlocker's original map-based
	// lookup was itself scoped by the caller's context region, so region is
	// captured at creation time and re-checked on every ID-based lookup (see
	// UpdateSyncBlocker) to preserve that scoping even though ID itself is
	// already globally unique.
	region string
}

// RepositorySyncDefinition is a mapping from a repository branch to the AWS
// resource(s) being synced from that branch (see AWS docs for
// RepositorySyncDefinition).
type RepositorySyncDefinition struct {
	Branch    string
	Directory string
	Parent    string
	Target    string
}
