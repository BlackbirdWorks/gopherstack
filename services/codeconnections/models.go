package codeconnections

import "time"

// SyncBlocker status values (real AWS BlockerStatus enum).
const (
	SyncBlockerStatusActive   = "ACTIVE"
	SyncBlockerStatusResolved = "RESOLVED"
)

// validProviderTypes is the set of provider types accepted by AWS CodeConnections.
func validProviderTypes() map[string]bool {
	return map[string]bool{
		"Bitbucket":              true,
		"GitHub":                 true,
		"GitHubEnterpriseServer": true,
		"GitLab":                 true,
		"GitLabSelfManaged":      true,
		"AzureDevOps":            true,
	}
}

// validSyncTypes is the set of sync configuration types accepted by AWS CodeConnections.
func validSyncTypes() map[string]bool {
	return map[string]bool{
		"CFN_STACK_SYNC": true,
	}
}

// validEnabledDisabled is the {"", "ENABLED", "DISABLED"} value set shared by
// PublishDeploymentStatus and PullRequestComment (aws-sdk-go-v2/service/
// codeconnections@v1.13.4 types/enums.go:91-95/110-114); "" means the field
// was left unset on the request. Neither field is required on
// CreateSyncConfigurationInput/UpdateSyncConfigurationInput.
func validEnabledDisabled() map[string]bool {
	return map[string]bool{"": true, "ENABLED": true, "DISABLED": true}
}

// validTriggerResourceUpdateOn is the value set for TriggerResourceUpdateOn
// (types/enums.go:194-198); "" means the field was left unset.
func validTriggerResourceUpdateOn() map[string]bool {
	return map[string]bool{"": true, "ANY_CHANGE": true, "FILE_CHANGE": true}
}

// Connection represents an AWS CodeConnections connection.
//
// ConnectionArn already embeds its own region (arn:partition:service:region:
// account:resource, see regionFromARN), so Connection needs no hidden region
// field: store_setup.go's connections table is keyed directly by
// ConnectionArn and its byRegion index derives region from the ARN.
type Connection struct {
	Tags           map[string]string `json:"tags,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	ConnectionName string            `json:"connectionName"`
	ConnectionArn  string            `json:"connectionArn"`
	HostArn        string            `json:"hostArn,omitempty"`
	OwnerAccountID string            `json:"ownerAccountID"`
	ProviderType   string            `json:"providerType"`
	Status         string            `json:"status"`
}

// VpcConfiguration holds the VPC connectivity settings for a host (real
// aws-sdk-go-v2/service/codeconnections@v1.13.4 types.VpcConfiguration --
// SecurityGroupIds/SubnetIds/VpcId are required wire members, TlsCertificate
// is optional).
type VpcConfiguration struct {
	VpcID            string   `json:"vpcId"`
	TLSCertificate   string   `json:"tlsCertificate,omitempty"`
	SubnetIDs        []string `json:"subnetIds"`
	SecurityGroupIDs []string `json:"securityGroupIds"`
}

// Host represents an AWS CodeConnections host (infrastructure endpoint).
//
// Like Connection, HostArn already embeds its own region, so Host needs no
// hidden region field either.
type Host struct {
	VpcConfiguration *VpcConfiguration `json:"vpcConfiguration,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	Name             string            `json:"name"`
	HostArn          string            `json:"hostArn"`
	ProviderType     string            `json:"providerType"`
	ProviderEndpoint string            `json:"providerEndpoint"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
}

// RepositoryLink represents an AWS CodeConnections repository link.
type RepositoryLink struct {
	Tags              map[string]string `json:"tags,omitempty"`
	CreatedAt         time.Time         `json:"createdAt"`
	ConnectionArn     string            `json:"connectionArn"`
	OwnerID           string            `json:"ownerID"`
	RepositoryName    string            `json:"repositoryName"`
	RepositoryLinkID  string            `json:"repositoryLinkID"`
	RepositoryLinkArn string            `json:"repositoryLinkArn"`
	ProviderType      string            `json:"providerType"`
	EncryptionKeyArn  string            `json:"encryptionKeyArn,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey and
	// store_setup.go); it is unexported so a plain json.Marshal(RepositoryLink)
	// never sees it and is instead carried through persistence via a
	// regionalDTO (see persistence.go). GetRepositoryLink/DeleteRepositoryLink/
	// UpdateRepositoryLink resolve their region from the caller's context
	// rather than from any ARN (a bare RepositoryLinkID carries no region of
	// its own), so region must be captured explicitly at creation time and
	// used for every subsequent lookup by ID.
	region string
}

// SyncConfiguration represents an AWS CodeConnections sync configuration.
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
	// PullRequestComment mirrors the real SyncConfiguration/
	// CreateSyncConfigurationInput/UpdateSyncConfigurationInput
	// PullRequestComment member (aws-sdk-go-v2/service/codeconnections@
	// v1.13.4 types.PullRequestComment, ENABLED|DISABLED) -- present in this
	// service's pinned SDK but previously never implemented here.
	PullRequestComment string `json:"pullRequestComment,omitempty"`
	// region is the store.Table composite-key qualifier: ResourceName+SyncType
	// carries no region of its own and every lookup is scoped by the caller's
	// context region, exactly like RepositoryLink.region above. See
	// persistence.go for how it survives Snapshot/Restore.
	region string
}

// syncConfigKey returns the composite lookup key for a sync configuration.
// ResourceName values must not contain "/" to avoid key collisions with SyncType.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

// RepositorySyncStatus holds the latest sync attempt information for a repository link.
type RepositorySyncStatus struct {
	StartedAt time.Time
	Status    string
	Events    []SyncEvent
}

// SyncEvent is a single event in a sync attempt.
type SyncEvent struct {
	Time       time.Time
	Event      string
	Type       string
	ExternalID string
}

// Revision mirrors AWS CodeConnections' Revision type: the state of an AWS
// resource as declared by its linked repository at a specific commit (real
// aws-sdk-go-v2/service/codeconnections@v1.13.4 types.Revision -- Branch/
// Directory/OwnerId/ProviderType/RepositoryName/Sha are all required wire
// members).
type Revision struct {
	Branch         string
	Directory      string
	OwnerID        string
	ProviderType   string
	RepositoryName string
	Sha            string
}

// ResourceSyncAttempt mirrors the real ResourceSyncAttempt type used for both
// GetResourceSyncStatusOutput.LatestSync and .LatestSuccessfulSync (real
// wire-required members: Events/InitialRevision/StartedAt/Status/Target/
// TargetRevision).
type ResourceSyncAttempt struct {
	StartedAt       time.Time
	Status          string
	Target          string
	InitialRevision Revision
	TargetRevision  Revision
	Events          []SyncEvent
}

// ResourceSyncStatus mirrors GetResourceSyncStatusOutput: the latest sync
// attempt for an AWS resource, its desired state, and (when available) the
// latest successful attempt.
type ResourceSyncStatus struct {
	LatestSuccessfulSync *ResourceSyncAttempt
	DesiredState         Revision
	LatestSync           ResourceSyncAttempt
}

// RepositorySyncDefinition describes a mapping from a repository branch to an
// Amazon Web Services resource that is being synced from that branch.
type RepositorySyncDefinition struct {
	Branch    string
	Directory string
	Parent    string
	Target    string
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
	// alone is not region-unique, and UpdateSyncBlocker's lookup by ID must
	// still be scoped to the caller's context region, so region is captured
	// at creation time and re-checked on every ID-based lookup.
	region string
}
