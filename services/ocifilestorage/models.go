package ocifilestorage

import "time"

// Lifecycle states shared by the resources.
const (
	stCreating = "CREATING"
	stActive   = "ACTIVE"
	stUpdating = "UPDATING"
	stDeleting = "DELETING"
	stDeleted  = "DELETED"
	stFailed   = "FAILED"
	stInactive = "INACTIVE"
)

type lock struct {
	TimeCreated       *time.Time `json:"timeCreated,omitempty"`
	Type              string     `json:"type"`
	RelatedResourceID string     `json:"relatedResourceId,omitempty"`
	Message           string     `json:"message,omitempty"`
}

type lockDuration struct {
	LockDuration    *int   `json:"lockDuration,omitempty"`
	CoolOffDuration *int   `json:"coolOffDuration,omitempty"`
	LockMode        string `json:"lockMode"`
}

type base struct {
	TimeCreated  time.Time                 `json:"timeCreated"`
	FreeformTags map[string]string         `json:"freeformTags"`
	DefinedTags  map[string]map[string]any `json:"definedTags"`
	SystemTags   map[string]map[string]any `json:"systemTags,omitempty"`
	ID           string                    `json:"id"`
	State        string                    `json:"lifecycleState"`
	Locks        []lock                    `json:"locks"`
}

func (b *base) bs() *base { return b }

type resource interface{ bs() *base }

type sourceDetails struct {
	ParentFileSystemID string `json:"parentFileSystemId,omitempty"`
	SourceSnapshotID   string `json:"sourceSnapshotId,omitempty"`
}

type fsInt struct {
	HydratesAt      *time.Time `json:"hydratesAt,omitempty"`
	DetachAt        *time.Time `json:"detachAt,omitempty"`
	DetachOnHydrate bool       `json:"detachOnHydrate,omitempty"`
}

// FileSystem is the stored and wire form of a file system.
type FileSystem struct {
	IsHydrated                       *bool          `json:"isHydrated,omitempty"`
	Int                              *fsInt         `json:"_int,omitempty"`
	SourceDetails                    *sourceDetails `json:"sourceDetails,omitempty"`
	ReplicationTargetID              string         `json:"replicationTargetId,omitempty"`
	QuotaEnforcementState            string         `json:"quotaEnforcementState,omitempty"`
	DisplayName                      string         `json:"displayName"`
	AvailabilityDomain               string         `json:"availabilityDomain"`
	KmsKeyID                         string         `json:"kmsKeyId,omitempty"`
	CloneAttachStatus                string         `json:"cloneAttachStatus,omitempty"`
	LifecycleDetails                 string         `json:"lifecycleDetails"`
	CompartmentQuotaEnforcementState string         `json:"compartmentQuotaEnforcementState,omitempty"`
	FilesystemSnapshotPolicyID       string         `json:"filesystemSnapshotPolicyId,omitempty"`
	CompartmentID                    string         `json:"compartmentId"`
	base
	MeteredBytes           int64 `json:"meteredBytes"`
	CloneCount             int   `json:"cloneCount"`
	ReplicationSourceCount int   `json:"replicationSourceCount"`
	IsCloneParent          bool  `json:"isCloneParent"`
	IsTargetable           bool  `json:"isTargetable"`
	AreQuotaRulesEnabled   bool  `json:"areQuotaRulesEnabled"`
}

type ldapIdmap struct {
	CacheRefreshIntervalSeconds  *int   `json:"cacheRefreshIntervalSeconds,omitempty"`
	CacheLifetimeSeconds         *int   `json:"cacheLifetimeSeconds,omitempty"`
	NegativeCacheLifetimeSeconds *int   `json:"negativeCacheLifetimeSeconds,omitempty"`
	SchemaType                   string `json:"schemaType,omitempty"`
	UserSearchBase               string `json:"userSearchBase,omitempty"`
	GroupSearchBase              string `json:"groupSearchBase,omitempty"`
	OutboundConnector1ID         string `json:"outboundConnector1Id,omitempty"`
	OutboundConnector2ID         string `json:"outboundConnector2Id,omitempty"`
}

type kerberos struct {
	CurrentKeyTabSecretVersion *int   `json:"currentKeyTabSecretVersion,omitempty"`
	BackupKeyTabSecretVersion  *int   `json:"backupKeyTabSecretVersion,omitempty"`
	KerberosRealm              string `json:"kerberosRealm,omitempty"`
	KeyTabSecretID             string `json:"keyTabSecretId,omitempty"`
	IsKerberosEnabled          bool   `json:"isKerberosEnabled"`
}

type mtInt struct {
	DowngradeTo    *int64     `json:"downgradeTo,omitempty"`
	DowngradeAfter *time.Time `json:"downgradeAfter,omitempty"`
	VnicID         string     `json:"vnicId,omitempty"`
}

// MountTarget is the stored and wire form of a mount target.
type MountTarget struct {
	Int *mtInt `json:"_int,omitempty"`
	base
	LdapIdmap           *ldapIdmap                `json:"ldapIdmap,omitempty"`
	Kerberos            *kerberos                 `json:"kerberos,omitempty"`
	TimeBillingCycleEnd *time.Time                `json:"timeBillingCycleEnd,omitempty"`
	SecurityAttributes  map[string]map[string]any `json:"securityAttributes,omitempty"`
	CompartmentID       string                    `json:"compartmentId"`
	DisplayName         string                    `json:"displayName"`
	LifecycleDetails    string                    `json:"lifecycleDetails"`
	SubnetID            string                    `json:"subnetId"`
	AvailabilityDomain  string                    `json:"availabilityDomain"`
	ExportSetID         string                    `json:"exportSetId,omitempty"`
	IdmapType           string                    `json:"idmapType,omitempty"`
	PrivateIPIDs        []string                  `json:"privateIpIds"`
	MountTargetIpv6IDs  []string                  `json:"mountTargetIpv6Ids,omitempty"`
	NsgIDs              []string                  `json:"nsgIds"`
	ObservedThroughput  int64                     `json:"observedThroughput"`
	RequestedThroughput int64                     `json:"requestedThroughput"`
	ReservedStorage     int64                     `json:"reservedStorageCapacity"`
}

type esInt struct {
	MountTargetID string `json:"mountTargetId"`
}

// ExportSet is the stored and wire form of an export set.
type ExportSet struct {
	Int                *esInt `json:"_int,omitempty"`
	MaxFsStatBytes     *int64 `json:"maxFsStatBytes,omitempty"`
	MaxFsStatFiles     *int64 `json:"maxFsStatFiles,omitempty"`
	CompartmentID      string `json:"compartmentId"`
	DisplayName        string `json:"displayName"`
	VcnID              string `json:"vcnId"`
	AvailabilityDomain string `json:"availabilityDomain"`
	base
}

type clientOptions struct {
	RequirePrivilegedSourcePort *bool    `json:"requirePrivilegedSourcePort,omitempty"`
	AnonymousUID                *int64   `json:"anonymousUid,omitempty"`
	AnonymousGID                *int64   `json:"anonymousGid,omitempty"`
	IsAnonymousAccessAllowed    *bool    `json:"isAnonymousAccessAllowed,omitempty"`
	Source                      string   `json:"source"`
	Access                      string   `json:"access,omitempty"`
	IdentitySquash              string   `json:"identitySquash,omitempty"`
	AllowedAuth                 []string `json:"allowedAuth,omitempty"`
}

// Export is the stored and wire form of an export.
type Export struct {
	base
	IsIdmapGroupsForSysAuth *bool           `json:"isIdmapGroupsForSysAuth,omitempty"`
	ExportSetID             string          `json:"exportSetId"`
	FileSystemID            string          `json:"fileSystemId"`
	Path                    string          `json:"path"`
	ExportOptions           []clientOptions `json:"exportOptions"`
}

// Snapshot is the stored and wire form of a snapshot.
type Snapshot struct {
	SnapshotTime               time.Time     `json:"snapshotTime"`
	LockDurationDetails        *lockDuration `json:"lockDurationDetails,omitempty"`
	ExpirationTime             *time.Time    `json:"expirationTime,omitempty"`
	TimeLocked                 *time.Time    `json:"timeLocked,omitempty"`
	Name                       string        `json:"name"`
	FileSystemID               string        `json:"fileSystemId"`
	SnapshotType               string        `json:"snapshotType"`
	ProvenanceID               string        `json:"provenanceId"`
	LifecycleDetails           string        `json:"lifecycleDetails"`
	FilesystemSnapshotPolicyID string        `json:"filesystemSnapshotPolicyId,omitempty"`
	base
	ExclusiveBytes int64 `json:"exclusiveBytes"`
	IsCloneSource  bool  `json:"isCloneSource"`
}

type schedule struct {
	TimeScheduleStart          *time.Time    `json:"timeScheduleStart,omitempty"`
	RetentionDurationInSeconds *int64        `json:"retentionDurationInSeconds,omitempty"`
	HourOfDay                  *int          `json:"hourOfDay,omitempty"`
	DayOfMonth                 *int          `json:"dayOfMonth,omitempty"`
	LockDurationDetails        *lockDuration `json:"lockDurationDetails,omitempty"`
	Period                     string        `json:"period"`
	TimeZone                   string        `json:"timeZone"`
	SchedulePrefix             string        `json:"schedulePrefix,omitempty"`
	DayOfWeek                  string        `json:"dayOfWeek,omitempty"`
	Month                      string        `json:"month,omitempty"`
}

type polInt struct {
	CreatedAt time.Time   `json:"createdAt"`
	PausedAt  *time.Time  `json:"pausedAt,omitempty"`
	LastRun   []time.Time `json:"lastRun,omitempty"`
}

// Policy is the stored and wire form of a file system snapshot policy.
type Policy struct {
	Int *polInt `json:"_int,omitempty"`
	base
	CompartmentID      string     `json:"compartmentId"`
	AvailabilityDomain string     `json:"availabilityDomain"`
	DisplayName        string     `json:"displayName"`
	PolicyPrefix       string     `json:"policyPrefix,omitempty"`
	Schedules          []schedule `json:"schedules"`
}

type repInt struct {
	NextCycle *time.Time `json:"nextCycle,omitempty"`
	PhaseAt   *time.Time `json:"phaseAt,omitempty"`
	Number    int        `json:"number"`
	FinalPass bool       `json:"finalPass,omitempty"`
}

// Replication is the stored and wire form of a replication.
type Replication struct {
	DeltaProgress       *int64     `json:"deltaProgress,omitempty"`
	Int                 *repInt    `json:"_int,omitempty"`
	RecoveryPointTime   *time.Time `json:"recoveryPointTime,omitempty"`
	SourceID            string     `json:"sourceId"`
	CompartmentID       string     `json:"compartmentId"`
	DisplayName         string     `json:"displayName"`
	TargetID            string     `json:"targetId"`
	ReplicationTargetID string     `json:"replicationTargetId"`
	AvailabilityDomain  string     `json:"availabilityDomain"`
	LastSnapshotID      string     `json:"lastSnapshotId,omitempty"`
	DeltaStatus         string     `json:"deltaStatus,omitempty"`
	LifecycleDetails    string     `json:"lifecycleDetails"`
	base
	ReplicationInterval int64 `json:"replicationInterval"`
}

// ReplicationTarget is the stored and wire form of a replication target.
type ReplicationTarget struct {
	RecoveryPointTime  *time.Time `json:"recoveryPointTime,omitempty"`
	DeltaProgress      *int64     `json:"deltaProgress,omitempty"`
	CompartmentID      string     `json:"compartmentId"`
	DisplayName        string     `json:"displayName"`
	SourceID           string     `json:"sourceId"`
	TargetID           string     `json:"targetId"`
	ReplicationID      string     `json:"replicationId"`
	AvailabilityDomain string     `json:"availabilityDomain"`
	LastSnapshotID     string     `json:"lastSnapshotId,omitempty"`
	DeltaStatus        string     `json:"deltaStatus,omitempty"`
	LifecycleDetails   string     `json:"lifecycleDetails"`
	base
}

type endpoint struct {
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}

// Connector is the stored and wire form of an LDAPBIND outbound connector.
type Connector struct {
	base
	PasswordSecretVersion           *int       `json:"passwordSecretVersion,omitempty"`
	TrustedCertificateSecretVersion *int       `json:"trustedCertificateSecretVersion,omitempty"`
	CompartmentID                   string     `json:"compartmentId"`
	DisplayName                     string     `json:"displayName"`
	AvailabilityDomain              string     `json:"availabilityDomain"`
	ConnectorType                   string     `json:"connectorType"`
	BindDistinguishedName           string     `json:"bindDistinguishedName"`
	PasswordSecretID                string     `json:"passwordSecretId"`
	TrustedCertificateSecretID      string     `json:"trustedCertificateSecretId,omitempty"`
	Endpoints                       []endpoint `json:"endpoints"`
}

// QuotaRule is the stored and wire form of a quota rule.
type QuotaRule struct {
	TimeUpdated   time.Time `json:"timeUpdated"`
	PrincipalID   *int      `json:"principalId,omitempty"`
	FileSystemID  string    `json:"fileSystemId"`
	DisplayName   string    `json:"displayName"`
	PrincipalType string    `json:"principalType"`
	base
	QuotaLimitGB int  `json:"quotaLimitInGigabytes"`
	IsHardQuota  bool `json:"isHardQuota"`
}
