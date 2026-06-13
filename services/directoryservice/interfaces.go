package directoryservice

import "time"

// StorageBackend is the interface for DirectoryService storage operations.
type StorageBackend interface {
	CreateDirectory(name, shortName, description, password string, size DirectorySize, vpcSettings *DirectoryVpcSettings, tags []Tag) (*Directory, error)
	CreateMicrosoftAD(
		name, shortName, description, password string,
		edition DirectoryEdition,
		vpcSettings *DirectoryVpcSettings,
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

	AddIpRoutes(directoryID string, routes []IpRoute) error
	RemoveIpRoutes(directoryID string, cidrIPs []string) error
	ListIpRoutes(directoryID string, limit int32, nextToken string) ([]IpRoute, string, error)

	AddRegion(directoryID, regionName string) error
	RemoveRegion(directoryID string) error
	DescribeRegions(directoryID, regionName, nextToken string) ([]RegionDescription, string, error)

	StartSchemaExtension(directoryID, description, schemaExtensionBody string) (string, error)
	CancelSchemaExtension(directoryID, schemaExtensionID string) error
	ListSchemaExtensions(directoryID string, limit int32, nextToken string) ([]SchemaExtension, string, error)

	CreateConditionalForwarder(directoryID, remoteDomainName string, dnsIPAddrs []string) error
	UpdateConditionalForwarder(directoryID, remoteDomainName string, dnsIPAddrs []string) error
	DeleteConditionalForwarder(directoryID, remoteDomainName string) error
	DescribeConditionalForwarders(directoryID string, remoteDomainNames []string) ([]ConditionalForwarder, error)

	CreateLogSubscription(directoryID, logGroupName string) error
	DeleteLogSubscription(directoryID string) error
	ListLogSubscriptions(directoryID string, limit int32, nextToken string) ([]LogSubscription, string, error)

	RegisterEventTopic(directoryID, topicName string) error
	DeregisterEventTopic(directoryID, topicName string) error
	DescribeEventTopics(directoryID string, topicNames []string) ([]EventTopic, error)

	DescribeDomainControllers(
		directoryID string,
		domainControllerIDs []string,
		limit int32,
		nextToken string,
	) ([]DomainController, string, error)
	UpdateNumberOfDomainControllers(directoryID string, desiredNumber int32) error

	CreateTrust(directoryID, remoteDomainName, trustPassword, trustDirection, trustType string) (string, error)
	DeleteTrust(trustID string) (string, error)
	DescribeTrusts(
		directoryID string,
		trustIDs []string,
		limit int32,
		nextToken string,
	) ([]TrustInfo, string, error)
	UpdateTrust(trustID, selectiveAuth string) (string, error)
	VerifyTrust(trustID string) (string, error)

	ShareDirectory(directoryID, shareMethod, shareNotes, targetID string) (string, error)
	UnshareDirectory(directoryID, targetID string) (string, error)
	AcceptSharedDirectory(sharedDirectoryID string) (string, error)
	RejectSharedDirectory(sharedDirectoryID string) (string, error)
	DescribeSharedDirectories(
		ownerDirID string,
		sharedDirIDs []string,
		limit int32,
		nextToken string,
	) ([]SharedDirInfo, string, error)

	RegisterCertificate(directoryID, certData, certType string) (string, error)
	DeregisterCertificate(directoryID, certID string) error
	ListCertificates(directoryID string, limit int32, nextToken string) ([]CertInfo, string, error)
	DescribeCertificate(directoryID, certID string) (*CertDetail, error)
	EnableLDAPS(directoryID, ldapsType string) error
	DisableLDAPS(directoryID, ldapsType string) error
	DescribeLDAPSSettings(
		directoryID, ldapsType string,
		limit int32,
		nextToken string,
	) ([]LDAPSSetting, string, error)

	EnableClientAuthentication(directoryID, authType string) error
	DisableClientAuthentication(directoryID, authType string) error
	DescribeClientAuthenticationSettings(
		directoryID, authType string,
		limit int32,
		nextToken string,
	) ([]ClientAuthInfo, string, error)

	EnableRadius(directoryID string, settings RadiusSettingsInput) error
	DisableRadius(directoryID string) error
	UpdateRadius(directoryID string, settings RadiusSettingsInput) error

	EnableDirectoryDataAccess(directoryID string) error
	DisableDirectoryDataAccess(directoryID string) error
	DescribeDirectoryDataAccess(directoryID string) (*DirectoryDataAccessStatus, error)

	EnableCAEnrollmentPolicy(directoryID string) error
	DisableCAEnrollmentPolicy(directoryID string) error
	DescribeCAEnrollmentPolicy(directoryID string) (*CAEnrollmentPolicy, error)

	StartADAssessment(directoryID string) (string, error)
	DeleteADAssessment(directoryID, assessmentID string) error
	DescribeADAssessment(directoryID, assessmentID string) (*ADAssessmentInfo, error)
	ListADAssessments(directoryID string, limit int32, nextToken string) ([]ADAssessmentInfo, string, error)

	CreateHybridAD(
		name, shortName, description, password string,
		edition DirectoryEdition,
		tags []Tag,
	) (*Directory, string, error)
	UpdateHybridAD(directoryID string) (string, error)
	DescribeHybridADUpdate(directoryID string) ([]HybridADUpdateEntry, error)

	CreateComputer(directoryID, computerName, password string) (*ComputerInfo, error)

	UpdateSettings(directoryID string, settings []DirectorySetting) (string, error)
	DescribeSettings(directoryID, status, nextToken string) ([]SettingEntry, string, error)
	UpdateDirectorySetup(directoryID, updateType string, createSnapshotBeforeUpdate bool) error
	DescribeUpdateDirectory(directoryID, updateType, nextToken string) ([]UpdateInfoEntry, string, error)

	ResetUserPassword(directoryID, userName, newPassword string) error

	ConnectDirectory(name, shortName, description, password string, size DirectorySize, tags []Tag) (*Directory, error)

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
	DirectoryTypeSimpleAD          DirectoryType = "SimpleAD"
	DirectoryTypeMicrosoftAD       DirectoryType = "MicrosoftAD"
	DirectoryTypeADConnector       DirectoryType = "ADConnector"
	DirectoryTypeSharedMicrosoftAD DirectoryType = "SharedMicrosoftAD"
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

// DirectoryVpcSettings holds VPC networking settings for a directory.
type DirectoryVpcSettings struct {
	VpcID            string
	SubnetIDs        []string
	SecurityGroupIDs []string
	AvailabilityZones []string
}

// Directory represents an AWS Directory Service directory.
// LaunchTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Directory struct {
	LaunchTime  time.Time
	VpcSettings *DirectoryVpcSettings
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
