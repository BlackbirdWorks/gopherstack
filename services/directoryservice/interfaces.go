package directoryservice

import (
	"context"
	"time"
)

// StorageBackend is the interface for DirectoryService storage operations.
type StorageBackend interface {
	CreateDirectory(
		ctx context.Context,
		name, shortName, description, password string,
		size DirectorySize,
		vpcSettings *DirectoryVpcSettings,
		tags []Tag,
	) (*Directory, error)
	CreateMicrosoftAD(
		ctx context.Context,
		name, shortName, description, password string,
		edition DirectoryEdition,
		vpcSettings *DirectoryVpcSettings,
		tags []Tag,
	) (*Directory, error)
	DeleteDirectory(ctx context.Context, directoryID string) error
	DescribeDirectories(
		ctx context.Context,
		directoryIDs []string,
		limit int32,
		nextToken string,
	) ([]*Directory, string, error)
	CreateAlias(ctx context.Context, directoryID, alias string) error
	EnableSso(ctx context.Context, directoryID string) error
	DisableSso(ctx context.Context, directoryID string) error
	GetDirectoryLimits(ctx context.Context) *DirectoryLimits

	CreateSnapshot(ctx context.Context, directoryID, name string) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	DescribeSnapshots(
		ctx context.Context,
		directoryID string,
		snapshotIDs []string,
		limit int32,
		nextToken string,
	) ([]*Snapshot, string, error)
	GetSnapshotLimits(ctx context.Context, directoryID string) (*SnapshotLimits, error)
	RestoreFromSnapshot(ctx context.Context, snapshotID string) error

	AddTagsToResource(ctx context.Context, resourceID string, tags []Tag) error
	RemoveTagsFromResource(ctx context.Context, resourceID string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceID string, limit int32, nextToken string) ([]Tag, string, error)

	AddIpRoutes(ctx context.Context, directoryID string, routes []IpRoute) error
	RemoveIpRoutes(ctx context.Context, directoryID string, cidrIPs []string) error
	ListIpRoutes(ctx context.Context, directoryID string, limit int32, nextToken string) ([]IpRoute, string, error)

	AddRegion(ctx context.Context, directoryID, regionName string) error
	RemoveRegion(ctx context.Context, directoryID string) error
	DescribeRegions(ctx context.Context, directoryID, regionName, nextToken string) ([]RegionDescription, string, error)

	StartSchemaExtension(ctx context.Context, directoryID, description, schemaExtensionBody string) (string, error)
	CancelSchemaExtension(ctx context.Context, directoryID, schemaExtensionID string) error
	ListSchemaExtensions(
		ctx context.Context,
		directoryID string,
		limit int32,
		nextToken string,
	) ([]SchemaExtension, string, error)

	CreateConditionalForwarder(ctx context.Context, directoryID, remoteDomainName string, dnsIPAddrs []string) error
	UpdateConditionalForwarder(ctx context.Context, directoryID, remoteDomainName string, dnsIPAddrs []string) error
	DeleteConditionalForwarder(ctx context.Context, directoryID, remoteDomainName string) error
	DescribeConditionalForwarders(
		ctx context.Context,
		directoryID string,
		remoteDomainNames []string,
	) ([]ConditionalForwarder, error)

	CreateLogSubscription(ctx context.Context, directoryID, logGroupName string) error
	DeleteLogSubscription(ctx context.Context, directoryID string) error
	ListLogSubscriptions(
		ctx context.Context,
		directoryID string,
		limit int32,
		nextToken string,
	) ([]LogSubscription, string, error)

	RegisterEventTopic(ctx context.Context, directoryID, topicName string) error
	DeregisterEventTopic(ctx context.Context, directoryID, topicName string) error
	DescribeEventTopics(ctx context.Context, directoryID string, topicNames []string) ([]EventTopic, error)

	DescribeDomainControllers(
		ctx context.Context,
		directoryID string,
		domainControllerIDs []string,
		limit int32,
		nextToken string,
	) ([]DomainController, string, error)
	UpdateNumberOfDomainControllers(ctx context.Context, directoryID string, desiredNumber int32) error

	CreateTrust(
		ctx context.Context,
		directoryID, remoteDomainName, trustPassword, trustDirection, trustType string,
	) (string, error)
	DeleteTrust(ctx context.Context, trustID string) (string, error)
	DescribeTrusts(
		ctx context.Context,
		directoryID string,
		trustIDs []string,
		limit int32,
		nextToken string,
	) ([]TrustInfo, string, error)
	UpdateTrust(ctx context.Context, trustID, selectiveAuth string) (string, error)
	VerifyTrust(ctx context.Context, trustID string) (string, error)

	ShareDirectory(ctx context.Context, directoryID, shareMethod, shareNotes, targetID string) (string, error)
	UnshareDirectory(ctx context.Context, directoryID, targetID string) (string, error)
	AcceptSharedDirectory(ctx context.Context, sharedDirectoryID string) (string, error)
	RejectSharedDirectory(ctx context.Context, sharedDirectoryID string) (string, error)
	DescribeSharedDirectories(
		ctx context.Context,
		ownerDirID string,
		sharedDirIDs []string,
		limit int32,
		nextToken string,
	) ([]SharedDirInfo, string, error)

	RegisterCertificate(ctx context.Context, directoryID, certData, certType string) (string, error)
	DeregisterCertificate(ctx context.Context, directoryID, certID string) error
	ListCertificates(ctx context.Context, directoryID string, limit int32, nextToken string) ([]CertInfo, string, error)
	DescribeCertificate(ctx context.Context, directoryID, certID string) (*CertDetail, error)
	EnableLDAPS(ctx context.Context, directoryID, ldapsType string) error
	DisableLDAPS(ctx context.Context, directoryID, ldapsType string) error
	DescribeLDAPSSettings(
		ctx context.Context,
		directoryID, ldapsType string,
		limit int32,
		nextToken string,
	) ([]LDAPSSetting, string, error)

	EnableClientAuthentication(ctx context.Context, directoryID, authType string) error
	DisableClientAuthentication(ctx context.Context, directoryID, authType string) error
	DescribeClientAuthenticationSettings(
		ctx context.Context,
		directoryID, authType string,
		limit int32,
		nextToken string,
	) ([]ClientAuthInfo, string, error)

	EnableRadius(ctx context.Context, directoryID string, settings RadiusSettingsInput) error
	DisableRadius(ctx context.Context, directoryID string) error
	UpdateRadius(ctx context.Context, directoryID string, settings RadiusSettingsInput) error

	EnableDirectoryDataAccess(ctx context.Context, directoryID string) error
	DisableDirectoryDataAccess(ctx context.Context, directoryID string) error
	DescribeDirectoryDataAccess(ctx context.Context, directoryID string) (*DirectoryDataAccessStatus, error)

	EnableCAEnrollmentPolicy(ctx context.Context, directoryID string) error
	DisableCAEnrollmentPolicy(ctx context.Context, directoryID string) error
	DescribeCAEnrollmentPolicy(ctx context.Context, directoryID string) (*CAEnrollmentPolicy, error)

	StartADAssessment(ctx context.Context, directoryID string) (string, error)
	DeleteADAssessment(ctx context.Context, directoryID, assessmentID string) error
	DescribeADAssessment(ctx context.Context, directoryID, assessmentID string) (*ADAssessmentInfo, error)
	ListADAssessments(
		ctx context.Context,
		directoryID string,
		limit int32,
		nextToken string,
	) ([]ADAssessmentInfo, string, error)

	CreateHybridAD(
		ctx context.Context,
		name, shortName, description, password string,
		edition DirectoryEdition,
		tags []Tag,
	) (*Directory, string, error)
	UpdateHybridAD(ctx context.Context, directoryID string) (string, error)
	DescribeHybridADUpdate(ctx context.Context, directoryID string) ([]HybridADUpdateEntry, error)

	CreateComputer(ctx context.Context, directoryID, computerName, password string) (*ComputerInfo, error)

	UpdateSettings(ctx context.Context, directoryID string, settings []DirectorySetting) (string, error)
	DescribeSettings(ctx context.Context, directoryID, status, nextToken string) ([]SettingEntry, string, error)
	UpdateDirectorySetup(ctx context.Context, directoryID, updateType string, createSnapshotBeforeUpdate bool) error
	DescribeUpdateDirectory(
		ctx context.Context,
		directoryID, updateType, nextToken string,
	) ([]UpdateInfoEntry, string, error)

	ResetUserPassword(ctx context.Context, directoryID, userName, newPassword string) error

	ConnectDirectory(
		ctx context.Context,
		name, shortName, description, password string,
		size DirectorySize,
		tags []Tag,
	) (*Directory, error)

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
	VpcID             string
	SubnetIDs         []string
	SecurityGroupIDs  []string
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
