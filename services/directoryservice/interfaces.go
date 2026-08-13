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
		networkType NetworkType,
		vpcSettings *DirectoryVpcSettings,
		tags []Tag,
	) (*Directory, error)
	CreateMicrosoftAD(
		ctx context.Context,
		name, shortName, description, password string,
		edition DirectoryEdition,
		networkType NetworkType,
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

	AddRegion(ctx context.Context, directoryID, regionName string, vpcSettings *DirectoryVpcSettings) error
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

	CreateConditionalForwarder(
		ctx context.Context, directoryID, remoteDomainName string, dnsIPAddrs, dnsIPv6Addrs []string,
	) error
	UpdateConditionalForwarder(
		ctx context.Context, directoryID, remoteDomainName string, dnsIPAddrs, dnsIPv6Addrs []string,
	) error
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
		directoryID, remoteDomainName, trustPassword, trustDirection, trustType, selectiveAuth string,
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

	EnableCAEnrollmentPolicy(ctx context.Context, directoryID, pcaConnectorArn string) error
	DisableCAEnrollmentPolicy(ctx context.Context, directoryID string) error
	DescribeCAEnrollmentPolicy(ctx context.Context, directoryID string) (*CAEnrollmentPolicy, error)

	StartADAssessment(ctx context.Context, directoryID string, cfg *ADAssessmentConfiguration) (string, error)
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
		assessmentID, secretArn string,
		tags []Tag,
	) (*Directory, error)
	UpdateHybridAD(
		ctx context.Context,
		directoryID, adminAccountSecretArn string,
		selfManagedDNSIPs, selfManagedInstanceIDs []string,
	) (assessmentID string, err error)
	DescribeHybridADUpdate(
		ctx context.Context,
		directoryID, updateType string,
	) (hybridAdministratorAccount, selfManagedInstances []HybridADUpdateEntry, err error)

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
		networkType NetworkType,
		connectSettings ConnectSettingsInput,
		tags []Tag,
	) (*Directory, error)

	AccountID() string
	Region() string
	Reset()
	BackendSnapshot() []byte
	Restore(ctx context.Context, data []byte) error
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
	DirectoryStageRequested DirectoryStage = "Requested"
	DirectoryStageCreating  DirectoryStage = "Creating"
	DirectoryStageActive    DirectoryStage = "Active"
	DirectoryStageDeleted   DirectoryStage = "Deleted"
	DirectoryStageRestoring DirectoryStage = "Restoring"
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

// TrustDirection matches the AWS TrustDirection enum.
type TrustDirection string

const (
	TrustDirectionOneWayOutgoing TrustDirection = "One-Way: Outgoing"
	TrustDirectionOneWayIncoming TrustDirection = "One-Way: Incoming"
	TrustDirectionTwoWay         TrustDirection = "Two-Way"
)

// TrustType matches the AWS TrustType enum.
type TrustType string

const (
	TrustTypeForest   TrustType = "Forest"
	TrustTypeExternal TrustType = "External"
)

// SelectiveAuth matches the AWS SelectiveAuth enum.
type SelectiveAuth string

const (
	SelectiveAuthEnabled  SelectiveAuth = "Enabled"
	SelectiveAuthDisabled SelectiveAuth = "Disabled"
)

// LDAPSType matches the AWS LDAPSType enum.
type LDAPSType string

const (
	LDAPSTypeClient LDAPSType = "Client"
)

// ClientAuthenticationType matches the AWS ClientAuthenticationType enum.
type ClientAuthenticationType string

const (
	ClientAuthenticationTypeSmartCard           ClientAuthenticationType = "SmartCard"
	ClientAuthenticationTypeSmartCardOrPassword ClientAuthenticationType = "SmartCardOrPassword"
)

// UpdateType matches the AWS UpdateType enum.
type UpdateType string

const (
	UpdateTypeOS      UpdateType = "OS"
	UpdateTypeNetwork UpdateType = "NETWORK"
	UpdateTypeSize    UpdateType = "SIZE"
)

// NetworkType matches the AWS NetworkType enum.
type NetworkType string

const (
	NetworkTypeDualStack NetworkType = "Dual-stack"
	NetworkTypeIPv4Only  NetworkType = "IPv4"
	NetworkTypeIPv6Only  NetworkType = "IPv6"
)

// RadiusStatus matches the AWS RadiusStatus enum.
type RadiusStatus string

const (
	RadiusStatusCreating  RadiusStatus = "Creating"
	RadiusStatusCompleted RadiusStatus = "Completed"
	RadiusStatusFailed    RadiusStatus = "Failed"
)

// ShareMethod matches the AWS ShareMethod enum.
type ShareMethod string

const (
	ShareMethodOrganizations ShareMethod = "ORGANIZATIONS"
	ShareMethodHandshake     ShareMethod = "HANDSHAKE"
)

// ShareStatus matches the AWS ShareStatus enum.
type ShareStatus string

const (
	ShareStatusShared            ShareStatus = "Shared"
	ShareStatusPendingAcceptance ShareStatus = "PendingAcceptance"
	ShareStatusRejected          ShareStatus = "Rejected"
	ShareStatusRejecting         ShareStatus = "Rejecting"
	ShareStatusRejectFailed      ShareStatus = "RejectFailed"
	ShareStatusSharing           ShareStatus = "Sharing"
	ShareStatusShareFailed       ShareStatus = "ShareFailed"
	ShareStatusDeleted           ShareStatus = "Deleted"
	ShareStatusDeleting          ShareStatus = "Deleting"
)

// OSVersion matches the AWS OSVersion enum. This backend does not track a
// directory's underlying OS version (AWS assigns it internally and does not
// document a deterministic default), so Directory.OsVersion is always left
// as the zero value; see PARITY.md.
type OSVersion string

const (
	OSVersionVersion2012 OSVersion = "SERVER_2012"
	OSVersionVersion2019 OSVersion = "SERVER_2019"
)

// DirectoryVpcSettings holds VPC networking settings for a directory.
type DirectoryVpcSettings struct {
	VpcID             string
	SubnetIDs         []string
	SecurityGroupIDs  []string
	AvailabilityZones []string
}

// ConnectSettingsInput carries the AD Connector settings supplied to
// ConnectDirectory (matches AWS's DirectoryConnectSettings request shape).
type ConnectSettingsInput struct {
	CustomerUserName string
	VpcID            string
	SubnetIDs        []string
	CustomerDNSIPs   []string
	CustomerDNSIPsV6 []string
}

// DirectoryConnectSettingsDescription mirrors AWS's
// DirectoryConnectSettingsDescription, returned only for AD Connector
// directories. SecurityGroupID and AvailabilityZones are not populated: this
// backend does not model VPC/subnet-to-AZ or security-group provisioning, so
// there is no real value to derive them from (see PARITY.md).
type DirectoryConnectSettingsDescription struct {
	CustomerUserName  string
	SecurityGroupID   string
	VpcID             string
	SubnetIDs         []string
	AvailabilityZones []string
	ConnectIPs        []string
	ConnectIPsV6      []string
}

// RegionsInfo mirrors AWS's RegionsInfo, describing multi-Region replication
// for a Managed Microsoft AD directory.
type RegionsInfo struct {
	PrimaryRegion     string
	AdditionalRegions []string
}

// RadiusSettingsDescription mirrors AWS's RadiusSettings type as returned on
// DirectoryDescription.RadiusSettings. RadiusServersIPv6 is not populated:
// this backend's RADIUS settings storage does not track IPv6 server
// addresses (see PARITY.md).
type RadiusSettingsDescription struct {
	AuthenticationProtocol string
	DisplayLabel           string
	SharedSecret           string
	RadiusServers          []string
	RadiusServersIPv6      []string
	RadiusPort             int32
	RadiusRetries          int32
	RadiusTimeout          int32
	UseSameUsername        bool
}

// HybridSettingsDescription mirrors AWS's HybridSettingsDescription. Populated
// once a directory is hybridized via CreateHybridAD (non-nil, initially empty
// slices) and kept current by UpdateHybridAD's SelfManagedInstancesSettings
// (CustomerDnsIps/InstanceIds) -- the only real source of this data, since
// CreateHybridADInput itself carries none (see PARITY.md).
type HybridSettingsDescription struct {
	SelfManagedDNSIPAddrs  []string
	SelfManagedInstanceIDs []string
}

// OwnerDirectoryDescription mirrors AWS's OwnerDirectoryDescription, present
// on the directory-consumer's copy of a shared directory. This backend never
// populates it: shared directories are tracked only via SharedDirInfo
// (DescribeSharedDirectories) and are not materialized as a separate
// Directory entry in the consumer's DescribeDirectories view (see
// PARITY.md).
type OwnerDirectoryDescription struct {
	RadiusSettings *RadiusSettingsDescription
	VpcSettings    *DirectoryVpcSettings
	AccountID      string
	DirectoryID    string
	NetworkType    NetworkType
	RadiusStatus   RadiusStatus
	DNSIPAddrs     []string
	DNSIPv6Addrs   []string
}

// Directory represents an AWS Directory Service directory.
// LaunchTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Directory struct {
	LaunchTime                       time.Time
	StageLastUpdatedDateTime         time.Time
	VpcSettings                      *DirectoryVpcSettings
	ConnectSettings                  *DirectoryConnectSettingsDescription
	RegionsInfo                      *RegionsInfo
	RadiusSettings                   *RadiusSettingsDescription
	HybridSettings                   *HybridSettingsDescription
	OwnerDirectoryDescription        *OwnerDirectoryDescription
	DesiredNumberOfDomainControllers *int32
	StageReason                      *string
	DirectoryID                      string
	Name                             string
	ShortName                        string
	Description                      string
	Alias                            string
	AccessURL                        string
	ShareNotes                       string
	Type                             DirectoryType
	Stage                            DirectoryStage
	Size                             DirectorySize
	Edition                          DirectoryEdition
	NetworkType                      NetworkType
	RadiusStatus                     RadiusStatus
	ShareMethod                      ShareMethod
	ShareStatus                      ShareStatus
	OsVersion                        OSVersion
	DNSIPAddrs                       []string
	DNSIPv6Addrs                     []string
	SsoEnabled                       bool
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
