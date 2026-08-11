package opensearch

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Domain status constants.
const (
	domainStatusActive = "Active"
)

// Package/connection state constants.
const (
	pkgStateActive          = "ACTIVE"
	connectionStatusActive  = "ACTIVE"
	softwareUpdateCompleted = "COMPLETED"
)

// Package resource status constants, matching the AWS PackageStatus enum
// (types.PackageStatusAvailable). Note this is a different enum from
// DomainPackageStatus (pkgStateActive above is that one's ACTIVE value) --
// PackageStatus has no ACTIVE value at all, only AVAILABLE.
const pkgStatusAvailable = "AVAILABLE"

// reservedInstanceStateActive matches the documented (freeform, non-enum in
// the SDK) ReservedInstance.State value AWS returns for an active
// reservation: "payment-pending" | "active" | "payment-failed" | "retired",
// lowercase-hyphenated -- unlike most other status fields in this API, which
// are UPPER_SNAKE_CASE enums.
const reservedInstanceStateActive = "active"

// domainPackageStatusDissociating matches DomainPackageStatus's DISSOCIATING
// value (types.DomainPackageStatusDissociating). There is no terminal
// "DISSOCIATED" value in the real enum -- once dissociation completes the
// association record is gone, so the last observable status is the
// transient DISSOCIATING one, mirroring the DELETING-on-instant-removal
// pattern used elsewhere in this backend (see statusDeleting).
const domainPackageStatusDissociating = "DISSOCIATING"

// Cross-cluster connection status codes, matching the AWS
// InboundConnectionStatusCode / OutboundConnectionStatusCode enums.
const (
	connStatusPendingAcceptance = "PENDING_ACCEPTANCE"
	connStatusRejected          = "REJECTED"
)

// ConnectionMode values, matching the AWS ConnectionMode enum.
const (
	connectionModeDirect      = "DIRECT"
	connectionModeVPCEndpoint = "VPC_ENDPOINT"
)

// DomainProcessingStatus enum values, matching the AWS OpenSearch SDK
// DomainProcessingStatusType. These describe the transient lifecycle phase a
// domain is in while a create/update/upgrade/delete is being applied.
const (
	dpsCreating                = "Creating"
	dpsModifying               = "Modifying"
	dpsUpgrading               = "UpgradingEngineVersion"
	dpsUpdatingServiceSoftware = "UpdatingServiceSoftware"
	dpsDeleting                = "Deleting"
)

// statusDeleting is the transient DELETING state shared across sub-resources
// (connections, VPC endpoints, serverless collections) while a delete is in
// its processing window before the resource is finally removed.
const statusDeleting = "DELETING"

// Service software update status values, matching the AWS
// ServiceSoftwareUpdateStatus enum.
const (
	sswStatusPendingUpdate = "PENDING_UPDATE"
	sswStatusEligible      = "ELIGIBLE"
)

// DataSourceAttachmentStatus enum values, matching AWS
// types.DataSourceAttachmentStatus.
const (
	dsAttachmentStatusPending  = "PENDING"
	dsAttachmentStatusAttached = "ATTACHED"
	dsAttachmentStatusFailed   = "FAILED"
)

// dsAttachmentFailWindow is how long a PENDING attachment is allowed to wait
// for its referenced resource to become active before it is lazily resolved
// to FAILED, matching the real API's documented "not completed within 24
// hours" rule (see AttachDataSource's operation doc).
const dsAttachmentFailWindow = 24 * time.Hour

// CapabilityStatus enum values, matching AWS types.CapabilityStatus (the
// service uses lowercase_snake_case for this enum family, unlike most other
// status fields in this API).
const (
	capabilityStatusCreating = "creating"
	capabilityStatusActive   = "active"
	// capabilityStatusDeleting is the fixed value DeregisterCapabilityOutput
	// always reports, per its doc: "Returns deleting when the capability is
	// being removed" -- not a transient window like elsewhere in this
	// backend, an unconditional response value for this one op.
	capabilityStatusDeleting = "deleting"
)

// capabilityNamePattern matches the real API's CapabilityName constraint:
// 3-30 characters, alphanumeric and hyphen only.
var capabilityNamePattern = regexp.MustCompile(`^[a-zA-Z0-9-]{3,30}$`)

// MigrationStatus enum values, matching the freeform (non-Go-enum) status
// strings documented for StartMigration/GetMigration/ListMigrations.
const (
	migrationStatusPending    = "PENDING"
	migrationStatusInProgress = "IN_PROGRESS"
	migrationStatusSucceeded  = "SUCCEEDED"
)

// Repeated string literal constants.
const (
	statusDeleted                  = "DELETED"
	currencyUSD                    = "USD"
	instanceTypeR6gLarge           = "r6g.large.search"
	instanceTypeM6gLarge           = "m6g.large.search"
	instanceTypeR6gXLarge          = "r6g.xlarge.search"
	instanceTypeOR1Medium          = "or1.medium.search"
	changeProgressStub             = "no-change"
	jsonKeySourceVersion           = "SourceVersion"
	jsonKeyTargetVersions          = "TargetVersions"
	jsonKeyInstanceType            = "InstanceType"
	jsonKeyAppLogEnabled           = "AppLogEnabled"
	jsonKeyCognitoEnabled          = "CognitoEnabled"
	jsonKeyEncryptEnabled          = "EncryptionEnabled"
	jsonKeyWarmEnabled             = "WarmEnabled"
	engineVersionOpenSearch211     = "OpenSearch_2.11"
	engineVersionOpenSearch29      = "OpenSearch_2.9"
	engineVersionOpenSearch27      = "OpenSearch_2.7"
	engineVersionOpenSearch13      = "OpenSearch_1.3"
	nodeRoleData                   = "Data"
	jsonKeyAdvancedSecurityEnabled = "AdvancedSecurityEnabled"
	jsonKeyInstanceRole            = "InstanceRole"
)

// Reserved instance offering durations (seconds) and prices.
const (
	reservedDuration1Year           = 31536000
	reservedDuration3Year           = 94608000
	reservedPrice1YearAllUpfront    = 500.0
	reservedPrice1YearPartialFixed  = 300.0
	reservedPrice1YearPartialHourly = 0.05
	reservedPrice3YearNoUpfrontHrly = 0.15
)

// Default engine version applied when CreateDomain receives an empty EngineVersion.
const defaultEngineVersion = "OpenSearch_2.11"

const defaultShardsPerNode = 5

// DomainInformation identifies the source or destination domain of a
// cross-cluster connection. On the wire this nests inside a
// DomainInformationContainer under the AWSDomainInformation key (see
// types.AWSDomainInformation / types.DomainInformationContainer).
type DomainInformation struct {
	DomainName string `json:"domainName"`
	OwnerID    string `json:"ownerId,omitempty"`
	Region     string `json:"region,omitempty"`
}

// InboundConnection represents an OpenSearch inbound cross-cluster connection.
type InboundConnection struct {
	StatusUntil      time.Time         `json:"statusUntil,omitzero"`
	ConnectionID     string            `json:"connectionId"`
	ConnectionMode   string            `json:"connectionMode"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
	LocalDomainInfo  DomainInformation `json:"localDomainInfo"`
	RemoteDomainInfo DomainInformation `json:"remoteDomainInfo"`
}

// DataSource represents a data source attached to an OpenSearch domain.
type DataSource struct {
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Status         string          `json:"status,omitempty"`
	DomainName     string          `json:"-"`
	DataSourceType json.RawMessage `json:"dataSourceType,omitempty"`
}

// DirectQueryDataSource represents a direct-query data source.
type DirectQueryDataSource struct {
	// DataSourceType is stored as raw JSON for the same reason as
	// DataSource.DataSourceType above -- types.DirectQueryDataSourceType is
	// also a tagged union (CloudWatchLog / SecurityLake members).
	DataSourceType json.RawMessage `json:"dataSourceType,omitempty"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	DataSourceArn  string          `json:"dataSourceArn"`
	OpenSearchArns []string        `json:"openSearchArns"`
}

// DomainPackageDetails holds details about a package associated with a domain.
type DomainPackageDetails struct {
	PackageID   string  `json:"packageId"`
	DomainName  string  `json:"domainName"`
	State       string  `json:"state"`
	PackageName string  `json:"packageName,omitempty"`
	PackageType string  `json:"packageType,omitempty"`
	LastUpdated float64 `json:"lastUpdated,omitempty"`
}

// AuthorizedPrincipal represents an authorized principal for VPC endpoint access.
type AuthorizedPrincipal struct {
	Principal     string `json:"principal"`
	PrincipalType string `json:"principalType"`
}

// ServiceSoftwareOptions represents service software options for a domain.
type ServiceSoftwareOptions struct {
	CurrentVersion      string `json:"currentVersion"`
	NewVersion          string `json:"newVersion"`
	UpdateStatus        string `json:"updateStatus"`
	Description         string `json:"description"`
	AutomatedUpdateDate string `json:"automatedUpdateDate"`
	UpdateAvailable     bool   `json:"updateAvailable"`
	Cancellable         bool   `json:"cancellable"`
	OptionalDeployment  bool   `json:"optionalDeployment"`
}

// Application represents an OpenSearch UI application.
type Application struct {
	Tags          *tags.Tags      `json:"tags,omitempty"`
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	ARN           string          `json:"arn"`
	AppConfigs    []AppConfig     `json:"appConfigs"`
	DataSources   []AppDataSource `json:"dataSources"`
	CreatedAt     float64         `json:"createdAt"`
	LastUpdatedAt float64         `json:"lastUpdatedAt"`
}

// AppConfig represents an application configuration key-value pair.
type AppConfig struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// AppDataSource represents a data source linked to an application.
type AppDataSource struct {
	DataSourceArn string `json:"dataSourceArn"`
}

// OutboundConnection represents a cross-cluster outbound connection.
type OutboundConnection struct {
	StatusUntil      time.Time         `json:"statusUntil,omitzero"`
	ConnectionID     string            `json:"connectionId"`
	ConnectionAlias  string            `json:"connectionAlias"`
	ConnectionMode   string            `json:"connectionMode"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
	SkipUnavailable  string            `json:"skipUnavailable,omitempty"`
	Endpoint         string            `json:"endpoint,omitempty"`
	LocalDomainInfo  DomainInformation `json:"localDomainInfo"`
	RemoteDomainInfo DomainInformation `json:"remoteDomainInfo"`
}

// VpcEndpoint represents a VPC endpoint for an OpenSearch domain.
type VpcEndpoint struct {
	StatusUntil      time.Time      `json:"statusUntil,omitzero"`
	VpcOptions       map[string]any `json:"VpcOptions"`
	VpcEndpointID    string         `json:"VpcEndpointId"`
	VpcEndpointOwner string         `json:"VpcEndpointOwner"`
	DomainArn        string         `json:"DomainArn"`
	Status           string         `json:"Status"`
	Endpoint         string         `json:"Endpoint"`
}

// Package represents an OpenSearch package.
type Package struct {
	PackageSource            *PackageSource            `json:"PackageSource,omitempty"`
	PackageEncryptionOptions *PackageEncryptionOptions `json:"PackageEncryptionOptions,omitempty"`
	PackageID                string                    `json:"PackageID"`
	PackageName              string                    `json:"PackageName"`
	PackageType              string                    `json:"PackageType"`
	PackageDescription       string                    `json:"PackageDescription"`
	PackageStatus            string                    `json:"PackageStatus"`
	AvailablePackageVersion  string                    `json:"AvailablePackageVersion,omitempty"`
	VersionHistory           []*PackageVersionHistory  `json:"-"`
	CreatedAt                float64                   `json:"CreatedAt"`
}

// PackageVersionHistory records a version of a package.
type PackageVersionHistory struct {
	PackageVersion string  `json:"PackageVersion"`
	CommitMessage  string  `json:"CommitMessage"`
	CreatedAt      float64 `json:"CreatedAt"`
}

// ScheduledAction represents a scheduled action for a domain.
type ScheduledAction struct {
	ID            string  `json:"Id"`
	Type          string  `json:"Type"`
	Severity      string  `json:"Severity"`
	Description   string  `json:"Description"`
	ScheduledBy   string  `json:"ScheduledBy"`
	Status        string  `json:"Status"`
	ScheduledTime float64 `json:"ScheduledTime"`
	Mandatory     bool    `json:"Mandatory"`
	Cancellable   bool    `json:"Cancellable"`
}

// ReservedInstanceOffering is an available reserved instance offering.
type ReservedInstanceOffering struct {
	ReservedInstanceOfferingID string  `json:"ReservedInstanceOfferingId"`
	InstanceType               string  `json:"InstanceType"`
	CurrencyCode               string  `json:"CurrencyCode"`
	PaymentOption              string  `json:"PaymentOption"`
	Duration                   int     `json:"Duration"`
	FixedPrice                 float64 `json:"FixedPrice"`
	UsagePrice                 float64 `json:"UsagePrice"`
}

// ReservedInstance is a purchased reserved instance.
type ReservedInstance struct {
	ReservedInstanceID         string  `json:"ReservedInstanceId"`
	ReservedInstanceOfferingID string  `json:"ReservedInstanceOfferingId"`
	InstanceType               string  `json:"InstanceType"`
	ReservationName            string  `json:"ReservationName"`
	CurrencyCode               string  `json:"CurrencyCode"`
	PaymentOption              string  `json:"PaymentOption"`
	State                      string  `json:"State"`
	Duration                   int     `json:"Duration"`
	FixedPrice                 float64 `json:"FixedPrice"`
	UsagePrice                 float64 `json:"UsagePrice"`
	InstanceCount              int     `json:"InstanceCount"`
	StartTime                  float64 `json:"StartTime"`
}

// DomainMaintenance represents a maintenance action on a domain.
type DomainMaintenance struct {
	MaintenanceID string  `json:"MaintenanceId"`
	DomainName    string  `json:"DomainName"`
	Action        string  `json:"Action"`
	NodeID        string  `json:"NodeId,omitempty"`
	Status        string  `json:"Status"`
	StatusMessage string  `json:"StatusMessage,omitempty"`
	CreatedAt     float64 `json:"CreatedAt"`
	UpdatedAt     float64 `json:"UpdatedAt"`
}

// DomainIndex represents an OpenSearch index, including its stored documents.
type DomainIndex struct {
	Mappings map[string]any `json:"Mappings,omitempty"`
	Settings map[string]any `json:"Settings,omitempty"`
	Aliases  map[string]any `json:"Aliases,omitempty"`
	// Documents holds the real per-index document store keyed by document ID.
	Documents   map[string]map[string]any `json:"Documents,omitempty"`
	IndexName   string                    `json:"IndexName"`
	IndexStatus string                    `json:"IndexStatus"`
	// DomainName identifies the owning domain and is used only to key the
	// pkgs/store composite table (domainName#indexName); it is never
	// serialized on the wire, matching how the domain name was already
	// implied by the outer map key before the pkgs/store conversion.
	DomainName string `json:"-"`
	// DocumentCount is the number of documents currently stored in the index.
	DocumentCount int `json:"DocumentCount"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// ClusterConfig represents the cluster configuration for an OpenSearch domain.
type ClusterConfig struct {
	ZoneAwarenessConfig        *ZoneAwarenessConfig        `json:"zoneAwarenessConfig,omitempty"`
	BlueGreenDeploymentOptions *BlueGreenDeploymentOptions `json:"blueGreenDeploymentOptions,omitempty"`
	InstanceType               string                      `json:"instanceType"`
	DedicatedMasterType        string                      `json:"dedicatedMasterType,omitempty"`
	WarmType                   string                      `json:"warmType,omitempty"`
	InstanceCount              int                         `json:"instanceCount"`
	DedicatedMasterCount       int                         `json:"dedicatedMasterCount,omitempty"`
	WarmCount                  int                         `json:"warmCount,omitempty"`
	DedicatedMasterEnabled     bool                        `json:"dedicatedMasterEnabled,omitempty"`
	ZoneAwarenessEnabled       bool                        `json:"zoneAwarenessEnabled,omitempty"`
	WarmEnabled                bool                        `json:"warmEnabled,omitempty"`
	ColdStorageEnabled         bool                        `json:"coldStorageEnabled,omitempty"`
	MultiAZWithStandbyEnabled  bool                        `json:"multiAZWithStandbyEnabled,omitempty"`
}

// ZoneAwarenessConfig holds zone awareness settings.
type ZoneAwarenessConfig struct {
	AvailabilityZoneCount int `json:"availabilityZoneCount"`
}

// EBSOptions represents EBS volume settings for an OpenSearch domain.
type EBSOptions struct {
	VolumeType string `json:"volumeType,omitempty"`
	KMSKeyID   string `json:"kmsKeyId,omitempty"`
	VolumeSize int    `json:"volumeSize,omitempty"`
	IOPS       int    `json:"iops,omitempty"`
	Throughput int    `json:"throughput,omitempty"`
	EBSEnabled bool   `json:"ebsEnabled"`
}

// SnapshotOptions holds automated snapshot settings.
type SnapshotOptions struct {
	AutomatedSnapshotStartHour int `json:"automatedSnapshotStartHour"`
}

// OffPeakWindowOptions holds off-peak window settings for a domain.
type OffPeakWindowOptions struct {
	OffPeakWindow *OffPeakWindow `json:"offPeakWindow,omitempty"`
	Enabled       bool           `json:"enabled"`
}

// OffPeakWindow defines a custom start time for off-peak maintenance.
type OffPeakWindow struct {
	WindowStartTime *WindowStartTime `json:"windowStartTime,omitempty"`
}

// WindowStartTime holds hours and minutes for a maintenance window start.
type WindowStartTime struct {
	Hours   int `json:"hours"`
	Minutes int `json:"minutes"`
}

// IdentityCenterOptions holds IAM Identity Center integration settings. Field
// names match the current aws-sdk-go-v2 IdentityCenterOptions/-Input shapes
// (IdentityCenterInstanceARN/RolesKey/SubjectKey), which superseded the older
// IamIdentityCenterOptions shape (IamIdentityCenterArn/IamRoleFor...) that AWS
// no longer wires into CreateDomain/UpdateDomainConfig.
type IdentityCenterOptions struct {
	IdentityCenterInstanceARN    string `json:"identityCenterInstanceARN,omitempty"`
	IdentityCenterApplicationARN string `json:"identityCenterApplicationARN,omitempty"`
	IdentityStoreID              string `json:"identityStoreId,omitempty"`
	RolesKey                     string `json:"rolesKey,omitempty"`
	SubjectKey                   string `json:"subjectKey,omitempty"`
	EnabledAPIAccess             bool   `json:"enabledAPIAccess"`
}

// EnableSoftwareUpdateOptions holds settings for automatic software updates.
type EnableSoftwareUpdateOptions struct {
	AutoSoftwareUpdateEnabled bool `json:"autoSoftwareUpdateEnabled"`
}

// BlueGreenDeploymentOptions holds blue-green deployment settings.
type BlueGreenDeploymentOptions struct {
	Enabled bool `json:"enabled"`
}

// PackageSource holds the S3 source location for a custom package.
type PackageSource struct {
	S3BucketName string `json:"S3BucketName,omitempty"`
	S3Key        string `json:"S3Key,omitempty"`
}

// PackageEncryptionOptions holds encryption settings for a package.
type PackageEncryptionOptions struct {
	KmsKeyIdentifier  string `json:"KmsKeyIdentifier,omitempty"`
	EncryptionEnabled bool   `json:"EncryptionEnabled"`
}

// EncryptionAtRestOptions holds encryption at rest settings.
type EncryptionAtRestOptions struct {
	KMSKeyID string `json:"kmsKeyId,omitempty"`
	Enabled  bool   `json:"enabled"`
}

// NodeToNodeEncryptionOptions holds node-to-node encryption settings.
type NodeToNodeEncryptionOptions struct {
	Enabled bool `json:"enabled"`
}

// DomainEndpointOptions holds HTTPS and custom endpoint settings.
type DomainEndpointOptions struct {
	CustomEndpointCertificateArn string `json:"customEndpointCertificateArn,omitempty"`
	CustomEndpoint               string `json:"customEndpoint,omitempty"`
	TLSSecurityPolicy            string `json:"tlsSecurityPolicy,omitempty"`
	EnforceHTTPS                 bool   `json:"enforceHTTPS,omitempty"`
	CustomEndpointEnabled        bool   `json:"customEndpointEnabled,omitempty"`
}

// SAMLOptionsInput holds SAML configuration for AdvancedSecurityOptions.
type SAMLOptionsInput struct {
	IDPEntityID           string `json:"idpEntityId,omitempty"`
	IDPMetadataContent    string `json:"idpMetadataContent,omitempty"`
	RolesKey              string `json:"rolesKey,omitempty"`
	SubjectKey            string `json:"subjectKey,omitempty"`
	SessionTimeoutMinutes int    `json:"sessionTimeoutMinutes,omitempty"`
	Enabled               bool   `json:"enabled,omitempty"`
}

// AdvancedSecurityOptions holds fine-grained access control settings.
type AdvancedSecurityOptions struct {
	SAMLOptions                 *SAMLOptionsInput `json:"samlOptions,omitempty"`
	AnonymousAuthEnabled        bool              `json:"anonymousAuthEnabled,omitempty"`
	Enabled                     bool              `json:"enabled"`
	InternalUserDatabaseEnabled bool              `json:"internalUserDatabaseEnabled,omitempty"`
}

// VPCOptions holds VPC configuration for an OpenSearch domain.
type VPCOptions struct {
	VPCID            string   `json:"vpcId,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
	SubnetIDs        []string `json:"subnetIds,omitempty"`
}

// CognitoOptions holds Cognito configuration for Kibana authentication.
type CognitoOptions struct {
	IdentityPoolID string `json:"identityPoolId,omitempty"`
	RoleARN        string `json:"roleArn,omitempty"`
	UserPoolID     string `json:"userPoolId,omitempty"`
	Enabled        bool   `json:"enabled"`
}

// LogPublishingOption holds a single log type publishing configuration.
type LogPublishingOption struct {
	CloudWatchLogsLogGroupARN string `json:"cloudWatchLogsLogGroupArn,omitempty"`
	Enabled                   bool   `json:"enabled"`
}

// Domain represents an OpenSearch domain.
type Domain struct {
	ProcessingUntil             time.Time                       `json:"processingUntil,omitzero"`
	Tags                        *tags.Tags                      `json:"tags,omitempty"`
	SnapshotOptions             *SnapshotOptions                `json:"snapshotOptions,omitempty"`
	NodeToNodeEncryptionOptions *NodeToNodeEncryptionOptions    `json:"nodeToNodeEncryptionOptions,omitempty"`
	DomainEndpointOptions       *DomainEndpointOptions          `json:"domainEndpointOptions,omitempty"`
	AdvancedSecurityOptions     *AdvancedSecurityOptions        `json:"advancedSecurityOptions,omitempty"`
	VPCOptions                  *VPCOptions                     `json:"vpcOptions,omitempty"`
	CognitoOptions              *CognitoOptions                 `json:"cognitoOptions,omitempty"`
	OffPeakWindowOptions        *OffPeakWindowOptions           `json:"offPeakWindowOptions,omitempty"`
	IdentityCenterOptions       *IdentityCenterOptions          `json:"identityCenterOptions,omitempty"`
	EnableSoftwareUpdateOptions *EnableSoftwareUpdateOptions    `json:"enableSoftwareUpdateOptions,omitempty"`
	LogPublishingOptions        map[string]*LogPublishingOption `json:"logPublishingOptions,omitempty"`
	EBSOptions                  *EBSOptions                     `json:"ebsOptions,omitempty"`
	EncryptionAtRestOptions     *EncryptionAtRestOptions        `json:"encryptionAtRestOptions,omitempty"`
	ServiceSoftware             *ServiceSoftwareOptions         `json:"serviceSoftware,omitempty"`
	Name                        string                          `json:"name"`
	ARN                         string                          `json:"arn"`
	// DomainID is the AWS-format unique domain identifier ("{accountId}/{name}"),
	// a required field on DomainStatus (see aws-sdk-go-v2/service/opensearch
	// types.DomainStatus.DomainId) that real AWS always returns alongside ARN.
	DomainID         string        `json:"domainID"`
	EngineVersion    string        `json:"engineVersion"`
	Endpoint         string        `json:"endpoint"`
	Status           string        `json:"status"`
	LastChangeID     string        `json:"lastChangeID,omitempty"`
	ProcessingStatus string        `json:"processingStatus,omitempty"`
	AccessPolicies   string        `json:"accessPolicies,omitempty"`
	ClusterConfig    ClusterConfig `json:"clusterConfig"`
	Created          bool          `json:"created,omitempty"`
	Deleted          bool          `json:"deleted,omitempty"`
}

// CreateDomainInput holds all options for creating a new OpenSearch domain.
type CreateDomainInput struct {
	EBSOptions                  *EBSOptions
	SnapshotOptions             *SnapshotOptions
	EncryptionAtRestOptions     *EncryptionAtRestOptions
	NodeToNodeEncryptionOptions *NodeToNodeEncryptionOptions
	DomainEndpointOptions       *DomainEndpointOptions
	AdvancedSecurityOptions     *AdvancedSecurityOptions
	VPCOptions                  *VPCOptions
	CognitoOptions              *CognitoOptions
	OffPeakWindowOptions        *OffPeakWindowOptions
	IdentityCenterOptions       *IdentityCenterOptions
	EnableSoftwareUpdateOptions *EnableSoftwareUpdateOptions
	LogPublishingOptions        map[string]*LogPublishingOption
	Tags                        map[string]string
	Name                        string
	EngineVersion               string
	AccessPolicies              string
	ClusterConfig               ClusterConfig
}

// UpdateDomainConfigInput holds mutable fields for UpdateDomainConfig.
type UpdateDomainConfigInput struct {
	EBSOptions                  *EBSOptions
	SnapshotOptions             *SnapshotOptions
	EncryptionAtRestOptions     *EncryptionAtRestOptions
	NodeToNodeEncryptionOptions *NodeToNodeEncryptionOptions
	DomainEndpointOptions       *DomainEndpointOptions
	AdvancedSecurityOptions     *AdvancedSecurityOptions
	VPCOptions                  *VPCOptions
	CognitoOptions              *CognitoOptions
	OffPeakWindowOptions        *OffPeakWindowOptions
	IdentityCenterOptions       *IdentityCenterOptions
	EnableSoftwareUpdateOptions *EnableSoftwareUpdateOptions
	LogPublishingOptions        map[string]*LogPublishingOption
	ClusterConfig               *ClusterConfig
	AccessPolicies              string
	EngineVersion               string
}

// RollbackServiceSoftwareOptions represents the result of a
// RollbackServiceSoftwareUpdate call, matching
// types.RollbackServiceSoftwareOptions (PascalCase wire keys, same convention
// as ServiceSoftwareOptions/serviceSoftwareOptionsJSON above).
type RollbackServiceSoftwareOptions struct {
	CurrentVersion    string `json:"currentVersion"`
	NewVersion        string `json:"newVersion"`
	Description       string `json:"description"`
	RollbackAvailable bool   `json:"rollbackAvailable"`
}

// DataSourceAttachment represents an attachment of a real data source (an
// OpenSearch domain or an OpenSearch Serverless collection, identified by
// ARN) to an OpenSearch UI application. Matches
// types.DataSourceAttachmentSummary plus the identity fields
// (AttachmentId/DataSourceArn/Status) shared by Attach/Detach/
// DescribeDataSourceAttachment's outputs.
type DataSourceAttachment struct {
	CreatedAt     time.Time `json:"-"`
	AttachmentID  string    `json:"attachmentId"`
	ApplicationID string    `json:"applicationId"`
	DataSourceArn string    `json:"dataSourceArn"`
	Status        string    `json:"status"`
}

// Capability represents a registered capability (currently only AI Assistant,
// capabilityName "ai-capability") on an OpenSearch UI application. Matches
// types.CapabilityStatus / the ApplicationId+CapabilityName+Status fields
// shared by RegisterCapability/DeregisterCapability/GetCapability's outputs.
// CapabilityConfig itself is not modeled as stored state: the only union
// member the SDK defines (types.AIConfig) is an empty struct with no fields
// at all, so there is nothing beyond the name/status to persist -- see
// handler_capabilities.go.
type Capability struct {
	StatusUntil    time.Time `json:"statusUntil,omitzero"`
	ApplicationID  string    `json:"applicationId"`
	CapabilityName string    `json:"capabilityName"`
	Status         string    `json:"status"`
}

// Migration represents a saved-object migration job from a data source into
// an OpenSearch application workspace. Matches types.MigrationSummary.
// ExportedCount/ImportedCount are always 0: this emulator has no saved-object
// store (dashboards/visualizations/index-patterns) to actually migrate, so
// reporting non-zero counts would be fabricated. Status still genuinely
// transitions PENDING -> IN_PROGRESS -> SUCCEEDED against the backend's clock
// (see resolveMigrationStatus in migrations.go), it just always migrates zero
// objects -- an honest "nothing to migrate" result rather than invented data.
type Migration struct {
	CreatedAt     time.Time `json:"-"`
	UpdatedAt     time.Time `json:"-"`
	MigrationID   string    `json:"migrationId"`
	ApplicationID string    `json:"applicationId"`
	SourceArn     string    `json:"sourceArn"`
	Status        string    `json:"status"`
	ExportedCount int       `json:"exportedCount"`
	ImportedCount int       `json:"importedCount"`
}

// Workspace tracks the target-workspace side effect of AttachDataSource's
// optional WorkspaceConfiguration/WorkspaceId (types.WorkspaceConfigurationInput)
// and StartMigration's required MigrationOptions.Workspace
// (types.MigrationWorkspace). AWS defines no independent workspace resource
// API anywhere in this SDK: grepping every api_op_*.go in
// aws-sdk-go-v2/service/opensearch@v1.75.4 for "Workspace" turns up only
// these two request-side fields, there is no CreateWorkspace/GetWorkspace/
// ListWorkspaces/DeleteWorkspace operation, and no output struct in the
// entire service (not AttachDataSourceOutput, not
// DescribeDataSourceAttachmentOutput, not GetMigrationOutput/
// MigrationSummary) ever echoes a WorkspaceId back to the caller. This type
// exists purely so a WorkspaceId reference on either op can be validated
// against something real (existence, and that it belongs to the referencing
// application) instead of accepted as any string -- it is never surfaced
// through any handler response, matching what the SDK actually defines. See
// PARITY.md for why this stops short of a full CRUD resource model.
type Workspace struct {
	CreatedAt     time.Time `json:"-"`
	WorkspaceID   string    `json:"workspaceId"`
	ApplicationID string    `json:"applicationId"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
}

// DryRunStatus holds dry-run progress state for a domain.
type DryRunStatus struct {
	DryRunID           string           `json:"DryRunId"`
	DryRunStatus       string           `json:"DryRunStatus"`
	CreationDate       string           `json:"CreationDate"`
	UpdateDate         string           `json:"UpdateDate"`
	DomainName         string           `json:"-"`
	ValidationFailures []map[string]any `json:"ValidationFailures"`
}
