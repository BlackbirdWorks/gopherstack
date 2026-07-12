package opensearch

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Errors returned by the OpenSearch backend.
var (
	ErrDomainNotFound           = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists      = errors.New("ResourceAlreadyExistsException")
	ErrInvalidParameter         = errors.New("ValidationException")
	ErrValidation               = errors.New("ValidationException")
	ErrConnectionNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceAlreadyExists  = errors.New("ResourceAlreadyExistsException")
	ErrPackageNotFound          = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound      = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists = errors.New("ResourceAlreadyExistsException")
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

// InboundConnection represents an OpenSearch inbound cross-cluster connection.
type InboundConnection struct {
	StatusUntil  time.Time `json:"statusUntil,omitzero"`
	ConnectionID string    `json:"connectionId"`
	Status       string    `json:"status"`
}

// DataSource represents a data source attached to an OpenSearch domain.
type DataSource struct {
	Name           string `json:"name"`
	Description    string `json:"description"`
	DataSourceType string `json:"dataSourceType"`
	// DomainName identifies the owning domain and is used only to key the
	// pkgs/store composite table (domainName#name); it is never serialized on
	// the wire, matching how the domain name was already implied by the
	// outer map key before the pkgs/store conversion.
	DomainName string `json:"-"`
}

// DirectQueryDataSource represents a direct-query data source.
type DirectQueryDataSource struct {
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	DataSourceType string   `json:"dataSourceType"`
	DataSourceArn  string   `json:"dataSourceArn"`
	OpenSearchArns []string `json:"openSearchArns"`
}

// DomainPackageDetails holds details about a package associated with a domain.
type DomainPackageDetails struct {
	PackageID  string `json:"packageId"`
	DomainName string `json:"domainName"`
	State      string `json:"state"`
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
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	ARN         string          `json:"arn"`
	AppConfigs  []AppConfig     `json:"appConfigs"`
	DataSources []AppDataSource `json:"dataSources"`
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
	StatusUntil      time.Time      `json:"statusUntil,omitzero"`
	LocalDomainInfo  map[string]any `json:"LocalDomainInfo"`
	RemoteDomainInfo map[string]any `json:"RemoteDomainInfo"`
	ConnectionID     string         `json:"ConnectionId"`
	ConnectionAlias  string         `json:"ConnectionAlias"`
	Status           string         `json:"status"`
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

// AppSetting is a key-value pair for default application settings.
type AppSetting struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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

// InMemoryBackend is the in-memory store for OpenSearch domains.
//
// Most resource collections are *store.Table[T] registered on b.registry --
// see store_setup.go's registerAllTables doc for the full clean/dirty split
// and the reasoning behind the handful of fields left as plain maps.
type InMemoryBackend struct {
	dnsRegistrar              DNSRegistrar
	dryRuns                   *store.Table[DryRunStatus]
	reservedInstances         *store.Table[ReservedInstance]
	inboundConnections        *store.Table[InboundConnection]
	outboundConnections       *store.Table[OutboundConnection]
	domainDataSources         *store.Table[DataSource]
	domainDataSourcesByDomain *store.Index[DataSource]
	directQueryDataSources    *store.Table[DirectQueryDataSource]
	domains                   *store.Table[Domain]
	domainsByARN              *store.Index[Domain]
	vpcAuthorizations         map[string][]AuthorizedPrincipal
	vpcEndpoints              *store.Table[VpcEndpoint]
	applications              *store.Table[Application]
	applicationsByName        *store.Index[Application]
	packages                  *store.Table[Package]
	scheduledActions          map[string][]*ScheduledAction
	packageAssociations       map[string]map[string]bool
	domainMaintenances        map[string][]*DomainMaintenance
	domainIndexes             *store.Table[DomainIndex]
	domainIndexesByDomain     *store.Index[DomainIndex]
	upgradeHistory            map[string][]*UpgradeHistory
	domainPackages            map[string]map[string]bool
	autoTunes                 *store.Table[AutoTuneConfig]
	slNetworkPolicies         *store.Table[ServerlessNetworkPolicy]
	slCollections             *store.Table[ServerlessCollection]
	slAccessPolicies          *store.Table[ServerlessAccessPolicy]
	slSecurityConfigs         *store.Table[ServerlessSecurityConfig]
	slEncryptionPolicies      *store.Table[ServerlessEncryptionPolicy]
	defaultAppSettings        map[string][]AppSetting
	registry                  *store.Registry
	mu                        *lockmetrics.RWMutex
	now                       func() time.Time
	accountID                 string
	region                    string
	processingDelay           time.Duration
	appIDCounter              int
	connCounter               int
	vpcEndpointCounter        int
	packageCounter            int
	maintenanceCounter        int
	reservedCounter           int
	slCollCounter             int
	slSecConfigCounter        int
	docCounter                int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		packageAssociations: make(map[string]map[string]bool),
		domainPackages:      make(map[string]map[string]bool),
		vpcAuthorizations:   make(map[string][]AuthorizedPrincipal),
		scheduledActions:    make(map[string][]*ScheduledAction),
		domainMaintenances:  make(map[string][]*DomainMaintenance),
		upgradeHistory:      make(map[string][]*UpgradeHistory),
		defaultAppSettings:  make(map[string][]AppSetting),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("opensearch"),
		registry:            store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// SetDNSRegistrar wires a DNS server so OpenSearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = dns
}

// CreateDomain creates a new OpenSearch domain.
func (b *InMemoryBackend) CreateDomain(input CreateDomainInput) (*Domain, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	// Finalise any domains whose deleting window has elapsed so the name frees up.
	b.purgeExpiredDomainsLocked()

	if b.domains.Has(input.Name) {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, input.Name)
	}

	if input.EngineVersion == "" {
		input.EngineVersion = defaultEngineVersion
	}

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+input.Name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", input.Name, b.accountID, b.region)
	domainID := fmt.Sprintf("%s/%s", b.accountID, input.Name)

	if input.ClusterConfig.InstanceCount == 0 {
		input.ClusterConfig.InstanceCount = 1
	}

	if input.ClusterConfig.InstanceType == "" {
		input.ClusterConfig.InstanceType = instanceTypeT3Small
	}

	d := &Domain{
		Name:                        input.Name,
		ARN:                         domainARN,
		DomainID:                    domainID,
		EngineVersion:               input.EngineVersion,
		Endpoint:                    endpoint,
		Status:                      "Active",
		ClusterConfig:               input.ClusterConfig,
		Tags:                        tags.New("opensearch." + input.Name + ".tags"),
		EBSOptions:                  input.EBSOptions,
		SnapshotOptions:             input.SnapshotOptions,
		EncryptionAtRestOptions:     input.EncryptionAtRestOptions,
		NodeToNodeEncryptionOptions: input.NodeToNodeEncryptionOptions,
		DomainEndpointOptions:       input.DomainEndpointOptions,
		AdvancedSecurityOptions:     input.AdvancedSecurityOptions,
		VPCOptions:                  input.VPCOptions,
		CognitoOptions:              input.CognitoOptions,
		OffPeakWindowOptions:        input.OffPeakWindowOptions,
		IdentityCenterOptions:       input.IdentityCenterOptions,
		EnableSoftwareUpdateOptions: input.EnableSoftwareUpdateOptions,
		LogPublishingOptions:        input.LogPublishingOptions,
		AccessPolicies:              input.AccessPolicies,
	}

	if len(input.Tags) > 0 {
		d.Tags.Merge(input.Tags)
	}

	d.Created = true
	b.beginProcessing(d, dpsCreating)

	b.domains.Put(d)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *d

	return &cp, nil
}

// DeleteDomain removes a domain by name and cleans up all associated resources.
//
// When processingDelay is 0 the domain (and all its scoped resources) is removed
// immediately and the returned status reports Deleted:true / Deleting. When a
// delay is configured the domain enters a real, observable Deleting window: it
// remains describable with Deleted:true until the window elapses, after which it
// is cascaded away lazily on the next write.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	b.purgeExpiredDomainsLocked()

	d, exists := b.domains.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	d.Deleted = true
	b.beginProcessing(d, dpsDeleting)
	cp := *d

	if b.processingDelay == 0 {
		b.removeDomainLocked(name)
	}

	return &cp, nil
}

// DescribeDomain returns details about a domain. A domain whose deleting window
// has elapsed is reported as not found.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d

	return &cp, nil
}

// ListDomainNames returns the names of all live domains in sorted order.
func (b *InMemoryBackend) ListDomainNames() []string {
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	now := b.clock()
	names := make([]string, 0, b.domains.Len())

	for _, d := range b.domains.All() {
		if deleteWindowElapsed(d, now) {
			continue
		}

		names = append(names, d.Name)
	}

	slices.Sort(names)

	return names
}

// findDomainByARN returns the domain matching the given ARN, or nil if not found.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findDomainByARN(domainARN string) *Domain {
	matches := b.domainsByARN.Get(domainARN)
	if len(matches) == 0 {
		return nil
	}

	return matches[0]
}

// ListTags returns tags for the domain identified by ARN.
func (b *InMemoryBackend) ListTags(domainARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return nil, fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	return d.Tags.Clone(), nil
}

// AddTags adds or updates tags on the domain identified by ARN.
func (b *InMemoryBackend) AddTags(domainARN string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain identified by ARN.
func (b *InMemoryBackend) RemoveTags(domainARN string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.DeleteKeys(keys)

	return nil
}

// AcceptInboundConnection accepts an inbound cross-cluster connection by ID.
func (b *InMemoryBackend) AcceptInboundConnection(connectionID string) (*InboundConnection, error) {
	if connectionID == "" {
		return nil, fmt.Errorf("%w: ConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.Status = connectionStatusActive

	cp := *conn

	return &cp, nil
}

// AddDataSource adds a data source to a domain.
func (b *InMemoryBackend) AddDataSource(
	domainName, name, description, dataSourceType string,
) (string, error) {
	if domainName == "" {
		return "", fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if name == "" {
		return "", fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDataSource")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return "", fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if b.domainDataSources.Has(dataSourceKey(domainName, name)) {
		return "", fmt.Errorf(
			"%w: data source %s already exists on domain %s",
			ErrDataSourceAlreadyExists,
			name,
			domainName,
		)
	}

	b.domainDataSources.Put(&DataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
		DomainName:     domainName,
	})

	return "Data source created successfully", nil
}

// AddDirectQueryDataSource adds a direct-query data source.
func (b *InMemoryBackend) AddDirectQueryDataSource(
	name, description, dataSourceType string,
	openSearchArns []string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: DataSourceName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDirectQueryDataSource")
	defer b.mu.Unlock()

	if b.directQueryDataSources.Has(name) {
		return "", fmt.Errorf(
			"%w: direct query data source %s already exists",
			ErrDataSourceAlreadyExists,
			name,
		)
	}

	dsARN := arn.Build("opensearch", b.region, b.accountID, "directQueryDataSource/"+name)
	b.directQueryDataSources.Put(&DirectQueryDataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
		OpenSearchArns: openSearchArns,
		DataSourceArn:  dsARN,
	})

	return dsARN, nil
}

// AssociatePackage associates a package with a domain.
func (b *InMemoryBackend) AssociatePackage(
	packageID, domainName string,
) (*DomainPackageDetails, error) {
	if packageID == "" {
		return nil, fmt.Errorf("%w: PackageID is required", ErrInvalidParameter)
	}

	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	if !b.packages.Has(packageID) {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.addPackageAssociation(packageID, domainName)

	return &DomainPackageDetails{
		PackageID:  packageID,
		DomainName: domainName,
		State:      pkgStateActive,
	}, nil
}

// addPackageAssociation records a package↔domain association in both the
// forward (packageAssociations) and reverse (domainPackages) indexes.
// Caller must hold the write lock.
func (b *InMemoryBackend) addPackageAssociation(packageID, domainName string) {
	if b.packageAssociations[packageID] == nil {
		b.packageAssociations[packageID] = make(map[string]bool)
	}
	b.packageAssociations[packageID][domainName] = true

	if b.domainPackages[domainName] == nil {
		b.domainPackages[domainName] = make(map[string]bool)
	}
	b.domainPackages[domainName][packageID] = true
}

// removePackageAssociation removes a package↔domain association from both the
// forward and reverse indexes. Caller must hold the write lock.
func (b *InMemoryBackend) removePackageAssociation(packageID, domainName string) {
	if domains, ok := b.packageAssociations[packageID]; ok {
		delete(domains, domainName)
		if len(domains) == 0 {
			delete(b.packageAssociations, packageID)
		}
	}

	if pkgs, ok := b.domainPackages[domainName]; ok {
		delete(pkgs, packageID)
		if len(pkgs) == 0 {
			delete(b.domainPackages, domainName)
		}
	}
}

// AssociatePackages associates multiple packages with a domain.
func (b *InMemoryBackend) AssociatePackages(
	domainName string,
	packageIDs []string,
) ([]DomainPackageDetails, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if len(packageIDs) == 0 {
		return nil, fmt.Errorf("%w: PackageList must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AssociatePackages")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	results := make([]DomainPackageDetails, 0, len(packageIDs))

	for _, pkgID := range packageIDs {
		if !b.packages.Has(pkgID) {
			return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, pkgID)
		}

		b.addPackageAssociation(pkgID, domainName)
		results = append(results, DomainPackageDetails{
			PackageID:  pkgID,
			DomainName: domainName,
			State:      pkgStateActive,
		})
	}

	return results, nil
}

// AuthorizeVpcEndpointAccess grants VPC endpoint access for an account or service.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(
	domainName, account, service string,
) (*AuthorizedPrincipal, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	principal := account
	principalType := "AWS_ACCOUNT"

	if service != "" {
		principal = service
		principalType = "AWS_SERVICE"
	}

	p := AuthorizedPrincipal{
		Principal:     principal,
		PrincipalType: principalType,
	}
	b.vpcAuthorizations[domainName] = append(b.vpcAuthorizations[domainName], p)

	return &p, nil
}

// CancelDomainConfigChange cancels a pending configuration change on a domain.
func (b *InMemoryBackend) CancelDomainConfigChange(
	domainName string,
	dryRun bool,
) ([]string, bool, error) {
	if domainName == "" {
		return nil, false, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelDomainConfigChange")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, false, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	var cancelledChangeIDs []string

	if d.LastChangeID != "" {
		cancelledChangeIDs = append(cancelledChangeIDs, d.LastChangeID)
		d.LastChangeID = ""
	}

	return cancelledChangeIDs, dryRun, nil
}

// CancelServiceSoftwareUpdate cancels a pending service software update.
//
// AWS only permits cancellation while an update is actually scheduled
// (UpdateStatus == PENDING_UPDATE). When nothing is scheduled it rejects the
// request, so this mutates the stored software-update state and returns a
// ValidationException in that case rather than a canned success envelope.
func (b *InMemoryBackend) CancelServiceSoftwareUpdate(
	domainName string,
) (*ServiceSoftwareOptions, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelServiceSoftwareUpdate")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if d.ServiceSoftware == nil || d.ServiceSoftware.UpdateStatus != sswStatusPendingUpdate {
		return nil, fmt.Errorf(
			"%w: domain %s has no service software update in a cancellable (PENDING_UPDATE) state",
			ErrValidation,
			domainName,
		)
	}

	// The scheduled install is cancelled, but the newer version remains
	// available, so the domain returns to the ELIGIBLE state.
	d.ServiceSoftware.UpdateStatus = sswStatusEligible
	d.ServiceSoftware.Cancellable = false
	d.ServiceSoftware.UpdateAvailable = true
	d.ServiceSoftware.NewVersion = defaultEngineVersion
	d.ServiceSoftware.Description = "Cancellation complete. A new version is available to install."

	// Clear the software-update processing window opened by StartServiceSoftwareUpdate.
	if d.ProcessingStatus == dpsUpdatingServiceSoftware {
		d.ProcessingStatus = ""
		d.ProcessingUntil = time.Time{}
	}

	cp := *d.ServiceSoftware

	return &cp, nil
}

// CreateApplication creates an OpenSearch UI application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if len(b.applicationsByName.Get(name)) > 0 {
		return nil, fmt.Errorf(
			"%w: application %s already exists",
			ErrApplicationAlreadyExists,
			name,
		)
	}

	b.appIDCounter++
	id := fmt.Sprintf("app-%d", b.appIDCounter)
	appARN := arn.Build("opensearch", b.region, b.accountID, "application/"+id)

	if appConfigs == nil {
		appConfigs = []AppConfig{}
	}

	if dataSources == nil {
		dataSources = []AppDataSource{}
	}

	app := &Application{
		ID:          id,
		Name:        name,
		ARN:         appARN,
		AppConfigs:  appConfigs,
		DataSources: dataSources,
	}
	b.applications.Put(app)

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// Reset clears all backend state, releasing any resources held.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains.All() {
		d.Tags.Close()
	}

	// "Clean" tables registered on b.registry (see store_setup.go).
	b.registry.ResetAll()

	// "Dirty" tables, not registered on b.registry (see store_setup.go).
	b.dryRuns.Reset()
	b.autoTunes.Reset()
	b.domainDataSources.Reset()
	b.domainIndexes.Reset()

	// Plain maps left unconverted (see store_setup.go's registerAllTables doc).
	b.packageAssociations = make(map[string]map[string]bool)
	b.domainPackages = make(map[string]map[string]bool)
	b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	b.scheduledActions = make(map[string][]*ScheduledAction)
	b.domainMaintenances = make(map[string][]*DomainMaintenance)
	b.upgradeHistory = make(map[string][]*UpgradeHistory)
	b.defaultAppSettings = make(map[string][]AppSetting)

	b.appIDCounter = 0
	b.connCounter = 0
	b.vpcEndpointCounter = 0
	b.packageCounter = 0
	b.maintenanceCounter = 0
	b.reservedCounter = 0
	b.slCollCounter = 0
	b.slSecConfigCounter = 0
	b.docCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string {
	b.mu.RLock("Region")
	defer b.mu.RUnlock()

	return b.region
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// CreateOutboundConnection creates a new outbound cross-cluster connection.
func (b *InMemoryBackend) CreateOutboundConnection(
	connectionAlias string,
	localDomainInfo, remoteDomainInfo map[string]any,
) (*OutboundConnection, error) {
	b.mu.Lock("CreateOutboundConnection")
	defer b.mu.Unlock()

	b.connCounter++
	id := fmt.Sprintf("co-%d", b.connCounter)

	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  connectionAlias,
		LocalDomainInfo:  localDomainInfo,
		RemoteDomainInfo: remoteDomainInfo,
		Status:           connectionStatusActive,
	}
	b.outboundConnections.Put(conn)

	cp := *conn

	return &cp, nil
}

// DescribeOutboundConnections returns all outbound connections, excluding any
// whose deleting window has elapsed.
func (b *InMemoryBackend) DescribeOutboundConnections() []*OutboundConnection {
	b.mu.RLock("DescribeOutboundConnections")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*OutboundConnection, 0, b.outboundConnections.Len())

	for _, c := range b.outboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	return out
}

// DeleteOutboundConnection removes an outbound connection by ID. With a
// processing delay configured the connection first enters an observable DELETING
// window before it is finally removed.
func (b *InMemoryBackend) DeleteOutboundConnection(
	connectionID string,
) (*OutboundConnection, error) {
	b.mu.Lock("DeleteOutboundConnection")
	defer b.mu.Unlock()

	b.purgeExpiredOutboundLocked()

	conn, exists := b.outboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: outbound connection %s not found",
			ErrConnectionNotFound,
			connectionID,
		)
	}

	if b.processingDelay == 0 {
		cp := *conn
		cp.Status = statusDeleting
		b.outboundConnections.Delete(connectionID)

		return &cp, nil
	}

	conn.Status = statusDeleting
	conn.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *conn

	return &cp, nil
}

// purgeExpiredOutboundLocked removes outbound connections past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredOutboundLocked() {
	now := b.clock()
	// Table.All returns a fresh slice, so deleting from the table while
	// ranging over it here is safe.
	for _, c := range b.outboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			b.outboundConnections.Delete(c.ConnectionID)
		}
	}
}

// RejectInboundConnection sets an inbound connection status to REJECTED.
func (b *InMemoryBackend) RejectInboundConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("RejectInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: inbound connection %s not found",
			ErrConnectionNotFound,
			connectionID,
		)
	}

	conn.Status = "REJECTED"
	cp := *conn

	return &cp, nil
}

// DeleteInboundConnection removes an inbound connection by ID. With a processing
// delay configured the connection first enters an observable DELETING window.
func (b *InMemoryBackend) DeleteInboundConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("DeleteInboundConnection")
	defer b.mu.Unlock()

	b.purgeExpiredInboundLocked()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return &InboundConnection{ConnectionID: connectionID, Status: statusDeleting}, nil
	}

	if b.processingDelay == 0 {
		cp := *conn
		cp.Status = statusDeleting
		b.inboundConnections.Delete(connectionID)

		return &cp, nil
	}

	conn.Status = statusDeleting
	conn.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *conn

	return &cp, nil
}

// purgeExpiredInboundLocked removes inbound connections past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredInboundLocked() {
	now := b.clock()
	for _, c := range b.inboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			b.inboundConnections.Delete(c.ConnectionID)
		}
	}
}

// DescribeInboundConnections returns all inbound connections, excluding any
// whose deleting window has elapsed.
func (b *InMemoryBackend) DescribeInboundConnections() []*InboundConnection {
	b.mu.RLock("DescribeInboundConnections")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*InboundConnection, 0, b.inboundConnections.Len())

	for _, c := range b.inboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	return out
}

// CreateVpcEndpoint creates a new VPC endpoint.
func (b *InMemoryBackend) CreateVpcEndpoint(
	domainArn string,
	vpcOptions map[string]any,
) (*VpcEndpoint, error) {
	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	b.vpcEndpointCounter++
	id := fmt.Sprintf("vpce-%d", b.vpcEndpointCounter)

	ep := &VpcEndpoint{
		VpcEndpointID:    id,
		VpcEndpointOwner: b.accountID,
		DomainArn:        domainArn,
		Status:           pkgStateActive,
		Endpoint:         fmt.Sprintf("%s.vpc.es.amazonaws.com", id),
		VpcOptions:       vpcOptions,
	}
	b.vpcEndpoints.Put(ep)

	cp := *ep

	return &cp, nil
}

// DescribeVpcEndpoints returns matching VPC endpoints and errors for not-found IDs.
func (b *InMemoryBackend) DescribeVpcEndpoints(ids []string) ([]*VpcEndpoint, []map[string]any) {
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	now := b.clock()

	var endpoints []*VpcEndpoint
	var errs []map[string]any

	for _, id := range ids {
		ep, exists := b.vpcEndpoints.Get(id)
		if !exists || statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			errs = append(errs, map[string]any{
				"VpcEndpointId": id,
				"ErrorCode":     "EndpointNotFound",
				"ErrorMessage":  fmt.Sprintf("VPC endpoint %s not found", id),
			})

			continue
		}

		cp := *ep
		endpoints = append(endpoints, &cp)
	}

	if endpoints == nil {
		endpoints = []*VpcEndpoint{}
	}

	if errs == nil {
		errs = []map[string]any{}
	}

	return endpoints, errs
}

// UpdateVpcEndpoint updates the VPC options for a VPC endpoint.
func (b *InMemoryBackend) UpdateVpcEndpoint(
	id string,
	vpcOptions map[string]any,
) (*VpcEndpoint, error) {
	b.mu.Lock("UpdateVpcEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.vpcEndpoints.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	ep.VpcOptions = vpcOptions
	cp := *ep

	return &cp, nil
}

// DeleteVpcEndpoint removes a VPC endpoint by ID. With a processing delay
// configured the endpoint first enters an observable DELETING window before it
// is finally removed.
func (b *InMemoryBackend) DeleteVpcEndpoint(id string) (*VpcEndpoint, error) {
	b.mu.Lock("DeleteVpcEndpoint")
	defer b.mu.Unlock()

	b.purgeExpiredVpcEndpointsLocked()

	ep, exists := b.vpcEndpoints.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	if b.processingDelay == 0 {
		cp := *ep
		cp.Status = statusDeleting
		b.vpcEndpoints.Delete(id)

		return &cp, nil
	}

	ep.Status = statusDeleting
	ep.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *ep

	return &cp, nil
}

// purgeExpiredVpcEndpointsLocked removes VPC endpoints past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredVpcEndpointsLocked() {
	now := b.clock()
	for _, ep := range b.vpcEndpoints.All() {
		if statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			b.vpcEndpoints.Delete(ep.VpcEndpointID)
		}
	}
}

// ListVpcEndpoints returns all VPC endpoints, excluding any whose deleting
// window has elapsed.
func (b *InMemoryBackend) ListVpcEndpoints() []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*VpcEndpoint, 0, b.vpcEndpoints.Len())

	for _, ep := range b.vpcEndpoints.All() {
		if statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			continue
		}

		cp := *ep
		out = append(out, &cp)
	}

	return out
}

// ListVpcEndpointsForDomain returns VPC endpoints associated with a domain ARN,
// excluding any whose deleting window has elapsed.
func (b *InMemoryBackend) ListVpcEndpointsForDomain(domainArn string) []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpointsForDomain")
	defer b.mu.RUnlock()

	now := b.clock()

	var out []*VpcEndpoint

	for _, ep := range b.vpcEndpoints.All() {
		if ep.DomainArn == domainArn && !statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			cp := *ep
			out = append(out, &cp)
		}
	}

	if out == nil {
		out = []*VpcEndpoint{}
	}

	return out
}

// RevokeVpcEndpointAccess removes a principal from the VPC authorizations for a domain.
func (b *InMemoryBackend) RevokeVpcEndpointAccess(domainName, account string) error {
	b.mu.Lock("RevokeVpcEndpointAccess")
	defer b.mu.Unlock()

	principals := b.vpcAuthorizations[domainName]
	filtered := principals[:0]

	for _, p := range principals {
		if p.Principal != account {
			filtered = append(filtered, p)
		}
	}

	b.vpcAuthorizations[domainName] = filtered

	return nil
}

// ListVpcEndpointAccess returns authorized principals for a domain.
func (b *InMemoryBackend) ListVpcEndpointAccess(domainName string) ([]AuthorizedPrincipal, error) {
	b.mu.RLock("ListVpcEndpointAccess")
	defer b.mu.RUnlock()

	principals := b.vpcAuthorizations[domainName]
	out := make([]AuthorizedPrincipal, len(principals))
	copy(out, principals)

	return out, nil
}

// CreatePackage creates a new OpenSearch package.
func (b *InMemoryBackend) CreatePackage(
	name, pkgType, description string,
	source *PackageSource,
	encryptionOptions *PackageEncryptionOptions,
) (*Package, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PackageName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePackage")
	defer b.mu.Unlock()

	b.packageCounter++
	id := fmt.Sprintf("F%d", b.packageCounter)
	now := float64(time.Now().Unix())

	pkg := &Package{
		PackageID:                id,
		PackageName:              name,
		PackageType:              pkgType,
		PackageDescription:       description,
		PackageStatus:            pkgStateActive,
		PackageSource:            source,
		PackageEncryptionOptions: encryptionOptions,
		AvailablePackageVersion:  "1",
		CreatedAt:                now,
		VersionHistory: []*PackageVersionHistory{
			{
				PackageVersion: "1",
				CommitMessage:  "initial version",
				CreatedAt:      now,
			},
		},
	}
	b.packages.Put(pkg)

	cp := *pkg

	return &cp, nil
}

// DeletePackage removes a package by ID.
func (b *InMemoryBackend) DeletePackage(packageID string) (*Package, error) {
	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packages.Get(packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg
	b.packages.Delete(packageID)

	return &cp, nil
}

// DescribePackages returns packages matching the given IDs, or all packages if ids is empty.
func (b *InMemoryBackend) DescribePackages(ids []string) ([]*Package, error) {
	b.mu.RLock("DescribePackages")
	defer b.mu.RUnlock()

	if len(ids) == 0 {
		out := make([]*Package, 0, b.packages.Len())
		for _, pkg := range b.packages.All() {
			cp := *pkg
			out = append(out, &cp)
		}

		return out, nil
	}

	out := make([]*Package, 0, len(ids))

	for _, id := range ids {
		pkg, exists := b.packages.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, id)
		}

		cp := *pkg
		out = append(out, &cp)
	}

	return out, nil
}

// GetPackageVersionHistory returns the version history for a package.
func (b *InMemoryBackend) GetPackageVersionHistory(
	packageID string,
) ([]*PackageVersionHistory, error) {
	b.mu.RLock("GetPackageVersionHistory")
	defer b.mu.RUnlock()

	pkg, exists := b.packages.Get(packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	out := make([]*PackageVersionHistory, len(pkg.VersionHistory))
	for i, vh := range pkg.VersionHistory {
		cp := *vh
		out[i] = &cp
	}

	return out, nil
}

// UpdatePackage updates a package's description and adds a version history entry.
func (b *InMemoryBackend) UpdatePackage(packageID, description string) (*Package, error) {
	b.mu.Lock("UpdatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packages.Get(packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	pkg.PackageDescription = description
	pkg.VersionHistory = append(pkg.VersionHistory, &PackageVersionHistory{
		PackageVersion: strconv.Itoa(len(pkg.VersionHistory) + 1),
		CommitMessage:  "updated",
		CreatedAt:      float64(time.Now().Unix()),
	})

	cp := *pkg

	return &cp, nil
}

// UpdatePackageScope is a no-op that returns the package (scope is not tracked in-memory).
func (b *InMemoryBackend) UpdatePackageScope(packageID, _ string, _ []string) (*Package, error) {
	b.mu.RLock("UpdatePackageScope")
	defer b.mu.RUnlock()

	pkg, exists := b.packages.Get(packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg

	return &cp, nil
}

// ListPackagesForDomain returns packages associated with a domain.
func (b *InMemoryBackend) ListPackagesForDomain(domainName string) []*Package {
	b.mu.RLock("ListPackagesForDomain")
	defer b.mu.RUnlock()

	var out []*Package

	for pkgID := range b.domainPackages[domainName] {
		pkg, exists := b.packages.Get(pkgID)
		if exists {
			cp := *pkg
			out = append(out, &cp)
		}
	}

	if out == nil {
		out = []*Package{}
	}

	return out
}

// ListDomainsForPackage returns domain names associated with a package.
func (b *InMemoryBackend) ListDomainsForPackage(packageID string) []string {
	b.mu.RLock("ListDomainsForPackage")
	defer b.mu.RUnlock()

	domains := b.packageAssociations[packageID]
	out := make([]string, 0, len(domains))

	for d := range domains {
		out = append(out, d)
	}

	slices.Sort(out)

	return out
}

// GetDataSource returns a data source by domain and name.
func (b *InMemoryBackend) GetDataSource(domainName, name string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	ds, exists := b.domainDataSources.Get(dataSourceKey(domainName, name))
	if !exists {
		return nil, fmt.Errorf(
			"%w: data source %s not found on domain %s",
			ErrDataSourceNotFound,
			name,
			domainName,
		)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources returns all data sources for a domain.
func (b *InMemoryBackend) ListDataSources(domainName string) ([]*DataSource, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	group := b.domainDataSourcesByDomain.Get(domainName)
	out := make([]*DataSource, 0, len(group))

	for _, ds := range group {
		cp := *ds
		out = append(out, &cp)
	}

	return out, nil
}

// UpdateDataSource updates the description of a data source.
func (b *InMemoryBackend) UpdateDataSource(domainName, name, description string) error {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	ds, exists := b.domainDataSources.Get(dataSourceKey(domainName, name))
	if !exists {
		return fmt.Errorf(
			"%w: data source %s not found on domain %s",
			ErrDataSourceNotFound,
			name,
			domainName,
		)
	}

	ds.Description = description

	return nil
}

// DeleteDataSource removes a data source from a domain.
func (b *InMemoryBackend) DeleteDataSource(domainName, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	b.domainDataSources.Delete(dataSourceKey(domainName, name))

	return nil
}

// ListDirectQueryDataSources returns all direct-query data sources.
func (b *InMemoryBackend) ListDirectQueryDataSources() []*DirectQueryDataSource {
	b.mu.RLock("ListDirectQueryDataSources")
	defer b.mu.RUnlock()

	out := make([]*DirectQueryDataSource, 0, b.directQueryDataSources.Len())
	for _, ds := range b.directQueryDataSources.All() {
		cp := *ds
		out = append(out, &cp)
	}

	return out
}

// GetDirectQueryDataSource returns a direct-query data source by name.
func (b *InMemoryBackend) GetDirectQueryDataSource(name string) (*DirectQueryDataSource, error) {
	b.mu.RLock("GetDirectQueryDataSource")
	defer b.mu.RUnlock()

	ds, exists := b.directQueryDataSources.Get(name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: direct query data source %s not found",
			ErrDataSourceNotFound,
			name,
		)
	}

	cp := *ds

	return &cp, nil
}

// UpdateDirectQueryDataSource updates a direct-query data source.
func (b *InMemoryBackend) UpdateDirectQueryDataSource(
	name, description string,
	openSearchArns []string,
) (*DirectQueryDataSource, error) {
	b.mu.Lock("UpdateDirectQueryDataSource")
	defer b.mu.Unlock()

	ds, exists := b.directQueryDataSources.Get(name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: direct query data source %s not found",
			ErrDataSourceNotFound,
			name,
		)
	}

	ds.Description = description
	ds.OpenSearchArns = openSearchArns
	cp := *ds

	return &cp, nil
}

// DeleteDirectQueryDataSource removes a direct-query data source by name.
func (b *InMemoryBackend) DeleteDirectQueryDataSource(name string) error {
	b.mu.Lock("DeleteDirectQueryDataSource")
	defer b.mu.Unlock()

	b.directQueryDataSources.Delete(name)

	return nil
}

// ListScheduledActions returns scheduled actions for a domain.
func (b *InMemoryBackend) ListScheduledActions(domainName string) []*ScheduledAction {
	b.mu.RLock("ListScheduledActions")
	defer b.mu.RUnlock()

	src := b.scheduledActions[domainName]
	out := make([]*ScheduledAction, len(src))

	for i, sa := range src {
		cp := *sa
		out[i] = &cp
	}

	return out
}

// UpdateScheduledAction updates or adds a scheduled action for a domain.
func (b *InMemoryBackend) UpdateScheduledAction(
	domainName string,
	action *ScheduledAction,
) (*ScheduledAction, error) {
	b.mu.Lock("UpdateScheduledAction")
	defer b.mu.Unlock()

	actions := b.scheduledActions[domainName]
	for i, sa := range actions {
		if sa.ID == action.ID {
			*sa = *action
			cp := *actions[i]

			return &cp, nil
		}
	}

	cp := *action
	b.scheduledActions[domainName] = append(b.scheduledActions[domainName], &cp)

	ret := *action

	return &ret, nil
}

// staticReservedInstanceOfferings returns a fixed list of available offerings.
func staticReservedInstanceOfferings() []*ReservedInstanceOffering {
	return []*ReservedInstanceOffering{
		{
			ReservedInstanceOfferingID: "ri-offering-1",
			InstanceType:               instanceTypeT3Small,
			Duration:                   reservedDuration1Year,
			FixedPrice:                 reservedPrice1YearAllUpfront,
			UsagePrice:                 0.0,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "ALL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-2",
			InstanceType:               instanceTypeR6gLarge,
			Duration:                   reservedDuration1Year,
			FixedPrice:                 reservedPrice1YearPartialFixed,
			UsagePrice:                 reservedPrice1YearPartialHourly,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "PARTIAL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-3",
			InstanceType:               instanceTypeM6gLarge,
			Duration:                   reservedDuration3Year,
			FixedPrice:                 0.0,
			UsagePrice:                 reservedPrice3YearNoUpfrontHrly,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "NO_UPFRONT",
		},
	}
}

// DescribeReservedInstanceOfferings returns available reserved instance offerings.
func (b *InMemoryBackend) DescribeReservedInstanceOfferings() []*ReservedInstanceOffering {
	return staticReservedInstanceOfferings()
}

// DescribeReservedInstances returns all purchased reserved instances.
func (b *InMemoryBackend) DescribeReservedInstances() []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	out := make([]*ReservedInstance, 0, b.reservedInstances.Len())
	for _, ri := range b.reservedInstances.All() {
		cp := *ri
		out = append(out, &cp)
	}

	return out
}

// PurchaseReservedInstanceOffering purchases a reserved instance offering.
func (b *InMemoryBackend) PurchaseReservedInstanceOffering(
	offeringID, name string,
	count int,
) (*ReservedInstance, error) {
	var offering *ReservedInstanceOffering

	for _, o := range staticReservedInstanceOfferings() {
		if o.ReservedInstanceOfferingID == offeringID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf(
			"%w: reserved instance offering %s not found",
			ErrConnectionNotFound,
			offeringID,
		)
	}

	b.mu.Lock("PurchaseReservedInstanceOffering")
	defer b.mu.Unlock()

	b.reservedCounter++
	id := fmt.Sprintf("ri-%d", b.reservedCounter)

	ri := &ReservedInstance{
		ReservedInstanceID:         id,
		ReservedInstanceOfferingID: offeringID,
		InstanceType:               offering.InstanceType,
		ReservationName:            name,
		Duration:                   offering.Duration,
		FixedPrice:                 offering.FixedPrice,
		UsagePrice:                 offering.UsagePrice,
		InstanceCount:              count,
		CurrencyCode:               offering.CurrencyCode,
		PaymentOption:              offering.PaymentOption,
		State:                      pkgStateActive,
		StartTime:                  float64(time.Now().Unix()),
	}
	b.reservedInstances.Put(ri)

	cp := *ri

	return &cp, nil
}

// StartDomainMaintenance starts a maintenance action on a domain.
func (b *InMemoryBackend) StartDomainMaintenance(
	domainName, action, nodeID string,
) (*DomainMaintenance, error) {
	b.mu.Lock("StartDomainMaintenance")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.maintenanceCounter++
	id := fmt.Sprintf("m-%d", b.maintenanceCounter)
	now := float64(time.Now().Unix())

	m := &DomainMaintenance{
		MaintenanceID: id,
		DomainName:    domainName,
		Action:        action,
		NodeID:        nodeID,
		Status:        softwareUpdateCompleted,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.domainMaintenances[domainName] = append(b.domainMaintenances[domainName], m)
	// Trim to the cap, keeping the most recent entries.
	if len(b.domainMaintenances[domainName]) > maxMaintenancesPerDomain {
		records := b.domainMaintenances[domainName]
		b.domainMaintenances[domainName] = records[len(records)-maxMaintenancesPerDomain:]
	}

	cp := *m

	return &cp, nil
}

// GetDomainMaintenanceStatus returns a specific maintenance record.
func (b *InMemoryBackend) GetDomainMaintenanceStatus(
	domainName, maintenanceID string,
) (*DomainMaintenance, error) {
	b.mu.RLock("GetDomainMaintenanceStatus")
	defer b.mu.RUnlock()

	for _, m := range b.domainMaintenances[domainName] {
		if m.MaintenanceID == maintenanceID {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: maintenance %s not found on domain %s",
		ErrConnectionNotFound,
		maintenanceID,
		domainName,
	)
}

// ListDomainMaintenances returns all maintenance records for a domain.
func (b *InMemoryBackend) ListDomainMaintenances(domainName string) ([]*DomainMaintenance, error) {
	b.mu.RLock("ListDomainMaintenances")
	defer b.mu.RUnlock()

	src := b.domainMaintenances[domainName]
	out := make([]*DomainMaintenance, len(src))

	for i, m := range src {
		cp := *m
		out[i] = &cp
	}

	return out, nil
}

// CreateIndex creates an index for a domain.
func (b *InMemoryBackend) CreateIndex(
	domainName, indexName string,
	mappings, settings, aliases map[string]any,
) (*DomainIndex, error) {
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	idx := &DomainIndex{
		IndexName:     indexName,
		IndexStatus:   pkgStateActive,
		Mappings:      mappings,
		Settings:      settings,
		Aliases:       aliases,
		Documents:     make(map[string]map[string]any),
		DomainName:    domainName,
		DocumentCount: 0,
	}
	b.domainIndexes.Put(idx)

	cp := *idx

	return &cp, nil
}

// DeleteIndex removes an index from a domain.
func (b *InMemoryBackend) DeleteIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	cp := *idx
	b.domainIndexes.Delete(domainIndexKey(domainName, indexName))

	return &cp, nil
}

// GetIndex returns an index by domain and name.
func (b *InMemoryBackend) GetIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	cp := *idx

	return &cp, nil
}

// UpdateIndex updates the mappings and settings of an index.
func (b *InMemoryBackend) UpdateIndex(
	domainName, indexName string,
	mappings, settings map[string]any,
) (*DomainIndex, error) {
	b.mu.Lock("UpdateIndex")
	defer b.mu.Unlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	idx.Mappings = mappings
	idx.Settings = settings
	cp := *idx

	return &cp, nil
}

// GetApplication returns an application by ID.
func (b *InMemoryBackend) GetApplication(id string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, exists := b.applications.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]*Application, 0, b.applications.Len())
	for _, app := range b.applications.All() {
		cp := *app
		cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
		copy(cp.AppConfigs, app.AppConfigs)
		cp.DataSources = make([]AppDataSource, len(app.DataSources))
		copy(cp.DataSources, app.DataSources)
		out = append(out, &cp)
	}

	return out
}

// UpdateApplication updates an application's configs and data sources.
func (b *InMemoryBackend) UpdateApplication(
	id string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, exists := b.applications.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	if appConfigs != nil {
		app.AppConfigs = appConfigs
	}

	if dataSources != nil {
		app.DataSources = dataSources
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// DeleteApplication removes an application by ID.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(id) {
		return fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	b.applications.Delete(id)

	return nil
}

// StartServiceSoftwareUpdate marks a domain as having a pending software update.
// StartServiceSoftwareUpdate schedules a service software update for the domain.
// scheduleAt must be one of "NOW", "TIMESTAMP", "OFF_PEAK_WINDOW", or "".
func (b *InMemoryBackend) StartServiceSoftwareUpdate(
	domainName, scheduleAt string,
) (*ServiceSoftwareOptions, error) {
	b.mu.Lock("StartServiceSoftwareUpdate")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(domainName)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	desc := "A new service software version is ready to install."

	switch scheduleAt {
	case "OFF_PEAK_WINDOW":
		desc = "Service software update scheduled for the next off-peak window."
	case "TIMESTAMP":
		desc = "Service software update scheduled for the requested time."
	}

	// Record real, mutable software-update state so a subsequent
	// CancelServiceSoftwareUpdate has something concrete to act on.
	opts := &ServiceSoftwareOptions{
		CurrentVersion:  defaultEngineVersion,
		NewVersion:      defaultEngineVersion,
		UpdateAvailable: true,
		Cancellable:     true,
		UpdateStatus:    sswStatusPendingUpdate,
		Description:     desc,
	}
	d.ServiceSoftware = opts
	b.beginProcessing(d, dpsUpdatingServiceSoftware)

	cp := *opts

	return &cp, nil
}

// DescribeDomains returns a list of domains. If names is empty, all domains are returned.
// Missing names are silently skipped.
func (b *InMemoryBackend) DescribeDomains(names []string) ([]*Domain, error) {
	b.mu.RLock("DescribeDomains")
	defer b.mu.RUnlock()

	now := b.clock()

	if len(names) == 0 {
		out := make([]*Domain, 0, b.domains.Len())
		for _, d := range b.domains.All() {
			if deleteWindowElapsed(d, now) {
				continue
			}

			cp := *d
			out = append(out, &cp)
		}

		return out, nil
	}

	out := make([]*Domain, 0, len(names))

	for _, name := range names {
		d, exists := b.domains.Get(name)
		if !exists || deleteWindowElapsed(d, now) {
			continue
		}

		cp := *d
		out = append(out, &cp)
	}

	return out, nil
}

func applyClusterConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.ClusterConfig != nil {
		d.ClusterConfig = *input.ClusterConfig
	}

	if input.EngineVersion != "" {
		d.EngineVersion = input.EngineVersion
	}
}

func applyStorageConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.EBSOptions != nil {
		d.EBSOptions = input.EBSOptions
	}

	if input.SnapshotOptions != nil {
		d.SnapshotOptions = input.SnapshotOptions
	}
}

func applySecurityConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.EncryptionAtRestOptions != nil {
		d.EncryptionAtRestOptions = input.EncryptionAtRestOptions
	}

	if input.NodeToNodeEncryptionOptions != nil {
		d.NodeToNodeEncryptionOptions = input.NodeToNodeEncryptionOptions
	}

	if input.DomainEndpointOptions != nil {
		d.DomainEndpointOptions = input.DomainEndpointOptions
	}

	if input.AdvancedSecurityOptions != nil {
		d.AdvancedSecurityOptions = input.AdvancedSecurityOptions
	}
}

func applyNetworkConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.VPCOptions != nil {
		d.VPCOptions = input.VPCOptions
	}

	if input.CognitoOptions != nil {
		d.CognitoOptions = input.CognitoOptions
	}
}

func applyOperationalConfig(d *Domain, input UpdateDomainConfigInput) {
	if input.OffPeakWindowOptions != nil {
		d.OffPeakWindowOptions = input.OffPeakWindowOptions
	}

	if input.IdentityCenterOptions != nil {
		d.IdentityCenterOptions = input.IdentityCenterOptions
	}

	if input.EnableSoftwareUpdateOptions != nil {
		d.EnableSoftwareUpdateOptions = input.EnableSoftwareUpdateOptions
	}

	if input.LogPublishingOptions != nil {
		d.LogPublishingOptions = input.LogPublishingOptions
	}

	if input.AccessPolicies != "" {
		d.AccessPolicies = input.AccessPolicies
	}
}

// UpdateDomainConfig updates mutable fields on a domain and records a change ID.
func (b *InMemoryBackend) UpdateDomainConfig(
	name string,
	input UpdateDomainConfigInput,
) (*Domain, error) {
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	applyClusterConfig(d, input)
	applyStorageConfig(d, input)
	applySecurityConfig(d, input)
	applyNetworkConfig(d, input)
	applyOperationalConfig(d, input)

	changeID := fmt.Sprintf("change-%s-%d", name, time.Now().UnixNano())
	d.LastChangeID = changeID
	b.beginProcessing(d, dpsModifying)

	cp := *d

	return &cp, nil
}

// PreviewDomainConfig computes the domain configuration that UpdateDomainConfig
// would produce for input, without mutating stored state or advancing the
// processing/change-ID bookkeeping. This backs UpdateDomainConfig's
// DryRun=true mode (aws-sdk-go-v2 UpdateDomainConfigInput.DryRun): AWS
// validates and previews the change but never applies it.
func (b *InMemoryBackend) PreviewDomainConfig(
	name string,
	input UpdateDomainConfigInput,
) (*Domain, error) {
	b.mu.RLock("PreviewDomainConfig")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(name)
	if !exists || deleteWindowElapsed(d, b.clock()) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d
	applyClusterConfig(&cp, input)
	applyStorageConfig(&cp, input)
	applySecurityConfig(&cp, input)
	applyNetworkConfig(&cp, input)
	applyOperationalConfig(&cp, input)

	return &cp, nil
}

// GetDefaultApplicationSettings returns stored settings for the given applicationType.
func (b *InMemoryBackend) GetDefaultApplicationSettings(
	applicationType string,
) ([]AppSetting, error) {
	b.mu.RLock("GetDefaultApplicationSettings")
	defer b.mu.RUnlock()

	settings := b.defaultAppSettings[applicationType]
	out := make([]AppSetting, len(settings))
	copy(out, settings)

	return out, nil
}

// PutDefaultApplicationSettings stores settings for the given applicationType.
func (b *InMemoryBackend) PutDefaultApplicationSettings(
	applicationType string,
	settings []AppSetting,
) error {
	b.mu.Lock("PutDefaultApplicationSettings")
	defer b.mu.Unlock()

	stored := make([]AppSetting, len(settings))
	copy(stored, settings)
	b.defaultAppSettings[applicationType] = stored

	return nil
}

// GetDomainHealth returns computed health metrics for a domain.
func (b *InMemoryBackend) GetDomainHealth(domainName string) (map[string]any, error) {
	b.mu.RLock("GetDomainHealth")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	instanceCount := d.ClusterConfig.InstanceCount
	if instanceCount == 0 {
		instanceCount = 1
	}

	totalShards := instanceCount * defaultShardsPerNode

	warmNodes := 0
	if d.ClusterConfig.WarmEnabled {
		warmNodes = d.ClusterConfig.WarmCount
	}

	dedicatedMaster := d.ClusterConfig.DedicatedMasterEnabled

	docCount := 0
	for _, idx := range b.domainIndexesByDomain.Get(domainName) {
		docCount += idx.DocumentCount
	}

	return map[string]any{
		"DomainState":                 domainStatusActive,
		"TotalShards":                 totalShards,
		"ActiveShards":                totalShards,
		"UnAssignedShards":            0,
		"DataNodeCount":               instanceCount,
		"WarmNodeCount":               warmNodes,
		"DedicatedMaster":             dedicatedMaster,
		"ActiveAvailabilityZoneCount": 1,
		"DocumentCount":               docCount,
	}, nil
}

// GetDomainNodes returns a list of node descriptors based on cluster config.
func (b *InMemoryBackend) GetDomainNodes(domainName string) ([]map[string]any, error) {
	b.mu.RLock("GetDomainNodes")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	count := d.ClusterConfig.InstanceCount
	if count == 0 {
		count = 1
	}

	nodes := make([]map[string]any, 0, count)

	storageVolumeType := "EBS"
	if d.EBSOptions != nil && d.EBSOptions.VolumeType != "" {
		storageVolumeType = d.EBSOptions.VolumeType
	}

	for i := range count {
		nodes = append(nodes, map[string]any{
			"NodeId":            fmt.Sprintf("node-%d", i),
			"NodeType":          nodeRoleData,
			jsonKeyInstanceType: d.ClusterConfig.InstanceType,
			"NodeStatus":        domainStatusActive,
			"StorageVolumeType": storageVolumeType,
			"AvailabilityZone":  fmt.Sprintf("%sa", b.region),
		})
	}

	return nodes, nil
}

// GetDryRunProgress returns dry-run progress for a domain. Creates a default entry if none exists.
func (b *InMemoryBackend) GetDryRunProgress(domainName string) (*DryRunStatus, error) {
	b.mu.Lock("GetDryRunProgress")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	dr, exists := b.dryRuns.Get(domainName)
	if !exists {
		now := time.Now().UTC().Format(time.RFC3339)
		dr = &DryRunStatus{
			DryRunID:           fmt.Sprintf("dryrun-%s-%d", domainName, time.Now().UnixNano()),
			DryRunStatus:       softwareUpdateCompleted,
			CreationDate:       now,
			UpdateDate:         now,
			ValidationFailures: []map[string]any{},
			DomainName:         domainName,
		}
		b.dryRuns.Put(dr)
	}

	if dr.ValidationFailures == nil {
		dr.ValidationFailures = []map[string]any{}
	}

	cp := *dr

	return &cp, nil
}

// GetChangeProgress returns the last change progress for a domain.
func (b *InMemoryBackend) GetChangeProgress(domainName string) (map[string]any, error) {
	b.mu.RLock("GetChangeProgress")
	defer b.mu.RUnlock()

	d, exists := b.domains.Get(domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	changeID := d.LastChangeID
	if changeID == "" {
		changeID = changeProgressStub
	}

	now := time.Now().UTC().Format(time.RFC3339)

	return map[string]any{
		"ChangeId":            changeID,
		jsonKeyStatus:         softwareUpdateCompleted,
		"CompletedProperties": []any{},
		"PendingProperties":   []any{},
		"TotalNumberOfStages": 0,
		"StartTime":           now,
		"LastUpdatedTime":     now,
	}, nil
}

// ListInstanceTypeDetails returns a static list of common OpenSearch instance type details.
func (b *InMemoryBackend) ListInstanceTypeDetails(_, _ string) []map[string]any {
	dataRole := []string{nodeRoleData}
	warmRole := []string{nodeRoleData, "UltraWarm"}

	return []map[string]any{
		{
			jsonKeyInstanceType:            instanceTypeT3Small,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          false,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             false,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            dataRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeR6gLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeM6gLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeR6gXLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeOR1Medium,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          false,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             false,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            dataRole,
		},
	}
}

// GetCompatibleVersions returns static compatible version pairs.
// If domainName is non-empty, target versions are filtered to those
// reachable from the domain's current EngineVersion.
func (b *InMemoryBackend) GetCompatibleVersions(domainName string) []map[string]any {
	static := []map[string]any{
		{
			jsonKeySourceVersion:  engineVersionOpenSearch29,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch211},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch27,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch29, engineVersionOpenSearch211},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch13,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch27},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch211,
			jsonKeyTargetVersions: []string{},
		},
	}

	if domainName == "" {
		return static
	}

	b.mu.RLock("GetCompatibleVersions")
	d, exists := b.domains.Get(domainName)
	b.mu.RUnlock()

	if !exists {
		return static
	}

	for _, entry := range static {
		if entry["SourceVersion"] == d.EngineVersion {
			return []map[string]any{entry}
		}
	}

	return []map[string]any{
		{jsonKeySourceVersion: d.EngineVersion, jsonKeyTargetVersions: []string{}},
	}
}

// DissociatePackage removes a package association from a domain.
func (b *InMemoryBackend) DissociatePackage(
	packageID, domainName string,
) (*DomainPackageDetails, error) {
	if packageID == "" {
		return nil, fmt.Errorf("%w: PackageID is required", ErrInvalidParameter)
	}

	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DissociatePackage")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.removePackageAssociation(packageID, domainName)

	return &DomainPackageDetails{
		PackageID:  packageID,
		DomainName: domainName,
		State:      "DISSOCIATED",
	}, nil
}

// DissociatePackages removes multiple package associations from a domain.
func (b *InMemoryBackend) DissociatePackages(
	domainName string,
	packageIDs []string,
) ([]DomainPackageDetails, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DissociatePackages")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	results := make([]DomainPackageDetails, 0, len(packageIDs))

	for _, pkgID := range packageIDs {
		b.removePackageAssociation(pkgID, domainName)

		results = append(results, DomainPackageDetails{
			PackageID:  pkgID,
			DomainName: domainName,
			State:      "DISSOCIATED",
		})
	}

	return results, nil
}

// AddDomainInternal seeds a domain directly for use in tests.
func (b *InMemoryBackend) AddDomainInternal(name, engineVersion string) {
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	b.mu.Lock("AddDomainInternal")
	defer b.mu.Unlock()

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)
	b.domains.Put(&Domain{
		Name:          name,
		ARN:           domainARN,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        domainStatusActive,
		ClusterConfig: ClusterConfig{InstanceType: instanceTypeT3Small, InstanceCount: 1},
		Tags:          tags.New("opensearch." + name + ".tags"),
	})
}

// AddPackageInternal seeds a package directly for use in tests.
func (b *InMemoryBackend) AddPackageInternal(packageID, packageName, packageType string) {
	b.mu.Lock("AddPackageInternal")
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())
	b.packages.Put(&Package{
		PackageID:     packageID,
		PackageName:   packageName,
		PackageType:   packageType,
		PackageStatus: pkgStateActive,
		CreatedAt:     now,
		VersionHistory: []*PackageVersionHistory{
			{
				PackageVersion: "1",
				CommitMessage:  "initial version",
				CreatedAt:      now,
			},
		},
	})
}
