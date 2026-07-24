package elasticsearch

import (
	"regexp"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusActiveCap                = "Active"
	statusActive                   = "ACTIVE"
	reservedDurationOneYearSeconds = 31536000
	defaultElasticsearchVersion    = "7.10"
	elasticsearchVersion717        = "7.17"
	elasticsearchVersion716        = "7.16"
	elasticsearchVersion713        = "7.13"
	elasticsearchVersion79         = "7.9"
	elasticsearchVersion78         = "7.8"
	elasticsearchVersion77         = "7.7"
	elasticsearchVersion74         = "7.4"
	elasticsearchVersion71         = "7.1"
	elasticsearchVersion68         = "6.8"
	elasticsearchVersion67         = "6.7"
	elasticsearchVersion65         = "6.5"
	elasticsearchVersion64         = "6.4"
	elasticsearchVersion63         = "6.3"
	elasticsearchVersion62         = "6.2"
	elasticsearchVersion60         = "6.0"
	elasticsearchVersion56         = "5.6"
	elasticsearchVersion55         = "5.5"
	elasticsearchVersion53         = "5.3"
	elasticsearchVersion51         = "5.1"
	defaultInstanceType            = "t3.small.elasticsearch"
	largeInstanceType              = "m5.large.elasticsearch"
)

// domainNameRe validates Elasticsearch domain names:
// 3–28 lowercase alphanumeric characters or hyphens, must start with a letter.
var domainNameRe = regexp.MustCompile(`^[a-z][a-z0-9\-]{2,27}$`)

// validElasticsearchVersions is the set of versions accepted by AWS Elasticsearch Service.
var validElasticsearchVersions = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"1.5":                       true,
	"2.3":                       true,
	elasticsearchVersion51:      true,
	elasticsearchVersion53:      true,
	elasticsearchVersion55:      true,
	elasticsearchVersion56:      true,
	elasticsearchVersion60:      true,
	elasticsearchVersion62:      true,
	elasticsearchVersion63:      true,
	elasticsearchVersion64:      true,
	elasticsearchVersion65:      true,
	elasticsearchVersion67:      true,
	elasticsearchVersion68:      true,
	elasticsearchVersion71:      true,
	elasticsearchVersion74:      true,
	elasticsearchVersion77:      true,
	elasticsearchVersion78:      true,
	elasticsearchVersion79:      true,
	defaultElasticsearchVersion: true,
	elasticsearchVersion713:     true,
	elasticsearchVersion716:     true,
	elasticsearchVersion717:     true,
}

// validPackageTypes is the set of package types accepted by AWS Elasticsearch
// Service. NOTE: unlike the newer OpenSearch Service API (which also accepts
// ZIP-PLUGIN/PACKAGE-LICENSE/PACKAGE-CONFIG), the legacy elasticsearchservice
// PackageType enum supports only TXT-DICTIONARY -- confirmed against
// aws-sdk-go-v2/service/elasticsearchservice/types.PackageType, whose only
// enum value is PackageTypeTxtDictionary ("TXT-DICTIONARY").
var validPackageTypes = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"TXT-DICTIONARY": true,
}

// PackageSource holds the S3 bucket/key location a package was imported
// from (types.PackageSource). It is a required member of CreatePackageInput
// in the real API but is never echoed back by any Describe/Create/Update
// package response (types.PackageDetails has no PackageSource field), so it
// is stored on Package purely so the backend has captured real input state --
// it is intentionally not surfaced by toPackageJSON.
type PackageSource struct {
	S3BucketName string `json:"s3BucketName"`
	S3Key        string `json:"s3Key"`
}

// Package represents an Elasticsearch package (e.g., a custom dictionary or synonym file).
type Package struct {
	ID            string        `json:"packageID"`
	Name          string        `json:"packageName"`
	PackageType   string        `json:"packageType"`
	Description   string        `json:"packageDescription"`
	Status        string        `json:"packageStatus"`
	PackageSource PackageSource `json:"packageSource"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// backend.go); it is unexported so it is never marshaled by a plain
	// json.Marshal(Package) and is instead carried through persistence via
	// regionalDTO (see persistence.go).
	region string
}

// CrossClusterDomainInfo holds domain endpoint info used in cross-cluster connections.
type CrossClusterDomainInfo struct {
	OwnerID    string `json:"OwnerId"`
	DomainName string `json:"DomainName"`
	Region     string `json:"Region"`
}

// InboundConnection represents an inbound cross-cluster search connection.
type InboundConnection struct {
	ConnectionID     string                 `json:"connectionID"`
	ConnectionStatus string                 `json:"connectionStatus"`
	SourceDomainInfo CrossClusterDomainInfo `json:"sourceDomainInfo"`
	DestDomainInfo   CrossClusterDomainInfo `json:"destDomainInfo"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// backend.go); it is unexported so it is never marshaled by a plain
	// json.Marshal(InboundConnection) and is instead carried through
	// persistence via regionalDTO (see persistence.go).
	region string
}

// OutboundConnection represents an outbound cross-cluster search connection.
type OutboundConnection struct {
	ConnectionID     string                 `json:"connectionID"`
	ConnectionAlias  string                 `json:"connectionAlias"`
	ConnectionStatus string                 `json:"connectionStatus"`
	LocalDomainInfo  CrossClusterDomainInfo `json:"localDomainInfo"`
	RemoteDomainInfo CrossClusterDomainInfo `json:"remoteDomainInfo"`
	// region is the store.Table composite-key qualifier; see the identical
	// comment on InboundConnection above.
	region string
}

// VpcEndpoint represents a managed VPC endpoint for an Elasticsearch domain.
type VpcEndpoint struct {
	VpcOptions     map[string]string `json:"vpcOptions"`
	ID             string            `json:"vpcEndpointID"`
	OwnerAccountID string            `json:"ownerAccountID"`
	DomainARN      string            `json:"domainARN"`
	Endpoint       string            `json:"endpoint"`
	Status         string            `json:"status"`
	// region is the store.Table composite-key qualifier; see the identical
	// comment on InboundConnection above.
	region          string
	AuthorizedAccts []string `json:"authorizedAccounts"`
}

// ReservedInstanceOffering represents a reserved Elasticsearch instance offering.
type ReservedInstanceOffering struct {
	OfferingID    string  `json:"reservedElasticsearchInstanceOfferingId"`
	InstanceType  string  `json:"elasticsearchInstanceType"`
	PaymentOption string  `json:"paymentOption"`
	Currency      string  `json:"currencyCode"`
	FixedPrice    float64 `json:"fixedPrice"`
	UsagePrice    float64 `json:"usagePrice"`
	Duration      int     `json:"duration"`
}

// ReservedInstance represents a purchased reserved Elasticsearch instance.
type ReservedInstance struct {
	ReservationID   string `json:"reservedElasticsearchInstanceId"`
	ReservationName string `json:"reservationName"`
	OfferingID      string `json:"reservedElasticsearchInstanceOfferingId"`
	InstanceType    string `json:"elasticsearchInstanceType"`
	State           string `json:"state"`
	// region is the store.Table composite-key qualifier; see the identical
	// comment on InboundConnection above.
	region     string
	FixedPrice float64 `json:"fixedPrice"`
	UsagePrice float64 `json:"usagePrice"`
	Duration   int     `json:"duration"`
	Count      int     `json:"elasticsearchInstanceCount"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// ZoneAwarenessConfig holds the zone awareness configuration for a cluster.
type ZoneAwarenessConfig struct {
	AvailabilityZoneCount int `json:"availabilityZoneCount"`
}

// SnapshotOptions holds automated snapshot configuration for a domain.
type SnapshotOptions struct {
	AutomatedSnapshotStartHour int `json:"automatedSnapshotStartHour"`
}

// ClusterConfig represents the cluster configuration for an Elasticsearch domain.
type ClusterConfig struct {
	InstanceType           string              `json:"instanceType"`
	DedicatedMasterType    string              `json:"dedicatedMasterType,omitempty"`
	WarmType               string              `json:"warmType,omitempty"`
	ZoneAwarenessConfig    ZoneAwarenessConfig `json:"zoneAwarenessConfig"`
	InstanceCount          int                 `json:"instanceCount"`
	DedicatedMasterCount   int                 `json:"dedicatedMasterCount,omitempty"`
	WarmCount              int                 `json:"warmCount,omitempty"`
	DedicatedMasterEnabled bool                `json:"dedicatedMasterEnabled"`
	ZoneAwarenessEnabled   bool                `json:"zoneAwarenessEnabled"`
	WarmEnabled            bool                `json:"warmEnabled"`
	ColdStorageEnabled     bool                `json:"coldStorageEnabled"`
}

// EBSOptions represents the EBS storage options for an Elasticsearch domain.
type EBSOptions struct {
	VolumeType string `json:"volumeType"`
	VolumeSize int    `json:"volumeSize"`
	Iops       int    `json:"iops"`
	Throughput int    `json:"throughput"`
	EBSEnabled bool   `json:"ebsEnabled"`
}

// VPCOptions holds the subnet/security-group configuration for placing a
// domain inside a VPC (types.VPCOptions on request, types.VPCDerivedInfo on
// response). AvailabilityZones/VPCId are not modeled -- deriving them would
// require cross-service EC2 subnet lookups this backend does not perform
// (matches services/opensearch's identical simplification), so they are
// always left empty on the wire.
type VPCOptions struct {
	SubnetIDs        []string `json:"subnetIDs,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIDs,omitempty"`
}

// CognitoOptions holds the Cognito user/identity pool configuration for
// Kibana authentication (types.CognitoOptions -- the same shape is used for
// both the request and response).
type CognitoOptions struct {
	UserPoolID     string `json:"userPoolID,omitempty"`
	IdentityPoolID string `json:"identityPoolID,omitempty"`
	RoleARN        string `json:"roleARN,omitempty"`
	Enabled        bool   `json:"enabled"`
}

// LogPublishingOption holds a single log-type publishing configuration
// (types.LogPublishingOption), keyed by AWS LogType string
// (INDEX_SLOW_LOGS/SEARCH_SLOW_LOGS/ES_APPLICATION_LOGS/AUDIT_LOGS) in
// Domain.LogPublishingOptions.
type LogPublishingOption struct {
	CloudWatchLogsLogGroupARN string `json:"cloudWatchLogsLogGroupARN,omitempty"`
	Enabled                   bool   `json:"enabled"`
}

// AdvancedSecurityOptions holds fine-grained access control settings
// (types.AdvancedSecurityOptions on response). Master user credentials
// (MasterUserOptions on the *Input request shape) and SAML IdP configuration
// are intentionally not persisted here: real AWS never echoes
// MasterUserOptions back on any Describe/Create/Update response either, and
// full SAML modeling is out of scope for this pass (see PARITY.md gaps).
type AdvancedSecurityOptions struct {
	Enabled                     bool `json:"enabled"`
	InternalUserDatabaseEnabled bool `json:"internalUserDatabaseEnabled,omitempty"`
	AnonymousAuthEnabled        bool `json:"anonymousAuthEnabled,omitempty"`
}

// AutoTuneOptions holds the Auto-Tune desired state for a domain
// (types.AutoTuneOptionsInput's DesiredState member). MaintenanceSchedules is
// not modeled (see PARITY.md gaps).
type AutoTuneOptions struct {
	DesiredState string `json:"desiredState,omitempty"`
}

// Domain represents an Elasticsearch domain.
type Domain struct {
	ConfigUpdatedAt             time.Time                      `json:"configUpdatedAt,omitzero"`
	CreatedAt                   time.Time                      `json:"createdAt,omitzero"`
	VPCOptions                  *VPCOptions                    `json:"vpcOptions,omitempty"`
	AdvancedOptions             map[string]string              `json:"advancedOptions,omitempty"`
	LogPublishingOptions        map[string]LogPublishingOption `json:"logPublishingOptions,omitempty"`
	AutoTuneOptions             *AutoTuneOptions               `json:"autoTuneOptions,omitempty"`
	Tags                        *tags.Tags                     `json:"tags,omitempty"`
	AdvancedSecurityOptions     *AdvancedSecurityOptions       `json:"advancedSecurityOptions,omitempty"`
	CognitoOptions              *CognitoOptions                `json:"cognitoOptions,omitempty"`
	ElasticsearchVersion        string                         `json:"elasticsearchVersion"`
	AccessPolicies              string                         `json:"accessPolicies,omitempty"`
	Status                      string                         `json:"status"`
	TLSSecurityPolicy           string                         `json:"tlsSecurityPolicy,omitempty"`
	DomainID                    string                         `json:"domainID"`
	Name                        string                         `json:"name"`
	region                      string
	Endpoint                    string          `json:"endpoint"`
	ARN                         string          `json:"arn"`
	EBSOptions                  EBSOptions      `json:"ebsOptions"`
	ClusterConfig               ClusterConfig   `json:"clusterConfig"`
	SnapshotOptions             SnapshotOptions `json:"snapshotOptions"`
	ConfigVersion               int             `json:"configVersion"`
	EncryptionAtRestEnabled     bool            `json:"encryptionAtRestEnabled"`
	NodeToNodeEncryptionEnabled bool            `json:"nodeToNodeEncryptionEnabled"`
	EnforceHTTPS                bool            `json:"enforceHTTPS"`
}

// CreateDomainInput holds all parameters for CreateDomain.
type CreateDomainInput struct {
	VPCOptions                  *VPCOptions
	Tags                        map[string]string
	LogPublishingOptions        map[string]LogPublishingOption
	AutoTuneOptions             *AutoTuneOptions
	AdvancedOptions             map[string]string
	AdvancedSecurityOptions     *AdvancedSecurityOptions
	CognitoOptions              *CognitoOptions
	AccessPolicies              string
	TLSSecurityPolicy           string
	ElasticsearchVersion        string
	Name                        string
	EBSOptions                  EBSOptions
	ClusterConfig               ClusterConfig
	SnapshotOptions             SnapshotOptions
	EncryptionAtRestEnabled     bool
	NodeToNodeEncryptionEnabled bool
	EnforceHTTPS                bool
}

// UpdateConfig holds the fields that can be updated via UpdateDomainConfig.
type UpdateConfig struct {
	ClusterConfig               *ClusterConfig
	EBSOptions                  *EBSOptions
	SnapshotOptions             *SnapshotOptions
	AdvancedOptions             map[string]string
	VPCOptions                  *VPCOptions
	CognitoOptions              *CognitoOptions
	AdvancedSecurityOptions     *AdvancedSecurityOptions
	AutoTuneOptions             *AutoTuneOptions
	LogPublishingOptions        map[string]LogPublishingOption
	AccessPolicies              *string
	TLSSecurityPolicy           *string
	EncryptionAtRestEnabled     *bool
	NodeToNodeEncryptionEnabled *bool
	EnforceHTTPS                *bool
}
