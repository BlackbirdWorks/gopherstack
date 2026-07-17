package elasticsearch

import (
	"regexp"

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

// validPackageTypes is the set of package types accepted by AWS Elasticsearch Service.
var validPackageTypes = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"TXT-DICTIONARY": true,
	"ZIP-PLUGIN":     true,
}

// Package represents an Elasticsearch package (e.g., a custom dictionary or synonym file).
type Package struct {
	ID          string `json:"packageID"`
	Name        string `json:"packageName"`
	PackageType string `json:"packageType"`
	Description string `json:"packageDescription"`
	Status      string `json:"packageStatus"`
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

// Domain represents an Elasticsearch domain.
type Domain struct {
	Tags                        *tags.Tags        `json:"tags,omitempty"`
	AdvancedOptions             map[string]string `json:"advancedOptions,omitempty"`
	Status                      string            `json:"status"`
	AccessPolicies              string            `json:"accessPolicies,omitempty"`
	DomainID                    string            `json:"domainID"`
	ARN                         string            `json:"arn"`
	ElasticsearchVersion        string            `json:"elasticsearchVersion"`
	Endpoint                    string            `json:"endpoint"`
	region                      string
	Name                        string          `json:"name"`
	TLSSecurityPolicy           string          `json:"tlsSecurityPolicy,omitempty"`
	EBSOptions                  EBSOptions      `json:"ebsOptions"`
	ClusterConfig               ClusterConfig   `json:"clusterConfig"`
	SnapshotOptions             SnapshotOptions `json:"snapshotOptions"`
	EncryptionAtRestEnabled     bool            `json:"encryptionAtRestEnabled"`
	NodeToNodeEncryptionEnabled bool            `json:"nodeToNodeEncryptionEnabled"`
	EnforceHTTPS                bool            `json:"enforceHTTPS"`
}

// CreateDomainInput holds all parameters for CreateDomain.
type CreateDomainInput struct {
	AdvancedOptions             map[string]string
	Name                        string
	ElasticsearchVersion        string
	AccessPolicies              string
	TLSSecurityPolicy           string
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
	AccessPolicies              *string
	TLSSecurityPolicy           *string
	EncryptionAtRestEnabled     *bool
	NodeToNodeEncryptionEnabled *bool
	EnforceHTTPS                *bool
}
