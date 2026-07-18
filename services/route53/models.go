package route53

import (
	"time"
)

// HealthCheckType is the type of health check.
type HealthCheckType string

const (
	HealthCheckTypeHTTP             HealthCheckType = "HTTP"
	HealthCheckTypeHTTPS            HealthCheckType = "HTTPS"
	HealthCheckTypeTCP              HealthCheckType = "TCP"
	HealthCheckTypeCalculated       HealthCheckType = "CALCULATED"
	HealthCheckTypeCloudWatchMetric HealthCheckType = "CLOUDWATCH_METRIC"
	HealthCheckTypeRecoveryControl  HealthCheckType = "RECOVERY_CONTROL"
)

// AlarmIdentifier identifies a CloudWatch alarm for CLOUDWATCH_METRIC health checks.
type AlarmIdentifier struct {
	Name   string `json:"name"`
	Region string `json:"region"`
}

// HealthCheckConfig holds the configuration for a health check.
type HealthCheckConfig struct {
	AlarmIdentifier              *AlarmIdentifier `json:"alarmIdentifier,omitempty"`
	IPAddress                    string           `json:"ipAddress,omitempty"`
	FullyQualifiedDomainName     string           `json:"fullyQualifiedDomainName,omitempty"`
	ResourcePath                 string           `json:"resourcePath,omitempty"`
	SearchString                 string           `json:"searchString,omitempty"`
	InsufficientDataHealthStatus string           `json:"insufficientDataHealthStatus,omitempty"`
	RoutingControlArn            string           `json:"routingControlArn,omitempty"`
	Type                         HealthCheckType  `json:"type"`
	Regions                      []string         `json:"regions,omitempty"`
	ChildHealthChecks            []string         `json:"childHealthChecks,omitempty"`
	Port                         int              `json:"port,omitempty"`
	RequestInterval              int              `json:"requestInterval,omitempty"`
	FailureThreshold             int              `json:"failureThreshold,omitempty"`
	HealthThreshold              int              `json:"healthThreshold,omitempty"`
	EnableSNI                    bool             `json:"enableSNI,omitempty"`
	MeasureLatency               bool             `json:"measureLatency,omitempty"`
	Disabled                     bool             `json:"disabled,omitempty"`
	Inverted                     bool             `json:"inverted,omitempty"`
}

// HealthCheckObservation represents a single observation of a health check.
type HealthCheckObservation struct {
	CheckedTime time.Time `json:"checkedTime"`
	Region      string    `json:"region"`
	IPAddress   string    `json:"ipAddress"`
	Status      string    `json:"status"`
}

// HealthCheck represents a Route 53 health check.
type HealthCheck struct {
	CreatedAt       time.Time                `json:"createdAt"`
	ID              string                   `json:"id"`
	CallerReference string                   `json:"callerReference"`
	Status          string                   `json:"status"`
	Observations    []HealthCheckObservation `json:"observations"`
	Config          HealthCheckConfig        `json:"config"`
	// Version is a sequential counter AWS sets to 1 at creation and
	// increments by 1 on every UpdateHealthCheck. Wire field
	// HealthCheckVersion; UpdateHealthCheck's optional request-side
	// HealthCheckVersion is checked against this for optimistic
	// concurrency (see ErrHealthCheckVersionMismatch).
	Version int64 `json:"version"`
}

// FailoverPolicy is the failover role for a record set.
type FailoverPolicy string

const (
	// FailoverPrimary is the primary record in failover routing.
	FailoverPrimary FailoverPolicy = "PRIMARY"
	// FailoverSecondary is the secondary record in failover routing.
	FailoverSecondary FailoverPolicy = "SECONDARY"
)

// GeoLocation represents a geolocation routing target.
type GeoLocation struct {
	ContinentCode   string `json:"continentCode,omitempty"`
	CountryCode     string `json:"countryCode,omitempty"`
	SubdivisionCode string `json:"subdivisionCode,omitempty"`
}

// GeoProximityCoordinates holds lat/lon for a GeoProximity endpoint.
type GeoProximityCoordinates struct {
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

// GeoProximityLocation holds routing configuration for proximity-based routing.
type GeoProximityLocation struct {
	Coordinates    *GeoProximityCoordinates `json:"coordinates,omitempty"`
	AWSRegion      string                   `json:"awsRegion,omitempty"`
	LocalZoneGroup string                   `json:"localZoneGroup,omitempty"`
	Bias           int                      `json:"bias,omitempty"`
}

// CidrRoutingConfig ties a record set to a CIDR collection location.
type CidrRoutingConfig struct {
	CollectionID string `json:"collectionId"`
	LocationName string `json:"locationName"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
// RegisterRecord stores the actual record value (IP for A/AAAA, hostname for CNAME/ALIAS).
type DNSRegistrar interface {
	RegisterRecord(hostname, recordType string, values []string)
	Deregister(hostname string)
}

// HostedZone represents a Route 53 hosted zone.
type HostedZone struct {
	CreatedAt       time.Time `json:"createdAt"`
	Name            string    `json:"name"`
	ID              string    `json:"id"`
	CallerReference string    `json:"callerReference"`
	Comment         string    `json:"comment"`
	// DelegationSetID is the ID of the reusable delegation set this zone
	// was created with, or "" if the zone uses a system-assigned (non
	// reusable) delegation set. Not part of the wire "HostedZone" element
	// itself — real AWS only surfaces it via the separate DelegationSet
	// element on CreateHostedZone/GetHostedZone responses.
	DelegationSetID string `json:"delegationSetId,omitempty"`
	// NameServers are the authoritative name servers for this zone, fixed
	// at creation time: either the reusable delegation set's servers (when
	// DelegationSetID is set) or the default pair otherwise.
	NameServers            []string `json:"nameServers,omitempty"`
	ResourceRecordSetCount int      `json:"resourceRecordSetCount"`
	PrivateZone            bool     `json:"privateZone"`
}

// ResourceRecord holds a single DNS resource record value.
type ResourceRecord struct {
	Value string `json:"value"`
}

// AliasTarget represents an alias resource record set target.
type AliasTarget struct {
	HostedZoneID         string `json:"hostedZoneID"`
	DNSName              string `json:"dnsName"`
	EvaluateTargetHealth bool   `json:"evaluateTargetHealth"`
}

// ResourceRecordSet represents a DNS resource record set.
type ResourceRecordSet struct {
	AliasTarget          *AliasTarget          `json:"aliasTarget,omitempty"`
	GeoLocation          *GeoLocation          `json:"geoLocation,omitempty"`
	GeoProximityLocation *GeoProximityLocation `json:"geoProximityLocation,omitempty"`
	CidrRoutingConfig    *CidrRoutingConfig    `json:"cidrRoutingConfig,omitempty"`
	Weight               *int64                `json:"weight,omitempty"`
	SetIdentifier        string                `json:"setIdentifier,omitempty"`
	Type                 string                `json:"type"`
	Failover             FailoverPolicy        `json:"failover,omitempty"`
	Region               string                `json:"region,omitempty"`
	HealthCheckID        string                `json:"healthCheckId,omitempty"`
	Name                 string                `json:"name"`
	Records              []ResourceRecord      `json:"records"`
	TTL                  int64                 `json:"ttl"`
	MultiValueAnswer     bool                  `json:"multiValueAnswer,omitempty"`
}

// zoneData holds per-zone state.
type zoneData struct {
	records       map[string]*ResourceRecordSet // key: "name|TYPE" or "name|TYPE|SetIdentifier"
	zone          HostedZone
	dnssecEnabled bool
}

// KeySigningKey represents a Route 53 key signing key for DNSSEC.
type KeySigningKey struct {
	CreatedAt                time.Time `json:"createdAt"`
	HostedZoneID             string    `json:"hostedZoneId"`
	Name                     string    `json:"name"`
	KeyManagementServiceArn  string    `json:"keyManagementServiceArn"`
	Status                   string    `json:"status"`
	SigningAlgorithmMnemonic string    `json:"signingAlgorithmMnemonic"`
	DigestAlgorithmMnemonic  string    `json:"digestAlgorithmMnemonic"`
	PublicKey                string    `json:"publicKey"`
	DSRecord                 string    `json:"dsRecord"`
	DigestValue              string    `json:"digestValue"`
	Flag                     int       `json:"flag"`
	SigningAlgorithmType     int       `json:"signingAlgorithmType"`
	DigestAlgorithmType      int       `json:"digestAlgorithmType"`
	KeyTag                   int       `json:"keyTag"`
}

// VPCAssociationAuthorization records an authorized cross-account VPC association.
type VPCAssociationAuthorization struct {
	VPCID     string `json:"vpcId"`
	VPCRegion string `json:"vpcRegion"`
}

// ChangeInfo records the state of a Route 53 change batch.
type ChangeInfo struct {
	SubmittedAt time.Time `json:"submittedAt"`
	ID          string    `json:"id"`
	Status      string    `json:"status"`
}

// CidrCollection represents a Route 53 CIDR collection.
type CidrCollection struct {
	Locations map[string][]string `json:"locations"` // locationName → []CIDR
	ID        string              `json:"id"`
	Name      string              `json:"name"`
	ARN       string              `json:"arn"`
	Version   int64               `json:"version"`
}

// CidrCollectionChange represents a single change in a ChangeCidrCollection request.
type CidrCollectionChange struct {
	LocationName string   `json:"locationName"`
	Action       string   `json:"action"`
	CidrList     []string `json:"cidrList"`
}

// QueryLoggingConfig represents a Route 53 query logging configuration.
type QueryLoggingConfig struct {
	CreatedAt                 time.Time `json:"createdAt"`
	ID                        string    `json:"id"`
	HostedZoneID              string    `json:"hostedZoneId"`
	CloudWatchLogsLogGroupArn string    `json:"cloudWatchLogsLogGroupArn"`
}

// ReusableDelegationSet represents a Route 53 reusable delegation set.
type ReusableDelegationSet struct {
	CreatedAt       time.Time `json:"createdAt"`
	ID              string    `json:"id"`
	CallerReference string    `json:"callerReference"`
	NameServers     []string  `json:"nameServers"`
}

// TrafficPolicy represents a Route 53 traffic policy.
type TrafficPolicy struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Document string `json:"document"`
	Comment  string `json:"comment,omitempty"`
	Type     string `json:"type"`
	Version  int32  `json:"version"`
}

// TrafficPolicyInstance represents a Route 53 traffic policy instance.
type TrafficPolicyInstance struct {
	ID                   string `json:"id"`
	HostedZoneID         string `json:"hostedZoneId"`
	Name                 string `json:"name"`
	TrafficPolicyID      string `json:"trafficPolicyId"`
	TrafficPolicyType    string `json:"trafficPolicyType"`
	State                string `json:"state"`
	TTL                  int64  `json:"ttl"`
	TrafficPolicyVersion int32  `json:"trafficPolicyVersion"`
}

// TrafficPolicySummary is returned by ListTrafficPolicies and includes the version count.
type TrafficPolicySummary struct {
	TrafficPolicy
	VersionCount int32
}

// vpcAssociation records a VPC associated with a hosted zone.
type vpcAssociation struct {
	VPCID     string `json:"vpcId"`
	VPCRegion string `json:"vpcRegion"`
}

// ChangeAction is the action type for ChangeResourceRecordSets.
type ChangeAction string

const (
	ChangeActionCreate ChangeAction = "CREATE"
	ChangeActionDelete ChangeAction = "DELETE"
	ChangeActionUpsert ChangeAction = "UPSERT"
)

// Change represents a single change in a ChangeResourceRecordSets request.
type Change struct {
	Action            ChangeAction
	ResourceRecordSet ResourceRecordSet
}

// dnsOp represents a pending DNS registration to apply after record mutation.
type dnsOp struct {
	name       string
	recordType string
	values     []string
}

// RRSetPage is a page of ResourceRecordSets with pagination cursors.
type RRSetPage struct {
	NextName       string
	NextType       string
	NextIdentifier string
	Records        []ResourceRecordSet
	IsTruncated    bool
}
