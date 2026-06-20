package route53

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	dnsNS1Default = "ns1.gopherstack.invalid"
	dnsNS2Default = "ns2.gopherstack.invalid"
)

// Errors returned by the backend.
var (
	ErrHostedZoneNotFound              = errors.New("NoSuchHostedZone")
	ErrInvalidInput                    = errors.New("InvalidInput")
	ErrInvalidAction                   = errors.New("InvalidChangeBatch")
	ErrHealthCheckNotFound             = errors.New("NoSuchHealthCheck")
	ErrKeySigningKeyNotFound           = errors.New("NoSuchKeySigningKey")
	ErrCidrCollectionNotFound          = errors.New("NoSuchCidrCollection")
	ErrQueryLoggingConfigNotFound      = errors.New("NoSuchQueryLoggingConfig")
	ErrDelegationSetNotFound           = errors.New("NoSuchDelegationSet")
	ErrTrafficPolicyNotFound           = errors.New("NoSuchTrafficPolicy")
	ErrTrafficPolicyInstNotFound       = errors.New("NoSuchTrafficPolicyInstance")
	ErrChangeNotFound                  = errors.New("NoSuchChange")
	ErrNoSuchGeoLocation               = errors.New("NoSuchGeoLocation")
	ErrQueryLoggingConfigAlreadyExists = errors.New("QueryLoggingConfigAlreadyExists")
	ErrPublicZoneVPCAssociation        = errors.New("PublicZoneVPCAssociation")
	ErrVPCAssociationAuthorizationNF   = errors.New("VPCAssociationAuthorizationNotFound")
	ErrKeySigningKeyWithActiveStatusNF = errors.New("KeySigningKeyWithActiveStatusNotFound")
	ErrInvalidARecord                  = errors.New("invalid A record value")
	ErrInvalidAAAARecord               = errors.New("invalid AAAA record value")
	ErrInvalidMXRecord                 = errors.New("invalid MX record value")
	ErrInvalidSRVRecord                = errors.New("invalid SRV record value")
	ErrInvalidCAARecord                = errors.New("invalid CAA record value")
	ErrTrafficPolicyInUse              = errors.New("TrafficPolicyInUse")
	ErrKeySigningKeyNotInactive        = errors.New("KeySigningKeyNotInactive")
	ErrTrafficPolicyAlreadyExists      = errors.New("TrafficPolicyAlreadyExists")
	ErrHostedZoneNotEmpty              = errors.New("HostedZoneNotEmpty")
	ErrLastVPCAssociation              = errors.New("LastVPCAssociation")
)

const (
	recordTypeA     = "A"
	recordTypeAAAA  = "AAAA"
	recordTypeCNAME = "CNAME"
	recordTypeMX    = "MX"
	recordTypeTXT   = "TXT"
	recordTypeNS    = "NS"
	recordTypeSOA   = "SOA"
	recordTypePTR   = "PTR"
	recordTypeSRV   = "SRV"
	recordTypeCAA   = "CAA"
	recordTypeDS    = "DS"
	recordTypeHTTPS = "HTTPS"
	recordTypeSVCB  = "SVCB"
	recordTypeSSHFP = "SSHFP"
	recordTypeTLSA  = "TLSA"
	recordTypeNAPTR = "NAPTR"
	recordTypeSPF   = "SPF"
)

// validRecordTypes is the set of record types that AWS Route 53 accepts for
// user-managed ResourceRecordSets (DNSKEY/NSEC are DNSSEC-internal only).
//
//nolint:gochecknoglobals // package-level table initialized once at startup
var validRecordTypes = map[string]bool{
	recordTypeA: true, recordTypeAAAA: true, recordTypeCNAME: true,
	recordTypeMX: true, recordTypeTXT: true, recordTypeNS: true,
	recordTypeSOA: true, recordTypePTR: true, recordTypeSRV: true,
	recordTypeCAA: true, recordTypeDS: true, recordTypeHTTPS: true,
	recordTypeSVCB: true, recordTypeSSHFP: true, recordTypeTLSA: true,
	recordTypeNAPTR: true, recordTypeSPF: true,
}

const (
	maxChangesPerBatch = 1000
	maxTTL             = 2147483647
)

// record value format validators (minimal per-type).
var (
	reMX  = regexp.MustCompile(`^\d+ \S+$`)
	reSRV = regexp.MustCompile(`^\d+ \d+ \d+ \S+$`)
	reCAA = regexp.MustCompile(`^\d+ \S+ "`)
)

const (
	kskStatusActive   = "ACTIVE"
	kskStatusInactive = "INACTIVE"
	tpiStateApplied   = "Applied"
)

const (
	defaultRegion    = "us-east-1"
	defaultAccountID = "123456789012"
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

// HealthCheck represents a Route 53 health check.
type HealthCheck struct {
	CreatedAt       time.Time         `json:"createdAt"`
	ID              string            `json:"id"`
	CallerReference string            `json:"callerReference"`
	Status          string            `json:"status"`
	Config          HealthCheckConfig `json:"config"`
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
	CreatedAt              time.Time `json:"createdAt"`
	Name                   string    `json:"name"`
	ID                     string    `json:"id"`
	CallerReference        string    `json:"callerReference"`
	Comment                string    `json:"comment"`
	ResourceRecordSetCount int       `json:"resourceRecordSetCount"`
	PrivateZone            bool      `json:"privateZone"`
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
	Name                 string                `json:"name"`
	Type                 string                `json:"type"`
	SetIdentifier        string                `json:"setIdentifier,omitempty"`
	Failover             FailoverPolicy        `json:"failover,omitempty"`
	Region               string                `json:"region,omitempty"`
	HealthCheckID        string                `json:"healthCheckId,omitempty"`
	Records              []ResourceRecord      `json:"records"`
	TTL                  int64                 `json:"ttl"`
	Weight               int64                 `json:"weight,omitempty"`
	MultiValueAnswer     bool                  `json:"multiValueAnswer,omitempty"`
}

// recordSetKey builds the map key for a resource record set.
// When SetIdentifier is non-empty it is included so routing-policy records
// with the same name/type can coexist.
func recordSetKey(name, rrType, setIdentifier string) string {
	base := strings.ToLower(strings.TrimSuffix(name, ".")) + "|" + strings.ToUpper(rrType)
	if setIdentifier != "" {
		return base + "|" + setIdentifier
	}

	return base
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

// InMemoryBackend stores Route 53 state in memory.
type InMemoryBackend struct {
	dns                    DNSRegistrar
	zones                  map[string]*zoneData                     // key: zone ID
	healthChecks           map[string]*HealthCheck                  // key: health check ID
	keySigningKeys         map[string]*KeySigningKey                // key: "hostedZoneId|name"
	cidrCollections        map[string]*CidrCollection               // key: collection ID
	queryLoggingConfigs    map[string]*QueryLoggingConfig           // key: config ID
	reusableDelegationSets map[string]*ReusableDelegationSet        // key: delegation set ID
	trafficPolicies        map[string][]*TrafficPolicy              // key: policy ID, value: versions
	trafficPolicyInstances map[string]*TrafficPolicyInstance        // key: instance ID
	vpcAssociations        map[string][]vpcAssociation              // key: zone ID
	vpcAssocAuthorizations map[string][]VPCAssociationAuthorization // key: zone ID
	changes                map[string]*ChangeInfo                   // key: change ID
	mu                     *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		zones:                  make(map[string]*zoneData),
		healthChecks:           make(map[string]*HealthCheck),
		keySigningKeys:         make(map[string]*KeySigningKey),
		cidrCollections:        make(map[string]*CidrCollection),
		queryLoggingConfigs:    make(map[string]*QueryLoggingConfig),
		reusableDelegationSets: make(map[string]*ReusableDelegationSet),
		trafficPolicies:        make(map[string][]*TrafficPolicy),
		trafficPolicyInstances: make(map[string]*TrafficPolicyInstance),
		vpcAssociations:        make(map[string][]vpcAssociation),
		vpcAssocAuthorizations: make(map[string][]VPCAssociationAuthorization),
		changes:                make(map[string]*ChangeInfo),
		mu:                     lockmetrics.New("route53"),
	}
}

// SetDNSRegistrar wires a DNS server so A/CNAME records are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dns = dns
	b.mu.Unlock()
}

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return defaultRegion }

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

const (
	zoneIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	zoneIDLength = 13
)

// randomID generates a random ID of the given length using the provided character set.
func randomID(chars string, length int) string {
	buf := make([]byte, length)
	n := uint64(len(chars))

	for i := range buf {
		var v [8]byte
		_, _ = rand.Read(v[:])
		buf[i] = chars[binary.BigEndian.Uint64(v[:])%n]
	}

	return string(buf)
}

func randomZoneID() string { return randomID(zoneIDChars, zoneIDLength) }

// normaliseName ensures the zone/record name ends with a dot.
func normaliseName(name string) string {
	if !strings.HasSuffix(name, ".") {
		return name + "."
	}

	return name
}

// CreateHostedZone creates a new hosted zone.
func (b *InMemoryBackend) CreateHostedZone(
	name, callerRef, comment string,
	private bool,
) (*HostedZone, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	name = normaliseName(name)

	b.mu.Lock("CreateHostedZone")
	defer b.mu.Unlock()

	// CallerReference idempotency: AWS returns the existing zone when the same
	// CallerReference is reused, rather than creating a duplicate.
	for _, zd := range b.zones {
		if zd.zone.CallerReference == callerRef {
			cp := zd.zone
			cp.ResourceRecordSetCount = len(zd.records)

			return &cp, nil
		}
	}

	id := "Z" + randomZoneID()
	hz := HostedZone{
		ID:              id,
		Name:            name,
		CallerReference: callerRef,
		Comment:         comment,
		PrivateZone:     private,
		CreatedAt:       time.Now(),
	}

	b.zones[id] = &zoneData{
		zone:    hz,
		records: make(map[string]*ResourceRecordSet),
	}

	// Register a synthetic INSYNC change so that GetChange on the zone-creation
	// change ID (used by Terraform's waiter) returns INSYNC immediately.
	syntheticChangeID := "C" + id
	b.changes[syntheticChangeID] = &ChangeInfo{
		ID:          "/change/" + syntheticChangeID,
		Status:      "INSYNC",
		SubmittedAt: time.Now(),
	}

	cp := hz

	return &cp, nil
}

// DeleteHostedZone removes a hosted zone and all its record sets.
func (b *InMemoryBackend) DeleteHostedZone(zoneID string) error {
	b.mu.Lock("DeleteHostedZone")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	// AWS rejects deletion of zones that still contain resource record sets.
	// The zone is considered non-empty if it has any user-managed records.
	if len(zd.records) > 0 {
		return fmt.Errorf(
			"%w: hosted zone %s contains resource record sets that must be deleted first",
			ErrHostedZoneNotEmpty,
			zoneID,
		)
	}

	// Deregister all DNS records before deletion.
	if b.dns != nil {
		for _, rrs := range zd.records {
			if rrs.Type == recordTypeA || rrs.Type == recordTypeCNAME || rrs.Type == recordTypeAAAA ||
				rrs.AliasTarget != nil {
				b.dns.Deregister(rrs.Name)
			}
		}
	}

	// Cascade: delete VPC associations for this zone.
	delete(b.vpcAssociations, zoneID)
	// Cascade: delete query logging configs for this zone.
	for id, cfg := range b.queryLoggingConfigs {
		if cfg.HostedZoneID == zoneID {
			delete(b.queryLoggingConfigs, id)
		}
	}
	// Cascade: delete key signing keys for this zone.
	for k, ksk := range b.keySigningKeys {
		if ksk.HostedZoneID == zoneID {
			delete(b.keySigningKeys, k)
		}
	}

	delete(b.zones, zoneID)

	return nil
}

// GetHostedZone returns a single hosted zone.
func (b *InMemoryBackend) GetHostedZone(zoneID string) (*HostedZone, error) {
	b.mu.RLock("GetHostedZone")
	defer b.mu.RUnlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	cp := zd.zone
	cp.ResourceRecordSetCount = len(zd.records)

	return &cp, nil
}

const route53DefaultMaxItems = 100

// ListHostedZones returns hosted zones sorted by name, with optional pagination.
func (b *InMemoryBackend) ListHostedZones(
	marker string,
	maxItems int,
) (page.Page[HostedZone], error) {
	b.mu.RLock("ListHostedZones")
	defer b.mu.RUnlock()

	result := make([]HostedZone, 0, len(b.zones))
	for _, zd := range b.zones {
		cp := zd.zone
		cp.ResourceRecordSetCount = len(zd.records)
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
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

// validateRecordValue checks per-type value format constraints.
func validateRecordValue(rrType, value string) error {
	switch rrType {
	case recordTypeA:
		if net.ParseIP(value) == nil || strings.Contains(value, ":") {
			return fmt.Errorf("invalid IPv4 address for A record %q: %w", value, ErrInvalidARecord)
		}
	case recordTypeAAAA:
		if net.ParseIP(value) == nil || !strings.Contains(value, ":") {
			return fmt.Errorf("invalid IPv6 address for AAAA record %q: %w", value, ErrInvalidAAAARecord)
		}
	case recordTypeMX:
		if !reMX.MatchString(value) {
			return fmt.Errorf("MX record must be \"priority host\", got %q: %w", value, ErrInvalidMXRecord)
		}
	case recordTypeSRV:
		if !reSRV.MatchString(value) {
			return fmt.Errorf(
				"SRV record must be \"priority weight port target\", got %q: %w",
				value, ErrInvalidSRVRecord,
			)
		}
	case recordTypeCAA:
		if !reCAA.MatchString(value) {
			return fmt.Errorf("CAA record must be \"flag tag \\\"value\\\"\", got %q: %w", value, ErrInvalidCAARecord)
		}
	}

	return nil
}

// validateRoutingPolicy enforces AWS mutual-exclusion rules for routing policies.
//
//nolint:cyclop // AWS has many mutually exclusive routing policy combinations to check
func validateRoutingPolicy(rrs ResourceRecordSet) error {
	policyCount := 0
	// Weight=0 is a valid weighted routing value (used to stop sending traffic to a record).
	// Because the Weight field defaults to zero when omitted from XML, we use Weight > 0
	// as the weighted-routing indicator in policy counting; the weight range check below
	// allows 0 through 255 so an explicit Weight=0 is still accepted once policyCount is
	// confirmed to be 1 via another routing field or by the caller using a pointer type.
	if rrs.Weight > 0 {
		policyCount++
	}

	if rrs.Region != "" {
		policyCount++
	}

	if rrs.GeoLocation != nil {
		policyCount++
	}

	if rrs.Failover != "" {
		policyCount++
	}

	if rrs.MultiValueAnswer {
		policyCount++
	}

	if rrs.GeoProximityLocation != nil {
		policyCount++
	}

	if rrs.CidrRoutingConfig != nil {
		policyCount++
	}

	if policyCount > 1 {
		return fmt.Errorf(
			"%w: only one routing policy may be set per ResourceRecordSet",
			ErrInvalidInput,
		)
	}

	if policyCount == 1 && rrs.SetIdentifier == "" {
		return fmt.Errorf(
			"%w: SetIdentifier is required when a routing policy is specified",
			ErrInvalidInput,
		)
	}

	if policyCount == 0 && rrs.SetIdentifier != "" {
		return fmt.Errorf(
			"%w: SetIdentifier is not allowed without a routing policy",
			ErrInvalidInput,
		)
	}

	if rrs.Failover != "" && rrs.Failover != FailoverPrimary && rrs.Failover != FailoverSecondary {
		return fmt.Errorf("%w: Failover must be PRIMARY or SECONDARY", ErrInvalidInput)
	}

	if rrs.Weight < 0 || rrs.Weight > 255 {
		return fmt.Errorf("%w: Weight must be in range [0, 255]", ErrInvalidInput)
	}

	if err := validateGeoProximityLocation(rrs.GeoProximityLocation); err != nil {
		return err
	}

	return nil
}

// geoProximityLocationFieldCount counts how many of the three mutually exclusive
// routing fields are set on a GeoProximityLocation.
func geoProximityLocationFieldCount(gpl *GeoProximityLocation) int {
	count := 0
	if gpl.AWSRegion != "" {
		count++
	}
	if gpl.Coordinates != nil {
		count++
	}
	if gpl.LocalZoneGroup != "" {
		count++
	}

	return count
}

// validateGeoProximityCoordinates validates the lat/lon values inside a Coordinates block.
func validateGeoProximityCoordinates(coords *GeoProximityCoordinates) error {
	const (
		latMin = -90.0
		latMax = 90.0
		lonMin = -180.0
		lonMax = 180.0
	)

	lat, err := strconv.ParseFloat(coords.Latitude, 64)
	if err != nil || lat < latMin || lat > latMax {
		return fmt.Errorf(
			"%w: GeoProximityLocation Coordinates Latitude must be a number in [%g, %g]",
			ErrInvalidInput, latMin, latMax,
		)
	}

	lon, err := strconv.ParseFloat(coords.Longitude, 64)
	if err != nil || lon < lonMin || lon > lonMax {
		return fmt.Errorf(
			"%w: GeoProximityLocation Coordinates Longitude must be a number in [%g, %g]",
			ErrInvalidInput, lonMin, lonMax,
		)
	}

	return nil
}

// validateGeoProximityLocation enforces AWS constraints on GeoProximityLocation routing config.
func validateGeoProximityLocation(gpl *GeoProximityLocation) error {
	if gpl == nil {
		return nil
	}

	count := geoProximityLocationFieldCount(gpl)

	if count == 0 {
		return fmt.Errorf(
			"%w: GeoProximityLocation requires one of AWSRegion, Coordinates, or LocalZoneGroup",
			ErrInvalidInput,
		)
	}

	if count > 1 {
		return fmt.Errorf(
			"%w: GeoProximityLocation must specify exactly one of AWSRegion, Coordinates, or LocalZoneGroup",
			ErrInvalidInput,
		)
	}

	const (
		biasMin = -99
		biasMax = 99
	)

	if gpl.Bias < biasMin || gpl.Bias > biasMax {
		return fmt.Errorf(
			"%w: GeoProximityLocation Bias must be in range [%d, %d]",
			ErrInvalidInput, biasMin, biasMax,
		)
	}

	if gpl.Coordinates != nil {
		return validateGeoProximityCoordinates(gpl.Coordinates)
	}

	return nil
}

// validateHealthCheckConfig enforces AWS type-specific constraints on a HealthCheckConfig.
func validateHealthCheckConfig(cfg HealthCheckConfig) error {
	if cfg.Type == HealthCheckTypeCloudWatchMetric && cfg.AlarmIdentifier == nil {
		return fmt.Errorf(
			"%w: CLOUDWATCH_METRIC health checks require AlarmIdentifier",
			ErrInvalidInput,
		)
	}

	if cfg.Type == HealthCheckTypeRecoveryControl && cfg.RoutingControlArn == "" {
		return fmt.Errorf(
			"%w: RECOVERY_CONTROL health checks require RoutingControlArn",
			ErrInvalidInput,
		)
	}

	if cfg.InsufficientDataHealthStatus != "" {
		switch cfg.InsufficientDataHealthStatus {
		case defaultHealthStatus, "Unhealthy", "LastKnownStatus":
		default:
			return fmt.Errorf(
				"%w: InsufficientDataHealthStatus must be Healthy, Unhealthy, or LastKnownStatus",
				ErrInvalidInput,
			)
		}
	}

	return nil
}

// validateChange validates a single change against current zone state.
//
//nolint:gocognit,cyclop // complex by necessity: enforces all AWS record mutation constraints
func validateChange(zd *zoneData, ch Change) error {
	rrs := ch.ResourceRecordSet

	if !validRecordTypes[rrs.Type] {
		return fmt.Errorf("%w: unsupported record type %q", ErrInvalidAction, rrs.Type)
	}

	if rrs.AliasTarget == nil {
		if rrs.TTL <= 0 {
			return fmt.Errorf("%w: TTL must be > 0 for non-alias records", ErrInvalidAction)
		}

		if rrs.TTL > maxTTL {
			return fmt.Errorf("%w: TTL exceeds maximum value %d", ErrInvalidAction, maxTTL)
		}
	}

	if rrs.Type == recordTypeCNAME && rrs.Name == zd.zone.Name {
		return fmt.Errorf(
			"%w: CNAME record not permitted at zone apex %s",
			ErrInvalidAction,
			zd.zone.Name,
		)
	}

	if rrs.Type == recordTypeCNAME {
		key := recordSetKey(rrs.Name, rrs.Type, "")
		for existKey, existRRS := range zd.records {
			if existKey == key {
				continue
			}
			existName := strings.ToLower(strings.TrimSuffix(existRRS.Name, "."))
			rrsName := strings.ToLower(strings.TrimSuffix(rrs.Name, "."))
			if existName == rrsName && existRRS.Type != recordTypeCNAME {
				return fmt.Errorf(
					"%w: CNAME cannot coexist with other types at name %s",
					ErrInvalidAction, rrs.Name,
				)
			}
		}
	}

	for _, rr := range rrs.Records {
		if err := validateRecordValue(rrs.Type, rr.Value); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidAction, err.Error())
		}
	}

	if err := validateRoutingPolicy(rrs); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidAction, err.Error())
	}

	if ch.Action == ChangeActionDelete {
		key := recordSetKey(rrs.Name, rrs.Type, rrs.SetIdentifier)
		existing, exists := zd.records[key]
		if !exists {
			return fmt.Errorf(
				"%w: record set %s %s not found for DELETE",
				ErrInvalidAction,
				rrs.Name,
				rrs.Type,
			)
		}

		// AWS requires a DELETE to specify values that exactly match the existing
		// record set (TTL and all resource record values, or the AliasTarget).
		// If they do not match, Route 53 returns InvalidChangeBatch rather than
		// silently deleting the record.
		if err := deleteValuesMatch(existing, &rrs); err != nil {
			return err
		}
	}

	if ch.Action == ChangeActionCreate {
		key := recordSetKey(rrs.Name, rrs.Type, rrs.SetIdentifier)
		if _, exists := zd.records[key]; exists {
			return fmt.Errorf(
				"%w: record set %s %s already exists",
				ErrInvalidAction,
				rrs.Name,
				rrs.Type,
			)
		}
	}

	return nil
}

// deleteValuesMatch enforces AWS's DELETE exact-match rule. When deleting a
// resource record set you must supply the same TTL and the same set of resource
// record values (or the same AliasTarget) that the record currently holds. If
// the supplied change omits values/TTL entirely (a bare name+type delete) AWS
// still accepts it, so we only enforce a match when the caller actually provided
// values to compare against.
func deleteValuesMatch(existing, want *ResourceRecordSet) error {
	// Alias vs non-alias mismatch is always an error when an AliasTarget is given.
	if want.AliasTarget != nil || existing.AliasTarget != nil {
		if !aliasTargetsEqual(existing.AliasTarget, want.AliasTarget) {
			return deleteMismatchErr(want)
		}

		return nil
	}

	// Bare delete: no values and no TTL supplied — accept (matches AWS, which
	// keys the delete on name+type+SetIdentifier in that case).
	if len(want.Records) == 0 && want.TTL == 0 {
		return nil
	}

	if want.TTL != 0 && want.TTL != existing.TTL {
		return deleteMismatchErr(want)
	}

	if len(want.Records) > 0 && !sameValueSet(rrsValues(existing), rrsValues(want)) {
		return deleteMismatchErr(want)
	}

	return nil
}

// aliasTargetsEqual reports whether two AliasTargets are equivalent for the
// purpose of DELETE matching (DNS name compared case-insensitively, ignoring a
// trailing dot, alongside hosted-zone ID and EvaluateTargetHealth).
func aliasTargetsEqual(a, b *AliasTarget) bool {
	if a == nil || b == nil {
		return a == b
	}

	aName := strings.ToLower(strings.TrimSuffix(a.DNSName, "."))
	bName := strings.ToLower(strings.TrimSuffix(b.DNSName, "."))

	return aName == bName &&
		a.HostedZoneID == b.HostedZoneID &&
		a.EvaluateTargetHealth == b.EvaluateTargetHealth
}

// sameValueSet reports whether two value slices contain the same multiset of
// values, irrespective of order (Route 53 treats resource record values as an
// unordered set).
func sameValueSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}

	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}

	return true
}

// deleteMismatchErr builds the AWS-style InvalidChangeBatch error returned when
// a DELETE does not match the current values of the record set.
func deleteMismatchErr(rrs *ResourceRecordSet) error {
	return fmt.Errorf(
		"%w: Tried to delete resource record set [name='%s', type='%s'] "+
			"but the values provided do not match the current values",
		ErrInvalidAction,
		rrs.Name,
		rrs.Type,
	)
}

// dnsOp represents a pending DNS registration to apply after record mutation.
type dnsOp struct {
	name       string
	recordType string
	values     []string
}

// collectDNSRegisterOp returns a dnsOp (non-nil) if rrs requires DNS registration,
// or nil when no DNS side-effect is needed.
func collectDNSRegisterOp(rrs ResourceRecordSet) *dnsOp {
	switch rrs.Type {
	case recordTypeA, recordTypeAAAA:
		vals := make([]string, 0, len(rrs.Records))
		for _, r := range rrs.Records {
			vals = append(vals, r.Value)
		}
		if rrs.AliasTarget != nil {
			vals = append(vals, strings.TrimSuffix(rrs.AliasTarget.DNSName, "."))
		}
		if len(vals) > 0 {
			return &dnsOp{name: rrs.Name, recordType: rrs.Type, values: vals}
		}
	case recordTypeCNAME:
		vals := make([]string, 0, len(rrs.Records))
		for _, r := range rrs.Records {
			vals = append(vals, r.Value)
		}
		if rrs.AliasTarget != nil {
			vals = append(vals, strings.TrimSuffix(rrs.AliasTarget.DNSName, "."))
		}
		if len(vals) > 0 {
			return &dnsOp{name: rrs.Name, recordType: "CNAME", values: vals}
		}
	default:
		if rrs.AliasTarget != nil {
			return &dnsOp{
				name:       rrs.Name,
				recordType: "ALIAS",
				values:     []string{strings.TrimSuffix(rrs.AliasTarget.DNSName, ".")},
			}
		}
	}

	return nil
}

// applyChange mutates zd for one Change, collecting DNS ops into toRegister/toDeregister.
// Returns an error only for unknown actions.
func applyChange(zd *zoneData, ch Change, toRegister *[]dnsOp, toDeregister *[]string, hasDNS bool) error {
	rrs := ch.ResourceRecordSet
	key := recordSetKey(rrs.Name, rrs.Type, rrs.SetIdentifier)

	switch ch.Action {
	case ChangeActionCreate, ChangeActionUpsert:
		cp := rrs
		zd.records[key] = &cp

		if hasDNS {
			if op := collectDNSRegisterOp(rrs); op != nil {
				*toRegister = append(*toRegister, *op)
			}
		}

	case ChangeActionDelete:
		delete(zd.records, key)

		if hasDNS &&
			(rrs.Type == recordTypeA || rrs.Type == recordTypeCNAME || rrs.Type == recordTypeAAAA || rrs.AliasTarget != nil) {
			*toDeregister = append(*toDeregister, rrs.Name)
		}

	default:
		return fmt.Errorf("%w: unknown action %q", ErrInvalidAction, ch.Action)
	}

	return nil
}

// ChangeResourceRecordSets applies a batch of record set changes atomically.
// All changes are validated before any mutation is applied.
func (b *InMemoryBackend) ChangeResourceRecordSets(
	zoneID string,
	changes []Change,
) (string, error) {
	b.mu.Lock("ChangeResourceRecordSets")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return "", fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	if len(changes) > maxChangesPerBatch {
		return "", fmt.Errorf(
			"%w: batch exceeds maximum of %d changes",
			ErrInvalidAction, maxChangesPerBatch,
		)
	}

	// Normalise names and validate all changes before mutating anything.
	normalised := make([]Change, len(changes))
	for i, ch := range changes {
		ch.ResourceRecordSet.Name = normaliseName(ch.ResourceRecordSet.Name)
		normalised[i] = ch

		if err := validateChange(zd, ch); err != nil {
			return "", err
		}
	}

	// All valid — apply.
	var toRegister []dnsOp
	var toDeregister []string

	for _, ch := range normalised {
		if err := applyChange(zd, ch, &toRegister, &toDeregister, b.dns != nil); err != nil {
			return "", err
		}
	}

	// Register/deregister outside the record mutation loop.
	for _, op := range toRegister {
		b.dns.RegisterRecord(op.name, op.recordType, op.values)
	}

	for _, name := range toDeregister {
		b.dns.Deregister(name)
	}

	// Record the change for GetChange.
	changeID := "C" + randomID("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 14) //nolint:mnd // 14-char AWS-style change ID
	ci := &ChangeInfo{
		ID:          "/change/" + changeID,
		Status:      "INSYNC",
		SubmittedAt: time.Now(),
	}
	b.changes[changeID] = ci

	return ci.ID, nil
}

// GetChange returns the ChangeInfo for a given change ID.
func (b *InMemoryBackend) GetChange(changeID string) (*ChangeInfo, error) {
	b.mu.RLock("GetChange")
	defer b.mu.RUnlock()

	ci, ok := b.changes[changeID]
	if !ok {
		return nil, fmt.Errorf("%w: change %s not found", ErrChangeNotFound, changeID)
	}

	cp := *ci

	return &cp, nil
}

// RRSetPage is a page of ResourceRecordSets with pagination cursors.
type RRSetPage struct {
	NextName       string
	NextType       string
	NextIdentifier string
	Records        []ResourceRecordSet
	IsTruncated    bool
}

// ListResourceRecordSets returns resource record sets sorted by (Name, Type, SetIdentifier),
// starting after the given (startName, startType, startIdentifier) tuple, up to maxItems.
// Pass empty strings and 0 to get the first page.
//
//nolint:gocognit,cyclop // pagination + multi-field sort + filtering inherently complex
func (b *InMemoryBackend) ListResourceRecordSets(
	zoneID, startName, startType, startIdentifier string,
	maxItems int,
) (RRSetPage, error) {
	b.mu.RLock("ListResourceRecordSets")
	defer b.mu.RUnlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return RRSetPage{}, fmt.Errorf(
			"%w: hosted zone %s not found",
			ErrHostedZoneNotFound,
			zoneID,
		)
	}

	all := make([]ResourceRecordSet, 0, len(zd.records))
	for _, rrs := range zd.records {
		cp := *rrs
		all = append(all, cp)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Name != all[j].Name {
			return all[i].Name < all[j].Name
		}

		if all[i].Type != all[j].Type {
			return all[i].Type < all[j].Type
		}

		return all[i].SetIdentifier < all[j].SetIdentifier
	})

	// Seek to start position.
	start := 0
	if startName != "" {
		startName = normaliseName(startName)
		for start < len(all) {
			r := all[start]
			if r.Name > startName {
				break
			}
			if r.Name == startName && (startType == "" || r.Type >= startType) {
				if startType == "" || r.Type > startType || startIdentifier == "" ||
					r.SetIdentifier >= startIdentifier {
					break
				}
			}
			start++
		}
	}

	all = all[start:]

	if maxItems <= 0 || maxItems > route53DefaultMaxItems {
		maxItems = route53DefaultMaxItems
	}

	var pg RRSetPage
	if len(all) > maxItems {
		pg.Records = all[:maxItems]
		pg.IsTruncated = true
		next := all[maxItems]
		pg.NextName = next.Name
		pg.NextType = next.Type
		pg.NextIdentifier = next.SetIdentifier
	} else {
		pg.Records = all
	}

	return pg, nil
}

const (
	healthCheckIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	healthCheckIDLength = 36
	defaultHealthStatus = "Healthy"
)

func randomHealthCheckID() string { return randomID(healthCheckIDChars, healthCheckIDLength) }

// CreateHealthCheck creates a new health check.
func (b *InMemoryBackend) CreateHealthCheck(
	callerRef string,
	cfg HealthCheckConfig,
) (*HealthCheck, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	if cfg.Type == "" {
		return nil, fmt.Errorf("%w: health check type is required", ErrInvalidInput)
	}

	if err := validateHealthCheckConfig(cfg); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateHealthCheck")
	defer b.mu.Unlock()

	// CallerReference idempotency: AWS returns the existing health check when the
	// same CallerReference is reused, rather than creating a duplicate.
	for _, existing := range b.healthChecks {
		if existing.CallerReference == callerRef {
			cp := *existing

			return &cp, nil
		}
	}

	hc := &HealthCheck{
		ID:              randomHealthCheckID(),
		CallerReference: callerRef,
		Config:          cfg,
		Status:          defaultHealthStatus,
		CreatedAt:       time.Now(),
	}

	b.healthChecks[hc.ID] = hc

	cp := *hc

	return &cp, nil
}

// GetHealthCheck returns a single health check.
func (b *InMemoryBackend) GetHealthCheck(id string) (*HealthCheck, error) {
	b.mu.RLock("GetHealthCheck")
	defer b.mu.RUnlock()

	hc, ok := b.healthChecks[id]
	if !ok {
		return nil, fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	cp := *hc

	return &cp, nil
}

// ListHealthChecks returns all health checks.
func (b *InMemoryBackend) ListHealthChecks(
	marker string,
	maxItems int,
) (page.Page[HealthCheck], error) {
	b.mu.RLock("ListHealthChecks")
	defer b.mu.RUnlock()

	result := make([]HealthCheck, 0, len(b.healthChecks))
	for _, hc := range b.healthChecks {
		cp := *hc
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
}

// DeleteHealthCheck removes a health check.
func (b *InMemoryBackend) DeleteHealthCheck(id string) error {
	b.mu.Lock("DeleteHealthCheck")
	defer b.mu.Unlock()

	if _, ok := b.healthChecks[id]; !ok {
		return fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	delete(b.healthChecks, id)

	return nil
}

// UpdateHealthCheck updates configuration fields of an existing health check.
func (b *InMemoryBackend) UpdateHealthCheck(
	id string,
	cfg HealthCheckConfig,
) (*HealthCheck, error) {
	b.mu.Lock("UpdateHealthCheck")
	defer b.mu.Unlock()

	hc, ok := b.healthChecks[id]
	if !ok {
		return nil, fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	hc.Config = cfg

	cp := *hc

	return &cp, nil
}

// GetHealthCheckStatus returns the mocked health status for a health check.
func (b *InMemoryBackend) GetHealthCheckStatus(id string) (string, error) {
	b.mu.RLock("GetHealthCheckStatus")
	defer b.mu.RUnlock()

	hc, ok := b.healthChecks[id]
	if !ok {
		return "", fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	return hc.Status, nil
}

// SetHealthCheckStatus overrides the mocked health status for a health check.
// This allows tests to simulate failover scenarios.
func (b *InMemoryBackend) SetHealthCheckStatus(id, status string) error {
	b.mu.Lock("SetHealthCheckStatus")
	defer b.mu.Unlock()

	hc, ok := b.healthChecks[id]
	if !ok {
		return fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	hc.Status = status

	return nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.zones = make(map[string]*zoneData)
	b.healthChecks = make(map[string]*HealthCheck)
	b.keySigningKeys = make(map[string]*KeySigningKey)
	b.cidrCollections = make(map[string]*CidrCollection)
	b.queryLoggingConfigs = make(map[string]*QueryLoggingConfig)
	b.reusableDelegationSets = make(map[string]*ReusableDelegationSet)
	b.trafficPolicies = make(map[string][]*TrafficPolicy)
	b.trafficPolicyInstances = make(map[string]*TrafficPolicyInstance)
	b.vpcAssociations = make(map[string][]vpcAssociation)
	b.vpcAssocAuthorizations = make(map[string][]VPCAssociationAuthorization)
	b.changes = make(map[string]*ChangeInfo)
}

// kskKey builds the map key for a key signing key.
func kskKey(hostedZoneID, name string) string { return hostedZoneID + "|" + name }

// CreateKeySigningKey creates a new key signing key for a hosted zone.
func (b *InMemoryBackend) CreateKeySigningKey(
	hostedZoneID, _ /* callerRef */, name, kmsArn, status string,
) (*KeySigningKey, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateKeySigningKey")
	defer b.mu.Unlock()

	if _, ok := b.zones[hostedZoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	if _, exists := b.keySigningKeys[kskKey(hostedZoneID, name)]; exists {
		return nil, fmt.Errorf(
			"%w: key signing key %s already exists in zone %s",
			ErrInvalidInput,
			name,
			hostedZoneID,
		)
	}

	if status == "" {
		status = kskStatusInactive
	}

	// Generate deterministic stub signing-algorithm fields per AWS spec (ECDSAP256SHA256).
	const (
		kskKeyTagLen         = 5
		kskKeyTagBase        = 10
		kskPublicKeyLen      = 88
		kskDigestLen         = 64
		kskAlgorithmType     = 13
		kskDigestAlgType     = 2
		kskDNSKEYFlag        = 257
		kskDSRecordAlgType   = 13
		kskDSRecordDigestAlg = 2
	)
	keyTagRaw := randomID("0123456789", kskKeyTagLen)
	keyTag := 0
	for _, ch := range keyTagRaw {
		keyTag = keyTag*kskKeyTagBase + int(ch-'0')
	}
	pubKey := randomID("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", kskPublicKeyLen)
	digest := randomID("0123456789abcdef", kskDigestLen)
	dsRecord := fmt.Sprintf("%d %d %d %s", keyTag, kskDSRecordAlgType, kskDSRecordDigestAlg, digest)

	ksk := &KeySigningKey{
		HostedZoneID:             hostedZoneID,
		Name:                     name,
		KeyManagementServiceArn:  kmsArn,
		Status:                   status,
		CreatedAt:                time.Now(),
		Flag:                     kskDNSKEYFlag,
		SigningAlgorithmMnemonic: "ECDSAP256SHA256",
		SigningAlgorithmType:     kskAlgorithmType,
		DigestAlgorithmMnemonic:  "SHA-256",
		DigestAlgorithmType:      kskDigestAlgType,
		KeyTag:                   keyTag,
		PublicKey:                pubKey,
		DigestValue:              digest,
		DSRecord:                 dsRecord,
	}

	b.keySigningKeys[kskKey(hostedZoneID, name)] = ksk

	cp := *ksk

	return &cp, nil
}

// ActivateKeySigningKey activates an existing key signing key.
func (b *InMemoryBackend) ActivateKeySigningKey(hostedZoneID, name string) (*KeySigningKey, error) {
	b.mu.Lock("ActivateKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)

	ksk, ok := b.keySigningKeys[key]
	if !ok {
		return nil, fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	ksk.Status = kskStatusActive

	cp := *ksk

	return &cp, nil
}

// DeactivateKeySigningKey deactivates an existing key signing key.
func (b *InMemoryBackend) DeactivateKeySigningKey(
	hostedZoneID, name string,
) (*KeySigningKey, error) {
	b.mu.Lock("DeactivateKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)
	ksk, ok := b.keySigningKeys[key]
	if !ok {
		return nil, fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	ksk.Status = kskStatusInactive
	cp := *ksk

	return &cp, nil
}

// DeleteKeySigningKey deletes a key signing key.
// The KSK must be INACTIVE; deleting an ACTIVE KSK returns ErrKeySigningKeyNotInactive.
func (b *InMemoryBackend) DeleteKeySigningKey(hostedZoneID, name string) error {
	b.mu.Lock("DeleteKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)
	ksk, ok := b.keySigningKeys[key]
	if !ok {
		return fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	if ksk.Status == kskStatusActive {
		return fmt.Errorf(
			"%w: key signing key %s must be INACTIVE before deletion",
			ErrKeySigningKeyNotInactive,
			name,
		)
	}

	delete(b.keySigningKeys, key)

	return nil
}

// EnableHostedZoneDNSSEC enables DNSSEC for a hosted zone.
// Requires at least one ACTIVE KSK; returns ErrKeySigningKeyWithActiveStatusNF otherwise.
func (b *InMemoryBackend) EnableHostedZoneDNSSEC(zoneID string) error {
	b.mu.Lock("EnableHostedZoneDNSSEC")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	hasActiveKSK := false

	for _, ksk := range b.keySigningKeys {
		if ksk.HostedZoneID == zoneID && ksk.Status == kskStatusActive {
			hasActiveKSK = true

			break
		}
	}

	if !hasActiveKSK {
		return fmt.Errorf(
			"%w: hosted zone %s has no ACTIVE key signing key",
			ErrKeySigningKeyWithActiveStatusNF, zoneID,
		)
	}

	zd.dnssecEnabled = true

	return nil
}

// DisableHostedZoneDNSSEC disables DNSSEC for a hosted zone.
func (b *InMemoryBackend) DisableHostedZoneDNSSEC(zoneID string) error {
	b.mu.Lock("DisableHostedZoneDNSSEC")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	zd.dnssecEnabled = false

	return nil
}

// GetDNSSEC returns the DNSSEC status and key signing keys for a hosted zone.
func (b *InMemoryBackend) GetDNSSEC(zoneID string) (bool, []KeySigningKey, error) {
	b.mu.RLock("GetDNSSEC")
	defer b.mu.RUnlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return false, nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	var ksks []KeySigningKey
	for _, ksk := range b.keySigningKeys {
		if ksk.HostedZoneID == zoneID {
			cp := *ksk
			ksks = append(ksks, cp)
		}
	}

	sort.Slice(ksks, func(i, j int) bool { return ksks[i].Name < ksks[j].Name })

	return zd.dnssecEnabled, ksks, nil
}

// AssociateVPCWithHostedZone associates a VPC with a private hosted zone.
// Returns ErrPublicZoneVPCAssociation when called on a public zone.
func (b *InMemoryBackend) AssociateVPCWithHostedZone(zoneID, vpcID, vpcRegion string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VPCId is required", ErrInvalidInput)
	}

	b.mu.Lock("AssociateVPCWithHostedZone")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	if !zd.zone.PrivateZone {
		return fmt.Errorf(
			"%w: cannot associate a VPC with public hosted zone %s",
			ErrPublicZoneVPCAssociation,
			zoneID,
		)
	}

	for _, existing := range b.vpcAssociations[zoneID] {
		if existing.VPCID == vpcID {
			return fmt.Errorf(
				"%w: VPC %s already associated with hosted zone %s",
				ErrInvalidInput,
				vpcID,
				zoneID,
			)
		}
	}

	b.vpcAssociations[zoneID] = append(b.vpcAssociations[zoneID], vpcAssociation{
		VPCID:     vpcID,
		VPCRegion: vpcRegion,
	})

	return nil
}

// DisassociateVPCFromHostedZone removes a VPC association from a private hosted zone.
func (b *InMemoryBackend) DisassociateVPCFromHostedZone(zoneID, vpcID string) error {
	b.mu.Lock("DisassociateVPCFromHostedZone")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	if !zd.zone.PrivateZone {
		return fmt.Errorf("%w: hosted zone %s is not private", ErrInvalidInput, zoneID)
	}

	assocs := b.vpcAssociations[zoneID]
	newAssocs := assocs[:0:0]

	for _, a := range assocs {
		if a.VPCID != vpcID {
			newAssocs = append(newAssocs, a)
		}
	}

	if len(newAssocs) == len(assocs) {
		return fmt.Errorf(
			"%w: VPC %s is not associated with hosted zone %s",
			ErrInvalidInput,
			vpcID,
			zoneID,
		)
	}

	// AWS rejects removal of the last VPC from a private hosted zone.
	if len(newAssocs) == 0 {
		return fmt.Errorf(
			"%w: cannot disassociate the last VPC from private hosted zone %s",
			ErrLastVPCAssociation,
			zoneID,
		)
	}

	b.vpcAssociations[zoneID] = newAssocs

	return nil
}

// ListVPCAssociations returns all VPCs associated with a hosted zone.
func (b *InMemoryBackend) ListVPCAssociations(zoneID string) ([]vpcAssociation, error) {
	b.mu.RLock("ListVPCAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.zones[zoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	result := make([]vpcAssociation, len(b.vpcAssociations[zoneID]))
	copy(result, b.vpcAssociations[zoneID])

	return result, nil
}

// CreateVPCAssociationAuthorization authorizes a VPC to be associated with a hosted zone
// from another AWS account.
func (b *InMemoryBackend) CreateVPCAssociationAuthorization(
	zoneID, vpcID, vpcRegion string,
) (*VPCAssociationAuthorization, error) {
	b.mu.Lock("CreateVPCAssociationAuthorization")
	defer b.mu.Unlock()

	if _, ok := b.zones[zoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	for _, auth := range b.vpcAssocAuthorizations[zoneID] {
		if auth.VPCID == vpcID {
			return nil, fmt.Errorf(
				"%w: VPC %s authorization already exists for zone %s",
				ErrInvalidInput,
				vpcID,
				zoneID,
			)
		}
	}

	auth := VPCAssociationAuthorization{VPCID: vpcID, VPCRegion: vpcRegion}
	b.vpcAssocAuthorizations[zoneID] = append(b.vpcAssocAuthorizations[zoneID], auth)

	cp := auth

	return &cp, nil
}

// DeleteVPCAssociationAuthorization removes a VPC association authorization.
func (b *InMemoryBackend) DeleteVPCAssociationAuthorization(zoneID, vpcID string) error {
	b.mu.Lock("DeleteVPCAssociationAuthorization")
	defer b.mu.Unlock()

	if _, ok := b.zones[zoneID]; !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	auths := b.vpcAssocAuthorizations[zoneID]
	newAuths := auths[:0:0]

	for _, a := range auths {
		if a.VPCID != vpcID {
			newAuths = append(newAuths, a)
		}
	}

	if len(newAuths) == len(auths) {
		return fmt.Errorf(
			"%w: authorization for VPC %s not found in zone %s",
			ErrVPCAssociationAuthorizationNF,
			vpcID,
			zoneID,
		)
	}

	if len(newAuths) == 0 {
		delete(b.vpcAssocAuthorizations, zoneID)
	} else {
		b.vpcAssocAuthorizations[zoneID] = newAuths
	}

	return nil
}

// ListVPCAssociationAuthorizations returns all VPC association authorizations for a hosted zone.
func (b *InMemoryBackend) ListVPCAssociationAuthorizations(
	zoneID string,
) ([]VPCAssociationAuthorization, error) {
	b.mu.RLock("ListVPCAssociationAuthorizations")
	defer b.mu.RUnlock()

	if _, ok := b.zones[zoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	result := make([]VPCAssociationAuthorization, len(b.vpcAssocAuthorizations[zoneID]))
	copy(result, b.vpcAssocAuthorizations[zoneID])

	return result, nil
}

// ListHostedZonesByVPC returns all private hosted zones that have a VPC association with the given VPC.
func (b *InMemoryBackend) ListHostedZonesByVPC(vpcID, vpcRegion string) ([]HostedZone, error) {
	b.mu.RLock("ListHostedZonesByVPC")
	defer b.mu.RUnlock()

	var result []HostedZone

	for zoneID, assocs := range b.vpcAssociations {
		for _, a := range assocs {
			if a.VPCID == vpcID && (vpcRegion == "" || a.VPCRegion == vpcRegion) {
				zd := b.zones[zoneID]
				if zd != nil {
					cp := zd.zone
					cp.ResourceRecordSetCount = len(zd.records)
					result = append(result, cp)
				}

				break
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// CreateCidrCollection creates a new CIDR collection.
func (b *InMemoryBackend) CreateCidrCollection(
	name, _ /* callerRef */ string,
) (*CidrCollection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateCidrCollection")
	defer b.mu.Unlock()

	id := "Z" + randomZoneID()
	col := &CidrCollection{
		ID:        id,
		Name:      name,
		Version:   1,
		ARN:       "arn:aws:route53:::cidrcollection/" + id,
		Locations: make(map[string][]string),
	}

	b.cidrCollections[id] = col

	cp := *col
	cp.Locations = copyLocations(col.Locations)

	return &cp, nil
}

func copyLocations(m map[string][]string) map[string][]string {
	out := make(map[string][]string, len(m))
	for k, v := range m {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}

	return out
}

// ChangeCidrCollection applies PUT/DELETE_IF_EXISTS changes to a CIDR collection's locations.
//
//nolint:gocognit // validates and applies multiple change actions with per-action branching
func (b *InMemoryBackend) ChangeCidrCollection(
	collectionID string,
	changes []CidrCollectionChange,
) (*CidrCollection, error) {
	b.mu.Lock("ChangeCidrCollection")
	defer b.mu.Unlock()

	col, ok := b.cidrCollections[collectionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	if col.Locations == nil {
		col.Locations = make(map[string][]string)
	}

	for _, ch := range changes {
		switch ch.Action {
		case "PUT":
			existing := col.Locations[ch.LocationName]
			cidrSet := make(map[string]bool, len(existing))
			for _, c := range existing {
				cidrSet[c] = true
			}

			for _, c := range ch.CidrList {
				if !cidrSet[c] {
					existing = append(existing, c)
					cidrSet[c] = true
				}
			}

			col.Locations[ch.LocationName] = existing

		case "DELETE_IF_EXISTS":
			existing := col.Locations[ch.LocationName]
			remove := make(map[string]bool, len(ch.CidrList))
			for _, c := range ch.CidrList {
				remove[c] = true
			}

			kept := existing[:0:0]
			for _, c := range existing {
				if !remove[c] {
					kept = append(kept, c)
				}
			}

			if len(kept) == 0 {
				delete(col.Locations, ch.LocationName)
			} else {
				col.Locations[ch.LocationName] = kept
			}
		}
	}

	col.Version++

	cp := *col
	cp.Locations = copyLocations(col.Locations)

	return &cp, nil
}

// ListCidrLocations returns all location names in a CIDR collection.
func (b *InMemoryBackend) ListCidrLocations(collectionID string) ([]string, error) {
	b.mu.RLock("ListCidrLocations")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections[collectionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	locations := make([]string, 0, len(col.Locations))
	for name := range col.Locations {
		locations = append(locations, name)
	}

	sort.Strings(locations)

	return locations, nil
}

// ListCidrBlocks returns all CIDR blocks for a given location in a collection.
func (b *InMemoryBackend) ListCidrBlocks(collectionID, locationName string) ([]string, error) {
	b.mu.RLock("ListCidrBlocks")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections[collectionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	cidrs := col.Locations[locationName]
	result := make([]string, len(cidrs))
	copy(result, cidrs)

	return result, nil
}

// CreateQueryLoggingConfig creates a new query logging configuration.
// AWS allows at most one config per hosted zone.
func (b *InMemoryBackend) CreateQueryLoggingConfig(
	hostedZoneID, logGroupArn string,
) (*QueryLoggingConfig, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if logGroupArn == "" {
		return nil, fmt.Errorf("%w: cloudWatchLogsLogGroupArn is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateQueryLoggingConfig")
	defer b.mu.Unlock()

	if _, ok := b.zones[hostedZoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	for _, cfg := range b.queryLoggingConfigs {
		if cfg.HostedZoneID == hostedZoneID {
			return nil, fmt.Errorf(
				"%w: a query logging config already exists for hosted zone %s",
				ErrQueryLoggingConfigAlreadyExists, hostedZoneID,
			)
		}
	}

	id := "Z" + randomZoneID()
	cfg := &QueryLoggingConfig{
		ID:                        id,
		HostedZoneID:              hostedZoneID,
		CloudWatchLogsLogGroupArn: logGroupArn,
		CreatedAt:                 time.Now(),
	}

	b.queryLoggingConfigs[id] = cfg

	cp := *cfg

	return &cp, nil
}

// GetQueryLoggingConfig returns a query logging config by ID.
func (b *InMemoryBackend) GetQueryLoggingConfig(id string) (*QueryLoggingConfig, error) {
	b.mu.RLock("GetQueryLoggingConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.queryLoggingConfigs[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: query logging config %s not found",
			ErrQueryLoggingConfigNotFound,
			id,
		)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteQueryLoggingConfig deletes a query logging config.
func (b *InMemoryBackend) DeleteQueryLoggingConfig(id string) error {
	b.mu.Lock("DeleteQueryLoggingConfig")
	defer b.mu.Unlock()

	if _, ok := b.queryLoggingConfigs[id]; !ok {
		return fmt.Errorf(
			"%w: query logging config %s not found",
			ErrQueryLoggingConfigNotFound,
			id,
		)
	}

	delete(b.queryLoggingConfigs, id)

	return nil
}

// ListQueryLoggingConfigs returns all query logging configs, optionally filtered by zone.
func (b *InMemoryBackend) ListQueryLoggingConfigs(
	hostedZoneID string,
) ([]*QueryLoggingConfig, error) {
	b.mu.RLock("ListQueryLoggingConfigs")
	defer b.mu.RUnlock()

	var result []*QueryLoggingConfig

	for _, cfg := range b.queryLoggingConfigs {
		if hostedZoneID == "" || cfg.HostedZoneID == hostedZoneID {
			cp := *cfg
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

const (
	delegationSetIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	delegationSetIDLength = 13
)

func randomDelegationSetID() string {
	return randomID(delegationSetIDChars, delegationSetIDLength)
}

// CreateReusableDelegationSet creates a new reusable delegation set.
func (b *InMemoryBackend) CreateReusableDelegationSet(
	callerRef, _ /* hostedZoneID */ string,
) (*ReusableDelegationSet, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateReusableDelegationSet")
	defer b.mu.Unlock()

	id := "/delegationset/N" + randomDelegationSetID()
	ds := &ReusableDelegationSet{
		ID:              id,
		CallerReference: callerRef,
		NameServers:     []string{dnsNS1Default, dnsNS2Default},
		CreatedAt:       time.Now(),
	}

	b.reusableDelegationSets[id] = ds

	cp := *ds

	return &cp, nil
}

const (
	trafficPolicyIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
	trafficPolicyIDLength = 36
)

func randomTrafficPolicyID() string {
	return randomID(trafficPolicyIDChars, trafficPolicyIDLength)
}

// CreateTrafficPolicy creates a new traffic policy.
func (b *InMemoryBackend) CreateTrafficPolicy(
	name, document, comment string,
) (*TrafficPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if document == "" {
		return nil, fmt.Errorf("%w: document is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicy")
	defer b.mu.Unlock()

	for _, versions := range b.trafficPolicies {
		if len(versions) > 0 && versions[0].Name == name {
			return nil, fmt.Errorf(
				"%w: traffic policy with name %s already exists",
				ErrTrafficPolicyAlreadyExists,
				name,
			)
		}
	}

	id := randomTrafficPolicyID()
	tp := &TrafficPolicy{
		ID:       id,
		Name:     name,
		Document: document,
		Comment:  comment,
		Type:     "DNS",
		Version:  1,
	}

	b.trafficPolicies[id] = []*TrafficPolicy{tp}

	cp := *tp

	return &cp, nil
}

// CreateTrafficPolicyVersion creates a new version of an existing traffic policy.
func (b *InMemoryBackend) CreateTrafficPolicyVersion(
	id, document, comment string,
) (*TrafficPolicy, error) {
	if document == "" {
		return nil, fmt.Errorf("%w: document is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicyVersion")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	latest := versions[len(versions)-1]
	newVersion := &TrafficPolicy{
		ID:       id,
		Name:     latest.Name,
		Document: document,
		Comment:  comment,
		Type:     latest.Type,
		Version:  latest.Version + 1,
	}

	b.trafficPolicies[id] = append(b.trafficPolicies[id], newVersion)

	cp := *newVersion

	return &cp, nil
}

const (
	tpiIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
	tpiIDLength = 36
)

func randomTPIID() string { return randomID(tpiIDChars, tpiIDLength) }

// CreateTrafficPolicyInstance creates a new traffic policy instance.
func (b *InMemoryBackend) CreateTrafficPolicyInstance(
	hostedZoneID, name, tpID string,
	tpVersion int32,
	ttl int64,
) (*TrafficPolicyInstance, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if tpID == "" {
		return nil, fmt.Errorf("%w: trafficPolicyId is required", ErrInvalidInput)
	}

	if ttl < 1 {
		return nil, fmt.Errorf("%w: TTL must be >= 1 for traffic policy instances", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicyInstance")
	defer b.mu.Unlock()

	if _, ok := b.zones[hostedZoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	versions, ok := b.trafficPolicies[tpID]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, tpID)
	}

	tpType := "DNS"
	for _, v := range versions {
		if v.Version == tpVersion {
			tpType = v.Type

			break
		}
	}

	id := randomTPIID()
	inst := &TrafficPolicyInstance{
		ID:                   id,
		HostedZoneID:         hostedZoneID,
		Name:                 normaliseName(name),
		TrafficPolicyID:      tpID,
		TrafficPolicyVersion: tpVersion,
		TrafficPolicyType:    tpType,
		TTL:                  ttl,
		State:                tpiStateApplied,
	}

	b.trafficPolicyInstances[id] = inst

	cp := *inst

	return &cp, nil
}

// AddZoneInternal adds a hosted zone directly into the backend for testing.
func (b *InMemoryBackend) AddZoneInternal(hz HostedZone) {
	b.mu.Lock("AddZoneInternal")
	defer b.mu.Unlock()
	b.zones[hz.ID] = &zoneData{zone: hz, records: make(map[string]*ResourceRecordSet)}
}

// DeleteTrafficPolicy deletes a specific version of a traffic policy.
// If it is the last version, the entire policy is removed.
func (b *InMemoryBackend) DeleteTrafficPolicy(id string, version int32) error {
	b.mu.Lock("DeleteTrafficPolicy")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	idx := -1
	for i, tp := range versions {
		if tp.Version == version {
			idx = i

			break
		}
	}

	if idx == -1 {
		return fmt.Errorf(
			"%w: traffic policy %s version %d not found",
			ErrTrafficPolicyNotFound,
			id,
			version,
		)
	}

	for _, inst := range b.trafficPolicyInstances {
		if inst.TrafficPolicyID == id && inst.TrafficPolicyVersion == version {
			return fmt.Errorf(
				"%w: traffic policy %s version %d is still in use by instance %s",
				ErrTrafficPolicyInUse,
				id,
				version,
				inst.ID,
			)
		}
	}

	if len(versions) == 1 {
		delete(b.trafficPolicies, id)

		return nil
	}

	b.trafficPolicies[id] = append(versions[:idx], versions[idx+1:]...)

	return nil
}

// GetTrafficPolicy returns a specific version of a traffic policy.
func (b *InMemoryBackend) GetTrafficPolicy(id string, version int32) (*TrafficPolicy, error) {
	b.mu.RLock("GetTrafficPolicy")
	defer b.mu.RUnlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	for _, tp := range versions {
		if tp.Version == version {
			cp := *tp

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: traffic policy %s version %d not found",
		ErrTrafficPolicyNotFound,
		id,
		version,
	)
}

// DeleteTrafficPolicyInstance deletes a traffic policy instance.
func (b *InMemoryBackend) DeleteTrafficPolicyInstance(id string) error {
	b.mu.Lock("DeleteTrafficPolicyInstance")
	defer b.mu.Unlock()

	if _, ok := b.trafficPolicyInstances[id]; !ok {
		return fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	delete(b.trafficPolicyInstances, id)

	return nil
}

// GetTrafficPolicyInstance returns a traffic policy instance by ID.
func (b *InMemoryBackend) GetTrafficPolicyInstance(id string) (*TrafficPolicyInstance, error) {
	b.mu.RLock("GetTrafficPolicyInstance")
	defer b.mu.RUnlock()

	inst, ok := b.trafficPolicyInstances[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	cp := *inst

	return &cp, nil
}

// UpdateTrafficPolicyComment updates the comment on a specific version of a traffic policy.
func (b *InMemoryBackend) UpdateTrafficPolicyComment(
	id string, version int32, comment string,
) (*TrafficPolicy, error) {
	b.mu.Lock("UpdateTrafficPolicyComment")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	for _, tp := range versions {
		if tp.Version == version {
			tp.Comment = comment
			cp := *tp

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: traffic policy %s version %d not found",
		ErrTrafficPolicyNotFound,
		id,
		version,
	)
}

// ListTrafficPolicies returns the latest version of each traffic policy with its version count.
func (b *InMemoryBackend) ListTrafficPolicies() ([]*TrafficPolicySummary, error) {
	b.mu.RLock("ListTrafficPolicies")
	defer b.mu.RUnlock()

	result := make([]*TrafficPolicySummary, 0, len(b.trafficPolicies))
	for _, versions := range b.trafficPolicies {
		if len(versions) == 0 {
			continue
		}

		latest := versions[len(versions)-1]
		cp := *latest
		result = append(result, &TrafficPolicySummary{
			TrafficPolicy: cp,
			VersionCount:  int32(len(versions)), //nolint:gosec // version count fits in int32
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// ListTrafficPolicyVersions returns all versions of a traffic policy.
func (b *InMemoryBackend) ListTrafficPolicyVersions(id string) ([]*TrafficPolicy, error) {
	b.mu.RLock("ListTrafficPolicyVersions")
	defer b.mu.RUnlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	result := make([]*TrafficPolicy, len(versions))
	for i, tp := range versions {
		cp := *tp
		result[i] = &cp
	}

	return result, nil
}

// ListTrafficPolicyInstances returns all traffic policy instances.
func (b *InMemoryBackend) ListTrafficPolicyInstances() ([]*TrafficPolicyInstance, error) {
	b.mu.RLock("ListTrafficPolicyInstances")
	defer b.mu.RUnlock()

	result := make([]*TrafficPolicyInstance, 0, len(b.trafficPolicyInstances))
	for _, inst := range b.trafficPolicyInstances {
		cp := *inst
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// DeleteCidrCollection deletes a CIDR collection.
func (b *InMemoryBackend) DeleteCidrCollection(id string) error {
	b.mu.Lock("DeleteCidrCollection")
	defer b.mu.Unlock()

	if _, ok := b.cidrCollections[id]; !ok {
		return fmt.Errorf("%w: CIDR collection %s not found", ErrCidrCollectionNotFound, id)
	}

	delete(b.cidrCollections, id)

	return nil
}

// ListCidrCollections returns all CIDR collections.
func (b *InMemoryBackend) ListCidrCollections() ([]*CidrCollection, error) {
	b.mu.RLock("ListCidrCollections")
	defer b.mu.RUnlock()

	result := make([]*CidrCollection, 0, len(b.cidrCollections))
	for _, col := range b.cidrCollections {
		cp := *col
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// UpdateHostedZoneComment updates the comment on an existing hosted zone.
func (b *InMemoryBackend) UpdateHostedZoneComment(zoneID, comment string) (*HostedZone, error) {
	b.mu.Lock("UpdateHostedZoneComment")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	zd.zone.Comment = comment
	cp := zd.zone
	cp.ResourceRecordSetCount = len(zd.records)

	return &cp, nil
}

// EnableHostedZoneDNSSEC now validates that at least one ACTIVE KSK exists.
// The original method body is preserved in the new implementation below.

// GetReusableDelegationSet returns a reusable delegation set by ID.
func (b *InMemoryBackend) GetReusableDelegationSet(id string) (*ReusableDelegationSet, error) {
	b.mu.RLock("GetReusableDelegationSet")
	defer b.mu.RUnlock()

	ds, ok := b.reusableDelegationSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: delegation set %s not found", ErrDelegationSetNotFound, id)
	}

	cp := *ds

	return &cp, nil
}

// DeleteReusableDelegationSet deletes a reusable delegation set.
func (b *InMemoryBackend) DeleteReusableDelegationSet(id string) error {
	b.mu.Lock("DeleteReusableDelegationSet")
	defer b.mu.Unlock()

	if _, ok := b.reusableDelegationSets[id]; !ok {
		return fmt.Errorf("%w: delegation set %s not found", ErrDelegationSetNotFound, id)
	}

	delete(b.reusableDelegationSets, id)

	return nil
}

// ListReusableDelegationSets returns all reusable delegation sets.
func (b *InMemoryBackend) ListReusableDelegationSets() ([]*ReusableDelegationSet, error) {
	b.mu.RLock("ListReusableDelegationSets")
	defer b.mu.RUnlock()

	result := make([]*ReusableDelegationSet, 0, len(b.reusableDelegationSets))
	for _, ds := range b.reusableDelegationSets {
		cp := *ds
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// UpdateTrafficPolicyInstance updates the TTL of a traffic policy instance.
func (b *InMemoryBackend) UpdateTrafficPolicyInstance(
	id, tpID string,
	tpVersion int32,
	ttl int64,
) (*TrafficPolicyInstance, error) {
	b.mu.Lock("UpdateTrafficPolicyInstance")
	defer b.mu.Unlock()

	inst, ok := b.trafficPolicyInstances[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	if tpID != "" {
		inst.TrafficPolicyID = tpID
		inst.TrafficPolicyVersion = tpVersion
	}

	if ttl > 0 {
		inst.TTL = ttl
	}

	cp := *inst

	return &cp, nil
}

// ListTrafficPolicyInstancesByHostedZone filters instances by hosted zone ID.
func (b *InMemoryBackend) ListTrafficPolicyInstancesByHostedZone(
	hostedZoneID string,
) ([]*TrafficPolicyInstance, error) {
	b.mu.RLock("ListTrafficPolicyInstancesByHostedZone")
	defer b.mu.RUnlock()

	var result []*TrafficPolicyInstance

	for _, inst := range b.trafficPolicyInstances {
		if inst.HostedZoneID == hostedZoneID {
			cp := *inst
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// ListTrafficPolicyInstancesByPolicy filters instances by traffic policy ID and version.
func (b *InMemoryBackend) ListTrafficPolicyInstancesByPolicy(
	tpID string,
	tpVersion int32,
) ([]*TrafficPolicyInstance, error) {
	b.mu.RLock("ListTrafficPolicyInstancesByPolicy")
	defer b.mu.RUnlock()

	var result []*TrafficPolicyInstance

	for _, inst := range b.trafficPolicyInstances {
		if inst.TrafficPolicyID == tpID &&
			(tpVersion == 0 || inst.TrafficPolicyVersion == tpVersion) {
			cp := *inst
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// AddHealthCheckInternal adds a health check directly into the backend for testing.
func (b *InMemoryBackend) AddHealthCheckInternal(hc HealthCheck) {
	b.mu.Lock("AddHealthCheckInternal")
	defer b.mu.Unlock()
	cp := hc
	b.healthChecks[hc.ID] = &cp
}

// AddKeySigningKeyInternal adds a KSK directly into the backend for testing.
func (b *InMemoryBackend) AddKeySigningKeyInternal(ksk KeySigningKey) {
	b.mu.Lock("AddKeySigningKeyInternal")
	defer b.mu.Unlock()
	cp := ksk
	b.keySigningKeys[kskKey(ksk.HostedZoneID, ksk.Name)] = &cp
}

// AddTrafficPolicyInternal adds a traffic policy directly into the backend for testing.
func (b *InMemoryBackend) AddTrafficPolicyInternal(tp TrafficPolicy) {
	b.mu.Lock("AddTrafficPolicyInternal")
	defer b.mu.Unlock()
	cp := tp
	b.trafficPolicies[tp.ID] = append(b.trafficPolicies[tp.ID], &cp)
}

func rrsValues(rrs *ResourceRecordSet) []string {
	if rrs.AliasTarget != nil {
		return []string{strings.TrimSuffix(rrs.AliasTarget.DNSName, ".")}
	}

	values := make([]string, 0, len(rrs.Records))
	for _, r := range rrs.Records {
		values = append(values, r.Value)
	}

	return values
}

// TestDNSAnswer looks up a record in the hosted zone and returns matching values.
// When routing-policy records (latency, geolocation, weighted, failover) exist for the
// name/type, it selects the best match by routing policy rather than requiring a bare
// (non-SetIdentifier) key.
func (b *InMemoryBackend) TestDNSAnswer(zoneID, recordName, recordType string) ([]string, error) {
	b.mu.RLock("TestDNSAnswer")
	defer b.mu.RUnlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	name := normaliseName(recordName)
	key := recordSetKey(name, recordType, "")

	// Try simple (non-routing-policy) lookup first.
	if rrs, found := zd.records[key]; found {
		return rrsValues(rrs), nil
	}

	// Fall back to routing-policy records: collect all records for name/type that
	// have a SetIdentifier (latency, geolocation, weighted, failover routing).
	prefix := strings.ToLower(strings.TrimSuffix(name, ".")) + "|" + strings.ToUpper(recordType) + "|"
	var candidates []*ResourceRecordSet

	for k, rrs := range zd.records {
		if strings.HasPrefix(k, prefix) && rrs.SetIdentifier != "" {
			candidates = append(candidates, rrs)
		}
	}

	if len(candidates) == 0 {
		return []string{}, nil
	}

	// Sort candidates deterministically by SetIdentifier for stable test results.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].SetIdentifier < candidates[j].SetIdentifier
	})

	// For failover routing: prefer PRIMARY if available.
	for _, rrs := range candidates {
		if rrs.Failover == FailoverPrimary {
			return rrsValues(rrs), nil
		}
	}

	// Default: return first candidate (deterministic by SetIdentifier sort).
	return rrsValues(candidates[0]), nil
}
