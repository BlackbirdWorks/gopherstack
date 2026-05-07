package route53

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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
	ErrHostedZoneNotFound         = errors.New("NoSuchHostedZone")
	ErrInvalidInput               = errors.New("InvalidInput")
	ErrInvalidAction              = errors.New("InvalidChangeBatch")
	ErrHealthCheckNotFound        = errors.New("NoSuchHealthCheck")
	ErrKeySigningKeyNotFound      = errors.New("NoSuchKeySigningKey")
	ErrCidrCollectionNotFound     = errors.New("NoSuchCidrCollection")
	ErrQueryLoggingConfigNotFound = errors.New("NoSuchQueryLoggingConfig")
	ErrDelegationSetNotFound      = errors.New("NoSuchDelegationSet")
	ErrTrafficPolicyNotFound      = errors.New("NoSuchTrafficPolicy")
	ErrTrafficPolicyInstNotFound  = errors.New("NoSuchTrafficPolicyInstance")
)

const (
	// recordTypeA is the DNS A record type.
	recordTypeA = "A"
	// recordTypeCNAME is the DNS CNAME record type.
	recordTypeCNAME = "CNAME"
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
	// HealthCheckTypeHTTP is an HTTP health check.
	HealthCheckTypeHTTP HealthCheckType = "HTTP"
	// HealthCheckTypeHTTPS is an HTTPS health check.
	HealthCheckTypeHTTPS HealthCheckType = "HTTPS"
	// HealthCheckTypeTCP is a TCP health check.
	HealthCheckTypeTCP HealthCheckType = "TCP"
	// HealthCheckTypeCalculated is a calculated health check.
	HealthCheckTypeCalculated HealthCheckType = "CALCULATED"
	// HealthCheckTypeCloudWatchMetric is a CloudWatch alarm health check.
	HealthCheckTypeCloudWatchMetric HealthCheckType = "CLOUDWATCH_METRIC"
)

// HealthCheckConfig holds the configuration for a health check.
type HealthCheckConfig struct {
	IPAddress                string          `json:"ipAddress,omitempty"`
	FullyQualifiedDomainName string          `json:"fullyQualifiedDomainName,omitempty"`
	ResourcePath             string          `json:"resourcePath,omitempty"`
	Type                     HealthCheckType `json:"type"`
	ChildHealthChecks        []string        `json:"childHealthChecks,omitempty"`
	Port                     int             `json:"port,omitempty"`
	RequestInterval          int             `json:"requestInterval,omitempty"`
	FailureThreshold         int             `json:"failureThreshold,omitempty"`
	HealthThreshold          int             `json:"healthThreshold,omitempty"`
	Inverted                 bool            `json:"inverted,omitempty"`
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

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
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
	AliasTarget   *AliasTarget     `json:"aliasTarget,omitempty"`
	GeoLocation   *GeoLocation     `json:"geoLocation,omitempty"`
	Name          string           `json:"name"`
	Type          string           `json:"type"`
	SetIdentifier string           `json:"setIdentifier,omitempty"`
	Failover      FailoverPolicy   `json:"failover,omitempty"`
	Region        string           `json:"region,omitempty"`
	HealthCheckID string           `json:"healthCheckId,omitempty"`
	Records       []ResourceRecord `json:"records"`
	TTL           int64            `json:"ttl"`
	Weight        int64            `json:"weight,omitempty"`
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
	CreatedAt               time.Time `json:"createdAt"`
	HostedZoneID            string    `json:"hostedZoneId"`
	Name                    string    `json:"name"`
	KeyManagementServiceArn string    `json:"keyManagementServiceArn"`
	Status                  string    `json:"status"`
}

// CidrCollection represents a Route 53 CIDR collection.
type CidrCollection struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	ARN     string `json:"arn"`
	Version int64  `json:"version"`
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

// vpcAssociation records a VPC associated with a hosted zone.
type vpcAssociation struct {
	VPCID     string `json:"vpcId"`
	VPCRegion string `json:"vpcRegion"`
}

// InMemoryBackend stores Route 53 state in memory.
type InMemoryBackend struct {
	dns                    DNSRegistrar
	zones                  map[string]*zoneData              // key: zone ID
	healthChecks           map[string]*HealthCheck           // key: health check ID
	keySigningKeys         map[string]*KeySigningKey         // key: "hostedZoneId|name"
	cidrCollections        map[string]*CidrCollection        // key: collection ID
	queryLoggingConfigs    map[string]*QueryLoggingConfig    // key: config ID
	reusableDelegationSets map[string]*ReusableDelegationSet // key: delegation set ID
	trafficPolicies        map[string][]*TrafficPolicy       // key: policy ID, value: versions
	trafficPolicyInstances map[string]*TrafficPolicyInstance // key: instance ID
	vpcAssociations        map[string][]vpcAssociation       // key: zone ID
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
func (b *InMemoryBackend) CreateHostedZone(name, callerRef, comment string, private bool) (*HostedZone, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	name = normaliseName(name)

	b.mu.Lock("CreateHostedZone")
	defer b.mu.Unlock()

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

	// Deregister all DNS records before deletion.
	if b.dns != nil {
		for _, rrs := range zd.records {
			if rrs.Type == recordTypeA || rrs.Type == recordTypeCNAME {
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
func (b *InMemoryBackend) ListHostedZones(marker string, maxItems int) (page.Page[HostedZone], error) {
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

// ChangeResourceRecordSets applies a batch of record set changes to a hosted zone.
func (b *InMemoryBackend) ChangeResourceRecordSets(zoneID string, changes []Change) error {
	b.mu.Lock("ChangeResourceRecordSets")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	for _, ch := range changes {
		rrs := ch.ResourceRecordSet
		rrs.Name = normaliseName(rrs.Name)
		key := recordSetKey(rrs.Name, rrs.Type, rrs.SetIdentifier)

		switch ch.Action {
		case ChangeActionCreate, ChangeActionUpsert:
			cp := rrs
			zd.records[key] = &cp

			// Register hostname with DNS server.
			if b.dns != nil && (rrs.Type == recordTypeA || rrs.Type == recordTypeCNAME) {
				b.dns.Register(rrs.Name)
			}

		case ChangeActionDelete:
			if _, exists := zd.records[key]; !exists {
				return fmt.Errorf("%w: record set %s %s not found", ErrInvalidAction, rrs.Name, rrs.Type)
			}

			delete(zd.records, key)

			// Deregister hostname from DNS server.
			if b.dns != nil && (rrs.Type == recordTypeA || rrs.Type == recordTypeCNAME) {
				b.dns.Deregister(rrs.Name)
			}

		default:
			return fmt.Errorf("%w: unknown action %q", ErrInvalidAction, ch.Action)
		}
	}

	return nil
}

// ListResourceRecordSets returns all resource record sets for a hosted zone.
func (b *InMemoryBackend) ListResourceRecordSets(zoneID string) ([]ResourceRecordSet, error) {
	b.mu.RLock("ListResourceRecordSets")
	defer b.mu.RUnlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	result := make([]ResourceRecordSet, 0, len(zd.records))
	for _, rrs := range zd.records {
		cp := *rrs
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}

		if result[i].Type != result[j].Type {
			return result[i].Type < result[j].Type
		}

		return result[i].SetIdentifier < result[j].SetIdentifier
	})

	return result, nil
}

const (
	healthCheckIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	healthCheckIDLength = 36
	defaultHealthStatus = "Healthy"
)

func randomHealthCheckID() string { return randomID(healthCheckIDChars, healthCheckIDLength) }

// CreateHealthCheck creates a new health check.
func (b *InMemoryBackend) CreateHealthCheck(callerRef string, cfg HealthCheckConfig) (*HealthCheck, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	if cfg.Type == "" {
		return nil, fmt.Errorf("%w: health check type is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateHealthCheck")
	defer b.mu.Unlock()

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
func (b *InMemoryBackend) ListHealthChecks(marker string, maxItems int) (page.Page[HealthCheck], error) {
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
func (b *InMemoryBackend) UpdateHealthCheck(id string, cfg HealthCheckConfig) (*HealthCheck, error) {
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
		return nil, fmt.Errorf("%w: key signing key %s already exists in zone %s", ErrInvalidInput, name, hostedZoneID)
	}

	if status == "" {
		status = kskStatusInactive
	}

	ksk := &KeySigningKey{
		HostedZoneID:            hostedZoneID,
		Name:                    name,
		KeyManagementServiceArn: kmsArn,
		Status:                  status,
		CreatedAt:               time.Now(),
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
func (b *InMemoryBackend) DeactivateKeySigningKey(hostedZoneID, name string) (*KeySigningKey, error) {
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
func (b *InMemoryBackend) DeleteKeySigningKey(hostedZoneID, name string) error {
	b.mu.Lock("DeleteKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)
	if _, ok := b.keySigningKeys[key]; !ok {
		return fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	delete(b.keySigningKeys, key)

	return nil
}

// EnableHostedZoneDNSSEC enables DNSSEC for a hosted zone.
func (b *InMemoryBackend) EnableHostedZoneDNSSEC(zoneID string) error {
	b.mu.Lock("EnableHostedZoneDNSSEC")
	defer b.mu.Unlock()

	zd, ok := b.zones[zoneID]
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
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
func (b *InMemoryBackend) AssociateVPCWithHostedZone(zoneID, vpcID, vpcRegion string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VPCId is required", ErrInvalidInput)
	}

	b.mu.Lock("AssociateVPCWithHostedZone")
	defer b.mu.Unlock()

	if _, ok := b.zones[zoneID]; !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	for _, existing := range b.vpcAssociations[zoneID] {
		if existing.VPCID == vpcID {
			return fmt.Errorf("%w: VPC %s already associated with hosted zone %s", ErrInvalidInput, vpcID, zoneID)
		}
	}

	b.vpcAssociations[zoneID] = append(b.vpcAssociations[zoneID], vpcAssociation{
		VPCID:     vpcID,
		VPCRegion: vpcRegion,
	})

	return nil
}

// CreateCidrCollection creates a new CIDR collection.
func (b *InMemoryBackend) CreateCidrCollection(name, _ /* callerRef */ string) (*CidrCollection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateCidrCollection")
	defer b.mu.Unlock()

	id := "Z" + randomZoneID()
	col := &CidrCollection{
		ID:      id,
		Name:    name,
		Version: 1,
		ARN:     "arn:aws:route53:::cidrcollection/" + id,
	}

	b.cidrCollections[id] = col

	cp := *col

	return &cp, nil
}

// ChangeCidrCollection applies changes to a CIDR collection.
func (b *InMemoryBackend) ChangeCidrCollection(
	collectionID string,
	_ []CidrCollectionChange,
) (*CidrCollection, error) {
	b.mu.Lock("ChangeCidrCollection")
	defer b.mu.Unlock()

	col, ok := b.cidrCollections[collectionID]
	if !ok {
		return nil, fmt.Errorf("%w: CIDR collection %s not found", ErrCidrCollectionNotFound, collectionID)
	}

	// In-memory stub: simply increment the version.
	col.Version++

	cp := *col

	return &cp, nil
}

// CreateQueryLoggingConfig creates a new query logging configuration.
func (b *InMemoryBackend) CreateQueryLoggingConfig(hostedZoneID, logGroupArn string) (*QueryLoggingConfig, error) {
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
func (b *InMemoryBackend) CreateTrafficPolicy(name, document, comment string) (*TrafficPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if document == "" {
		return nil, fmt.Errorf("%w: document is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicy")
	defer b.mu.Unlock()

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
func (b *InMemoryBackend) CreateTrafficPolicyVersion(id, document, comment string) (*TrafficPolicy, error) {
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
		return fmt.Errorf("%w: traffic policy %s version %d not found", ErrTrafficPolicyNotFound, id, version)
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

	return nil, fmt.Errorf("%w: traffic policy %s version %d not found", ErrTrafficPolicyNotFound, id, version)
}

// DeleteTrafficPolicyInstance deletes a traffic policy instance.
func (b *InMemoryBackend) DeleteTrafficPolicyInstance(id string) error {
	b.mu.Lock("DeleteTrafficPolicyInstance")
	defer b.mu.Unlock()

	if _, ok := b.trafficPolicyInstances[id]; !ok {
		return fmt.Errorf("%w: traffic policy instance %s not found", ErrTrafficPolicyInstNotFound, id)
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
		return nil, fmt.Errorf("%w: traffic policy instance %s not found", ErrTrafficPolicyInstNotFound, id)
	}

	cp := *inst

	return &cp, nil
}

// ListTrafficPolicies returns the latest version of each traffic policy.
func (b *InMemoryBackend) ListTrafficPolicies() ([]*TrafficPolicy, error) {
	b.mu.RLock("ListTrafficPolicies")
	defer b.mu.RUnlock()

	result := make([]*TrafficPolicy, 0, len(b.trafficPolicies))
	for _, versions := range b.trafficPolicies {
		if len(versions) == 0 {
			continue
		}

		cp := *versions[len(versions)-1]
		result = append(result, &cp)
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

// GetQueryLoggingConfig returns a single query logging configuration by ID.
func (b *InMemoryBackend) GetQueryLoggingConfig(id string) (*QueryLoggingConfig, error) {
	b.mu.RLock("GetQueryLoggingConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.queryLoggingConfigs[id]
	if !ok {
		return nil, fmt.Errorf("%w: query logging config %s not found", ErrQueryLoggingConfigNotFound, id)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteQueryLoggingConfig removes a query logging configuration.
func (b *InMemoryBackend) DeleteQueryLoggingConfig(id string) error {
	b.mu.Lock("DeleteQueryLoggingConfig")
	defer b.mu.Unlock()

	if _, ok := b.queryLoggingConfigs[id]; !ok {
		return fmt.Errorf("%w: query logging config %s not found", ErrQueryLoggingConfigNotFound, id)
	}

	delete(b.queryLoggingConfigs, id)

	return nil
}

// ListQueryLoggingConfigs returns all query logging configurations, optionally filtered by hosted zone.
func (b *InMemoryBackend) ListQueryLoggingConfigs(hostedZoneID string) ([]*QueryLoggingConfig, error) {
	b.mu.RLock("ListQueryLoggingConfigs")
	defer b.mu.RUnlock()

	result := make([]*QueryLoggingConfig, 0, len(b.queryLoggingConfigs))
	for _, cfg := range b.queryLoggingConfigs {
		if hostedZoneID != "" && cfg.HostedZoneID != hostedZoneID {
			continue
		}

		cp := *cfg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// GetReusableDelegationSet returns a single reusable delegation set by ID.
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

// DeleteReusableDelegationSet removes a reusable delegation set.
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

// DisassociateVPCFromHostedZone removes a VPC association from a hosted zone.
func (b *InMemoryBackend) DisassociateVPCFromHostedZone(zoneID, vpcID string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VPCId is required", ErrInvalidInput)
	}

	b.mu.Lock("DisassociateVPCFromHostedZone")
	defer b.mu.Unlock()

	if _, ok := b.zones[zoneID]; !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	assocs := b.vpcAssociations[zoneID]
	newAssocs := assocs[:0:0]

	for _, a := range assocs {
		if a.VPCID != vpcID {
			newAssocs = append(newAssocs, a)
		}
	}

	b.vpcAssociations[zoneID] = newAssocs

	return nil
}

// GetVPCAssociations returns VPC associations for a hosted zone.
func (b *InMemoryBackend) GetVPCAssociations(zoneID string) ([]vpcAssociation, error) {
	b.mu.RLock("GetVPCAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.zones[zoneID]; !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	assocs := b.vpcAssociations[zoneID]
	result := make([]vpcAssociation, len(assocs))
	copy(result, assocs)

	return result, nil
}

// rrsValues extracts the DNS values from a resource record set.
// For alias targets, it returns the alias DNS name as a synthetic value.
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
