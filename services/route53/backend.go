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

// Errors returned by the backend.
var (
	ErrHostedZoneNotFound  = errors.New("NoSuchHostedZone")
	ErrInvalidInput        = errors.New("InvalidInput")
	ErrInvalidAction       = errors.New("InvalidChangeBatch")
	ErrHealthCheckNotFound = errors.New("NoSuchHealthCheck")
)

const (
	// recordTypeA is the DNS A record type.
	recordTypeA = "A"
	// recordTypeCNAME is the DNS CNAME record type.
	recordTypeCNAME = "CNAME"
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
	records map[string]*ResourceRecordSet // key: "name|TYPE" or "name|TYPE|SetIdentifier"
	zone    HostedZone
}

// InMemoryBackend stores Route 53 state in memory.
type InMemoryBackend struct {
	dns          DNSRegistrar
	zones        map[string]*zoneData    // key: zone ID
	healthChecks map[string]*HealthCheck // key: health check ID
	mu           *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		zones:        make(map[string]*zoneData),
		healthChecks: make(map[string]*HealthCheck),
		mu:           lockmetrics.New("route53"),
	}
}

// SetDNSRegistrar wires a DNS server so A/CNAME records are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dns = dns
	b.mu.Unlock()
}

const (
	zoneIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	zoneIDLength = 13
)

func randomZoneID() string {
	buf := make([]byte, zoneIDLength)
	n := uint64(len(zoneIDChars))

	for i := range buf {
		var v [8]byte
		_, _ = rand.Read(v[:])
		buf[i] = zoneIDChars[binary.BigEndian.Uint64(v[:])%n]
	}

	return string(buf)
}

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

func randomHealthCheckID() string {
	buf := make([]byte, healthCheckIDLength)
	n := uint64(len(healthCheckIDChars))

	for i := range buf {
		var v [8]byte
		_, _ = rand.Read(v[:])
		buf[i] = healthCheckIDChars[binary.BigEndian.Uint64(v[:])%n]
	}

	return string(buf)
}

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
