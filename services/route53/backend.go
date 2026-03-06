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
)

// Errors returned by the backend.
var (
	ErrHostedZoneNotFound = errors.New("NoSuchHostedZone")
	ErrInvalidInput       = errors.New("InvalidInput")
	ErrInvalidAction      = errors.New("InvalidChangeBatch")
)

const (
	// recordTypeA is the DNS A record type.
	recordTypeA = "A"
	// recordTypeCNAME is the DNS CNAME record type.
	recordTypeCNAME = "CNAME"
)

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
	AliasTarget *AliasTarget     `json:"aliasTarget,omitempty"`
	Name        string           `json:"name"`
	Type        string           `json:"type"`
	Records     []ResourceRecord `json:"records"`
	TTL         int64            `json:"ttl"`
}

// recordSetKey builds the map key for a resource record set.
func recordSetKey(name, rrType string) string {
	return strings.ToLower(strings.TrimSuffix(name, ".")) + "|" + strings.ToUpper(rrType)
}

// zoneData holds per-zone state.
type zoneData struct {
	records map[string]*ResourceRecordSet // key: "name|TYPE"
	zone    HostedZone
}

// InMemoryBackend stores Route 53 state in memory.
type InMemoryBackend struct {
	dns   DNSRegistrar
	zones map[string]*zoneData // key: zone ID
	mu    *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		zones: make(map[string]*zoneData),
		mu:    lockmetrics.New("route53"),
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

// ListHostedZones returns all hosted zones sorted by name.
func (b *InMemoryBackend) ListHostedZones() ([]HostedZone, error) {
	b.mu.RLock("ListHostedZones")
	defer b.mu.RUnlock()

	result := make([]HostedZone, 0, len(b.zones))
	for _, zd := range b.zones {
		cp := zd.zone
		cp.ResourceRecordSetCount = len(zd.records)
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
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
		key := recordSetKey(rrs.Name, rrs.Type)

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

		return result[i].Type < result[j].Type
	})

	return result, nil
}
