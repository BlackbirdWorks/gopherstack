package route53

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
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

// InMemoryBackend stores Route 53 state in memory.
//
// Most resource collections are *store.Table[T], registered once on registry
// at construction time (see store_setup.go); registry.ResetAll() then
// collapses their runtime reset to one call in Reset below. A handful of
// fields are deliberately left as plain maps -- see the doc comment atop
// store_setup.go for the full list and why.
type InMemoryBackend struct {
	dns                          DNSRegistrar
	registry                     *store.Registry
	zones                        *store.Table[zoneData]
	healthChecks                 *store.Table[HealthCheck]
	keySigningKeys               *store.Table[KeySigningKey]
	keySigningKeysByZone         *store.Index[KeySigningKey]
	cidrCollections              *store.Table[CidrCollection]
	queryLoggingConfigs          *store.Table[QueryLoggingConfig]
	queryLoggingConfigsByZone    *store.Index[QueryLoggingConfig]
	reusableDelegationSets       *store.Table[ReusableDelegationSet]
	trafficPolicies              map[string][]*TrafficPolicy // key: policy ID, value: versions
	trafficPolicyInstances       *store.Table[TrafficPolicyInstance]
	trafficPolicyInstancesByZone *store.Index[TrafficPolicyInstance]
	vpcAssociations              map[string][]vpcAssociation              // key: zone ID
	vpcAssocAuthorizations       map[string][]VPCAssociationAuthorization // key: zone ID
	changes                      *store.Table[ChangeInfo]
	tags                         map[string]*svcTags.Tags
	mu                           *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:               store.NewRegistry(),
		trafficPolicies:        make(map[string][]*TrafficPolicy),
		vpcAssociations:        make(map[string][]vpcAssociation),
		vpcAssocAuthorizations: make(map[string][]VPCAssociationAuthorization),
		tags:                   make(map[string]*svcTags.Tags),
		mu:                     lockmetrics.New("route53"),
	}

	b.registerTables()

	return b
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

const route53DefaultMaxItems = 100

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.trafficPolicies = make(map[string][]*TrafficPolicy)
	b.vpcAssociations = make(map[string][]vpcAssociation)
	b.vpcAssocAuthorizations = make(map[string][]VPCAssociationAuthorization)
	b.tags = make(map[string]*svcTags.Tags)
}
