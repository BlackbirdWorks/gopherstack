package elasticsearch

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// InMemoryBackend is the in-memory store for Elasticsearch domains.
//
// Elasticsearch resources are region-scoped in AWS. Phase 3.3 of the
// datalayer refactor replaces the resource collections that used to be
// nested by region (outer key = region, e.g. map[string]map[string]*Domain)
// with a flat *store.Table, keyed by the composite "region|id" string (see
// regionKey), with a companion *store.Index grouping entries by region for
// the per-region scans the nested maps used to answer directly -- the same
// region-qualified-table pattern services/emr and services/codeartifact use.
// None of Domain/Package/InboundConnection/OutboundConnection/VpcEndpoint/
// ReservedInstance carried a region field of their own before this
// conversion, so each gained an unexported region field purely for this
// composite key; persistence.go carries it through via a shared regionalDTO
// wrapper (store.Table.Snapshot's plain json.Marshal cannot see unexported
// fields).
//
// arnIndex, packagesByName, packageAssociations, and vpcAccess are
// deliberately NOT converted: store.Table requires a *V value with its own
// identity, but each entry in these four maps is a bare string or string
// slice with no identifier of its own. They remain plain region-nested maps,
// unchanged by this refactor.
type InMemoryBackend struct {
	dnsRegistrar                DNSRegistrar
	domains                     *store.Table[Domain]
	domainsByRegion             *store.Index[Domain]
	arnIndex                    map[string]map[string]string // region → ARN → domain name
	packages                    *store.Table[Package]
	packagesByRegion            *store.Index[Package]
	packagesByName              map[string]map[string]string   // region → package name → package ID
	packageAssociations         map[string]map[string][]string // region → package ID → []domain names
	inboundConnections          *store.Table[InboundConnection]
	inboundConnectionsByRegion  *store.Index[InboundConnection]
	outboundConnections         *store.Table[OutboundConnection]
	outboundConnectionsByRegion *store.Index[OutboundConnection]
	vpcEndpoints                *store.Table[VpcEndpoint]
	vpcEndpointsByRegion        *store.Index[VpcEndpoint]
	vpcAccess                   map[string]map[string][]string
	reservedInstances           *store.Table[ReservedInstance]
	reservedInstancesByRegion   *store.Index[ReservedInstance]
	registry                    *store.Registry
	mu                          *lockmetrics.RWMutex
	accountID                   string
	region                      string
	nextID                      int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		arnIndex:            make(map[string]map[string]string),
		packagesByName:      make(map[string]map[string]string),
		packageAssociations: make(map[string]map[string][]string),
		vpcAccess:           make(map[string]map[string][]string),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("elasticsearch"),
		registry:            store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the backend's default AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The following Get/Put/Delete/InRegion helpers replace the old lazy
// per-region map accessors (domainsStore(region) etc.) with store.Table /
// store.Index operations. Callers must still hold b.mu, exactly as before --
// store.Table performs no locking of its own (see pkgs/store's package doc).

func (b *InMemoryBackend) domainGet(region, name string) (*Domain, bool) {
	return b.domains.Get(regionKey(region, name))
}

func (b *InMemoryBackend) domainPut(v *Domain) { b.domains.Put(v) }

func (b *InMemoryBackend) domainDelete(region, name string) {
	b.domains.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) domainsInRegion(region string) []*Domain {
	return b.domainsByRegion.Get(region)
}

func (b *InMemoryBackend) arnIndexStore(region string) map[string]string {
	if b.arnIndex[region] == nil {
		b.arnIndex[region] = make(map[string]string)
	}

	return b.arnIndex[region]
}

func (b *InMemoryBackend) packageGet(region, id string) (*Package, bool) {
	return b.packages.Get(regionKey(region, id))
}

func (b *InMemoryBackend) packagePut(v *Package) { b.packages.Put(v) }

func (b *InMemoryBackend) packageDelete(region, id string) {
	b.packages.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) packagesInRegion(region string) []*Package {
	return b.packagesByRegion.Get(region)
}

func (b *InMemoryBackend) packagesByNameStore(region string) map[string]string {
	if b.packagesByName[region] == nil {
		b.packagesByName[region] = make(map[string]string)
	}

	return b.packagesByName[region]
}

func (b *InMemoryBackend) packageAssociationsStore(region string) map[string][]string {
	if b.packageAssociations[region] == nil {
		b.packageAssociations[region] = make(map[string][]string)
	}

	return b.packageAssociations[region]
}

func (b *InMemoryBackend) inboundConnectionGet(region, id string) (*InboundConnection, bool) {
	return b.inboundConnections.Get(regionKey(region, id))
}

func (b *InMemoryBackend) inboundConnectionPut(v *InboundConnection) { b.inboundConnections.Put(v) }

func (b *InMemoryBackend) inboundConnectionDelete(region, id string) {
	b.inboundConnections.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) inboundConnectionsInRegion(region string) []*InboundConnection {
	return b.inboundConnectionsByRegion.Get(region)
}

func (b *InMemoryBackend) outboundConnectionGet(region, id string) (*OutboundConnection, bool) {
	return b.outboundConnections.Get(regionKey(region, id))
}

func (b *InMemoryBackend) outboundConnectionPut(v *OutboundConnection) {
	b.outboundConnections.Put(v)
}

func (b *InMemoryBackend) outboundConnectionDelete(region, id string) {
	b.outboundConnections.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) outboundConnectionsInRegion(region string) []*OutboundConnection {
	return b.outboundConnectionsByRegion.Get(region)
}

func (b *InMemoryBackend) vpcEndpointGet(region, id string) (*VpcEndpoint, bool) {
	return b.vpcEndpoints.Get(regionKey(region, id))
}

func (b *InMemoryBackend) vpcEndpointPut(v *VpcEndpoint) { b.vpcEndpoints.Put(v) }

func (b *InMemoryBackend) vpcEndpointDelete(region, id string) {
	b.vpcEndpoints.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) vpcEndpointsInRegion(region string) []*VpcEndpoint {
	return b.vpcEndpointsByRegion.Get(region)
}

func (b *InMemoryBackend) vpcAccessStore(region string) map[string][]string {
	if b.vpcAccess[region] == nil {
		b.vpcAccess[region] = make(map[string][]string)
	}

	return b.vpcAccess[region]
}

func (b *InMemoryBackend) reservedInstancePut(v *ReservedInstance) { b.reservedInstances.Put(v) }

func (b *InMemoryBackend) reservedInstancesInRegion(region string) []*ReservedInstance {
	return b.reservedInstancesByRegion.Get(region)
}

// SetDNSRegistrar wires a DNS server so Elasticsearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()

	b.dnsRegistrar = dns
}

// Reset clears all in-memory state. It closes all domain Tags to release
// Prometheus metrics before discarding the domain maps.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains.Snapshot() {
		d.Tags.Close()
	}

	b.registry.ResetAll()
	b.arnIndex = make(map[string]map[string]string)
	b.packagesByName = make(map[string]map[string]string)
	b.packageAssociations = make(map[string]map[string][]string)
	b.vpcAccess = make(map[string]map[string][]string)
	b.nextID = 0
}

// nextIDLocked returns the next unique integer ID and increments the counter.
// Caller must hold the write lock.
func (b *InMemoryBackend) nextIDLocked() int {
	b.nextID++

	return b.nextID
}

// domainCopy returns a copy of d with Tags set to nil (so callers cannot
// accidentally mutate or close the stored Tags collection) and every
// mutable reference field (maps/slices/option pointers) deep-cloned so that
// mutating the returned copy can never alias the backend's stored state.
func domainCopy(d *Domain) *Domain {
	cp := *d
	cp.Tags = nil
	cp.AdvancedOptions = maps.Clone(d.AdvancedOptions)
	cp.VPCOptions = cloneVPCOptions(d.VPCOptions)
	cp.CognitoOptions = cloneCognitoOptions(d.CognitoOptions)
	cp.AdvancedSecurityOptions = cloneAdvancedSecurityOptions(d.AdvancedSecurityOptions)
	cp.AutoTuneOptions = cloneAutoTuneOptions(d.AutoTuneOptions)
	cp.LogPublishingOptions = cloneLogPublishingOptions(d.LogPublishingOptions)

	return &cp
}

// cloneVPCOptions returns a deep copy of v (including its subnet/security
// group slices), or nil if v is nil.
func cloneVPCOptions(v *VPCOptions) *VPCOptions {
	if v == nil {
		return nil
	}

	cp := *v
	cp.SubnetIDs = slices.Clone(v.SubnetIDs)
	cp.SecurityGroupIDs = slices.Clone(v.SecurityGroupIDs)

	return &cp
}

// cloneCognitoOptions returns a shallow copy of v (all fields are scalar),
// or nil if v is nil.
func cloneCognitoOptions(v *CognitoOptions) *CognitoOptions {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// cloneAdvancedSecurityOptions returns a shallow copy of v (all fields are
// scalar), or nil if v is nil.
func cloneAdvancedSecurityOptions(v *AdvancedSecurityOptions) *AdvancedSecurityOptions {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// cloneAutoTuneOptions returns a shallow copy of v (all fields are scalar),
// or nil if v is nil.
func cloneAutoTuneOptions(v *AutoTuneOptions) *AutoTuneOptions {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// cloneLogPublishingOptions returns a deep copy of the map (LogPublishingOption
// values are scalar so the top-level map clone is sufficient), or nil if v is nil.
func cloneLogPublishingOptions(v map[string]LogPublishingOption) map[string]LogPublishingOption {
	return maps.Clone(v)
}

func vpcEndpointCopy(endpoint *VpcEndpoint) *VpcEndpoint {
	cp := *endpoint
	cp.VpcOptions = maps.Clone(endpoint.VpcOptions)
	cp.AuthorizedAccts = slices.Clone(endpoint.AuthorizedAccts)

	return &cp
}
