package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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

// Errors returned by the Elasticsearch backend.
var (
	ErrDomainNotFound      = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists = errors.New("ResourceAlreadyExistsException")
	// ErrValidation is returned for invalid or missing input parameters.
	ErrValidation = errors.New("ValidationException")
	// ErrInvalidParameter is an alias for ErrValidation kept for compatibility.
	ErrInvalidParameter    = ErrValidation
	ErrConnectionNotFound  = errors.New("ResourceNotFoundException")
	ErrPackageNotFound     = errors.New("ResourceNotFoundException")
	ErrVpcEndpointNotFound = errors.New("ResourceNotFoundException")
	// ErrPackageAlreadyAssociated is returned when AssociatePackage targets a
	// (package, domain) pair that is already associated. AWS returns ConflictException.
	ErrPackageAlreadyAssociated = errors.New("ConflictException")
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

// CreateDomain creates a new Elasticsearch domain.
func (b *InMemoryBackend) CreateDomain(ctx context.Context, inp CreateDomainInput) (*Domain, error) {
	if inp.Name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrValidation)
	}

	if !domainNameRe.MatchString(inp.Name) {
		return nil, fmt.Errorf(
			"%w: DomainName must be 3-28 lowercase alphanumeric characters or hyphens and start with a letter",
			ErrValidation,
		)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, exists := b.domainGet(region, inp.Name); exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, inp.Name)
	}

	esVersion := inp.ElasticsearchVersion
	if esVersion == "" {
		esVersion = defaultElasticsearchVersion
	} else if !validElasticsearchVersions[esVersion] {
		return nil, fmt.Errorf("%w: invalid ElasticsearchVersion %q", ErrValidation, esVersion)
	}

	domainARN := arn.Build("es", region, b.accountID, "domain/"+inp.Name)
	domainID := b.accountID + "/" + inp.Name
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", inp.Name, b.accountID, region)

	clusterConfig := inp.ClusterConfig
	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = defaultInstanceType
	}

	d := &Domain{
		region:                      region,
		Name:                        inp.Name,
		DomainID:                    domainID,
		ARN:                         domainARN,
		ElasticsearchVersion:        esVersion,
		Endpoint:                    endpoint,
		Status:                      statusActiveCap,
		ClusterConfig:               clusterConfig,
		EBSOptions:                  inp.EBSOptions,
		SnapshotOptions:             inp.SnapshotOptions,
		AdvancedOptions:             inp.AdvancedOptions,
		AccessPolicies:              inp.AccessPolicies,
		EncryptionAtRestEnabled:     inp.EncryptionAtRestEnabled,
		NodeToNodeEncryptionEnabled: inp.NodeToNodeEncryptionEnabled,
		EnforceHTTPS:                inp.EnforceHTTPS,
		TLSSecurityPolicy:           inp.TLSSecurityPolicy,
		Tags:                        tags.New("elasticsearch." + region + "." + inp.Name + ".tags"),
	}
	b.domainPut(d)
	b.arnIndexStore(region)[domainARN] = inp.Name

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return domainCopy(d), nil
}

// DeleteDomain removes a domain by name.
func (b *InMemoryBackend) DeleteDomain(ctx context.Context, name string) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := domainCopy(d)
	d.Tags.Close()
	delete(b.arnIndexStore(region), d.ARN)
	b.domainDelete(region, name)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return cp, nil
}

// DescribeDomain returns details about a domain.
func (b *InMemoryBackend) DescribeDomain(ctx context.Context, name string) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	return domainCopy(d), nil
}

// ListDomainNames returns the sorted names of all domains in the request's region.
func (b *InMemoryBackend) ListDomainNames(ctx context.Context) []string {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	domains := b.domainsInRegion(region)
	names := make([]string, 0, len(domains))
	for _, d := range domains {
		names = append(names, d.Name)
	}

	slices.Sort(names)

	return names
}

// UpdateDomainConfig updates the cluster configuration and/or EBS options for a domain.
func (b *InMemoryBackend) UpdateDomainConfig(ctx context.Context, name string, cfg UpdateConfig) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	if cfg.ClusterConfig != nil {
		d.ClusterConfig = *cfg.ClusterConfig
	}

	if cfg.EBSOptions != nil {
		d.EBSOptions = *cfg.EBSOptions
	}

	if cfg.SnapshotOptions != nil {
		d.SnapshotOptions = *cfg.SnapshotOptions
	}

	if cfg.AdvancedOptions != nil {
		d.AdvancedOptions = cfg.AdvancedOptions
	}

	if cfg.AccessPolicies != nil {
		d.AccessPolicies = *cfg.AccessPolicies
	}

	if cfg.EncryptionAtRestEnabled != nil {
		d.EncryptionAtRestEnabled = *cfg.EncryptionAtRestEnabled
	}

	if cfg.NodeToNodeEncryptionEnabled != nil {
		d.NodeToNodeEncryptionEnabled = *cfg.NodeToNodeEncryptionEnabled
	}

	if cfg.EnforceHTTPS != nil {
		d.EnforceHTTPS = *cfg.EnforceHTTPS
	}

	if cfg.TLSSecurityPolicy != nil {
		d.TLSSecurityPolicy = *cfg.TLSSecurityPolicy
	}

	return domainCopy(d), nil
}

// findDomainByARN returns the domain matching the given ARN within the given
// region, or nil if not found. Caller must hold at least a read lock.
func (b *InMemoryBackend) findDomainByARN(region, domainARN string) *Domain {
	name, ok := b.arnIndexStore(region)[domainARN]
	if !ok {
		return nil
	}

	d, _ := b.domainGet(region, name)

	return d
}

// ListTags returns tags for the domain identified by ARN. The region is resolved
// from the ARN, falling back to the ctx region.
func (b *InMemoryBackend) ListTags(ctx context.Context, domainARN string) (map[string]string, error) {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return nil, fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	return d.Tags.Clone(), nil
}

// AddTags adds or updates tags on the domain identified by ARN.
func (b *InMemoryBackend) AddTags(ctx context.Context, domainARN string, kv map[string]string) error {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain identified by ARN.
func (b *InMemoryBackend) RemoveTags(ctx context.Context, domainARN string, keys []string) error {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.DeleteKeys(keys)

	return nil
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

// CreatePackage creates a new Elasticsearch package (e.g., a dictionary file).
func (b *InMemoryBackend) CreatePackage(ctx context.Context, name, packageType, description string) (*Package, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PackageName is required", ErrValidation)
	}

	if !validPackageTypes[packageType] {
		return nil, fmt.Errorf(
			"%w: PackageType must be TXT-DICTIONARY or ZIP-PLUGIN, got %q",
			ErrValidation,
			packageType,
		)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreatePackage")
	defer b.mu.Unlock()

	packagesByName := b.packagesByNameStore(region)
	if _, exists := packagesByName[name]; exists {
		return nil, fmt.Errorf("%w: package %s already exists", ErrDomainAlreadyExists, name)
	}

	id := fmt.Sprintf("F%010d", b.nextIDLocked())
	pkg := &Package{
		ID:          id,
		Name:        name,
		PackageType: packageType,
		Description: description,
		Status:      "AVAILABLE",
		region:      region,
	}
	b.packagePut(pkg)
	packagesByName[name] = id

	cp := *pkg

	return &cp, nil
}

// AssociatePackage associates an Elasticsearch package with a domain.
func (b *InMemoryBackend) AssociatePackage(ctx context.Context, packageID, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	if _, exists := b.packageGet(region, packageID); !exists {
		return fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	assocs := b.packageAssociationsStore(region)
	if slices.Contains(assocs[packageID], domainName) {
		return fmt.Errorf(
			"%w: package %s is already associated with domain %s",
			ErrPackageAlreadyAssociated, packageID, domainName,
		)
	}

	assocs[packageID] = append(assocs[packageID], domainName)

	return nil
}

// AcceptInboundCrossClusterSearchConnection accepts a pending inbound cross-cluster
// search connection.
func (b *InMemoryBackend) AcceptInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AcceptInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.ConnectionStatus = statusActive
	cp := *conn

	return &cp, nil
}

// AddInboundConnectionInternal seeds an inbound connection for testing.
func (b *InMemoryBackend) AddInboundConnectionInternal(ctx context.Context, conn InboundConnection) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddInboundConnectionInternal")
	defer b.mu.Unlock()

	cp := conn
	cp.region = region
	b.inboundConnectionPut(&cp)
}

// CreateOutboundCrossClusterSearchConnection creates a new outbound cross-cluster
// search connection request.
func (b *InMemoryBackend) CreateOutboundCrossClusterSearchConnection(
	ctx context.Context,
	localDomain, remoteDomain CrossClusterDomainInfo,
	alias string,
) (*OutboundConnection, error) {
	if alias == "" {
		return nil, fmt.Errorf("%w: ConnectionAlias is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateOutboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	id := fmt.Sprintf("out-%010d", b.nextIDLocked())
	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  alias,
		ConnectionStatus: "VALIDATING",
		LocalDomainInfo:  localDomain,
		RemoteDomainInfo: remoteDomain,
		region:           region,
	}
	b.outboundConnectionPut(conn)
	cp := *conn

	return &cp, nil
}

// CreateVpcEndpoint creates a managed VPC endpoint for an Elasticsearch domain.
// The endpoint's region is resolved from the domain ARN, falling back to ctx.
func (b *InMemoryBackend) CreateVpcEndpoint(
	ctx context.Context, domainARN string, vpcOptions map[string]string,
) (*VpcEndpoint, error) {
	if domainARN == "" {
		return nil, fmt.Errorf("%w: DomainArn is required", ErrValidation)
	}

	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	// Deep-copy vpcOptions so the stored map is independent of the caller's map.
	optsCopy := make(map[string]string, len(vpcOptions))
	maps.Copy(optsCopy, vpcOptions)

	id := fmt.Sprintf("vpc-endpoint-%010d", b.nextIDLocked())
	endpoint := &VpcEndpoint{
		ID:             id,
		OwnerAccountID: b.accountID,
		DomainARN:      domainARN,
		Endpoint:       fmt.Sprintf("vpc-%s.%s.es.amazonaws.com", id, region),
		Status:         statusActive,
		VpcOptions:     optsCopy,
		region:         region,
	}
	b.vpcEndpointPut(endpoint)

	return vpcEndpointCopy(endpoint), nil
}

// AuthorizeVpcEndpointAccess grants an account or service access to the domain's VPC endpoint.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(ctx context.Context, domainName, account string) error {
	if account == "" {
		return fmt.Errorf("%w: account principal is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	access := b.vpcAccessStore(region)
	if !slices.Contains(access[domainName], account) {
		access[domainName] = append(access[domainName], account)
		slices.Sort(access[domainName])
	}

	return nil
}

// CancelDomainConfigChange cancels any in-progress configuration change for a domain.
// Because the in-memory backend applies changes synchronously this is a no-op.
func (b *InMemoryBackend) CancelDomainConfigChange(ctx context.Context, domainName string) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("CancelDomainConfigChange")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// CancelElasticsearchServiceSoftwareUpdate cancels a scheduled software update.
// Because the in-memory backend never schedules updates this is a no-op.
func (b *InMemoryBackend) CancelElasticsearchServiceSoftwareUpdate(
	ctx context.Context, domainName string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("CancelElasticsearchServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// DeleteElasticsearchServiceRole deletes the Elasticsearch service-linked IAM role.
// The in-memory backend has no IAM state so this is always a no-op success.
func (b *InMemoryBackend) DeleteElasticsearchServiceRole() error {
	return nil
}

// domainCopy returns a shallow copy of d with Tags set to nil so that callers
// cannot accidentally mutate or close the stored Tags collection.
func domainCopy(d *Domain) *Domain {
	cp := *d
	cp.Tags = nil

	return &cp
}

// AddDomainInternal seeds a domain directly into the backend for testing.
// Tags are initialised fresh for the seeded domain.
func (b *InMemoryBackend) AddDomainInternal(ctx context.Context, d Domain) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddDomainInternal")
	defer b.mu.Unlock()

	cp := d
	if cp.Tags == nil {
		cp.Tags = tags.New("elasticsearch." + region + "." + cp.Name + ".tags")
	}

	cp.region = region
	b.domainPut(&cp)

	if cp.ARN != "" {
		b.arnIndexStore(region)[cp.ARN] = cp.Name
	}
}

// DeleteInboundCrossClusterSearchConnection removes an inbound cross-cluster connection.
func (b *InMemoryBackend) DeleteInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	b.inboundConnectionDelete(region, connectionID)

	return &cp, nil
}

// DeleteOutboundCrossClusterSearchConnection removes an outbound cross-cluster connection.
func (b *InMemoryBackend) DeleteOutboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*OutboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteOutboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.outboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: outbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	b.outboundConnectionDelete(region, connectionID)

	return &cp, nil
}

// RejectInboundCrossClusterSearchConnection rejects a pending inbound connection.
func (b *InMemoryBackend) RejectInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RejectInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.ConnectionStatus = "REJECTED"
	cp := *conn

	return &cp, nil
}

// DescribeInboundCrossClusterSearchConnections returns all inbound cross-cluster connections.
func (b *InMemoryBackend) DescribeInboundCrossClusterSearchConnections(ctx context.Context) []*InboundConnection {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeInboundCrossClusterSearchConnections")
	defer b.mu.RUnlock()

	conns := b.inboundConnectionsInRegion(region)
	result := make([]*InboundConnection, 0, len(conns))
	for _, conn := range conns {
		cp := *conn
		result = append(result, &cp)
	}

	return result
}

// DescribeOutboundCrossClusterSearchConnections returns all outbound cross-cluster connections.
func (b *InMemoryBackend) DescribeOutboundCrossClusterSearchConnections(ctx context.Context) []*OutboundConnection {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeOutboundCrossClusterSearchConnections")
	defer b.mu.RUnlock()

	conns := b.outboundConnectionsInRegion(region)
	result := make([]*OutboundConnection, 0, len(conns))
	for _, conn := range conns {
		cp := *conn
		result = append(result, &cp)
	}

	return result
}

// DeletePackage removes a package by ID.
func (b *InMemoryBackend) DeletePackage(ctx context.Context, packageID string) (*Package, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg
	delete(b.packagesByNameStore(region), pkg.Name)
	b.packageDelete(region, packageID)
	delete(b.packageAssociationsStore(region), packageID)

	return &cp, nil
}

// DescribePackages returns packages matching the given IDs, or all packages if the list is empty.
func (b *InMemoryBackend) DescribePackages(ctx context.Context, packageIDs []string) []*Package {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribePackages")
	defer b.mu.RUnlock()

	if len(packageIDs) == 0 {
		packages := b.packagesInRegion(region)
		result := make([]*Package, 0, len(packages))
		for _, pkg := range packages {
			cp := *pkg
			result = append(result, &cp)
		}

		return result
	}

	result := make([]*Package, 0, len(packageIDs))
	for _, id := range packageIDs {
		if pkg, exists := b.packageGet(region, id); exists {
			cp := *pkg
			result = append(result, &cp)
		}
	}

	return result
}

// DissociatePackage removes a package association from a domain.
func (b *InMemoryBackend) DissociatePackage(ctx context.Context, packageID, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DissociatePackage")
	defer b.mu.Unlock()

	if _, exists := b.packageGet(region, packageID); !exists {
		return fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	associations := b.packageAssociationsStore(region)
	assocs := associations[packageID]
	for i, name := range assocs {
		if name == domainName {
			associations[packageID] = append(assocs[:i], assocs[i+1:]...)

			return nil
		}
	}

	return nil
}

// GetPackageVersionHistory returns the version history for a package.
func (b *InMemoryBackend) GetPackageVersionHistory(ctx context.Context, packageID string) ([]*Package, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetPackageVersionHistory")
	defer b.mu.RUnlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg

	return []*Package{&cp}, nil
}

// ListDomainsForPackage returns all domain names associated with a package.
func (b *InMemoryBackend) ListDomainsForPackage(ctx context.Context, packageID string) ([]string, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListDomainsForPackage")
	defer b.mu.RUnlock()

	if _, exists := b.packageGet(region, packageID); !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	assocs := b.packageAssociationsStore(region)[packageID]
	result := make([]string, len(assocs))
	copy(result, assocs)

	return result, nil
}

// ListPackagesForDomain returns all packages associated with a domain.
func (b *InMemoryBackend) ListPackagesForDomain(ctx context.Context, domainName string) []*Package {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListPackagesForDomain")
	defer b.mu.RUnlock()

	var result []*Package
	for packageID, assocs := range b.packageAssociationsStore(region) {
		if slices.Contains(assocs, domainName) {
			if pkg, exists := b.packageGet(region, packageID); exists {
				cp := *pkg
				result = append(result, &cp)
			}
		}
	}

	return result
}

// UpdatePackage updates a package description.
func (b *InMemoryBackend) UpdatePackage(ctx context.Context, packageID, description string) (*Package, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	pkg.Description = description
	cp := *pkg

	return &cp, nil
}

// DeleteVpcEndpoint removes a VPC endpoint by ID.
func (b *InMemoryBackend) DeleteVpcEndpoint(ctx context.Context, vpcEndpointID string) (*VpcEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteVpcEndpoint")
	defer b.mu.Unlock()

	endpoint, exists := b.vpcEndpointGet(region, vpcEndpointID)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrVpcEndpointNotFound, vpcEndpointID)
	}

	cp := *endpoint
	b.vpcEndpointDelete(region, vpcEndpointID)

	return vpcEndpointCopy(&cp), nil
}

// DescribeVpcEndpoints returns VPC endpoints matching the given IDs, or all endpoints if empty.
func (b *InMemoryBackend) DescribeVpcEndpoints(ctx context.Context, vpcEndpointIDs []string) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	if len(vpcEndpointIDs) == 0 {
		endpoints := b.vpcEndpointsInRegion(region)
		result := make([]*VpcEndpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			result = append(result, vpcEndpointCopy(ep))
		}

		return result
	}

	result := make([]*VpcEndpoint, 0, len(vpcEndpointIDs))
	for _, id := range vpcEndpointIDs {
		if ep, exists := b.vpcEndpointGet(region, id); exists {
			result = append(result, vpcEndpointCopy(ep))
		}
	}

	return result
}

// ListVpcEndpointAccess returns authorized account principals for a domain's VPC endpoint access.
func (b *InMemoryBackend) ListVpcEndpointAccess(ctx context.Context, domainName string) ([]string, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpointAccess")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return slices.Clone(b.vpcAccessStore(region)[domainName]), nil
}

// ListVpcEndpoints returns all VPC endpoints in the request's region.
func (b *InMemoryBackend) ListVpcEndpoints(ctx context.Context) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	endpoints := b.vpcEndpointsInRegion(region)
	result := make([]*VpcEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		result = append(result, vpcEndpointCopy(ep))
	}

	return result
}

// ListVpcEndpointsForDomain returns VPC endpoints associated with a specific domain ARN.
func (b *InMemoryBackend) ListVpcEndpointsForDomain(ctx context.Context, domainName string) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpointsForDomain")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil
	}

	var result []*VpcEndpoint
	for _, ep := range b.vpcEndpointsInRegion(region) {
		if ep.DomainARN == d.ARN {
			result = append(result, vpcEndpointCopy(ep))
		}
	}

	return result
}

// RevokeVpcEndpointAccess revokes an account's access to a domain's VPC endpoint.
func (b *InMemoryBackend) RevokeVpcEndpointAccess(ctx context.Context, domainName, account string) error {
	if account == "" {
		return fmt.Errorf("%w: account principal is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("RevokeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	access := b.vpcAccessStore(region)
	accounts := access[domainName]
	for i, authorized := range accounts {
		if authorized == account {
			access[domainName] = append(accounts[:i], accounts[i+1:]...)

			break
		}
	}

	return nil
}

// UpdateVpcEndpoint updates the VPC options of a VPC endpoint.
func (b *InMemoryBackend) UpdateVpcEndpoint(
	ctx context.Context, vpcEndpointID string, vpcOptions map[string]string,
) (*VpcEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdateVpcEndpoint")
	defer b.mu.Unlock()

	endpoint, exists := b.vpcEndpointGet(region, vpcEndpointID)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrVpcEndpointNotFound, vpcEndpointID)
	}

	newOpts := make(map[string]string, len(vpcOptions))
	maps.Copy(newOpts, vpcOptions)
	endpoint.VpcOptions = newOpts

	return vpcEndpointCopy(endpoint), nil
}

func vpcEndpointCopy(endpoint *VpcEndpoint) *VpcEndpoint {
	cp := *endpoint
	cp.VpcOptions = maps.Clone(endpoint.VpcOptions)
	cp.AuthorizedAccts = slices.Clone(endpoint.AuthorizedAccts)

	return &cp
}

// DescribeDomainAutoTunes validates a domain exists and returns (the in-memory backend has no auto-tune state).
func (b *InMemoryBackend) DescribeDomainAutoTunes(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainAutoTunes")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// DescribeDomainChangeProgress validates a domain exists and returns (changes are synchronous in-memory).
func (b *InMemoryBackend) DescribeDomainChangeProgress(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainChangeProgress")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// GetUpgradeHistory validates a domain exists and returns empty history (no upgrade state tracked).
func (b *InMemoryBackend) GetUpgradeHistory(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeHistory")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// GetUpgradeStatus validates a domain exists and returns (no upgrade in progress in-memory).
func (b *InMemoryBackend) GetUpgradeStatus(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeStatus")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// StartElasticsearchServiceSoftwareUpdate schedules a software update (no-op in-memory).
func (b *InMemoryBackend) StartElasticsearchServiceSoftwareUpdate(
	ctx context.Context, domainName string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("StartElasticsearchServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// UpgradeElasticsearchDomain upgrades a domain to the target version.
func (b *InMemoryBackend) UpgradeElasticsearchDomain(
	ctx context.Context, domainName, targetVersion string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpgradeElasticsearchDomain")
	defer b.mu.Unlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if targetVersion != "" {
		d.ElasticsearchVersion = targetVersion
	}

	return domainCopy(d), nil
}

// DescribeReservedElasticsearchInstanceOfferings returns available reserved instance offerings.
func (b *InMemoryBackend) DescribeReservedElasticsearchInstanceOfferings() []ReservedInstanceOffering {
	return []ReservedInstanceOffering{{
		OfferingID:    "offer-t3-small-1y",
		InstanceType:  defaultInstanceType,
		PaymentOption: "NO_UPFRONT",
		Currency:      "USD",
		Duration:      reservedDurationOneYearSeconds,
	}}
}

// DescribeReservedElasticsearchInstances returns purchased reserved instances for the request's region.
func (b *InMemoryBackend) DescribeReservedElasticsearchInstances(ctx context.Context) []ReservedInstance {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeReservedElasticsearchInstances")
	defer b.mu.RUnlock()

	reserved := b.reservedInstancesInRegion(region)
	instances := make([]ReservedInstance, 0, len(reserved))
	for _, instance := range reserved {
		instances = append(instances, *instance)
	}

	slices.SortFunc(instances, func(a, c ReservedInstance) int {
		return strings.Compare(a.ReservationID, c.ReservationID)
	})

	return instances
}

// PurchaseReservedElasticsearchInstanceOffering purchases a reserved instance offering.
func (b *InMemoryBackend) PurchaseReservedElasticsearchInstanceOffering(
	ctx context.Context, offeringID, name string, count int,
) (*ReservedInstance, error) {
	if offeringID == "" {
		return nil, fmt.Errorf("%w: ReservedElasticsearchInstanceOfferingId is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("PurchaseReservedElasticsearchInstanceOffering")
	defer b.mu.Unlock()

	if count == 0 {
		count = 1
	}

	id := fmt.Sprintf("ri-%010d", b.nextIDLocked())
	instance := &ReservedInstance{
		ReservationID:   id,
		ReservationName: name,
		OfferingID:      offeringID,
		Count:           count,
		State:           statusActive,
		region:          region,
	}
	for _, offering := range b.DescribeReservedElasticsearchInstanceOfferings() {
		if offering.OfferingID == offeringID {
			instance.InstanceType = offering.InstanceType
			instance.FixedPrice = offering.FixedPrice
			instance.UsagePrice = offering.UsagePrice
			instance.Duration = offering.Duration

			break
		}
	}

	b.reservedInstancePut(instance)
	cp := *instance

	return &cp, nil
}
