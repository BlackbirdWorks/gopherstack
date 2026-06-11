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
	elasticsearchVersion71         = "7.1"
	elasticsearchVersion68         = "6.8"
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
	"1.5":  true,
	"2.3":  true,
	"5.1":  true,
	"5.3":  true,
	"5.5":  true,
	"5.6":  true,
	"6.0":  true,
	"6.2":  true,
	"6.3":  true,
	"6.4":  true,
	"6.5":  true,
	"6.7":  true,
	"6.8":  true,
	"7.1":  true,
	"7.4":  true,
	"7.7":  true,
	"7.8":  true,
	"7.9":  true,
	"7.10": true,
	"7.13": true,
	"7.16": true,
	"7.17": true,
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
}

// OutboundConnection represents an outbound cross-cluster search connection.
type OutboundConnection struct {
	ConnectionID     string                 `json:"connectionID"`
	ConnectionAlias  string                 `json:"connectionAlias"`
	ConnectionStatus string                 `json:"connectionStatus"`
	LocalDomainInfo  CrossClusterDomainInfo `json:"localDomainInfo"`
	RemoteDomainInfo CrossClusterDomainInfo `json:"remoteDomainInfo"`
}

// VpcEndpoint represents a managed VPC endpoint for an Elasticsearch domain.
type VpcEndpoint struct {
	ID              string            `json:"vpcEndpointID"`
	OwnerAccountID  string            `json:"ownerAccountID"`
	DomainARN       string            `json:"domainARN"`
	Endpoint        string            `json:"endpoint"`
	Status          string            `json:"status"`
	VpcOptions      map[string]string `json:"vpcOptions"`
	AuthorizedAccts []string          `json:"authorizedAccounts"`
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
	ReservationID   string  `json:"reservedElasticsearchInstanceId"`
	ReservationName string  `json:"reservationName"`
	OfferingID      string  `json:"reservedElasticsearchInstanceOfferingId"`
	InstanceType    string  `json:"elasticsearchInstanceType"`
	State           string  `json:"state"`
	FixedPrice      float64 `json:"fixedPrice"`
	UsagePrice      float64 `json:"usagePrice"`
	Duration        int     `json:"duration"`
	Count           int     `json:"elasticsearchInstanceCount"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// ClusterConfig represents the cluster configuration for an Elasticsearch domain.
type ClusterConfig struct {
	InstanceType  string `json:"instanceType"`
	InstanceCount int    `json:"instanceCount"`
}

// EBSOptions represents the EBS storage options for an Elasticsearch domain.
type EBSOptions struct {
	VolumeType string `json:"volumeType"`
	VolumeSize int    `json:"volumeSize"`
	EBSEnabled bool   `json:"ebsEnabled"`
}

// Domain represents an Elasticsearch domain.
type Domain struct {
	Tags                 *tags.Tags    `json:"tags,omitempty"`
	Name                 string        `json:"name"`
	DomainID             string        `json:"domainID"`
	ARN                  string        `json:"arn"`
	ElasticsearchVersion string        `json:"elasticsearchVersion"`
	Endpoint             string        `json:"endpoint"`
	Status               string        `json:"status"`
	ClusterConfig        ClusterConfig `json:"clusterConfig"`
	EBSOptions           EBSOptions    `json:"ebsOptions"`
}

// UpdateConfig holds the fields that can be updated via UpdateDomainConfig.
type UpdateConfig struct {
	ClusterConfig *ClusterConfig
	EBSOptions    *EBSOptions
}

// InMemoryBackend is the in-memory store for Elasticsearch domains.
//
// All resource maps are nested by region (outer key = region) so that same-named
// resources in different regions are fully isolated. Elasticsearch resources are
// region-scoped in AWS, so every map carries a region dimension.
type InMemoryBackend struct {
	dnsRegistrar        DNSRegistrar
	domains             map[string]map[string]*Domain
	arnIndex            map[string]map[string]string // region → ARN → domain name
	packages            map[string]map[string]*Package
	packagesByName      map[string]map[string]string   // region → package name → package ID
	packageAssociations map[string]map[string][]string // region → package ID → []domain names
	inboundConnections  map[string]map[string]*InboundConnection
	outboundConnections map[string]map[string]*OutboundConnection
	vpcEndpoints        map[string]map[string]*VpcEndpoint
	vpcAccess           map[string]map[string][]string
	reservedInstances   map[string]map[string]*ReservedInstance
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
	nextID              int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:             make(map[string]map[string]*Domain),
		arnIndex:            make(map[string]map[string]string),
		packages:            make(map[string]map[string]*Package),
		packagesByName:      make(map[string]map[string]string),
		packageAssociations: make(map[string]map[string][]string),
		inboundConnections:  make(map[string]map[string]*InboundConnection),
		outboundConnections: make(map[string]map[string]*OutboundConnection),
		vpcEndpoints:        make(map[string]map[string]*VpcEndpoint),
		vpcAccess:           make(map[string]map[string][]string),
		reservedInstances:   make(map[string]map[string]*ReservedInstance),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("elasticsearch"),
	}
}

// Region returns the backend's default AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) domainsStore(region string) map[string]*Domain {
	if b.domains[region] == nil {
		b.domains[region] = make(map[string]*Domain)
	}

	return b.domains[region]
}

func (b *InMemoryBackend) arnIndexStore(region string) map[string]string {
	if b.arnIndex[region] == nil {
		b.arnIndex[region] = make(map[string]string)
	}

	return b.arnIndex[region]
}

func (b *InMemoryBackend) packagesStore(region string) map[string]*Package {
	if b.packages[region] == nil {
		b.packages[region] = make(map[string]*Package)
	}

	return b.packages[region]
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

func (b *InMemoryBackend) inboundConnectionsStore(region string) map[string]*InboundConnection {
	if b.inboundConnections[region] == nil {
		b.inboundConnections[region] = make(map[string]*InboundConnection)
	}

	return b.inboundConnections[region]
}

func (b *InMemoryBackend) outboundConnectionsStore(region string) map[string]*OutboundConnection {
	if b.outboundConnections[region] == nil {
		b.outboundConnections[region] = make(map[string]*OutboundConnection)
	}

	return b.outboundConnections[region]
}

func (b *InMemoryBackend) vpcEndpointsStore(region string) map[string]*VpcEndpoint {
	if b.vpcEndpoints[region] == nil {
		b.vpcEndpoints[region] = make(map[string]*VpcEndpoint)
	}

	return b.vpcEndpoints[region]
}

func (b *InMemoryBackend) vpcAccessStore(region string) map[string][]string {
	if b.vpcAccess[region] == nil {
		b.vpcAccess[region] = make(map[string][]string)
	}

	return b.vpcAccess[region]
}

func (b *InMemoryBackend) reservedInstancesStore(region string) map[string]*ReservedInstance {
	if b.reservedInstances[region] == nil {
		b.reservedInstances[region] = make(map[string]*ReservedInstance)
	}

	return b.reservedInstances[region]
}

// SetDNSRegistrar wires a DNS server so Elasticsearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()

	b.dnsRegistrar = dns
}

// CreateDomain creates a new Elasticsearch domain.
func (b *InMemoryBackend) CreateDomain(
	ctx context.Context,
	name, esVersion string,
	clusterConfig ClusterConfig,
	ebsOpts EBSOptions,
) (*Domain, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrValidation)
	}

	if !domainNameRe.MatchString(name) {
		return nil, fmt.Errorf(
			"%w: DomainName must be 3-28 lowercase alphanumeric characters or hyphens and start with a letter",
			ErrValidation,
		)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	domains := b.domainsStore(region)
	if _, exists := domains[name]; exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
	}

	if esVersion == "" {
		esVersion = defaultElasticsearchVersion
	} else if !validElasticsearchVersions[esVersion] {
		return nil, fmt.Errorf("%w: invalid ElasticsearchVersion %q", ErrValidation, esVersion)
	}

	domainARN := arn.Build("es", region, b.accountID, "domain/"+name)
	domainID := b.accountID + "/" + name
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, region)

	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = defaultInstanceType
	}

	d := &Domain{
		Name:                 name,
		DomainID:             domainID,
		ARN:                  domainARN,
		ElasticsearchVersion: esVersion,
		Endpoint:             endpoint,
		Status:               statusActiveCap,
		ClusterConfig:        clusterConfig,
		EBSOptions:           ebsOpts,
		Tags:                 tags.New("elasticsearch." + region + "." + name + ".tags"),
	}
	domains[name] = d
	b.arnIndexStore(region)[domainARN] = name

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

	domains := b.domainsStore(region)
	d, exists := domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := domainCopy(d)
	d.Tags.Close()
	delete(b.arnIndexStore(region), d.ARN)
	delete(domains, name)

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

	d, exists := b.domainsStore(region)[name]
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

	domains := b.domainsStore(region)
	names := make([]string, 0, len(domains))
	for name := range domains {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

// UpdateDomainConfig updates the cluster configuration and/or EBS options for a domain.
func (b *InMemoryBackend) UpdateDomainConfig(ctx context.Context, name string, cfg UpdateConfig) (*Domain, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domainsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	if cfg.ClusterConfig != nil {
		d.ClusterConfig = *cfg.ClusterConfig
	}

	if cfg.EBSOptions != nil {
		d.EBSOptions = *cfg.EBSOptions
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

	return b.domainsStore(region)[name]
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

	for _, regionDomains := range b.domains {
		for _, d := range regionDomains {
			d.Tags.Close()
		}
	}

	b.domains = make(map[string]map[string]*Domain)
	b.arnIndex = make(map[string]map[string]string)
	b.packages = make(map[string]map[string]*Package)
	b.packagesByName = make(map[string]map[string]string)
	b.packageAssociations = make(map[string]map[string][]string)
	b.inboundConnections = make(map[string]map[string]*InboundConnection)
	b.outboundConnections = make(map[string]map[string]*OutboundConnection)
	b.vpcEndpoints = make(map[string]map[string]*VpcEndpoint)
	b.vpcAccess = make(map[string]map[string][]string)
	b.reservedInstances = make(map[string]map[string]*ReservedInstance)
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
	}
	b.packagesStore(region)[id] = pkg
	packagesByName[name] = id

	cp := *pkg

	return &cp, nil
}

// AssociatePackage associates an Elasticsearch package with a domain.
func (b *InMemoryBackend) AssociatePackage(ctx context.Context, packageID, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	if _, exists := b.packagesStore(region)[packageID]; !exists {
		return fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists := b.domainsStore(region)[domainName]; !exists {
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

	conn, exists := b.inboundConnectionsStore(region)[connectionID]
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
	b.inboundConnectionsStore(region)[conn.ConnectionID] = &cp
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
	}
	b.outboundConnectionsStore(region)[id] = conn
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
	}
	b.vpcEndpointsStore(region)[id] = endpoint

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

	if _, exists := b.domainsStore(region)[domainName]; !exists {
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

	d, exists := b.domainsStore(region)[domainName]
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

	d, exists := b.domainsStore(region)[domainName]
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

	b.domainsStore(region)[cp.Name] = &cp

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

	conns := b.inboundConnectionsStore(region)
	conn, exists := conns[connectionID]
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	delete(conns, connectionID)

	return &cp, nil
}

// DeleteOutboundCrossClusterSearchConnection removes an outbound cross-cluster connection.
func (b *InMemoryBackend) DeleteOutboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*OutboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteOutboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conns := b.outboundConnectionsStore(region)
	conn, exists := conns[connectionID]
	if !exists {
		return nil, fmt.Errorf("%w: outbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	delete(conns, connectionID)

	return &cp, nil
}

// RejectInboundCrossClusterSearchConnection rejects a pending inbound connection.
func (b *InMemoryBackend) RejectInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RejectInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionsStore(region)[connectionID]
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

	conns := b.inboundConnectionsStore(region)
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

	conns := b.outboundConnectionsStore(region)
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

	packages := b.packagesStore(region)
	pkg, exists := packages[packageID]
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg
	delete(b.packagesByNameStore(region), pkg.Name)
	delete(packages, packageID)
	delete(b.packageAssociationsStore(region), packageID)

	return &cp, nil
}

// DescribePackages returns packages matching the given IDs, or all packages if the list is empty.
func (b *InMemoryBackend) DescribePackages(ctx context.Context, packageIDs []string) []*Package {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribePackages")
	defer b.mu.RUnlock()

	packages := b.packagesStore(region)
	if len(packageIDs) == 0 {
		result := make([]*Package, 0, len(packages))
		for _, pkg := range packages {
			cp := *pkg
			result = append(result, &cp)
		}

		return result
	}

	result := make([]*Package, 0, len(packageIDs))
	for _, id := range packageIDs {
		if pkg, exists := packages[id]; exists {
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

	if _, exists := b.packagesStore(region)[packageID]; !exists {
		return fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists := b.domainsStore(region)[domainName]; !exists {
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

	pkg, exists := b.packagesStore(region)[packageID]
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

	if _, exists := b.packagesStore(region)[packageID]; !exists {
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

	packages := b.packagesStore(region)
	var result []*Package
	for packageID, assocs := range b.packageAssociationsStore(region) {
		if slices.Contains(assocs, domainName) {
			if pkg, exists := packages[packageID]; exists {
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

	pkg, exists := b.packagesStore(region)[packageID]
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

	endpoints := b.vpcEndpointsStore(region)
	endpoint, exists := endpoints[vpcEndpointID]
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrVpcEndpointNotFound, vpcEndpointID)
	}

	cp := *endpoint
	delete(endpoints, vpcEndpointID)

	return vpcEndpointCopy(&cp), nil
}

// DescribeVpcEndpoints returns VPC endpoints matching the given IDs, or all endpoints if empty.
func (b *InMemoryBackend) DescribeVpcEndpoints(ctx context.Context, vpcEndpointIDs []string) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	endpoints := b.vpcEndpointsStore(region)
	if len(vpcEndpointIDs) == 0 {
		result := make([]*VpcEndpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			result = append(result, vpcEndpointCopy(ep))
		}

		return result
	}

	result := make([]*VpcEndpoint, 0, len(vpcEndpointIDs))
	for _, id := range vpcEndpointIDs {
		if ep, exists := endpoints[id]; exists {
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

	if _, exists := b.domainsStore(region)[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return slices.Clone(b.vpcAccessStore(region)[domainName]), nil
}

// ListVpcEndpoints returns all VPC endpoints in the request's region.
func (b *InMemoryBackend) ListVpcEndpoints(ctx context.Context) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	endpoints := b.vpcEndpointsStore(region)
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

	d, exists := b.domainsStore(region)[domainName]
	if !exists {
		return nil
	}

	var result []*VpcEndpoint
	for _, ep := range b.vpcEndpointsStore(region) {
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

	if _, exists := b.domainsStore(region)[domainName]; !exists {
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

	endpoint, exists := b.vpcEndpointsStore(region)[vpcEndpointID]
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

	if _, exists := b.domainsStore(region)[domainName]; !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// DescribeDomainChangeProgress validates a domain exists and returns (changes are synchronous in-memory).
func (b *InMemoryBackend) DescribeDomainChangeProgress(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDomainChangeProgress")
	defer b.mu.RUnlock()

	if _, exists := b.domainsStore(region)[domainName]; !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// GetUpgradeHistory validates a domain exists and returns empty history (no upgrade state tracked).
func (b *InMemoryBackend) GetUpgradeHistory(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeHistory")
	defer b.mu.RUnlock()

	if _, exists := b.domainsStore(region)[domainName]; !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// GetUpgradeStatus validates a domain exists and returns (no upgrade in progress in-memory).
func (b *InMemoryBackend) GetUpgradeStatus(ctx context.Context, domainName string) error {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetUpgradeStatus")
	defer b.mu.RUnlock()

	if _, exists := b.domainsStore(region)[domainName]; !exists {
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

	d, exists := b.domainsStore(region)[domainName]
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

	d, exists := b.domainsStore(region)[domainName]
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

	reserved := b.reservedInstancesStore(region)
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

	b.reservedInstancesStore(region)[id] = instance
	cp := *instance

	return &cp, nil
}
