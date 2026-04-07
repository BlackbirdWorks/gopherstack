package elasticsearch

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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
)

// domainNameRe validates Elasticsearch domain names:
// 3–28 lowercase alphanumeric characters or hyphens, must start with a letter.
var domainNameRe = regexp.MustCompile(`^[a-z][a-z0-9\-]{2,27}$`)

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
type InMemoryBackend struct {
	dnsRegistrar        DNSRegistrar
	domains             map[string]*Domain
	arnIndex            map[string]string // ARN → domain name
	packages            map[string]*Package
	packagesByName      map[string]string   // package name → package ID
	packageAssociations map[string][]string // package ID → []domain names
	inboundConnections  map[string]*InboundConnection
	outboundConnections map[string]*OutboundConnection
	vpcEndpoints        map[string]*VpcEndpoint
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
	nextID              int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:             make(map[string]*Domain),
		arnIndex:            make(map[string]string),
		packages:            make(map[string]*Package),
		packagesByName:      make(map[string]string),
		packageAssociations: make(map[string][]string),
		inboundConnections:  make(map[string]*InboundConnection),
		outboundConnections: make(map[string]*OutboundConnection),
		vpcEndpoints:        make(map[string]*VpcEndpoint),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("elasticsearch"),
	}
}

// SetDNSRegistrar wires a DNS server so Elasticsearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = dns
	b.mu.Unlock()
}

// CreateDomain creates a new Elasticsearch domain.
func (b *InMemoryBackend) CreateDomain(
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

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, exists := b.domains[name]; exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
	}

	if esVersion == "" {
		esVersion = "7.10"
	}

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	domainID := b.accountID + "/" + name
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)

	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = "t3.small.elasticsearch"
	}

	d := &Domain{
		Name:                 name,
		DomainID:             domainID,
		ARN:                  domainARN,
		ElasticsearchVersion: esVersion,
		Endpoint:             endpoint,
		Status:               "Active",
		ClusterConfig:        clusterConfig,
		EBSOptions:           ebsOpts,
		Tags:                 tags.New("elasticsearch." + name + ".tags"),
	}
	b.domains[name] = d
	b.arnIndex[domainARN] = name

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return domainCopy(d), nil
}

// DeleteDomain removes a domain by name.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := domainCopy(d)
	d.Tags.Close()
	delete(b.arnIndex, d.ARN)
	delete(b.domains, name)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return cp, nil
}

// DescribeDomain returns details about a domain.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	return domainCopy(d), nil
}

// ListDomainNames returns the sorted names of all domains.
func (b *InMemoryBackend) ListDomainNames() []string {
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.domains))
	for name := range b.domains {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

// UpdateDomainConfig updates the cluster configuration and/or EBS options for a domain.
func (b *InMemoryBackend) UpdateDomainConfig(name string, cfg UpdateConfig) (*Domain, error) {
	b.mu.Lock("UpdateDomainConfig")
	defer b.mu.Unlock()

	d, exists := b.domains[name]
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

// findDomainByARN returns the domain matching the given ARN, or nil if not found.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findDomainByARN(domainARN string) *Domain {
	name, ok := b.arnIndex[domainARN]
	if !ok {
		return nil
	}

	return b.domains[name]
}

// ListTags returns tags for the domain identified by ARN.
func (b *InMemoryBackend) ListTags(domainARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return nil, fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	return d.Tags.Clone(), nil
}

// AddTags adds or updates tags on the domain identified by ARN.
func (b *InMemoryBackend) AddTags(domainARN string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain identified by ARN.
func (b *InMemoryBackend) RemoveTags(domainARN string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.DeleteKeys(keys)

	return nil
}

// Reset clears all in-memory state. It closes all domain Tags to release
// Prometheus metrics before discarding the domain map.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		d.Tags.Close()
	}

	b.domains = make(map[string]*Domain)
	b.arnIndex = make(map[string]string)
	b.packages = make(map[string]*Package)
	b.packagesByName = make(map[string]string)
	b.packageAssociations = make(map[string][]string)
	b.inboundConnections = make(map[string]*InboundConnection)
	b.outboundConnections = make(map[string]*OutboundConnection)
	b.vpcEndpoints = make(map[string]*VpcEndpoint)
	b.nextID = 0
}

// nextIDLocked returns the next unique integer ID and increments the counter.
// Caller must hold the write lock.
func (b *InMemoryBackend) nextIDLocked() int {
	b.nextID++

	return b.nextID
}

// CreatePackage creates a new Elasticsearch package (e.g., a dictionary file).
func (b *InMemoryBackend) CreatePackage(name, packageType, description string) (*Package, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PackageName is required", ErrValidation)
	}

	b.mu.Lock("CreatePackage")
	defer b.mu.Unlock()

	if _, exists := b.packagesByName[name]; exists {
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
	b.packages[id] = pkg
	b.packagesByName[name] = id

	cp := *pkg

	return &cp, nil
}

// AssociatePackage associates an Elasticsearch package with a domain.
func (b *InMemoryBackend) AssociatePackage(packageID, domainName string) error {
	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	if _, exists := b.packages[packageID]; !exists {
		return fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists := b.domains[domainName]; !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if slices.Contains(b.packageAssociations[packageID], domainName) {
		return nil // already associated
	}

	b.packageAssociations[packageID] = append(b.packageAssociations[packageID], domainName)

	return nil
}

// AcceptInboundCrossClusterSearchConnection accepts a pending inbound cross-cluster
// search connection.
func (b *InMemoryBackend) AcceptInboundCrossClusterSearchConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("AcceptInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections[connectionID]
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.ConnectionStatus = "ACTIVE"
	cp := *conn

	return &cp, nil
}

// AddInboundConnectionInternal seeds an inbound connection for testing.
func (b *InMemoryBackend) AddInboundConnectionInternal(conn InboundConnection) {
	b.mu.Lock("AddInboundConnectionInternal")
	defer b.mu.Unlock()

	cp := conn
	b.inboundConnections[conn.ConnectionID] = &cp
}

// CreateOutboundCrossClusterSearchConnection creates a new outbound cross-cluster
// search connection request.
func (b *InMemoryBackend) CreateOutboundCrossClusterSearchConnection(
	localDomain, remoteDomain CrossClusterDomainInfo,
	alias string,
) (*OutboundConnection, error) {
	if alias == "" {
		return nil, fmt.Errorf("%w: ConnectionAlias is required", ErrValidation)
	}

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
	b.outboundConnections[id] = conn
	cp := *conn

	return &cp, nil
}

// CreateVpcEndpoint creates a managed VPC endpoint for an Elasticsearch domain.
func (b *InMemoryBackend) CreateVpcEndpoint(domainARN string, vpcOptions map[string]string) (*VpcEndpoint, error) {
	if domainARN == "" {
		return nil, fmt.Errorf("%w: DomainArn is required", ErrValidation)
	}

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
		Endpoint:       fmt.Sprintf("vpc-%s.%s.es.amazonaws.com", id, b.region),
		Status:         "ACTIVE",
		VpcOptions:     optsCopy,
	}
	b.vpcEndpoints[id] = endpoint

	// Return a copy with its own VpcOptions map to prevent aliasing.
	retOpts := make(map[string]string, len(optsCopy))
	maps.Copy(retOpts, optsCopy)
	cp := *endpoint
	cp.VpcOptions = retOpts

	return &cp, nil
}

// AuthorizeVpcEndpointAccess grants an account or service access to the domain's VPC endpoint.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(domainName, account string) error {
	if account == "" {
		return fmt.Errorf("%w: account principal is required", ErrValidation)
	}

	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return nil
}

// CancelDomainConfigChange cancels any in-progress configuration change for a domain.
// Because the in-memory backend applies changes synchronously this is a no-op.
func (b *InMemoryBackend) CancelDomainConfigChange(domainName string) (*Domain, error) {
	b.mu.RLock("CancelDomainConfigChange")
	defer b.mu.RUnlock()

	d, exists := b.domains[domainName]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return domainCopy(d), nil
}

// CancelElasticsearchServiceSoftwareUpdate cancels a scheduled software update.
// Because the in-memory backend never schedules updates this is a no-op.
func (b *InMemoryBackend) CancelElasticsearchServiceSoftwareUpdate(domainName string) (*Domain, error) {
	b.mu.RLock("CancelElasticsearchServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	d, exists := b.domains[domainName]
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
func (b *InMemoryBackend) AddDomainInternal(d Domain) {
	b.mu.Lock("AddDomainInternal")
	defer b.mu.Unlock()

	cp := d
	if cp.Tags == nil {
		cp.Tags = tags.New("elasticsearch." + cp.Name + ".tags")
	}

	b.domains[cp.Name] = &cp

	if cp.ARN != "" {
		b.arnIndex[cp.ARN] = cp.Name
	}
}
