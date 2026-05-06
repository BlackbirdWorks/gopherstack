package opensearch

import (
	"errors"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Errors returned by the OpenSearch backend.
var (
	ErrDomainNotFound           = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists      = errors.New("ResourceAlreadyExistsException")
	ErrInvalidParameter         = errors.New("ValidationException")
	ErrValidation               = errors.New("ValidationException")
	ErrConnectionNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceNotFound       = errors.New("ResourceNotFoundException")
	ErrDataSourceAlreadyExists  = errors.New("ResourceAlreadyExistsException")
	ErrPackageNotFound          = errors.New("ResourceNotFoundException")
	ErrApplicationNotFound      = errors.New("ResourceNotFoundException")
	ErrApplicationAlreadyExists = errors.New("ResourceAlreadyExistsException")
)

// Domain status constants.
const (
	domainStatusActive = "Active"
)

// Package/connection state constants.
const (
	pkgStateActive          = "ACTIVE"
	connectionStatusActive  = "ACTIVE"
	softwareUpdateCompleted = "COMPLETED"
)

// Default engine version applied when CreateDomain receives an empty EngineVersion.
const defaultEngineVersion = "OpenSearch_2.11"

// InboundConnection represents an OpenSearch inbound cross-cluster connection.
type InboundConnection struct {
	ConnectionID string `json:"connectionId"`
	Status       string `json:"status"`
}

// DataSource represents a data source attached to an OpenSearch domain.
type DataSource struct {
	Name           string `json:"name"`
	Description    string `json:"description"`
	DataSourceType string `json:"dataSourceType"`
}

// DirectQueryDataSource represents a direct-query data source.
type DirectQueryDataSource struct {
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	DataSourceType string   `json:"dataSourceType"`
	DataSourceArn  string   `json:"dataSourceArn"`
	OpenSearchArns []string `json:"openSearchArns"`
}

// DomainPackageDetails holds details about a package associated with a domain.
type DomainPackageDetails struct {
	PackageID  string `json:"packageId"`
	DomainName string `json:"domainName"`
	State      string `json:"state"`
}

// AuthorizedPrincipal represents an authorized principal for VPC endpoint access.
type AuthorizedPrincipal struct {
	Principal     string `json:"principal"`
	PrincipalType string `json:"principalType"`
}

// ServiceSoftwareOptions represents service software options for a domain.
type ServiceSoftwareOptions struct {
	CurrentVersion      string `json:"currentVersion"`
	NewVersion          string `json:"newVersion"`
	UpdateStatus        string `json:"updateStatus"`
	Description         string `json:"description"`
	AutomatedUpdateDate string `json:"automatedUpdateDate"`
	UpdateAvailable     bool   `json:"updateAvailable"`
	Cancellable         bool   `json:"cancellable"`
	OptionalDeployment  bool   `json:"optionalDeployment"`
}

// Application represents an OpenSearch UI application.
type Application struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	ARN         string          `json:"arn"`
	AppConfigs  []AppConfig     `json:"appConfigs"`
	DataSources []AppDataSource `json:"dataSources"`
}

// AppConfig represents an application configuration key-value pair.
type AppConfig struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// AppDataSource represents a data source linked to an application.
type AppDataSource struct {
	DataSourceArn string `json:"dataSourceArn"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// ClusterConfig represents the cluster configuration for an OpenSearch domain.
type ClusterConfig struct {
	InstanceType  string `json:"instanceType"`
	InstanceCount int    `json:"instanceCount"`
}

// Domain represents an OpenSearch domain.
type Domain struct {
	Tags          *tags.Tags    `json:"tags,omitempty"`
	Name          string        `json:"name"`
	ARN           string        `json:"arn"`
	EngineVersion string        `json:"engineVersion"`
	Endpoint      string        `json:"endpoint"`
	Status        string        `json:"status"`
	ClusterConfig ClusterConfig `json:"clusterConfig"`
}

// InMemoryBackend is the in-memory store for OpenSearch domains.
type InMemoryBackend struct {
	dnsRegistrar           DNSRegistrar
	packageAssociations    map[string]map[string]bool
	arnIndex               map[string]string
	inboundConnections     map[string]*InboundConnection
	domainDataSources      map[string]map[string]*DataSource
	directQueryDataSources map[string]*DirectQueryDataSource
	domains                map[string]*Domain
	vpcAuthorizations      map[string][]AuthorizedPrincipal
	applications           map[string]*Application
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	appIDCounter           int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:                make(map[string]*Domain),
		arnIndex:               make(map[string]string),
		inboundConnections:     make(map[string]*InboundConnection),
		domainDataSources:      make(map[string]map[string]*DataSource),
		directQueryDataSources: make(map[string]*DirectQueryDataSource),
		packageAssociations:    make(map[string]map[string]bool),
		vpcAuthorizations:      make(map[string][]AuthorizedPrincipal),
		applications:           make(map[string]*Application),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("opensearch"),
	}
}

// SetDNSRegistrar wires a DNS server so OpenSearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = dns
}

// CreateDomain creates a new OpenSearch domain.
func (b *InMemoryBackend) CreateDomain(name, engineVersion string, clusterConfig ClusterConfig) (*Domain, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if _, exists := b.domains[name]; exists {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
	}

	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)

	if clusterConfig.InstanceCount == 0 {
		clusterConfig.InstanceCount = 1
	}

	if clusterConfig.InstanceType == "" {
		clusterConfig.InstanceType = "t3.small.search"
	}

	d := &Domain{
		Name:          name,
		ARN:           domainARN,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        "Active",
		ClusterConfig: clusterConfig,
		Tags:          tags.New("opensearch." + name + ".tags"),
	}
	b.domains[name] = d
	b.arnIndex[domainARN] = name

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *d

	return &cp, nil
}

// DeleteDomain removes a domain by name and cleans up all associated resources.
func (b *InMemoryBackend) DeleteDomain(name string) (*Domain, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d
	delete(b.domains, name)
	delete(b.arnIndex, d.ARN)
	d.Tags.Close()

	// Cascade-clean all domain-scoped resources.
	delete(b.domainDataSources, name)
	delete(b.vpcAuthorizations, name)

	for pkgID, domains := range b.packageAssociations {
		delete(domains, name)

		if len(domains) == 0 {
			delete(b.packageAssociations, pkgID)
		}
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeDomain returns details about a domain.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, exists := b.domains[name]
	if !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, name)
	}

	cp := *d

	return &cp, nil
}

// ListDomainNames returns the names of all domains in sorted order.
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

// AcceptInboundConnection accepts an inbound cross-cluster connection by ID.
func (b *InMemoryBackend) AcceptInboundConnection(connectionID string) (*InboundConnection, error) {
	if connectionID == "" {
		return nil, fmt.Errorf("%w: ConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections[connectionID]
	if !exists {
		conn = &InboundConnection{
			ConnectionID: connectionID,
			Status:       connectionStatusActive,
		}
		b.inboundConnections[connectionID] = conn
	} else {
		conn.Status = connectionStatusActive
	}

	cp := *conn

	return &cp, nil
}

// AddDataSource adds a data source to a domain.
func (b *InMemoryBackend) AddDataSource(domainName, name, description, dataSourceType string) (string, error) {
	if domainName == "" {
		return "", fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if name == "" {
		return "", fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDataSource")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return "", fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if b.domainDataSources[domainName] == nil {
		b.domainDataSources[domainName] = make(map[string]*DataSource)
	}

	if _, exists := b.domainDataSources[domainName][name]; exists {
		return "", fmt.Errorf(
			"%w: data source %s already exists on domain %s",
			ErrDataSourceAlreadyExists,
			name,
			domainName,
		)
	}

	b.domainDataSources[domainName][name] = &DataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
	}

	return "Data source created successfully", nil
}

// AddDirectQueryDataSource adds a direct-query data source.
func (b *InMemoryBackend) AddDirectQueryDataSource(
	name, description, dataSourceType string,
	openSearchArns []string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: DataSourceName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDirectQueryDataSource")
	defer b.mu.Unlock()

	if _, exists := b.directQueryDataSources[name]; exists {
		return "", fmt.Errorf("%w: direct query data source %s already exists", ErrDataSourceAlreadyExists, name)
	}

	dsARN := arn.Build("opensearch", b.region, b.accountID, "directQueryDataSource/"+name)
	b.directQueryDataSources[name] = &DirectQueryDataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
		OpenSearchArns: openSearchArns,
		DataSourceArn:  dsARN,
	}

	return dsARN, nil
}

// AssociatePackage associates a package with a domain.
func (b *InMemoryBackend) AssociatePackage(packageID, domainName string) (*DomainPackageDetails, error) {
	if packageID == "" {
		return nil, fmt.Errorf("%w: PackageID is required", ErrInvalidParameter)
	}

	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if b.packageAssociations[packageID] == nil {
		b.packageAssociations[packageID] = make(map[string]bool)
	}

	b.packageAssociations[packageID][domainName] = true

	return &DomainPackageDetails{
		PackageID:  packageID,
		DomainName: domainName,
		State:      pkgStateActive,
	}, nil
}

// AssociatePackages associates multiple packages with a domain.
func (b *InMemoryBackend) AssociatePackages(domainName string, packageIDs []string) ([]DomainPackageDetails, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if len(packageIDs) == 0 {
		return nil, fmt.Errorf("%w: PackageList must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AssociatePackages")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	results := make([]DomainPackageDetails, 0, len(packageIDs))

	for _, pkgID := range packageIDs {
		if b.packageAssociations[pkgID] == nil {
			b.packageAssociations[pkgID] = make(map[string]bool)
		}

		b.packageAssociations[pkgID][domainName] = true
		results = append(results, DomainPackageDetails{
			PackageID:  pkgID,
			DomainName: domainName,
			State:      pkgStateActive,
		})
	}

	return results, nil
}

// AuthorizeVpcEndpointAccess grants VPC endpoint access for an account or service.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(
	domainName, account, service string,
) (*AuthorizedPrincipal, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	principal := account
	principalType := "AWS_ACCOUNT"

	if service != "" {
		principal = service
		principalType = "AWS_SERVICE"
	}

	p := AuthorizedPrincipal{
		Principal:     principal,
		PrincipalType: principalType,
	}
	b.vpcAuthorizations[domainName] = append(b.vpcAuthorizations[domainName], p)

	return &p, nil
}

// CancelDomainConfigChange cancels a pending configuration change on a domain.
func (b *InMemoryBackend) CancelDomainConfigChange(domainName string, dryRun bool) ([]string, bool, error) {
	if domainName == "" {
		return nil, false, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.RLock("CancelDomainConfigChange")
	defer b.mu.RUnlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, false, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return []string{}, dryRun, nil
}

// CancelServiceSoftwareUpdate cancels a pending service software update.
func (b *InMemoryBackend) CancelServiceSoftwareUpdate(domainName string) (*ServiceSoftwareOptions, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.RLock("CancelServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return &ServiceSoftwareOptions{
		CurrentVersion:  defaultEngineVersion,
		NewVersion:      "",
		UpdateAvailable: false,
		Cancellable:     false,
		UpdateStatus:    softwareUpdateCompleted,
		Description:     "There is no software update available for this domain.",
	}, nil
}

// CreateApplication creates an OpenSearch UI application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	for _, app := range b.applications {
		if app.Name == name {
			return nil, fmt.Errorf("%w: application %s already exists", ErrApplicationAlreadyExists, name)
		}
	}

	b.appIDCounter++
	id := fmt.Sprintf("app-%d", b.appIDCounter)
	appARN := arn.Build("opensearch", b.region, b.accountID, "application/"+id)

	if appConfigs == nil {
		appConfigs = []AppConfig{}
	}

	if dataSources == nil {
		dataSources = []AppDataSource{}
	}

	app := &Application{
		ID:          id,
		Name:        name,
		ARN:         appARN,
		AppConfigs:  appConfigs,
		DataSources: dataSources,
	}
	b.applications[id] = app

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// Reset clears all backend state, releasing any resources held.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains {
		d.Tags.Close()
	}

	b.domains = make(map[string]*Domain)
	b.arnIndex = make(map[string]string)
	b.inboundConnections = make(map[string]*InboundConnection)
	b.domainDataSources = make(map[string]map[string]*DataSource)
	b.directQueryDataSources = make(map[string]*DirectQueryDataSource)
	b.packageAssociations = make(map[string]map[string]bool)
	b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	b.applications = make(map[string]*Application)
	b.appIDCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string {
	b.mu.RLock("Region")
	defer b.mu.RUnlock()

	return b.region
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// AddDomainInternal seeds a domain directly for use in tests.
func (b *InMemoryBackend) AddDomainInternal(name, engineVersion string) {
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	b.mu.Lock("AddDomainInternal")
	defer b.mu.Unlock()

	domainARN := arn.Build("es", b.region, b.accountID, "domain/"+name)
	endpoint := fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, b.accountID, b.region)
	b.domains[name] = &Domain{
		Name:          name,
		ARN:           domainARN,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Status:        domainStatusActive,
		ClusterConfig: ClusterConfig{InstanceType: "t3.small.search", InstanceCount: 1},
		Tags:          tags.New("opensearch." + name + ".tags"),
	}
	b.arnIndex[domainARN] = name
}
