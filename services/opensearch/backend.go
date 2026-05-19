package opensearch

import (
	"errors"
	"fmt"
	"slices"
	"time"

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

// OutboundConnection represents a cross-cluster outbound connection.
type OutboundConnection struct {
	ConnectionID     string         `json:"ConnectionId"`
	ConnectionAlias  string         `json:"ConnectionAlias"`
	LocalDomainInfo  map[string]any `json:"LocalDomainInfo"`
	RemoteDomainInfo map[string]any `json:"RemoteDomainInfo"`
	Status           string         `json:"status"`
}

// VpcEndpoint represents a VPC endpoint for an OpenSearch domain.
type VpcEndpoint struct {
	VpcEndpointID    string         `json:"VpcEndpointId"`
	VpcEndpointOwner string         `json:"VpcEndpointOwner"`
	DomainArn        string         `json:"DomainArn"`
	Status           string         `json:"Status"`
	Endpoint         string         `json:"Endpoint"`
	VpcOptions       map[string]any `json:"VpcOptions"`
}

// Package represents an OpenSearch package.
type Package struct {
	PackageID          string                   `json:"PackageID"`
	PackageName        string                   `json:"PackageName"`
	PackageType        string                   `json:"PackageType"`
	PackageDescription string                   `json:"PackageDescription"`
	PackageStatus      string                   `json:"PackageStatus"`
	CreatedAt          float64                  `json:"CreatedAt"`
	VersionHistory     []*PackageVersionHistory `json:"-"`
}

// PackageVersionHistory records a version of a package.
type PackageVersionHistory struct {
	PackageVersion string  `json:"PackageVersion"`
	CommitMessage  string  `json:"CommitMessage"`
	CreatedAt      float64 `json:"CreatedAt"`
}

// ScheduledAction represents a scheduled action for a domain.
type ScheduledAction struct {
	ID            string  `json:"Id"`
	Type          string  `json:"Type"`
	Severity      string  `json:"Severity"`
	ScheduledTime float64 `json:"ScheduledTime"`
	Description   string  `json:"Description"`
	ScheduledBy   string  `json:"ScheduledBy"`
	Status        string  `json:"Status"`
	Mandatory     bool    `json:"Mandatory"`
	Cancellable   bool    `json:"Cancellable"`
}

// ReservedInstanceOffering is an available reserved instance offering.
type ReservedInstanceOffering struct {
	ReservedInstanceOfferingID string  `json:"ReservedInstanceOfferingId"`
	InstanceType               string  `json:"InstanceType"`
	Duration                   int     `json:"Duration"`
	FixedPrice                 float64 `json:"FixedPrice"`
	UsagePrice                 float64 `json:"UsagePrice"`
	CurrencyCode               string  `json:"CurrencyCode"`
	PaymentOption              string  `json:"PaymentOption"`
}

// ReservedInstance is a purchased reserved instance.
type ReservedInstance struct {
	ReservedInstanceID         string  `json:"ReservedInstanceId"`
	ReservedInstanceOfferingID string  `json:"ReservedInstanceOfferingId"`
	InstanceType               string  `json:"InstanceType"`
	ReservationName            string  `json:"ReservationName"`
	Duration                   int     `json:"Duration"`
	FixedPrice                 float64 `json:"FixedPrice"`
	UsagePrice                 float64 `json:"UsagePrice"`
	InstanceCount              int     `json:"InstanceCount"`
	CurrencyCode               string  `json:"CurrencyCode"`
	PaymentOption              string  `json:"PaymentOption"`
	State                      string  `json:"State"`
	StartTime                  float64 `json:"StartTime"`
}

// DomainMaintenance represents a maintenance action on a domain.
type DomainMaintenance struct {
	MaintenanceID string  `json:"MaintenanceId"`
	DomainName    string  `json:"DomainName"`
	Action        string  `json:"Action"`
	NodeID        string  `json:"NodeId,omitempty"`
	Status        string  `json:"Status"`
	StatusMessage string  `json:"StatusMessage,omitempty"`
	CreatedAt     float64 `json:"CreatedAt"`
	UpdatedAt     float64 `json:"UpdatedAt"`
}

// DomainIndex represents an OpenSearch index.
type DomainIndex struct {
	IndexName   string         `json:"IndexName"`
	IndexStatus string         `json:"IndexStatus"`
	Mappings    map[string]any `json:"Mappings,omitempty"`
	Settings    map[string]any `json:"Settings,omitempty"`
	Aliases     map[string]any `json:"Aliases,omitempty"`
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
	outboundConnections    map[string]*OutboundConnection
	domainDataSources      map[string]map[string]*DataSource
	directQueryDataSources map[string]*DirectQueryDataSource
	domains                map[string]*Domain
	vpcAuthorizations      map[string][]AuthorizedPrincipal
	vpcEndpoints           map[string]*VpcEndpoint
	applications           map[string]*Application
	packages               map[string]*Package
	scheduledActions       map[string][]*ScheduledAction
	reservedInstances      map[string]*ReservedInstance
	domainMaintenances     map[string][]*DomainMaintenance
	domainIndexes          map[string]map[string]*DomainIndex
	upgradeHistory         map[string][]*UpgradeHistory // key: upgradeHistoryKey(domainName)
	autoTunes              map[string]*AutoTuneConfig   // key: autoTuneKey(domainName)
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	appIDCounter           int
	connCounter            int
	vpcEndpointCounter     int
	packageCounter         int
	maintenanceCounter     int
	reservedCounter        int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		domains:                make(map[string]*Domain),
		arnIndex:               make(map[string]string),
		inboundConnections:     make(map[string]*InboundConnection),
		outboundConnections:    make(map[string]*OutboundConnection),
		domainDataSources:      make(map[string]map[string]*DataSource),
		directQueryDataSources: make(map[string]*DirectQueryDataSource),
		packageAssociations:    make(map[string]map[string]bool),
		vpcAuthorizations:      make(map[string][]AuthorizedPrincipal),
		vpcEndpoints:           make(map[string]*VpcEndpoint),
		applications:           make(map[string]*Application),
		packages:               make(map[string]*Package),
		scheduledActions:       make(map[string][]*ScheduledAction),
		reservedInstances:      make(map[string]*ReservedInstance),
		domainMaintenances:     make(map[string][]*DomainMaintenance),
		domainIndexes:          make(map[string]map[string]*DomainIndex),
		upgradeHistory:         make(map[string][]*UpgradeHistory),
		autoTunes:              make(map[string]*AutoTuneConfig),
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
		clusterConfig.InstanceType = instanceTypeT3Small
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
	b.outboundConnections = make(map[string]*OutboundConnection)
	b.domainDataSources = make(map[string]map[string]*DataSource)
	b.directQueryDataSources = make(map[string]*DirectQueryDataSource)
	b.packageAssociations = make(map[string]map[string]bool)
	b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	b.vpcEndpoints = make(map[string]*VpcEndpoint)
	b.applications = make(map[string]*Application)
	b.packages = make(map[string]*Package)
	b.scheduledActions = make(map[string][]*ScheduledAction)
	b.reservedInstances = make(map[string]*ReservedInstance)
	b.domainMaintenances = make(map[string][]*DomainMaintenance)
	b.domainIndexes = make(map[string]map[string]*DomainIndex)
	b.upgradeHistory = make(map[string][]*UpgradeHistory)
	b.autoTunes = make(map[string]*AutoTuneConfig)
	b.appIDCounter = 0
	b.connCounter = 0
	b.vpcEndpointCounter = 0
	b.packageCounter = 0
	b.maintenanceCounter = 0
	b.reservedCounter = 0
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

// CreateOutboundConnection creates a new outbound cross-cluster connection.
func (b *InMemoryBackend) CreateOutboundConnection(
	connectionAlias string,
	localDomainInfo, remoteDomainInfo map[string]any,
) (*OutboundConnection, error) {
	b.mu.Lock("CreateOutboundConnection")
	defer b.mu.Unlock()

	b.connCounter++
	id := fmt.Sprintf("co-%d", b.connCounter)

	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  connectionAlias,
		LocalDomainInfo:  localDomainInfo,
		RemoteDomainInfo: remoteDomainInfo,
		Status:           connectionStatusActive,
	}
	b.outboundConnections[id] = conn

	cp := *conn

	return &cp, nil
}

// DescribeOutboundConnections returns all outbound connections.
func (b *InMemoryBackend) DescribeOutboundConnections() []*OutboundConnection {
	b.mu.RLock("DescribeOutboundConnections")
	defer b.mu.RUnlock()

	out := make([]*OutboundConnection, 0, len(b.outboundConnections))
	for _, c := range b.outboundConnections {
		cp := *c
		out = append(out, &cp)
	}

	return out
}

// DeleteOutboundConnection removes an outbound connection by ID.
func (b *InMemoryBackend) DeleteOutboundConnection(connectionID string) (*OutboundConnection, error) {
	b.mu.Lock("DeleteOutboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.outboundConnections[connectionID]
	if !exists {
		return nil, fmt.Errorf("%w: outbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	cp.Status = "DELETED"
	delete(b.outboundConnections, connectionID)

	return &cp, nil
}

// RejectInboundConnection sets an inbound connection status to REJECTED.
func (b *InMemoryBackend) RejectInboundConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("RejectInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections[connectionID]
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.Status = "REJECTED"
	cp := *conn

	return &cp, nil
}

// DeleteInboundConnection removes an inbound connection by ID.
func (b *InMemoryBackend) DeleteInboundConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("DeleteInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections[connectionID]
	if !exists {
		conn = &InboundConnection{ConnectionID: connectionID, Status: "DELETED"}
		return conn, nil
	}

	cp := *conn
	cp.Status = "DELETED"
	delete(b.inboundConnections, connectionID)

	return &cp, nil
}

// DescribeInboundConnections returns all inbound connections.
func (b *InMemoryBackend) DescribeInboundConnections() []*InboundConnection {
	b.mu.RLock("DescribeInboundConnections")
	defer b.mu.RUnlock()

	out := make([]*InboundConnection, 0, len(b.inboundConnections))
	for _, c := range b.inboundConnections {
		cp := *c
		out = append(out, &cp)
	}

	return out
}

// CreateVpcEndpoint creates a new VPC endpoint.
func (b *InMemoryBackend) CreateVpcEndpoint(domainArn string, vpcOptions map[string]any) (*VpcEndpoint, error) {
	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	b.vpcEndpointCounter++
	id := fmt.Sprintf("vpce-%d", b.vpcEndpointCounter)

	ep := &VpcEndpoint{
		VpcEndpointID:    id,
		VpcEndpointOwner: b.accountID,
		DomainArn:        domainArn,
		Status:           pkgStateActive,
		Endpoint:         fmt.Sprintf("%s.vpc.es.amazonaws.com", id),
		VpcOptions:       vpcOptions,
	}
	b.vpcEndpoints[id] = ep

	cp := *ep

	return &cp, nil
}

// DescribeVpcEndpoints returns matching VPC endpoints and errors for not-found IDs.
func (b *InMemoryBackend) DescribeVpcEndpoints(ids []string) ([]*VpcEndpoint, []map[string]any) {
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	var endpoints []*VpcEndpoint
	var errs []map[string]any

	for _, id := range ids {
		ep, exists := b.vpcEndpoints[id]
		if !exists {
			errs = append(errs, map[string]any{
				"VpcEndpointId": id,
				"ErrorCode":     "EndpointNotFound",
				"ErrorMessage":  fmt.Sprintf("VPC endpoint %s not found", id),
			})

			continue
		}

		cp := *ep
		endpoints = append(endpoints, &cp)
	}

	if endpoints == nil {
		endpoints = []*VpcEndpoint{}
	}

	if errs == nil {
		errs = []map[string]any{}
	}

	return endpoints, errs
}

// UpdateVpcEndpoint updates the VPC options for a VPC endpoint.
func (b *InMemoryBackend) UpdateVpcEndpoint(id string, vpcOptions map[string]any) (*VpcEndpoint, error) {
	b.mu.Lock("UpdateVpcEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.vpcEndpoints[id]
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	ep.VpcOptions = vpcOptions
	cp := *ep

	return &cp, nil
}

// DeleteVpcEndpoint removes a VPC endpoint by ID.
func (b *InMemoryBackend) DeleteVpcEndpoint(id string) (*VpcEndpoint, error) {
	b.mu.Lock("DeleteVpcEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.vpcEndpoints[id]
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	cp := *ep
	cp.Status = "DELETED"
	delete(b.vpcEndpoints, id)

	return &cp, nil
}

// ListVpcEndpoints returns all VPC endpoints.
func (b *InMemoryBackend) ListVpcEndpoints() []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	out := make([]*VpcEndpoint, 0, len(b.vpcEndpoints))
	for _, ep := range b.vpcEndpoints {
		cp := *ep
		out = append(out, &cp)
	}

	return out
}

// ListVpcEndpointsForDomain returns VPC endpoints associated with a domain ARN.
func (b *InMemoryBackend) ListVpcEndpointsForDomain(domainArn string) []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpointsForDomain")
	defer b.mu.RUnlock()

	var out []*VpcEndpoint
	for _, ep := range b.vpcEndpoints {
		if ep.DomainArn == domainArn {
			cp := *ep
			out = append(out, &cp)
		}
	}

	if out == nil {
		out = []*VpcEndpoint{}
	}

	return out
}

// RevokeVpcEndpointAccess removes a principal from the VPC authorizations for a domain.
func (b *InMemoryBackend) RevokeVpcEndpointAccess(domainName, account string) error {
	b.mu.Lock("RevokeVpcEndpointAccess")
	defer b.mu.Unlock()

	principals := b.vpcAuthorizations[domainName]
	filtered := principals[:0]

	for _, p := range principals {
		if p.Principal != account {
			filtered = append(filtered, p)
		}
	}

	b.vpcAuthorizations[domainName] = filtered

	return nil
}

// ListVpcEndpointAccess returns authorized principals for a domain.
func (b *InMemoryBackend) ListVpcEndpointAccess(domainName string) ([]AuthorizedPrincipal, error) {
	b.mu.RLock("ListVpcEndpointAccess")
	defer b.mu.RUnlock()

	principals := b.vpcAuthorizations[domainName]
	out := make([]AuthorizedPrincipal, len(principals))
	copy(out, principals)

	return out, nil
}

// CreatePackage creates a new OpenSearch package.
func (b *InMemoryBackend) CreatePackage(name, pkgType, description string) (*Package, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PackageName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePackage")
	defer b.mu.Unlock()

	b.packageCounter++
	id := fmt.Sprintf("F%d", b.packageCounter)
	now := float64(time.Now().Unix())

	pkg := &Package{
		PackageID:          id,
		PackageName:        name,
		PackageType:        pkgType,
		PackageDescription: description,
		PackageStatus:      pkgStateActive,
		CreatedAt:          now,
		VersionHistory: []*PackageVersionHistory{
			{
				PackageVersion: "1",
				CommitMessage:  "initial version",
				CreatedAt:      now,
			},
		},
	}
	b.packages[id] = pkg

	cp := *pkg

	return &cp, nil
}

// DeletePackage removes a package by ID.
func (b *InMemoryBackend) DeletePackage(packageID string) (*Package, error) {
	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packages[packageID]
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg
	delete(b.packages, packageID)

	return &cp, nil
}

// DescribePackages returns packages matching the given IDs, or all packages if ids is empty.
func (b *InMemoryBackend) DescribePackages(ids []string) ([]*Package, error) {
	b.mu.RLock("DescribePackages")
	defer b.mu.RUnlock()

	if len(ids) == 0 {
		out := make([]*Package, 0, len(b.packages))
		for _, pkg := range b.packages {
			cp := *pkg
			out = append(out, &cp)
		}

		return out, nil
	}

	out := make([]*Package, 0, len(ids))

	for _, id := range ids {
		pkg, exists := b.packages[id]
		if !exists {
			return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, id)
		}

		cp := *pkg
		out = append(out, &cp)
	}

	return out, nil
}

// GetPackageVersionHistory returns the version history for a package.
func (b *InMemoryBackend) GetPackageVersionHistory(packageID string) ([]*PackageVersionHistory, error) {
	b.mu.RLock("GetPackageVersionHistory")
	defer b.mu.RUnlock()

	pkg, exists := b.packages[packageID]
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	out := make([]*PackageVersionHistory, len(pkg.VersionHistory))
	for i, vh := range pkg.VersionHistory {
		cp := *vh
		out[i] = &cp
	}

	return out, nil
}

// UpdatePackage updates a package's description and adds a version history entry.
func (b *InMemoryBackend) UpdatePackage(packageID, description string) (*Package, error) {
	b.mu.Lock("UpdatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packages[packageID]
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	pkg.PackageDescription = description
	pkg.VersionHistory = append(pkg.VersionHistory, &PackageVersionHistory{
		PackageVersion: fmt.Sprintf("%d", len(pkg.VersionHistory)+1),
		CommitMessage:  "updated",
		CreatedAt:      float64(time.Now().Unix()),
	})

	cp := *pkg

	return &cp, nil
}

// UpdatePackageScope is a no-op that returns the package (scope is not tracked in-memory).
func (b *InMemoryBackend) UpdatePackageScope(packageID, operation string, domainNames []string) (*Package, error) {
	b.mu.RLock("UpdatePackageScope")
	defer b.mu.RUnlock()

	pkg, exists := b.packages[packageID]
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg

	return &cp, nil
}

// ListPackagesForDomain returns packages associated with a domain.
func (b *InMemoryBackend) ListPackagesForDomain(domainName string) []*Package {
	b.mu.RLock("ListPackagesForDomain")
	defer b.mu.RUnlock()

	var out []*Package

	for pkgID, domains := range b.packageAssociations {
		if domains[domainName] {
			pkg, exists := b.packages[pkgID]
			if exists {
				cp := *pkg
				out = append(out, &cp)
			}
		}
	}

	if out == nil {
		out = []*Package{}
	}

	return out
}

// ListDomainsForPackage returns domain names associated with a package.
func (b *InMemoryBackend) ListDomainsForPackage(packageID string) []string {
	b.mu.RLock("ListDomainsForPackage")
	defer b.mu.RUnlock()

	domains := b.packageAssociations[packageID]
	out := make([]string, 0, len(domains))

	for d := range domains {
		out = append(out, d)
	}

	slices.Sort(out)

	return out
}

// GetDataSource returns a data source by domain and name.
func (b *InMemoryBackend) GetDataSource(domainName, name string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	dsMap, exists := b.domainDataSources[domainName]
	if !exists {
		return nil, fmt.Errorf("%w: data source %s not found on domain %s", ErrDataSourceNotFound, name, domainName)
	}

	ds, exists := dsMap[name]
	if !exists {
		return nil, fmt.Errorf("%w: data source %s not found on domain %s", ErrDataSourceNotFound, name, domainName)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources returns all data sources for a domain.
func (b *InMemoryBackend) ListDataSources(domainName string) ([]*DataSource, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	dsMap := b.domainDataSources[domainName]
	out := make([]*DataSource, 0, len(dsMap))

	for _, ds := range dsMap {
		cp := *ds
		out = append(out, &cp)
	}

	return out, nil
}

// UpdateDataSource updates the description of a data source.
func (b *InMemoryBackend) UpdateDataSource(domainName, name, description string) error {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	dsMap, exists := b.domainDataSources[domainName]
	if !exists {
		return fmt.Errorf("%w: data source %s not found on domain %s", ErrDataSourceNotFound, name, domainName)
	}

	ds, exists := dsMap[name]
	if !exists {
		return fmt.Errorf("%w: data source %s not found on domain %s", ErrDataSourceNotFound, name, domainName)
	}

	ds.Description = description

	return nil
}

// DeleteDataSource removes a data source from a domain.
func (b *InMemoryBackend) DeleteDataSource(domainName, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	dsMap, exists := b.domainDataSources[domainName]
	if !exists {
		return nil
	}

	delete(dsMap, name)

	return nil
}

// ListDirectQueryDataSources returns all direct-query data sources.
func (b *InMemoryBackend) ListDirectQueryDataSources() []*DirectQueryDataSource {
	b.mu.RLock("ListDirectQueryDataSources")
	defer b.mu.RUnlock()

	out := make([]*DirectQueryDataSource, 0, len(b.directQueryDataSources))
	for _, ds := range b.directQueryDataSources {
		cp := *ds
		out = append(out, &cp)
	}

	return out
}

// GetDirectQueryDataSource returns a direct-query data source by name.
func (b *InMemoryBackend) GetDirectQueryDataSource(name string) (*DirectQueryDataSource, error) {
	b.mu.RLock("GetDirectQueryDataSource")
	defer b.mu.RUnlock()

	ds, exists := b.directQueryDataSources[name]
	if !exists {
		return nil, fmt.Errorf("%w: direct query data source %s not found", ErrDataSourceNotFound, name)
	}

	cp := *ds

	return &cp, nil
}

// UpdateDirectQueryDataSource updates a direct-query data source.
func (b *InMemoryBackend) UpdateDirectQueryDataSource(
	name, description string,
	openSearchArns []string,
) (*DirectQueryDataSource, error) {
	b.mu.Lock("UpdateDirectQueryDataSource")
	defer b.mu.Unlock()

	ds, exists := b.directQueryDataSources[name]
	if !exists {
		return nil, fmt.Errorf("%w: direct query data source %s not found", ErrDataSourceNotFound, name)
	}

	ds.Description = description
	ds.OpenSearchArns = openSearchArns
	cp := *ds

	return &cp, nil
}

// DeleteDirectQueryDataSource removes a direct-query data source by name.
func (b *InMemoryBackend) DeleteDirectQueryDataSource(name string) error {
	b.mu.Lock("DeleteDirectQueryDataSource")
	defer b.mu.Unlock()

	delete(b.directQueryDataSources, name)

	return nil
}

// ListScheduledActions returns scheduled actions for a domain.
func (b *InMemoryBackend) ListScheduledActions(domainName string) []*ScheduledAction {
	b.mu.RLock("ListScheduledActions")
	defer b.mu.RUnlock()

	src := b.scheduledActions[domainName]
	out := make([]*ScheduledAction, len(src))

	for i, sa := range src {
		cp := *sa
		out[i] = &cp
	}

	return out
}

// UpdateScheduledAction updates or adds a scheduled action for a domain.
func (b *InMemoryBackend) UpdateScheduledAction(domainName string, action *ScheduledAction) (*ScheduledAction, error) {
	b.mu.Lock("UpdateScheduledAction")
	defer b.mu.Unlock()

	actions := b.scheduledActions[domainName]
	for i, sa := range actions {
		if sa.ID == action.ID {
			*sa = *action
			cp := *actions[i]

			return &cp, nil
		}
	}

	cp := *action
	b.scheduledActions[domainName] = append(b.scheduledActions[domainName], &cp)

	ret := *action

	return &ret, nil
}

// staticReservedInstanceOfferings returns a fixed list of available offerings.
func staticReservedInstanceOfferings() []*ReservedInstanceOffering {
	return []*ReservedInstanceOffering{
		{
			ReservedInstanceOfferingID: "ri-offering-1",
			InstanceType:               instanceTypeT3Small,
			Duration:                   31536000,
			FixedPrice:                 500.0,
			UsagePrice:                 0.0,
			CurrencyCode:               "USD",
			PaymentOption:              "ALL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-2",
			InstanceType:               "r6g.large.search",
			Duration:                   31536000,
			FixedPrice:                 300.0,
			UsagePrice:                 0.05,
			CurrencyCode:               "USD",
			PaymentOption:              "PARTIAL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-3",
			InstanceType:               "m6g.large.search",
			Duration:                   94608000,
			FixedPrice:                 0.0,
			UsagePrice:                 0.15,
			CurrencyCode:               "USD",
			PaymentOption:              "NO_UPFRONT",
		},
	}
}

// DescribeReservedInstanceOfferings returns available reserved instance offerings.
func (b *InMemoryBackend) DescribeReservedInstanceOfferings() []*ReservedInstanceOffering {
	return staticReservedInstanceOfferings()
}

// DescribeReservedInstances returns all purchased reserved instances.
func (b *InMemoryBackend) DescribeReservedInstances() []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	out := make([]*ReservedInstance, 0, len(b.reservedInstances))
	for _, ri := range b.reservedInstances {
		cp := *ri
		out = append(out, &cp)
	}

	return out
}

// PurchaseReservedInstanceOffering purchases a reserved instance offering.
func (b *InMemoryBackend) PurchaseReservedInstanceOffering(
	offeringID, name string,
	count int,
) (*ReservedInstance, error) {
	var offering *ReservedInstanceOffering

	for _, o := range staticReservedInstanceOfferings() {
		if o.ReservedInstanceOfferingID == offeringID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf("%w: reserved instance offering %s not found", ErrConnectionNotFound, offeringID)
	}

	b.mu.Lock("PurchaseReservedInstanceOffering")
	defer b.mu.Unlock()

	b.reservedCounter++
	id := fmt.Sprintf("ri-%d", b.reservedCounter)

	ri := &ReservedInstance{
		ReservedInstanceID:         id,
		ReservedInstanceOfferingID: offeringID,
		InstanceType:               offering.InstanceType,
		ReservationName:            name,
		Duration:                   offering.Duration,
		FixedPrice:                 offering.FixedPrice,
		UsagePrice:                 offering.UsagePrice,
		InstanceCount:              count,
		CurrencyCode:               offering.CurrencyCode,
		PaymentOption:              offering.PaymentOption,
		State:                      pkgStateActive,
		StartTime:                  float64(time.Now().Unix()),
	}
	b.reservedInstances[id] = ri

	cp := *ri

	return &cp, nil
}

// StartDomainMaintenance starts a maintenance action on a domain.
func (b *InMemoryBackend) StartDomainMaintenance(domainName, action, nodeID string) (*DomainMaintenance, error) {
	b.mu.Lock("StartDomainMaintenance")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.maintenanceCounter++
	id := fmt.Sprintf("m-%d", b.maintenanceCounter)
	now := float64(time.Now().Unix())

	m := &DomainMaintenance{
		MaintenanceID: id,
		DomainName:    domainName,
		Action:        action,
		NodeID:        nodeID,
		Status:        softwareUpdateCompleted,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.domainMaintenances[domainName] = append(b.domainMaintenances[domainName], m)

	cp := *m

	return &cp, nil
}

// GetDomainMaintenanceStatus returns a specific maintenance record.
func (b *InMemoryBackend) GetDomainMaintenanceStatus(domainName, maintenanceID string) (*DomainMaintenance, error) {
	b.mu.RLock("GetDomainMaintenanceStatus")
	defer b.mu.RUnlock()

	for _, m := range b.domainMaintenances[domainName] {
		if m.MaintenanceID == maintenanceID {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: maintenance %s not found on domain %s", ErrConnectionNotFound, maintenanceID, domainName)
}

// ListDomainMaintenances returns all maintenance records for a domain.
func (b *InMemoryBackend) ListDomainMaintenances(domainName string) ([]*DomainMaintenance, error) {
	b.mu.RLock("ListDomainMaintenances")
	defer b.mu.RUnlock()

	src := b.domainMaintenances[domainName]
	out := make([]*DomainMaintenance, len(src))

	for i, m := range src {
		cp := *m
		out[i] = &cp
	}

	return out, nil
}

// CreateIndex creates an index for a domain.
func (b *InMemoryBackend) CreateIndex(
	domainName, indexName string,
	mappings, settings, aliases map[string]any,
) (*DomainIndex, error) {
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if b.domainIndexes[domainName] == nil {
		b.domainIndexes[domainName] = make(map[string]*DomainIndex)
	}

	idx := &DomainIndex{
		IndexName:   indexName,
		IndexStatus: pkgStateActive,
		Mappings:    mappings,
		Settings:    settings,
		Aliases:     aliases,
	}
	b.domainIndexes[domainName][indexName] = idx

	cp := *idx

	return &cp, nil
}

// DeleteIndex removes an index from a domain.
func (b *InMemoryBackend) DeleteIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	idxMap := b.domainIndexes[domainName]
	if idxMap == nil {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	idx, exists := idxMap[indexName]
	if !exists {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	cp := *idx
	delete(idxMap, indexName)

	return &cp, nil
}

// GetIndex returns an index by domain and name.
func (b *InMemoryBackend) GetIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	idxMap := b.domainIndexes[domainName]
	if idxMap == nil {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	idx, exists := idxMap[indexName]
	if !exists {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	cp := *idx

	return &cp, nil
}

// UpdateIndex updates the mappings and settings of an index.
func (b *InMemoryBackend) UpdateIndex(
	domainName, indexName string,
	mappings, settings map[string]any,
) (*DomainIndex, error) {
	b.mu.Lock("UpdateIndex")
	defer b.mu.Unlock()

	idxMap := b.domainIndexes[domainName]
	if idxMap == nil {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	idx, exists := idxMap[indexName]
	if !exists {
		return nil, fmt.Errorf("%w: index %s not found on domain %s", ErrConnectionNotFound, indexName, domainName)
	}

	idx.Mappings = mappings
	idx.Settings = settings
	cp := *idx

	return &cp, nil
}

// GetApplication returns an application by ID.
func (b *InMemoryBackend) GetApplication(id string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, exists := b.applications[id]
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
		cp := *app
		cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
		copy(cp.AppConfigs, app.AppConfigs)
		cp.DataSources = make([]AppDataSource, len(app.DataSources))
		copy(cp.DataSources, app.DataSources)
		out = append(out, &cp)
	}

	return out
}

// UpdateApplication updates an application's configs and data sources.
func (b *InMemoryBackend) UpdateApplication(
	id string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, exists := b.applications[id]
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	if appConfigs != nil {
		app.AppConfigs = appConfigs
	}

	if dataSources != nil {
		app.DataSources = dataSources
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// DeleteApplication removes an application by ID.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, exists := b.applications[id]; !exists {
		return fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	delete(b.applications, id)

	return nil
}

// StartServiceSoftwareUpdate marks a domain as having a pending software update.
func (b *InMemoryBackend) StartServiceSoftwareUpdate(domainName string) (*ServiceSoftwareOptions, error) {
	b.mu.RLock("StartServiceSoftwareUpdate")
	defer b.mu.RUnlock()

	if _, exists := b.domains[domainName]; !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return &ServiceSoftwareOptions{
		CurrentVersion:  defaultEngineVersion,
		NewVersion:      defaultEngineVersion,
		UpdateAvailable: true,
		Cancellable:     true,
		UpdateStatus:    "PENDING_UPDATE",
		Description:     "A new service software version is ready to install.",
	}, nil
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
		ClusterConfig: ClusterConfig{InstanceType: instanceTypeT3Small, InstanceCount: 1},
		Tags:          tags.New("opensearch." + name + ".tags"),
	}
	b.arnIndex[domainARN] = name
}
