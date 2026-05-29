package opensearch

// StorageBackend defines the interface for OpenSearch backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Domain operations
	CreateDomain(input CreateDomainInput) (*Domain, error)
	DeleteDomain(name string) (*Domain, error)
	DescribeDomain(name string) (*Domain, error)
	DescribeDomains(names []string) ([]*Domain, error)
	ListDomainNames() []string
	UpdateDomainConfig(name string, input UpdateDomainConfigInput) (*Domain, error)
	GetDomainHealth(domainName string) (map[string]any, error)
	GetDomainNodes(domainName string) ([]map[string]any, error)
	GetChangeProgress(domainName string) (map[string]any, error)
	GetDryRunProgress(domainName string) (*DryRunStatus, error)

	// Tag operations
	ListTags(domainARN string) (map[string]string, error)
	AddTags(domainARN string, kv map[string]string) error
	RemoveTags(domainARN string, keys []string) error

	// Inbound cross-cluster connection operations
	AcceptInboundConnection(connectionID string) (*InboundConnection, error)
	RejectInboundConnection(connectionID string) (*InboundConnection, error)
	DeleteInboundConnection(connectionID string) (*InboundConnection, error)
	DescribeInboundConnections() []*InboundConnection

	// Outbound cross-cluster connection operations
	CreateOutboundConnection(
		connectionAlias string,
		localDomainInfo, remoteDomainInfo map[string]any,
	) (*OutboundConnection, error)
	DescribeOutboundConnections() []*OutboundConnection
	DeleteOutboundConnection(connectionID string) (*OutboundConnection, error)

	// Data source operations
	AddDataSource(domainName, name, description, dataSourceType string) (string, error)
	GetDataSource(domainName, name string) (*DataSource, error)
	ListDataSources(domainName string) ([]*DataSource, error)
	UpdateDataSource(domainName, name, description string) error
	DeleteDataSource(domainName, name string) error

	// Direct-query data source operations
	AddDirectQueryDataSource(name, description, dataSourceType string, openSearchArns []string) (string, error)
	ListDirectQueryDataSources() []*DirectQueryDataSource
	GetDirectQueryDataSource(name string) (*DirectQueryDataSource, error)
	UpdateDirectQueryDataSource(name, description string, openSearchArns []string) (*DirectQueryDataSource, error)
	DeleteDirectQueryDataSource(name string) error

	// Package operations
	AssociatePackage(packageID, domainName string) (*DomainPackageDetails, error)
	AssociatePackages(domainName string, packageIDs []string) ([]DomainPackageDetails, error)
	DissociatePackage(packageID, domainName string) (*DomainPackageDetails, error)
	DissociatePackages(domainName string, packageIDs []string) ([]DomainPackageDetails, error)
	CreatePackage(
		name, pkgType, description string,
		source *PackageSource,
		encryptionOpts *PackageEncryptionOptions,
	) (*Package, error)
	DeletePackage(packageID string) (*Package, error)
	DescribePackages(ids []string) ([]*Package, error)
	GetPackageVersionHistory(packageID string) ([]*PackageVersionHistory, error)
	UpdatePackage(packageID, description string) (*Package, error)
	UpdatePackageScope(packageID, operation string, domainNames []string) (*Package, error)
	ListPackagesForDomain(domainName string) []*Package
	ListDomainsForPackage(packageID string) []string

	// VPC endpoint operations
	AuthorizeVpcEndpointAccess(domainName, account, service string) (*AuthorizedPrincipal, error)
	CreateVpcEndpoint(domainArn string, vpcOptions map[string]any) (*VpcEndpoint, error)
	DescribeVpcEndpoints(ids []string) ([]*VpcEndpoint, []map[string]any)
	UpdateVpcEndpoint(id string, vpcOptions map[string]any) (*VpcEndpoint, error)
	DeleteVpcEndpoint(id string) (*VpcEndpoint, error)
	ListVpcEndpoints() []*VpcEndpoint
	ListVpcEndpointsForDomain(domainArn string) []*VpcEndpoint
	RevokeVpcEndpointAccess(domainName, account string) error
	ListVpcEndpointAccess(domainName string) ([]AuthorizedPrincipal, error)

	// Config/software operations
	CancelDomainConfigChange(domainName string, dryRun bool) ([]string, bool, error)
	CancelServiceSoftwareUpdate(domainName string) (*ServiceSoftwareOptions, error)
	StartServiceSoftwareUpdate(domainName string) (*ServiceSoftwareOptions, error)

	// Application operations
	CreateApplication(name string, appConfigs []AppConfig, dataSources []AppDataSource) (*Application, error)
	GetApplication(id string) (*Application, error)
	ListApplications() []*Application
	UpdateApplication(id string, appConfigs []AppConfig, dataSources []AppDataSource) (*Application, error)
	DeleteApplication(id string) error
	GetDefaultApplicationSettings(applicationType string) ([]AppSetting, error)
	PutDefaultApplicationSettings(applicationType string, settings []AppSetting) error

	// Scheduled actions
	ListScheduledActions(domainName string) []*ScheduledAction
	UpdateScheduledAction(domainName string, action *ScheduledAction) (*ScheduledAction, error)

	// Reserved instances
	DescribeReservedInstanceOfferings() []*ReservedInstanceOffering
	DescribeReservedInstances() []*ReservedInstance
	PurchaseReservedInstanceOffering(offeringID, name string, count int) (*ReservedInstance, error)

	// Domain maintenance
	StartDomainMaintenance(domainName, action, nodeID string) (*DomainMaintenance, error)
	GetDomainMaintenanceStatus(domainName, maintenanceID string) (*DomainMaintenance, error)
	ListDomainMaintenances(domainName string) ([]*DomainMaintenance, error)

	// Index operations
	CreateIndex(domainName, indexName string, mappings, settings, aliases map[string]any) (*DomainIndex, error)
	DeleteIndex(domainName, indexName string) (*DomainIndex, error)
	GetIndex(domainName, indexName string) (*DomainIndex, error)
	UpdateIndex(domainName, indexName string, mappings, settings map[string]any) (*DomainIndex, error)

	// Upgrade operations
	UpgradeDomain(domainName, upgradeName string) error
	GetUpgradeHistory(domainName string) ([]*UpgradeHistory, error)
	GetUpgradeStatus(domainName string) (upgradeName, upgradeStatus, upgradeStep string, err error)

	// Auto-tune operations
	SetAutoTune(domainName, desiredState string, schedules []AutoTuneMaintenanceSchedule) error
	GetAutoTune(domainName string) ([]*AutoTune, error)

	// Instance type limits
	DescribeInstanceTypeLimits(instanceType, engineVersion string) (*InstanceTypeLimits, error)

	// Instance type details and compatible versions
	ListInstanceTypeDetails(engineVersion, instanceType string) []map[string]any
	GetCompatibleVersions(domainName string) []map[string]any

	// Engine-type filtered list
	ListDomainNamesByEngine(engineType string) []string

	// Serverless collection operations
	CreateServerlessCollection(
		name, collectionType, description, kmsKeyArn string,
		tags map[string]string,
	) (*ServerlessCollection, error)
	BatchGetServerlessCollections(ids, names []string) []*ServerlessCollection
	DeleteServerlessCollection(id string) (*ServerlessCollection, error)

	// Serverless access policy operations
	CreateServerlessAccessPolicy(policyType, name, description, policy string) (*ServerlessAccessPolicy, error)
	GetServerlessAccessPolicy(policyType, name string) (*ServerlessAccessPolicy, error)
	ListServerlessAccessPolicies(policyType string) []*ServerlessAccessPolicy
	UpdateServerlessAccessPolicy(
		policyType, name, description, policy, policyVersion string,
	) (*ServerlessAccessPolicy, error)
	DeleteServerlessAccessPolicy(policyType, name string) error

	// Serverless security config operations
	CreateServerlessSecurityConfig(
		configType, description string,
		samlOptions *ServerlessSAMLOptions,
	) (*ServerlessSecurityConfig, error)
	GetServerlessSecurityConfig(id string) (*ServerlessSecurityConfig, error)
	ListServerlessSecurityConfigs(configType string) []*ServerlessSecurityConfig
	UpdateServerlessSecurityConfig(
		id, description, configVersion string,
		samlOptions *ServerlessSAMLOptions,
	) (*ServerlessSecurityConfig, error)
	DeleteServerlessSecurityConfig(id string) error

	// Serverless encryption policy operations
	CreateServerlessEncryptionPolicy(policyType, name, description, policy string) (*ServerlessEncryptionPolicy, error)
	GetServerlessEncryptionPolicy(policyType, name string) (*ServerlessEncryptionPolicy, error)
	ListServerlessEncryptionPolicies(policyType string) []*ServerlessEncryptionPolicy
	UpdateServerlessEncryptionPolicy(
		policyType, name, description, policy, policyVersion string,
	) (*ServerlessEncryptionPolicy, error)
	DeleteServerlessEncryptionPolicy(policyType, name string) error

	// Serverless network policy operations
	CreateServerlessNetworkPolicy(policyType, name, description, policy string) (*ServerlessNetworkPolicy, error)
	ListServerlessNetworkPolicies(policyType string) []*ServerlessNetworkPolicy
	DeleteServerlessNetworkPolicy(policyType, name string) error

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
