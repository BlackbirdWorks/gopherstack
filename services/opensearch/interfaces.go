package opensearch

// StorageBackend defines the interface for OpenSearch backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Domain operations
	CreateDomain(name, engineVersion string, clusterConfig ClusterConfig) (*Domain, error)
	DeleteDomain(name string) (*Domain, error)
	DescribeDomain(name string) (*Domain, error)
	ListDomainNames() []string

	// Tag operations
	ListTags(domainARN string) (map[string]string, error)
	AddTags(domainARN string, kv map[string]string) error
	RemoveTags(domainARN string, keys []string) error

	// Cross-cluster connection operations
	AcceptInboundConnection(connectionID string) (*InboundConnection, error)

	// Data source operations
	AddDataSource(domainName, name, description, dataSourceType string) (string, error)
	AddDirectQueryDataSource(name, description, dataSourceType string, openSearchArns []string) (string, error)

	// Package operations
	AssociatePackage(packageID, domainName string) (*DomainPackageDetails, error)
	AssociatePackages(domainName string, packageIDs []string) ([]DomainPackageDetails, error)

	// VPC endpoint operations
	AuthorizeVpcEndpointAccess(domainName, account, service string) (*AuthorizedPrincipal, error)

	// Config/software operations
	CancelDomainConfigChange(domainName string, dryRun bool) ([]string, bool, error)
	CancelServiceSoftwareUpdate(domainName string) (*ServiceSoftwareOptions, error)

	// Application operations
	CreateApplication(name string, appConfigs []AppConfig, dataSources []AppDataSource) (*Application, error)

	// Upgrade operations
	UpgradeDomain(domainName, upgradeName string) error
	GetUpgradeHistory(domainName string) ([]*UpgradeHistory, error)
	GetUpgradeStatus(domainName string) (upgradeName, upgradeStatus, upgradeStep string, err error)

	// Auto-tune operations
	SetAutoTune(domainName, desiredState string, schedules []AutoTuneMaintenanceSchedule) error
	GetAutoTune(domainName string) ([]*AutoTune, error)

	// Instance type limits
	DescribeInstanceTypeLimits(instanceType, engineVersion string) (*InstanceTypeLimits, error)

	// Engine-type filtered list
	ListDomainNamesByEngine(engineType string) []string

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
