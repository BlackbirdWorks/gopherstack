package opensearch

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory store for OpenSearch domains.
//
// Most resource collections are *store.Table[T] registered on b.registry --
// see store_setup.go's registerAllTables doc for the full clean/dirty split
// and the reasoning behind the handful of fields left as plain maps.
type InMemoryBackend struct {
	dnsRegistrar               DNSRegistrar
	accountCapacityLimits      ServerlessCapacityLimits
	slLifecyclePolicies        *store.Table[ServerlessLifecyclePolicy]
	upgradeHistory             map[string][]*UpgradeHistory
	outboundConnections        *store.Table[OutboundConnection]
	domainDataSources          *store.Table[DataSource]
	domainDataSourcesByDomain  *store.Index[DataSource]
	directQueryDataSources     *store.Table[DirectQueryDataSource]
	domains                    *store.Table[Domain]
	domainsByARN               *store.Index[Domain]
	vpcAuthorizations          map[string][]AuthorizedPrincipal
	vpcEndpoints               *store.Table[VpcEndpoint]
	applications               *store.Table[Application]
	applicationsByName         *store.Index[Application]
	applicationsByARN          *store.Index[Application]
	packages                   *store.Table[Package]
	scheduledActions           map[string][]*ScheduledAction
	packageAssociations        map[string]map[string]bool
	domainMaintenances         map[string][]*DomainMaintenance
	domainIndexes              *store.Table[DomainIndex]
	domainIndexesByDomain      *store.Index[DomainIndex]
	reservedInstances          *store.Table[ReservedInstance]
	domainPackages             map[string]map[string]bool
	slNetworkPolicies          *store.Table[ServerlessNetworkPolicy]
	slCollections              *store.Table[ServerlessCollection]
	slAccessPolicies           *store.Table[ServerlessAccessPolicy]
	slSecurityConfigs          *store.Table[ServerlessSecurityConfig]
	slEncryptionPolicies       *store.Table[ServerlessEncryptionPolicy]
	inboundConnections         *store.Table[InboundConnection]
	dataSourceAttachments      *store.Table[DataSourceAttachment]
	slCollectionGroups         *store.Table[ServerlessCollectionGroup]
	slIndexes                  *store.Table[ServerlessIndex]
	slVpcEndpoints             *store.Table[ServerlessVpcEndpoint]
	dataSourceAttachmentsByApp *store.Index[DataSourceAttachment]
	capabilities               *store.Table[Capability]
	migrations                 *store.Table[Migration]
	migrationsByApp            *store.Index[Migration]
	workspaces                 *store.Table[Workspace]
	workspacesByApp            *store.Index[Workspace]
	registry                   *store.Registry
	mu                         *lockmetrics.RWMutex
	now                        func() time.Time
	dryRuns                    *store.Table[DryRunStatus]
	region                     string
	defaultApplicationArn      string
	accountID                  string
	processingDelay            time.Duration
	appIDCounter               int
	connCounter                int
	reservedCounter            int
	packageCounter             int
	maintenanceCounter         int
	vpcEndpointCounter         int
	slCollCounter              int
	slSecConfigCounter         int
	slCollGroupCounter         int
	slVpcEndpointCounter       int
	docCounter                 int
	dsAttachCounter            int
	migrationCounter           int
	workspaceCounter           int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		packageAssociations: make(map[string]map[string]bool),
		domainPackages:      make(map[string]map[string]bool),
		vpcAuthorizations:   make(map[string][]AuthorizedPrincipal),
		scheduledActions:    make(map[string][]*ScheduledAction),
		domainMaintenances:  make(map[string][]*DomainMaintenance),
		upgradeHistory:      make(map[string][]*UpgradeHistory),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("opensearch"),
		registry:            store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// SetDNSRegistrar wires a DNS server so OpenSearch domain hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = dns
}

// Reset clears all backend state, releasing any resources held.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, d := range b.domains.All() {
		d.Tags.Close()
	}

	// "Clean" tables registered on b.registry (see store_setup.go).
	b.registry.ResetAll()

	// "Dirty" tables, not registered on b.registry (see store_setup.go).
	b.dryRuns.Reset()
	b.domainDataSources.Reset()
	b.domainIndexes.Reset()
	b.vpcEndpoints.Reset()
	b.dataSourceAttachments.Reset()
	b.packages.Reset()

	// Plain maps left unconverted (see store_setup.go's registerAllTables doc).
	b.packageAssociations = make(map[string]map[string]bool)
	b.domainPackages = make(map[string]map[string]bool)
	b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	b.scheduledActions = make(map[string][]*ScheduledAction)
	b.domainMaintenances = make(map[string][]*DomainMaintenance)
	b.upgradeHistory = make(map[string][]*UpgradeHistory)
	b.defaultApplicationArn = ""
	b.accountCapacityLimits = ServerlessCapacityLimits{}

	b.appIDCounter = 0
	b.connCounter = 0
	b.vpcEndpointCounter = 0
	b.packageCounter = 0
	b.maintenanceCounter = 0
	b.reservedCounter = 0
	b.slCollCounter = 0
	b.slSecConfigCounter = 0
	b.slCollGroupCounter = 0
	b.slVpcEndpointCounter = 0
	b.docCounter = 0
	b.dsAttachCounter = 0
	b.migrationCounter = 0
	b.workspaceCounter = 0
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
