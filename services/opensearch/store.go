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
	dnsRegistrar              DNSRegistrar
	dryRuns                   *store.Table[DryRunStatus]
	reservedInstances         *store.Table[ReservedInstance]
	inboundConnections        *store.Table[InboundConnection]
	outboundConnections       *store.Table[OutboundConnection]
	domainDataSources         *store.Table[DataSource]
	domainDataSourcesByDomain *store.Index[DataSource]
	directQueryDataSources    *store.Table[DirectQueryDataSource]
	domains                   *store.Table[Domain]
	domainsByARN              *store.Index[Domain]
	vpcAuthorizations         map[string][]AuthorizedPrincipal
	vpcEndpoints              *store.Table[VpcEndpoint]
	applications              *store.Table[Application]
	applicationsByName        *store.Index[Application]
	packages                  *store.Table[Package]
	scheduledActions          map[string][]*ScheduledAction
	packageAssociations       map[string]map[string]bool
	domainMaintenances        map[string][]*DomainMaintenance
	domainIndexes             *store.Table[DomainIndex]
	domainIndexesByDomain     *store.Index[DomainIndex]
	upgradeHistory            map[string][]*UpgradeHistory
	domainPackages            map[string]map[string]bool
	autoTunes                 *store.Table[AutoTuneConfig]
	slNetworkPolicies         *store.Table[ServerlessNetworkPolicy]
	slCollections             *store.Table[ServerlessCollection]
	slAccessPolicies          *store.Table[ServerlessAccessPolicy]
	slSecurityConfigs         *store.Table[ServerlessSecurityConfig]
	slEncryptionPolicies      *store.Table[ServerlessEncryptionPolicy]
	defaultAppSettings        map[string][]AppSetting
	registry                  *store.Registry
	mu                        *lockmetrics.RWMutex
	now                       func() time.Time
	accountID                 string
	region                    string
	processingDelay           time.Duration
	appIDCounter              int
	connCounter               int
	vpcEndpointCounter        int
	packageCounter            int
	maintenanceCounter        int
	reservedCounter           int
	slCollCounter             int
	slSecConfigCounter        int
	docCounter                int
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
		defaultAppSettings:  make(map[string][]AppSetting),
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
	b.autoTunes.Reset()
	b.domainDataSources.Reset()
	b.domainIndexes.Reset()

	// Plain maps left unconverted (see store_setup.go's registerAllTables doc).
	b.packageAssociations = make(map[string]map[string]bool)
	b.domainPackages = make(map[string]map[string]bool)
	b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	b.scheduledActions = make(map[string][]*ScheduledAction)
	b.domainMaintenances = make(map[string][]*DomainMaintenance)
	b.upgradeHistory = make(map[string][]*UpgradeHistory)
	b.defaultAppSettings = make(map[string][]AppSetting)

	b.appIDCounter = 0
	b.connCounter = 0
	b.vpcEndpointCounter = 0
	b.packageCounter = 0
	b.maintenanceCounter = 0
	b.reservedCounter = 0
	b.slCollCounter = 0
	b.slSecConfigCounter = 0
	b.docCounter = 0
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
