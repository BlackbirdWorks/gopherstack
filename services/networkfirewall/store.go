package networkfirewall

import (
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName = "network-firewall"

	defaultTransitionDelay = 500 * time.Millisecond
)

// SubnetResolver looks up EC2 subnets so firewall endpoints get real zones and VPCs.
type SubnetResolver interface {
	SubnetAZ(id string) (string, bool)
	SubnetNetwork(id string) (vpcID string, ipv6 bool, ok bool)
}

// InMemoryBackend is the in-memory Network Firewall backend for one account and region.
type InMemoryBackend struct {
	registry         *store.Registry
	firewalls        *store.Table[firewallRecord]
	policies         *store.Table[policyRecord]
	ruleGroups       *store.Table[ruleGroupRecord]
	tlsConfigs       *store.Table[tlsRecord]
	resourcePolicies *store.Table[resourcePolicyRecord]
	proxies          *store.Table[proxyRecord]
	proxyConfigs     *store.Table[proxyConfigRecord]
	proxyRuleGroups  *store.Table[proxyRuleGroupRecord]
	vpcEndpoints     *store.Table[vpcEndpointRecord]
	flowOps          *store.Table[flowOpRecord]
	analysisReports  *store.Table[analysisReportRecord]
	containers       *store.Table[containerRecord]
	subnets          SubnetResolver
	tgws             TransitGatewayResolver
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
	delay            time.Duration
}

// NewInMemoryBackend creates a backend that mints ARNs for accountID and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("networkfirewall"),
		accountID: accountID,
		region:    region,
		delay:     defaultTransitionDelay,
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

// SetSubnetResolver wires EC2 subnet lookups; nil keeps standalone behaviour.
func (b *InMemoryBackend) SetSubnetResolver(r SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	defer b.mu.Unlock()

	b.subnets = r
}

// SetTransitionDelay overrides the PROVISIONING/DELETING/sync window; zero makes transitions instant.
func (b *InMemoryBackend) SetTransitionDelay(d time.Duration) {
	b.mu.Lock("SetTransitionDelay")
	defer b.mu.Unlock()

	b.delay = d
}

// SetLifecycleDelay sets the PROVISIONING/DELETING/sync window; d <= 0 keeps the built-in 500ms.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	if d <= 0 {
		d = defaultTransitionDelay
	}

	b.SetTransitionDelay(d)
}

func (b *InMemoryBackend) arnFor(kind, name string) string {
	return arn.Build(serviceName, b.region, b.accountID, kind+"/"+name)
}

func newToken() string { return uuid.NewString() }

func newResourceID() string { return uuid.NewString() }

func (b *InMemoryBackend) resolver() SubnetResolver {
	b.mu.RLock("resolver")
	defer b.mu.RUnlock()

	return b.subnets
}

func (b *InMemoryBackend) transitionDelay() time.Duration {
	b.mu.RLock("transitionDelay")
	defer b.mu.RUnlock()

	return b.delay
}

func (b *InMemoryBackend) tgwResolver() TransitGatewayResolver {
	b.mu.RLock("tgwResolver")
	defer b.mu.RUnlock()

	return b.tgws
}
