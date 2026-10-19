package timestreaminfluxdb

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "timestream-influxdb"
	defaultRegion = "us-east-1"
	defaultDwell  = 500 * time.Millisecond
	idLen         = 10
	farFuture     = 100 * 365 * 24 * time.Hour
)

// SubnetResolver looks up the IPv4 CIDR of a VPC subnet.
type SubnetResolver interface {
	SubnetCIDR(region, subnetID string) (string, bool)
}

// InMemoryBackend is the Timestream for InfluxDB state; one coarse lock guards every table.
type InMemoryBackend struct {
	resolver      SubnetResolver
	bg            context.Context
	engine        Engine
	registry      *store.Registry
	clusters      *store.Table[DBCluster]
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	backups       *store.Table[DBBackup]
	groups        *store.Table[DBParameterGroup]
	live          map[string]EngineHandle
	instances     *store.Table[DBInstance]
	cancel        context.CancelFunc
	accountID     string
	region        string
	wg            sync.WaitGroup
	dwellOverride time.Duration
	closed        bool
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetLifecycleDelay overrides the transitional status window.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetSubnetResolver wires the EC2 lookup used to validate subnets.
func (b *InMemoryBackend) SetSubnetResolver(r SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	defer b.mu.Unlock()

	b.resolver = r
}

// Reset clears every table and stops engine containers.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")

	var live []*DBInstance
	b.instances.Range(func(i *DBInstance) bool {
		live = append(live, i)

		return true
	})

	handles := b.detachEnginesLocked(live)
	b.registry.ResetAll()
	b.mu.Unlock()

	b.stopEngines(handles)
}

func (b *InMemoryBackend) dwell() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

// pend starts a transitional status; with engine it lasts until the engine reports.
func (b *InMemoryBackend) pend(kind string, engine bool) Pending {
	if engine {
		return Pending{Kind: kind, Until: b.now().Add(farFuture), Engine: true}
	}

	return Pending{Kind: kind, Until: b.now().Add(b.dwell())}
}

// state is the visible status: the pending kind until it elapses, then AVAILABLE.
func (b *InMemoryBackend) state(p Pending) string {
	if p.Kind == "" || !b.now().Before(p.Until) {
		return statusAvailable
	}

	return p.Kind
}

// gone reports a deleting resource whose window elapsed.
func (b *InMemoryBackend) gone(p Pending) bool {
	return p.Kind == statusDeleting && !p.Engine && !b.now().Before(p.Until)
}

type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	sc := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if sc.account == "" {
		sc.account = b.accountID
	}

	if sc.region == "" {
		sc.region = b.region
	}

	return sc
}

func (s scope) owns(account, region string) bool { return s.account == account && s.region == region }

func (s scope) arn(resource string) string {
	return arn.Build(serviceName, s.region, s.account, resource)
}

func (s scope) endpoint(id string) string {
	return id + "-" + s.account + ".timestream-influxdb." + s.region + ".on.aws"
}

func newID() string { return awsid.NewN("", idLen) }

func instanceKey(v *DBInstance) string    { return v.ID }
func clusterKey(v *DBCluster) string      { return v.ID }
func groupKey(v *DBParameterGroup) string { return v.ID }
func backupKey(v *DBBackup) string        { return v.ID }

func registerAllTables(b *InMemoryBackend) {
	b.instances = store.Register(b.registry, "dbInstances", store.New(instanceKey))
	b.clusters = store.Register(b.registry, "dbClusters", store.New(clusterKey))
	b.groups = store.Register(b.registry, "dbParameterGroups", store.New(groupKey))
	b.backups = store.Register(b.registry, "dbBackups", store.New(backupKey))
}

// purgeLocked drops deleting resources whose window elapsed.
func (b *InMemoryBackend) purgeLocked() {
	for _, i := range b.instances.All() {
		if b.gone(i.Pending) {
			b.instances.Delete(i.ID)
		}
	}

	for _, c := range b.clusters.All() {
		if b.gone(c.Pending) {
			b.clusters.Delete(c.ID)
		}
	}

	for _, bk := range b.backups.All() {
		if b.gone(bk.Pending) {
			b.backups.Delete(bk.ID)
		}
	}
}

func (b *InMemoryBackend) checkSubnets(sc scope, subnets []string) error {
	if b.resolver == nil {
		return nil
	}

	for _, s := range subnets {
		if _, ok := b.resolver.SubnetCIDR(sc.region, s); !ok {
			return invalid("subnet %s does not exist", s)
		}
	}

	return nil
}

func lowerEq(a, b string) bool { return strings.EqualFold(a, b) }
