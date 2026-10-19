package globalaccelerator

import (
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "globalaccelerator"
	homeRegion      = "us-west-2"
	defaultDwell    = 500 * time.Millisecond
	defaultPageSize = 10
	maxPageSize     = 100
)

// SubnetResolver looks up the IPv4 CIDR of a VPC subnet for custom routing port mappings.
type SubnetResolver interface {
	SubnetCIDR(region, subnetID string) (string, bool)
}

// InMemoryBackend is the Global Accelerator state; one coarse lock guards every table.
type InMemoryBackend struct {
	accelerators   *store.Table[Accelerator]
	listeners      *store.Table[Listener]
	endpointGroups *store.Table[EndpointGroup]
	attachments    *store.Table[Attachment]
	byoip          *store.Table[ByoipCidr]
	registry       *store.Registry
	mu             *lockmetrics.RWMutex
	now            func() time.Time
	resolver       SubnetResolver
	accountID      string
	region         string
	seq            int64
	dwellOverride  time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = homeRegion
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

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the owning account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetLifecycleDelay overrides how long accelerators, endpoints and BYOIP ranges stay in a transitional state.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetSubnetResolver wires the EC2 lookup used by custom routing endpoints.
func (b *InMemoryBackend) SetSubnetResolver(r SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	defer b.mu.Unlock()

	b.resolver = r
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.seq = 0
}

func (b *InMemoryBackend) dwell() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

func (b *InMemoryBackend) nextSeqLocked() int64 {
	b.seq++

	return b.seq
}

func (b *InMemoryBackend) acceleratorARN(id string) string {
	return arn.BuildGlobal(serviceName, b.region, b.accountID, "accelerator/"+id)
}

func (b *InMemoryBackend) attachmentARN(id string) string {
	return arn.BuildGlobal(serviceName, b.region, b.accountID, "attachment/"+id)
}

func newID() string { return uuid.NewString() }

func (b *InMemoryBackend) pendingUntil(now time.Time) time.Time { return now.Add(b.dwell()) }
