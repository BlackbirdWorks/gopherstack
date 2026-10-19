package bcmdataexports

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName = "bcmdataexports"
	homeRegion  = "us-east-1"
)

// InMemoryBackend is the Data Exports state; one coarse lock guards the tables and wired sinks.
type InMemoryBackend struct {
	exports    *store.Table[Export]
	executions *store.Table[Execution]
	registry   *store.Registry
	mu         *lockmetrics.RWMutex
	now        func() time.Time
	objects    billingexport.ObjectStore
	costs      billingexport.CostSource
	accountID  string
	region     string
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

	b.exports = store.Register(b.registry, "exports", store.New(func(v *Export) string { return v.ARN }))
	b.executions = store.Register(b.registry, "executions", store.New(func(v *Execution) string { return v.Key }))

	return b
}

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetObjectStore wires the S3 exports are delivered to.
func (b *InMemoryBackend) SetObjectStore(s billingexport.ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = s
}

// SetCostSource wires the cost data the cost tables are derived from.
func (b *InMemoryBackend) SetCostSource(c billingexport.CostSource) {
	b.mu.Lock("SetCostSource")
	defer b.mu.Unlock()

	b.costs = c
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}
