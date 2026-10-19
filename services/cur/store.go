package cur

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName = "cur"
	homeRegion  = "us-east-1"
)

// InMemoryBackend is the CUR state; one coarse lock guards the report table and the wired sinks.
type InMemoryBackend struct {
	reports   *store.Table[Report]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	now       func() time.Time
	objects   billingexport.ObjectStore
	costs     billingexport.CostSource
	accountID string
	region    string
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

	b.reports = store.Register(b.registry, "reports", store.New(func(v *Report) string { return v.ReportName }))

	return b
}

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetObjectStore wires the S3 reports are delivered to.
func (b *InMemoryBackend) SetObjectStore(s billingexport.ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = s
}

// SetCostSource wires the cost data report rows are derived from.
func (b *InMemoryBackend) SetCostSource(c billingexport.CostSource) {
	b.mu.Lock("SetCostSource")
	defer b.mu.Unlock()

	b.costs = c
}

// Reset clears every report.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}
