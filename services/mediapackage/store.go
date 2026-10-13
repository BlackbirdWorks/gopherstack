package mediapackage

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultMaxResults = 20

	originationAllow = "ALLOW"

	harvestJobStatusInProgress = "IN_PROGRESS"
	harvestJobStatusSucceeded  = "SUCCEEDED"

	defaultManifestName = "index"

	defaultHarvestJobDuration = time.Second

	resourceTypeChannel        = "channels"
	resourceTypeOriginEndpoint = "origin_endpoints"
	resourceTypeHarvestJob     = "harvest_jobs"
)

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu                       *lockmetrics.RWMutex
	registry                 *store.Registry
	channels                 *store.Table[storedChannel]
	originEndpoints          *store.Table[storedOriginEndpoint]
	originEndpointsByChannel *store.Index[storedOriginEndpoint]
	harvestJobs              *store.Table[storedHarvestJob]
	tags                     map[string]map[string]string
	now                      func() time.Time
	accountID                string
	region                   string
	harvestDuration          time.Duration
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("mediapackage"),
		registry: store.NewRegistry(),
		tags:     make(map[string]map[string]string),
		now:      time.Now,

		harvestDuration: defaultHarvestJobDuration,
		accountID:       accountID,
		region:          region,
	}

	registerAllTables(b)

	return b
}

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) buildChannelARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeChannel+"/"+id)
}

func (b *InMemoryBackend) buildOriginEndpointARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeOriginEndpoint+"/"+id)
}

func (b *InMemoryBackend) buildHarvestJobARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeHarvestJob+"/"+id)
}

// SetHarvestJobDuration sets how long a harvest job stays IN_PROGRESS before reporting
// SUCCEEDED (default 1s). Zero completes jobs immediately.
func (b *InMemoryBackend) SetHarvestJobDuration(d time.Duration) {
	b.mu.Lock("SetHarvestJobDuration")
	defer b.mu.Unlock()

	b.harvestDuration = max(d, 0)
}
