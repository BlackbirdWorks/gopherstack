package mediapackage

import (
	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultMaxResults = 20

	originationAllow = "ALLOW"

	harvestJobStatusSucceeded = "SUCCEEDED"

	resourceTypeChannel                = "channels"
	resourceTypeOriginEndpoint         = "origin_endpoints"
	resourceTypeHarvestJob             = "harvest_jobs"
	resourceTypePackagingConfiguration = "packaging_configurations"
)

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu                       *lockmetrics.RWMutex
	registry                 *store.Registry
	channels                 *store.Table[storedChannel]
	originEndpoints          *store.Table[storedOriginEndpoint]
	originEndpointsByChannel *store.Index[storedOriginEndpoint]
	harvestJobs              *store.Table[storedHarvestJob]
	packagingConfigurations  *store.Table[storedPackagingConfiguration]
	tags                     map[string]map[string]string
	accountID                string
	region                   string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("mediapackage"),
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
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

func (b *InMemoryBackend) buildPackagingConfigARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypePackagingConfiguration+"/"+id)
}
