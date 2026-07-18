// Package elb provides an in-memory implementation of the AWS Classic Elastic
// Load Balancing (ELB) service.
package elb

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Classic ELB load balancers are isolated per region: every backend operation
// resolves the caller's region from the request context and operates only on
// that region's nested store. A Classic ELB and all of its listeners, policies,
// instances, and tags live entirely within a single region, so cross-region
// references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	protoHTTP = "HTTP"

	protoHTTPS = "HTTPS"

	protoTCP = "TCP"

	protoSSL = "SSL"
)

// InMemoryBackend implements StorageBackend using two [store.Table]s.
//
// Classic ELB load balancer names (and, transitively, policy names) are
// unique only WITHIN a region -- see getRegion's region-isolation doc above
// -- so both tables are keyed by a composite "region|name" key (see
// store_setup.go) rather than a bare name, and a secondary index groups
// entries by their owning region/load-balancer for region- and
// load-balancer-scoped scans. Callers must hold b.mu while accessing either
// table or index.
type InMemoryBackend struct {
	registry *store.Registry
	// lbs stores load balancers keyed by region+"|"+loadBalancerName.
	lbs *store.Table[LoadBalancer]
	// lbsByRegion indexes lbs by region for region-scoped listing.
	lbsByRegion *store.Index[LoadBalancer]
	// policies stores load balancer policies keyed by
	// region+"|"+loadBalancerName+"/"+policyName.
	policies *store.Table[LoadBalancerPolicy]
	// policiesByLB indexes policies by their owning load balancer
	// (region+"|"+loadBalancerName) for per-LB listing and cascade delete.
	policiesByLB *store.Index[LoadBalancerPolicy]
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New("elb"),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend was configured with. It is the
// fallback region used when a request context carries no region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all backend state across every region. All Tags registries are
// closed to avoid metric leaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, lb := range b.lbs.All() {
		if lb.Tags != nil {
			lb.Tags.Close()
		}
	}

	b.registry.ResetAll()
}
