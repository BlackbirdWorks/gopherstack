package elb

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c) /
// services/elbv2 (commit 5897b825) pilots for the pattern this follows.
//
// Classic ELB load balancer names -- and, transitively, policy names, which
// are only unique within a load balancer -- are unique only WITHIN a region
// (see getRegion's region-isolation doc in backend.go: the pre-conversion
// backend nested lbs/policies maps by an outer region key). [store.Table]
// has no notion of nesting, so both tables here use a composite
// "region|..." key instead, and a secondary [store.Index] recovers the
// region- and load-balancer-scoped iteration the nested maps used to provide
// directly.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// lbTableKey builds the composite primary key for the loadBalancers table.
func lbTableKey(region, name string) string { return region + "|" + name }

func loadBalancersKeyFn(v *LoadBalancer) string { return lbTableKey(v.Region, v.LoadBalancerName) }

// lbRegionIndexKeyFn groups load balancers by region, backing the
// b.lbsByRegion secondary index used for region-scoped listing
// (DescribeLoadBalancers with no name filter, the per-region CreateLoadBalancer
// count limit).
func lbRegionIndexKeyFn(v *LoadBalancer) string { return v.Region }

// policyTableKey builds the composite primary key for the policies table,
// layering region on top of the pre-existing loadBalancerName/policyName
// compound key (policyKey, in backend.go).
func policyTableKey(region, lbName, policyName string) string {
	return region + "|" + policyKey(lbName, policyName)
}

func policiesKeyFn(v *LoadBalancerPolicy) string {
	return policyTableKey(v.Region, v.LoadBalancerName, v.PolicyName)
}

// policyLBIndexKeyFn groups policies by their owning load balancer
// (region+"|"+loadBalancerName), backing the b.policiesByLB secondary index
// used for per-LB policy listing (DescribeLoadBalancerPolicies) and cascading
// load-balancer delete.
func policyLBIndexKeyFn(v *LoadBalancerPolicy) string {
	return lbTableKey(v.Region, v.LoadBalancerName)
}

// registerAllTables registers every resource map on b.registry exactly once.
// It must be called during construction only (immediately after b.registry is
// created), never on every reset -- store.Register panics on a duplicate
// name, so runtime resets go through registry.ResetAll() instead.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (plus its secondary region/owner index) to the concrete field and
// value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		lbs := store.Register(b.registry, "loadBalancers", store.New(loadBalancersKeyFn))
		b.lbs = lbs
		b.lbsByRegion = lbs.AddIndex("region", lbRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		policies := store.Register(b.registry, "policies", store.New(policiesKeyFn))
		b.policies = policies
		b.policiesByLB = policies.AddIndex("loadBalancer", policyLBIndexKeyFn)
	},
}
