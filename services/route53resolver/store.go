package route53resolver

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func currentTime() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionalKey builds the composite key used by every region-scoped
// store.Table below: id is unique only within a region (AWS resource IDs
// across route53resolver are not globally namespaced), so the primary key
// must fold the region in the same way the old map[region]map[id]*T nesting
// kept regions isolated. See store_setup.go for the per-type key functions
// built on top of this.
func regionalKey(region, id string) string {
	return region + "|" + id
}

type InMemoryBackend struct {
	// registry lets Reset collapse every table's lifecycle to one call
	// (registry.ResetAll()) instead of hand-rolled re-initialization of each
	// map. See store_setup.go for the full set of registrations.
	registry                              *store.Registry
	endpoints                             *store.Table[ResolverEndpoint]
	endpointsByRegion                     *store.Index[ResolverEndpoint]
	rules                                 *store.Table[ResolverRule]
	rulesByRegion                         *store.Index[ResolverRule]
	firewallRuleGroups                    *store.Table[FirewallRuleGroup]
	firewallRuleGroupsByRegion            *store.Index[FirewallRuleGroup]
	firewallRuleGroupAssociations         *store.Table[FirewallRuleGroupAssociation]
	firewallRuleGroupAssociationsByRegion *store.Index[FirewallRuleGroupAssociation]
	firewallDomainLists                   *store.Table[FirewallDomainList]
	firewallDomainListsByRegion           *store.Index[FirewallDomainList]
	firewallRules                         *store.Table[FirewallRule]
	firewallRulesByRegion                 *store.Index[FirewallRule]
	outpostResolvers                      *store.Table[OutpostResolver]
	outpostResolversByRegion              *store.Index[OutpostResolver]
	queryLogConfigs                       *store.Table[ResolverQueryLogConfig]
	queryLogConfigsByRegion               *store.Index[ResolverQueryLogConfig]
	queryLogConfigAssociations            *store.Table[ResolverQueryLogConfigAssociation]
	queryLogConfigAssociationsByRegion    *store.Index[ResolverQueryLogConfigAssociation]
	ruleAssociations                      *store.Table[ResolverRuleAssociation]
	ruleAssociationsByRegion              *store.Index[ResolverRuleAssociation]
	firewallConfigs                       *store.Table[FirewallConfig]
	firewallConfigsByRegion               *store.Index[FirewallConfig]
	resolverConfigs                       *store.Table[ResolverConfig]
	resolverConfigsByRegion               *store.Index[ResolverConfig]
	resolverDnssecConfigs                 *store.Table[ResolverDnssecConfig]
	resolverDnssecConfigsByRegion         *store.Index[ResolverDnssecConfig]

	// The following are deliberately left as plain region-nested maps (not
	// store.Table): their values are not *T (svcTags.KV slices and bare
	// policy-document strings), which store.Table's map[string]*V shape does
	// not fit. See store_setup.go's file doc comment for the full rationale.
	tags                      map[string]map[string][]svcTags.KV
	firewallRuleGroupPolicies map[string]map[string]string
	queryLogConfigPolicies    map[string]map[string]string
	resolverRulePolicies      map[string]map[string]string

	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                  store.NewRegistry(),
		tags:                      make(map[string]map[string][]svcTags.KV),
		firewallRuleGroupPolicies: make(map[string]map[string]string),
		queryLogConfigPolicies:    make(map[string]map[string]string),
		resolverRulePolicies:      make(map[string]map[string]string),
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("route53resolver"),
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to an empty initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.tags = make(map[string]map[string][]svcTags.KV)
	b.firewallRuleGroupPolicies = make(map[string]map[string]string)
	b.queryLogConfigPolicies = make(map[string]map[string]string)
	b.resolverRulePolicies = make(map[string]map[string]string)
}

// Per-region lazy store helpers for the maps left un-converted (see the
// InMemoryBackend field doc comment above).
