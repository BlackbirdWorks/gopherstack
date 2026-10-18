package networkfirewall

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func registerAllTables(b *InMemoryBackend) {
	b.firewalls = store.Register(b.registry, "firewalls", store.New(func(r *firewallRecord) string { return r.ARN }))
	b.policies = store.Register(b.registry, "policies", store.New(func(r *policyRecord) string { return r.ARN }))
	b.ruleGroups = store.Register(b.registry, "ruleGroups", store.New(func(r *ruleGroupRecord) string { return r.ARN }))
	b.tlsConfigs = store.Register(b.registry, "tlsConfigs", store.New(func(r *tlsRecord) string { return r.ARN }))
	b.resourcePolicies = store.Register(
		b.registry, "resourcePolicies", store.New(func(r *resourcePolicyRecord) string { return r.ResourceARN }),
	)
}
