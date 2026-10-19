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
	b.proxies = store.Register(b.registry, "proxies", store.New(func(r *proxyRecord) string { return r.ARN }))
	b.proxyConfigs = store.Register(
		b.registry, "proxyConfigs", store.New(func(r *proxyConfigRecord) string { return r.ARN }),
	)
	b.proxyRuleGroups = store.Register(
		b.registry, "proxyRuleGroups", store.New(func(r *proxyRuleGroupRecord) string { return r.ARN }),
	)
	b.vpcEndpoints = store.Register(
		b.registry, "vpcEndpoints", store.New(func(r *vpcEndpointRecord) string { return r.ARN }),
	)
	b.flowOps = store.Register(b.registry, "flowOps", store.New(func(r *flowOpRecord) string { return r.Key }))
	b.analysisReports = store.Register(
		b.registry, "analysisReports", store.New(func(r *analysisReportRecord) string { return r.Key }),
	)
	b.containers = store.Register(
		b.registry, "containers", store.New(func(r *containerRecord) string { return r.ARN }),
	)
}
