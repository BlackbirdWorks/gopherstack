package route53resolver

import (
	"encoding/json"
	"log/slog"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Endpoints                     map[string]*ResolverEndpoint                  `json:"endpoints"`
	Rules                         map[string]*ResolverRule                      `json:"rules"`
	Tags                          map[string][]svcTags.KV                       `json:"tags"`
	FirewallRuleGroups            map[string]*FirewallRuleGroup                 `json:"firewallRuleGroups"`
	FirewallRuleGroupAssociations map[string]*FirewallRuleGroupAssociation      `json:"firewallRuleGroupAssociations"`
	FirewallDomainLists           map[string]*FirewallDomainList                `json:"firewallDomainLists"`
	FirewallRules                 map[string]*FirewallRule                      `json:"firewallRules"`
	OutpostResolvers              map[string]*OutpostResolver                   `json:"outpostResolvers"`
	QueryLogConfigs               map[string]*ResolverQueryLogConfig            `json:"queryLogConfigs"`
	QueryLogConfigAssociations    map[string]*ResolverQueryLogConfigAssociation `json:"queryLogConfigAssociations"`
	RuleAssociations              map[string]*ResolverRuleAssociation           `json:"ruleAssociations"`
	FirewallConfigs               map[string]*FirewallConfig                    `json:"firewallConfigs"`
	ResolverConfigs               map[string]*ResolverConfig                    `json:"resolverConfigs"`
	ResolverDnssecConfigs         map[string]*ResolverDnssecConfig              `json:"resolverDnssecConfigs"`
	FirewallRuleGroupPolicies     map[string]string                             `json:"firewallRuleGroupPolicies"`
	QueryLogConfigPolicies        map[string]string                             `json:"queryLogConfigPolicies"`
	ResolverRulePolicies          map[string]string                             `json:"resolverRulePolicies"`
	AccountID                     string                                        `json:"accountID"`
	Region                        string                                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Endpoints:                     b.endpoints,
		Rules:                         b.rules,
		Tags:                          b.tags,
		FirewallRuleGroups:            b.firewallRuleGroups,
		FirewallRuleGroupAssociations: b.firewallRuleGroupAssociations,
		FirewallDomainLists:           b.firewallDomainLists,
		FirewallRules:                 b.firewallRules,
		OutpostResolvers:              b.outpostResolvers,
		QueryLogConfigs:               b.queryLogConfigs,
		QueryLogConfigAssociations:    b.queryLogConfigAssociations,
		RuleAssociations:              b.ruleAssociations,
		FirewallConfigs:               b.firewallConfigs,
		ResolverConfigs:               b.resolverConfigs,
		ResolverDnssecConfigs:         b.resolverDnssecConfigs,
		FirewallRuleGroupPolicies:     b.firewallRuleGroupPolicies,
		QueryLogConfigPolicies:        b.queryLogConfigPolicies,
		ResolverRulePolicies:          b.resolverRulePolicies,
		AccountID:                     b.accountID,
		Region:                        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("route53resolver: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureNonNilMaps(&snap)

	b.endpoints = snap.Endpoints
	b.rules = snap.Rules
	b.tags = snap.Tags
	b.firewallRuleGroups = snap.FirewallRuleGroups
	b.firewallRuleGroupAssociations = snap.FirewallRuleGroupAssociations
	b.firewallDomainLists = snap.FirewallDomainLists
	b.firewallRules = snap.FirewallRules
	b.outpostResolvers = snap.OutpostResolvers
	b.queryLogConfigs = snap.QueryLogConfigs
	b.queryLogConfigAssociations = snap.QueryLogConfigAssociations
	b.ruleAssociations = snap.RuleAssociations
	b.firewallConfigs = snap.FirewallConfigs
	b.resolverConfigs = snap.ResolverConfigs
	b.resolverDnssecConfigs = snap.ResolverDnssecConfigs
	b.firewallRuleGroupPolicies = snap.FirewallRuleGroupPolicies
	b.queryLogConfigPolicies = snap.QueryLogConfigPolicies
	b.resolverRulePolicies = snap.ResolverRulePolicies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	ensureNonNilCoreMaps(snap)
	ensureNonNilFirewallMaps(snap)
	ensureNonNilPolicyMaps(snap)
}

func ensureNonNilCoreMaps(snap *backendSnapshot) {
	if snap.Endpoints == nil {
		snap.Endpoints = make(map[string]*ResolverEndpoint)
	}
	if snap.Rules == nil {
		snap.Rules = make(map[string]*ResolverRule)
	}
	if snap.Tags == nil {
		snap.Tags = make(map[string][]svcTags.KV)
	}
	if snap.RuleAssociations == nil {
		snap.RuleAssociations = make(map[string]*ResolverRuleAssociation)
	}
	if snap.QueryLogConfigs == nil {
		snap.QueryLogConfigs = make(map[string]*ResolverQueryLogConfig)
	}
	if snap.QueryLogConfigAssociations == nil {
		snap.QueryLogConfigAssociations = make(map[string]*ResolverQueryLogConfigAssociation)
	}
}

func ensureNonNilFirewallMaps(snap *backendSnapshot) {
	if snap.FirewallRuleGroups == nil {
		snap.FirewallRuleGroups = make(map[string]*FirewallRuleGroup)
	}
	if snap.FirewallRuleGroupAssociations == nil {
		snap.FirewallRuleGroupAssociations = make(map[string]*FirewallRuleGroupAssociation)
	}
	if snap.FirewallDomainLists == nil {
		snap.FirewallDomainLists = make(map[string]*FirewallDomainList)
	}
	if snap.FirewallRules == nil {
		snap.FirewallRules = make(map[string]*FirewallRule)
	}
	if snap.OutpostResolvers == nil {
		snap.OutpostResolvers = make(map[string]*OutpostResolver)
	}
	if snap.FirewallConfigs == nil {
		snap.FirewallConfigs = make(map[string]*FirewallConfig)
	}
	if snap.ResolverConfigs == nil {
		snap.ResolverConfigs = make(map[string]*ResolverConfig)
	}
	if snap.ResolverDnssecConfigs == nil {
		snap.ResolverDnssecConfigs = make(map[string]*ResolverDnssecConfig)
	}
}

func ensureNonNilPolicyMaps(snap *backendSnapshot) {
	if snap.FirewallRuleGroupPolicies == nil {
		snap.FirewallRuleGroupPolicies = make(map[string]string)
	}
	if snap.QueryLogConfigPolicies == nil {
		snap.QueryLogConfigPolicies = make(map[string]string)
	}
	if snap.ResolverRulePolicies == nil {
		snap.ResolverRulePolicies = make(map[string]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
