package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateFirewallRuleParams holds all parameters for creating a firewall rule.
type CreateFirewallRuleParams struct {
	FirewallRuleGroupID  string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDNSType string
	Qtype                string
	ConfidenceThreshold  string
	CreatorRequestID     string
	FirewallDomainListID string
	BlockOverrideTTL     int32
	Priority             int32
}

// validateFirewallRuleBlockOverride enforces that BLOCK+OVERRIDE rules supply
// both BlockOverrideDomain and BlockOverrideDNSType.
func validateFirewallRuleBlockOverride(p CreateFirewallRuleParams) error {
	if p.Action != firewallActionBlock || p.BlockResponse != blockResponseOVERRIDE {
		return nil
	}
	if p.BlockOverrideDomain == "" {
		return fmt.Errorf(
			"%w: BlockOverrideDomain is required when BlockResponse is OVERRIDE",
			ErrValidation,
		)
	}
	if p.BlockOverrideDNSType == "" {
		return fmt.Errorf(
			"%w: BlockOverrideDNSType is required when BlockResponse is OVERRIDE",
			ErrValidation,
		)
	}

	return nil
}

// resolveFirewallRulePriority auto-assigns a priority when the caller didn't
// supply one, then validates that the (possibly auto-assigned) priority is
// unique within the rule group.
func resolveFirewallRulePriority(regionRules []*FirewallRule, p CreateFirewallRuleParams) (int32, error) {
	priority := p.Priority
	if priority == 0 {
		maxPriority := int32(0)
		for _, existing := range regionRules {
			if existing.FirewallRuleGroupID == p.FirewallRuleGroupID && existing.Priority > maxPriority {
				maxPriority = existing.Priority
			}
		}
		priority = maxPriority + firewallPriorityAutoIncrement
	}

	for _, existing := range regionRules {
		if existing.FirewallRuleGroupID == p.FirewallRuleGroupID && existing.Priority == priority {
			return 0, fmt.Errorf(
				"%w: a firewall rule with priority %d already exists in group %s",
				ErrValidation,
				priority,
				p.FirewallRuleGroupID,
			)
		}
	}

	return priority, nil
}

// validateFirewallRuleDomainListUnique enforces that a rule is identified on
// the wire by the (FirewallRuleGroupId, FirewallDomainListId) pair -- real
// AWS has no independent rule ID (verified against types.FirewallRule and
// api_op_{Update,Delete,List}FirewallRule.go, none of which have an
// Id/Arn/FirewallRuleId member). Enforcing uniqueness here means Update/Delete
// can always resolve a single rule by that pair.
func validateFirewallRuleDomainListUnique(regionRules []*FirewallRule, p CreateFirewallRuleParams) error {
	if p.FirewallDomainListID == "" {
		return nil
	}
	for _, existing := range regionRules {
		if existing.FirewallRuleGroupID == p.FirewallRuleGroupID &&
			existing.FirewallDomainListID == p.FirewallDomainListID {
			return fmt.Errorf(
				"%w: a firewall rule for domain list %s already exists in group %s",
				ErrAlreadyExists,
				p.FirewallDomainListID,
				p.FirewallRuleGroupID,
			)
		}
	}

	return nil
}

// CreateFirewallRule creates a new rule in a DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRule(ctx context.Context, p CreateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("CreateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	group, ok := b.firewallRuleGroups.Get(regionalKey(region, p.FirewallRuleGroupID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule group %s not found",
			ErrNotFound,
			p.FirewallRuleGroupID,
		)
	}
	regionRules := b.firewallRulesByRegion.Get(region)

	if err := validateFirewallRuleBlockOverride(p); err != nil {
		return nil, err
	}

	priority, err := resolveFirewallRulePriority(regionRules, p)
	if err != nil {
		return nil, err
	}
	p.Priority = priority

	if err = validateFirewallRuleDomainListUnique(regionRules, p); err != nil {
		return nil, err
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                   id,
		ARN:                  ruleARN,
		Name:                 p.Name,
		FirewallRuleGroupID:  p.FirewallRuleGroupID,
		FirewallDomainListID: p.FirewallDomainListID,
		Action:               p.Action,
		BlockResponse:        p.BlockResponse,
		BlockOverrideDomain:  p.BlockOverrideDomain,
		BlockOverrideDNSType: p.BlockOverrideDNSType,
		BlockOverrideTTL:     p.BlockOverrideTTL,
		Qtype:                p.Qtype,
		ConfidenceThreshold:  p.ConfidenceThreshold,
		CreatorRequestID:     p.CreatorRequestID,
		CreationTime:         now,
		ModificationTime:     now,
		Priority:             p.Priority,
		Region:               region,
	}
	b.firewallRules.Put(rule)

	// Increment rule count on the group.
	group.RuleCount++

	cp := *rule

	return &cp, nil
}

// AddFirewallRuleInternal adds a firewall rule directly to the backend (demo seed helper).
func (b *InMemoryBackend) AddFirewallRuleInternal(
	groupID, name, action, domainListID string,
	priority int32,
) *FirewallRule {
	b.mu.Lock("AddFirewallRuleInternal")
	defer b.mu.Unlock()

	grp, ok := b.firewallRuleGroups.Get(regionalKey(b.region, groupID))
	if !ok {
		return nil
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                   id,
		ARN:                  ruleARN,
		Name:                 name,
		FirewallRuleGroupID:  groupID,
		FirewallDomainListID: domainListID,
		Action:               action,
		Priority:             priority,
		CreationTime:         now,
		ModificationTime:     now,
		Region:               b.region,
	}
	b.firewallRules.Put(rule)
	grp.RuleCount++
	cp := *rule

	return &cp
}

// --- Firewall Rule operations ---

// findFirewallRule locates a rule by its real-AWS identifying pair --
// (FirewallRuleGroupId, FirewallDomainListId). Real AWS FirewallRule values
// have no independent Id/Arn (verified against types.FirewallRule); the
// caller-supplied internal store key (see CreateFirewallRule) is never part
// of the wire contract.
func (b *InMemoryBackend) findFirewallRule(
	region, firewallRuleGroupID, firewallDomainListID string,
) (*FirewallRule, bool) {
	for _, r := range b.firewallRulesByRegion.Get(region) {
		if r.FirewallRuleGroupID == firewallRuleGroupID && r.FirewallDomainListID == firewallDomainListID {
			return r, true
		}
	}

	return nil, false
}

// DeleteFirewallRule deletes a firewall rule identified by
// (firewallRuleGroupID, firewallDomainListID) and decrements the group rule count.
func (b *InMemoryBackend) DeleteFirewallRule(
	ctx context.Context,
	firewallRuleGroupID, firewallDomainListID string,
) (*FirewallRule, error) {
	b.mu.Lock("DeleteFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rule, ok := b.findFirewallRule(region, firewallRuleGroupID, firewallDomainListID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule for domain list %s in group %s not found",
			ErrNotFound,
			firewallDomainListID,
			firewallRuleGroupID,
		)
	}
	cp := *rule
	grp, exists := b.firewallRuleGroups.Get(regionalKey(region, rule.FirewallRuleGroupID))
	if exists && grp.RuleCount > 0 {
		grp.RuleCount--
	}
	b.firewallRules.Delete(regionalKey(region, rule.ID))

	return &cp, nil
}

// UpdateFirewallRuleParams holds all updatable fields for a firewall rule.
// FirewallRuleGroupID+FirewallDomainListID identify which rule to update --
// per the real API, the domain list a rule targets is part of its identity,
// not a mutable property (verified against api_op_UpdateFirewallRule.go).
type UpdateFirewallRuleParams struct {
	FirewallRuleGroupID  string
	FirewallDomainListID string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDNSType string
	Qtype                string
	ConfidenceThreshold  string
	BlockOverrideTTL     int32
	Priority             int32
}

// UpdateFirewallRule updates an existing firewall rule.
func (b *InMemoryBackend) UpdateFirewallRule(ctx context.Context, p UpdateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("UpdateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rule, ok := b.findFirewallRule(region, p.FirewallRuleGroupID, p.FirewallDomainListID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule for domain list %s in group %s not found",
			ErrNotFound,
			p.FirewallDomainListID,
			p.FirewallRuleGroupID,
		)
	}
	if p.Name != "" {
		rule.Name = p.Name
	}
	if p.Action != "" {
		rule.Action = p.Action
	}
	if p.BlockResponse != "" {
		rule.BlockResponse = p.BlockResponse
	}
	if p.BlockOverrideDomain != "" {
		rule.BlockOverrideDomain = p.BlockOverrideDomain
	}
	if p.BlockOverrideDNSType != "" {
		rule.BlockOverrideDNSType = p.BlockOverrideDNSType
	}
	if p.BlockOverrideTTL != 0 {
		rule.BlockOverrideTTL = p.BlockOverrideTTL
	}
	if p.Qtype != "" {
		rule.Qtype = p.Qtype
	}
	if p.ConfidenceThreshold != "" {
		rule.ConfidenceThreshold = p.ConfidenceThreshold
	}
	if p.Priority != 0 {
		rule.Priority = p.Priority
	}
	rule.ModificationTime = currentTime()
	cp := *rule

	return &cp, nil
}

// ListFirewallRules lists firewall rules, optionally filtered by rule group ID.
func (b *InMemoryBackend) ListFirewallRules(ctx context.Context, firewallRuleGroupID string) []*FirewallRule {
	b.mu.RLock("ListFirewallRules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionRules := b.firewallRulesByRegion.Get(region)
	list := make([]*FirewallRule, 0, len(regionRules))
	for _, r := range regionRules {
		if firewallRuleGroupID != "" && r.FirewallRuleGroupID != firewallRuleGroupID {
			continue
		}
		cp := *r
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Priority < list[j].Priority })

	return list
}

// --- Firewall Rule Group operations ---
