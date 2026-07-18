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

	// Validate BLOCK+OVERRIDE requires BlockOverrideDomain and BlockOverrideDNSType.
	if p.Action == firewallActionBlock && p.BlockResponse == blockResponseOVERRIDE {
		if p.BlockOverrideDomain == "" {
			return nil, fmt.Errorf(
				"%w: BlockOverrideDomain is required when BlockResponse is OVERRIDE",
				ErrValidation,
			)
		}
		if p.BlockOverrideDNSType == "" {
			return nil, fmt.Errorf(
				"%w: BlockOverrideDNSType is required when BlockResponse is OVERRIDE",
				ErrValidation,
			)
		}
	}

	// Auto-assign priority if not provided.
	if p.Priority == 0 {
		maxPriority := int32(0)
		for _, existing := range regionRules {
			if existing.FirewallRuleGroupID == p.FirewallRuleGroupID &&
				existing.Priority > maxPriority {
				maxPriority = existing.Priority
			}
		}
		p.Priority = maxPriority + firewallPriorityAutoIncrement
	}

	// Validate priority uniqueness within the rule group.
	for _, existing := range regionRules {
		if existing.FirewallRuleGroupID == p.FirewallRuleGroupID &&
			existing.Priority == p.Priority {
			return nil, fmt.Errorf(
				"%w: a firewall rule with priority %d already exists in group %s",
				ErrValidation,
				p.Priority,
				p.FirewallRuleGroupID,
			)
		}
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

// DeleteFirewallRule deletes a firewall rule by ID and decrements the group rule count.
func (b *InMemoryBackend) DeleteFirewallRule(ctx context.Context, id string) (*FirewallRule, error) {
	b.mu.Lock("DeleteFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rule, ok := b.firewallRules.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule %s not found", ErrNotFound, id)
	}
	cp := *rule
	grp, exists := b.firewallRuleGroups.Get(regionalKey(region, rule.FirewallRuleGroupID))
	if exists && grp.RuleCount > 0 {
		grp.RuleCount--
	}
	b.firewallRules.Delete(regionalKey(region, id))

	return &cp, nil
}

// UpdateFirewallRuleParams holds all updatable fields for a firewall rule.
type UpdateFirewallRuleParams struct {
	ID                   string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDNSType string
	Qtype                string
	ConfidenceThreshold  string
	FirewallDomainListID string
	BlockOverrideTTL     int32
	Priority             int32
}

// UpdateFirewallRule updates an existing firewall rule.
func (b *InMemoryBackend) UpdateFirewallRule(ctx context.Context, p UpdateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("UpdateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rule, ok := b.firewallRules.Get(regionalKey(region, p.ID))
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule %s not found", ErrNotFound, p.ID)
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
	if p.FirewallDomainListID != "" {
		rule.FirewallDomainListID = p.FirewallDomainListID
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
