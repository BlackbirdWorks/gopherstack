package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateFirewallRuleParams holds all parameters for creating a firewall rule.
//
// DnsThreatProtection and FirewallDomainListID are mutually exclusive match
// sources (verified against CreateFirewallRuleInput's doc comment in
// aws-sdk-go-v2/service/route53resolver@v1.48.4: "they are mutually
// exclusive"); a rule created via DnsThreatProtection has no domain list and
// is identified on the wire by a system-generated FirewallThreatProtectionID
// instead (see FirewallRule.FirewallThreatProtectionID). The FirewallRuleType
// tagged union (FirewallAdvancedContentCategory/FirewallAdvancedThreatCategory/
// PartnerThreatProtection) is intentionally not modeled -- see PARITY.md.
type CreateFirewallRuleParams struct {
	FirewallRuleGroupID             string
	Name                            string
	Action                          string
	BlockResponse                   string
	BlockOverrideDomain             string
	BlockOverrideDNSType            string
	Qtype                           string
	ConfidenceThreshold             string
	CreatorRequestID                string
	FirewallDomainListID            string
	DNSThreatProtection             string
	FirewallDomainRedirectionAction string
	BlockOverrideTTL                int32
	Priority                        int32
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

// validateFirewallRuleMatchSource enforces that FirewallDomainListID and
// DnsThreatProtection -- the two match sources this backend models -- are
// mutually exclusive, and validates DnsThreatProtection against its closed
// enum (DGA/DNS_TUNNELING/DICTIONARY_DGA) when supplied.
func validateFirewallRuleMatchSource(p CreateFirewallRuleParams) error {
	if p.FirewallDomainListID != "" && p.DNSThreatProtection != "" {
		return fmt.Errorf(
			"%w: FirewallDomainListId and DnsThreatProtection are mutually exclusive",
			ErrValidation,
		)
	}

	if p.DNSThreatProtection == "" {
		return nil
	}

	switch p.DNSThreatProtection {
	case dnsThreatProtectionDGA, dnsThreatProtectionDNSTunneling, dnsThreatProtectionDictionaryDGA:
		return nil
	default:
		return fmt.Errorf(
			"%w: DnsThreatProtection must be %s, %s, or %s",
			ErrValidation,
			dnsThreatProtectionDGA,
			dnsThreatProtectionDNSTunneling,
			dnsThreatProtectionDictionaryDGA,
		)
	}
}

// validateFirewallDomainRedirectionAction validates
// FirewallDomainRedirectionAction against its closed enum
// (INSPECT_REDIRECTION_DOMAIN/TRUST_REDIRECTION_DOMAIN) when supplied; an
// empty value is allowed (the caller applies the real default).
func validateFirewallDomainRedirectionAction(value string) error {
	switch value {
	case "", firewallDomainRedirectionInspect, firewallDomainRedirectionTrust:
		return nil
	default:
		return fmt.Errorf(
			"%w: FirewallDomainRedirectionAction must be %s or %s",
			ErrValidation,
			firewallDomainRedirectionInspect,
			firewallDomainRedirectionTrust,
		)
	}
}

// validateConfidenceThreshold validates ConfidenceThreshold against its
// closed enum (LOW/MEDIUM/HIGH -- types.ConfidenceThreshold in
// aws-sdk-go-v2/service/route53resolver@v1.48.4) whenever a value is
// supplied; an empty value is allowed here (see
// validateFirewallRuleConfidenceThreshold for the create-time
// required-when-DnsThreatProtection check).
func validateConfidenceThreshold(value string) error {
	switch value {
	case "", confidenceThresholdLow, confidenceThresholdMedium, confidenceThresholdHigh:
		return nil
	default:
		return fmt.Errorf(
			"%w: ConfidenceThreshold must be %s, %s, or %s",
			ErrValidation,
			confidenceThresholdLow,
			confidenceThresholdMedium,
			confidenceThresholdHigh,
		)
	}
}

// validateFirewallRuleConfidenceThreshold enforces that ConfidenceThreshold
// is supplied when creating a DnsThreatProtection rule -- verified against
// CreateFirewallRuleInput's doc comment ("The confidence threshold for DNS
// Firewall Advanced. You must provide this value when you create a DNS
// Firewall Advanced rule."). UpdateFirewallRuleInput carries the identical
// field but its doc comment drops that sentence, i.e. it is not re-required
// on update -- see validateConfidenceThreshold, called directly there.
func validateFirewallRuleConfidenceThreshold(p CreateFirewallRuleParams) error {
	if p.DNSThreatProtection != "" && p.ConfidenceThreshold == "" {
		return fmt.Errorf(
			"%w: ConfidenceThreshold is required when DnsThreatProtection is set",
			ErrValidation,
		)
	}

	return validateConfidenceThreshold(p.ConfidenceThreshold)
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

	if err := validateFirewallRuleMatchSource(p); err != nil {
		return nil, err
	}

	if err := validateFirewallRuleConfidenceThreshold(p); err != nil {
		return nil, err
	}

	if err := validateFirewallDomainRedirectionAction(p.FirewallDomainRedirectionAction); err != nil {
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

	// FirewallDomainRedirectionAction only applies to domain-list rules (it
	// governs redirection-chain inspection within the rule's own domain
	// list); default it to the real API's documented default in that case
	// only, leaving it empty for DnsThreatProtection rules.
	redirectionAction := p.FirewallDomainRedirectionAction
	if p.FirewallDomainListID != "" && redirectionAction == "" {
		redirectionAction = firewallDomainRedirectionInspect
	}

	// A DnsThreatProtection rule has no domain list, so it's identified by a
	// system-generated FirewallThreatProtectionId instead (see
	// CreateFirewallRuleParams's doc comment).
	var threatProtectionID string
	if p.DNSThreatProtection != "" {
		threatProtectionID = "rslvr-ftp-" + uuid.New().String()[:8]
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                              id,
		ARN:                             ruleARN,
		Name:                            p.Name,
		FirewallRuleGroupID:             p.FirewallRuleGroupID,
		FirewallDomainListID:            p.FirewallDomainListID,
		DNSThreatProtection:             p.DNSThreatProtection,
		FirewallThreatProtectionID:      threatProtectionID,
		FirewallDomainRedirectionAction: redirectionAction,
		Action:                          p.Action,
		BlockResponse:                   p.BlockResponse,
		BlockOverrideDomain:             p.BlockOverrideDomain,
		BlockOverrideDNSType:            p.BlockOverrideDNSType,
		BlockOverrideTTL:                p.BlockOverrideTTL,
		Qtype:                           p.Qtype,
		ConfidenceThreshold:             p.ConfidenceThreshold,
		CreatorRequestID:                p.CreatorRequestID,
		CreationTime:                    now,
		ModificationTime:                now,
		Priority:                        p.Priority,
		Region:                          region,
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

// findFirewallRuleByThreatProtectionID locates a rule by
// (FirewallRuleGroupId, FirewallThreatProtectionId) -- the DNS Firewall
// Advanced counterpart to findFirewallRule's (FirewallRuleGroupId,
// FirewallDomainListId) pair, used to identify a rule created via
// DnsThreatProtection (which has no domain list). Verified against
// api_op_{Update,Delete}FirewallRule.go: "Identify the rule using either
// FirewallDomainListId ... or FirewallThreatProtectionId ... together with
// FirewallRuleGroupId".
func (b *InMemoryBackend) findFirewallRuleByThreatProtectionID(
	region, firewallRuleGroupID, firewallThreatProtectionID string,
) (*FirewallRule, bool) {
	for _, r := range b.firewallRulesByRegion.Get(region) {
		if r.FirewallRuleGroupID == firewallRuleGroupID &&
			r.FirewallThreatProtectionID == firewallThreatProtectionID {
			return r, true
		}
	}

	return nil, false
}

// resolveFirewallRuleIdentity locates a rule using whichever identifier the
// caller supplied -- FirewallDomainListID for domain-list rules or
// FirewallThreatProtectionID for DnsThreatProtection rules (mutually
// exclusive identity paths; see findFirewallRule/
// findFirewallRuleByThreatProtectionID). Shared by DeleteFirewallRule and
// UpdateFirewallRule, the two ops whose real inputs accept either ID.
func (b *InMemoryBackend) resolveFirewallRuleIdentity(
	region, firewallRuleGroupID, firewallDomainListID, firewallThreatProtectionID string,
) (*FirewallRule, bool, error) {
	switch {
	case firewallDomainListID != "":
		rule, ok := b.findFirewallRule(region, firewallRuleGroupID, firewallDomainListID)

		return rule, ok, nil
	case firewallThreatProtectionID != "":
		rule, ok := b.findFirewallRuleByThreatProtectionID(region, firewallRuleGroupID, firewallThreatProtectionID)

		return rule, ok, nil
	default:
		return nil, false, fmt.Errorf(
			"%w: one of FirewallDomainListId or FirewallThreatProtectionId is required",
			ErrValidation,
		)
	}
}

// DeleteFirewallRule deletes a firewall rule identified by
// FirewallRuleGroupID plus EITHER firewallDomainListID (domain-list rules)
// OR firewallThreatProtectionID (DnsThreatProtection rules) -- see
// resolveFirewallRuleIdentity.
func (b *InMemoryBackend) DeleteFirewallRule(
	ctx context.Context,
	firewallRuleGroupID, firewallDomainListID, firewallThreatProtectionID string,
) (*FirewallRule, error) {
	b.mu.Lock("DeleteFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	rule, ok, err := b.resolveFirewallRuleIdentity(
		region, firewallRuleGroupID, firewallDomainListID, firewallThreatProtectionID,
	)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule for domain list %q / threat protection %q in group %s not found",
			ErrNotFound,
			firewallDomainListID,
			firewallThreatProtectionID,
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
// FirewallRuleGroupID plus EITHER FirewallDomainListID (domain-list rules)
// OR FirewallThreatProtectionID (DnsThreatProtection rules) identify which
// rule to update -- per the real API, a rule's match source is part of its
// identity, not a mutable property (verified against
// api_op_UpdateFirewallRule.go). FirewallDomainRedirectionAction IS a
// genuinely mutable property of a domain-list rule and is applied like any
// other updatable field below.
type UpdateFirewallRuleParams struct {
	FirewallRuleGroupID             string
	FirewallDomainListID            string
	FirewallThreatProtectionID      string
	Name                            string
	Action                          string
	BlockResponse                   string
	BlockOverrideDomain             string
	BlockOverrideDNSType            string
	Qtype                           string
	ConfidenceThreshold             string
	FirewallDomainRedirectionAction string
	BlockOverrideTTL                int32
	Priority                        int32
}

// UpdateFirewallRule updates an existing firewall rule.
func (b *InMemoryBackend) UpdateFirewallRule(ctx context.Context, p UpdateFirewallRuleParams) (*FirewallRule, error) {
	if err := validateFirewallDomainRedirectionAction(p.FirewallDomainRedirectionAction); err != nil {
		return nil, err
	}

	if err := validateConfidenceThreshold(p.ConfidenceThreshold); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	rule, ok, err := b.resolveFirewallRuleIdentity(
		region, p.FirewallRuleGroupID, p.FirewallDomainListID, p.FirewallThreatProtectionID,
	)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule for domain list %q / threat protection %q in group %s not found",
			ErrNotFound,
			p.FirewallDomainListID,
			p.FirewallThreatProtectionID,
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
	if p.FirewallDomainRedirectionAction != "" {
		rule.FirewallDomainRedirectionAction = p.FirewallDomainRedirectionAction
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
