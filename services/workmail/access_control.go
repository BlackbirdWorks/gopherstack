package workmail

import (
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"time"
)

// --- Access Control Rules ---

// PutAccessControlRule creates or updates an access control rule.
func (b *InMemoryBackend) PutAccessControlRule(
	orgID, name, effect, description string,
	ipRanges, notIPRanges []string,
	actions, notActions []string,
	userIDs, notUserIDs []string,
) (*AccessControlRule, error) {
	b.mu.Lock("PutAccessControlRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	now := time.Now().UTC()
	existing, _ := b.accessRules.Get(orgKey(orgID, name))

	rule := &AccessControlRule{
		DateCreated:  now,
		DateModified: now,
		Name:         name,
		Effect:       effect,
		Description:  description,
		IPRanges:     ipRanges,
		NotIPRanges:  notIPRanges,
		Actions:      actions,
		NotActions:   notActions,
		UserIDs:      userIDs,
		NotUserIDs:   notUserIDs,
		orgID:        orgID,
	}
	if existing != nil {
		rule.DateCreated = existing.DateCreated
	}

	b.accessRules.Put(rule)

	return rule, nil
}

// DeleteAccessControlRule removes an access control rule.
func (b *InMemoryBackend) DeleteAccessControlRule(orgID, name string) error {
	b.mu.Lock("DeleteAccessControlRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.accessRules.Delete(orgKey(orgID, name)) {
		return fmt.Errorf("%w: access control rule %q not found", ErrNotFound, name)
	}

	return nil
}

// GetAccessControlEffect evaluates access control rules.
func (b *InMemoryBackend) GetAccessControlEffect(
	orgID, ipAddr, action, userID string,
) (string, []string, error) {
	b.mu.RLock("GetAccessControlEffect")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	byOrg := b.accessRulesByOrg.Get(orgID)
	rules := make([]*AccessControlRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
	// AWS evaluates rules in creation order; sort by DateCreated for determinism
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].DateCreated.Before(rules[j].DateCreated)
	})

	for _, rule := range rules {
		if !ruleMatchesRequest(rule, ipAddr, action, userID) {
			continue
		}

		return rule.Effect, []string{rule.Name}, nil
	}

	return effectAllow, []string{}, nil
}

// ruleMatchesRequest returns true when ALL non-empty condition lists match.
func ruleMatchesRequest(rule *AccessControlRule, ipAddr, action, userID string) bool {
	if len(rule.IPRanges) > 0 && !matchesCIDRList(ipAddr, rule.IPRanges) {
		return false
	}
	if len(rule.NotIPRanges) > 0 && matchesCIDRList(ipAddr, rule.NotIPRanges) {
		return false
	}
	if len(rule.Actions) > 0 && !slices.Contains(rule.Actions, action) {
		return false
	}
	if len(rule.NotActions) > 0 && slices.Contains(rule.NotActions, action) {
		return false
	}
	if len(rule.UserIDs) > 0 && !slices.Contains(rule.UserIDs, userID) {
		return false
	}
	if len(rule.NotUserIDs) > 0 && slices.Contains(rule.NotUserIDs, userID) {
		return false
	}

	return true
}

func matchesCIDRList(ipAddr string, cidrs []string) bool {
	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return false
	}
	for _, cidr := range cidrs {
		if !strings.Contains(cidr, "/") {
			cidr += "/32"
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}

	return false
}

// ListAccessControlRules returns all access control rules.
func (b *InMemoryBackend) ListAccessControlRules(orgID string) ([]*AccessControlRule, error) {
	b.mu.RLock("ListAccessControlRules")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	byOrg := b.accessRulesByOrg.Get(orgID)
	rules := make([]*AccessControlRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })

	return rules, nil
}
