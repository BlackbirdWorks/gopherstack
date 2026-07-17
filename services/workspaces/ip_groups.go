package workspaces

// CreateIpGroup creates a new IP group and returns its ID.
func (b *InMemoryBackend) CreateIpGroup( //nolint:revive,staticcheck // existing issue.
	groupName, groupDesc string,
	userRules []ipRuleItem,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateIpGroup")
	defer b.mu.Unlock()

	id := b.nextID("wsipg-")
	rules := make([]ipRuleItem, len(userRules))
	copy(rules, userRules)

	b.ipGroups.Put(&storedIpGroup{
		GroupID:   id,
		GroupName: groupName,
		GroupDesc: groupDesc,
		UserRules: rules,
		Tags:      cloneTags(tags),
	})

	return id, nil
}

// DescribeIpGroups returns IP groups, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpGroups( //nolint:revive,staticcheck // existing issue.
	groupIDs []string, _ int32, _ string,
) ([]*storedIpGroup, string, error) {
	b.mu.RLock("DescribeIpGroups")
	defer b.mu.RUnlock()

	filter := buildFilter(groupIDs)
	var result []*storedIpGroup

	for _, g := range b.ipGroups.All() {
		if !matchesFilter(filter, g.GroupID) {
			continue
		}

		cp := *g
		cp.UserRules = make([]ipRuleItem, len(g.UserRules))
		copy(cp.UserRules, g.UserRules)
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedIpGroup{}
	}

	return result, "", nil
}

// DeleteIPGroup removes an IP group by ID.
func (b *InMemoryBackend) DeleteIPGroup(
	groupID string,
) error {
	b.mu.Lock("DeleteIpGroup")
	defer b.mu.Unlock()

	if !b.ipGroups.Has(groupID) {
		return errIpGroupNotFound
	}

	b.ipGroups.Delete(groupID)

	return nil
}

// AuthorizeIpRules appends rules to an IP group.
func (b *InMemoryBackend) AuthorizeIpRules( //nolint:revive,staticcheck // existing issue.
	groupID string,
	rules []ipRuleItem,
) error {
	b.mu.Lock("AuthorizeIpRules")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	g.UserRules = append(g.UserRules, rules...)

	return nil
}

// RevokeIpRules removes rules (by IpRule string) from an IP group.
func (b *InMemoryBackend) RevokeIpRules( //nolint:revive,staticcheck // existing issue.
	groupID string,
	ipRules []string,
) error {
	b.mu.Lock("RevokeIpRules")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	revoke := buildFilter(ipRules)
	kept := g.UserRules[:0]

	for _, r := range g.UserRules {
		if !matchesFilter(revoke, r.IpRule) {
			kept = append(kept, r)
		}
	}

	g.UserRules = kept

	return nil
}

// UpdateRulesOfIpGroup replaces all rules in an IP group.
func (b *InMemoryBackend) UpdateRulesOfIpGroup( //nolint:revive,staticcheck // existing issue.
	groupID string,
	rules []ipRuleItem,
) error {
	b.mu.Lock("UpdateRulesOfIpGroup")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	cp := make([]ipRuleItem, len(rules))
	copy(cp, rules)
	g.UserRules = cp

	return nil
}

// AssociateIpGroups associates IP groups with a directory.
func (b *InMemoryBackend) AssociateIpGroups( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	groupIDs []string,
) error {
	b.mu.Lock("AssociateIpGroups")
	defer b.mu.Unlock()

	if b.directoryIpGroups[directoryID] == nil {
		b.directoryIpGroups[directoryID] = make(map[string]struct{})
	}

	for _, gid := range groupIDs {
		b.directoryIpGroups[directoryID][gid] = struct{}{}
	}

	return nil
}

// DisassociateIpGroups removes IP group associations from a directory.
func (b *InMemoryBackend) DisassociateIpGroups( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	groupIDs []string,
) error {
	b.mu.Lock("DisassociateIpGroups")
	defer b.mu.Unlock()

	for _, gid := range groupIDs {
		delete(b.directoryIpGroups[directoryID], gid)
	}

	return nil
}
