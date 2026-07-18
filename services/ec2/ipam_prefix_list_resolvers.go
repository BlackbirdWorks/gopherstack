package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// ---- Prefix List Resolvers ----

// CreateIpamPrefixListResolver creates a new IPAM prefix list resolver: a component that
// selects CIDRs from an IPAM's tracked address space according to a set of rules, producing
// numbered "versions" that a Target can synchronize into a managed prefix list.
func (b *InMemoryBackend) CreateIpamPrefixListResolver(
	ipamID, addressFamily, description string, rules []IpamPrefixListResolverRule,
) (*IpamPrefixListResolver, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if addressFamily == "" {
		addressFamily = "ipv4"
	}

	b.mu.Lock("CreateIpamPrefixListResolver")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-prefix-list-resolver-" + uuid.New().String()[:8]
	resolver := &IpamPrefixListResolver{
		IpamPrefixListResolverID: id,
		IpamPrefixListResolverARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-prefix-list-resolver/" + id,
		IpamID:                    ipamID,
		IpamARN:                   ipam.IpamARN,
		IpamRegion:                b.Region,
		OwnerID:                   b.AccountID,
		AddressFamily:             addressFamily,
		Description:               description,
		State:                     ipamStateCreateComplete,
		LastVersionCreationStatus: ipamPrefixListResolverVersionStatusSuccess,
		Rules:                     append([]IpamPrefixListResolverRule(nil), rules...),
		CurrentVersion:            1,
	}
	b.ipamPrefixListResolvers.Put(resolver)
	b.ipamPrefixListResolverVersions[id] = []int64{1}

	return copyIpamPrefixListResolver(resolver), nil
}

// DeleteIpamPrefixListResolver removes a prefix list resolver and its version history.
func (b *InMemoryBackend) DeleteIpamPrefixListResolver(id string) (*IpamPrefixListResolver, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPrefixListResolver")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, id)
	}
	b.ipamPrefixListResolvers.Delete(id)
	delete(b.ipamPrefixListResolverVersions, id)

	for _, t := range b.ipamPrefixListResolverTargets.All() {
		targetID := ipamPrefixListResolverTargetsKeyFn(t)
		if t.IpamPrefixListResolverID == id {
			b.ipamPrefixListResolverTargets.Delete(targetID)
		}
	}

	cp := copyIpamPrefixListResolver(resolver)
	cp.State = ipamStateDeleteComplete

	return cp, nil
}

// DescribeIpamPrefixListResolvers returns prefix list resolvers, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamPrefixListResolvers(ids []string) []*IpamPrefixListResolver {
	b.mu.RLock("DescribeIpamPrefixListResolvers")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPrefixListResolver, 0, b.ipamPrefixListResolvers.Len())

	for _, r := range b.ipamPrefixListResolvers.All() {
		if len(idSet) > 0 && !idSet[r.IpamPrefixListResolverID] {
			continue
		}

		out = append(out, copyIpamPrefixListResolver(r))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPrefixListResolverID < out[j].IpamPrefixListResolverID
	})

	return out
}

// ModifyIpamPrefixListResolver updates a resolver's description and, if rulesProvided is true,
// replaces its CIDR selection rules entirely (creating a new version).
func (b *InMemoryBackend) ModifyIpamPrefixListResolver(
	id, description string, rules []IpamPrefixListResolverRule, rulesProvided bool,
) (*IpamPrefixListResolver, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPrefixListResolver")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, id)
	}

	if description != "" {
		resolver.Description = description
	}

	if rulesProvided {
		resolver.Rules = append([]IpamPrefixListResolverRule(nil), rules...)
		resolver.CurrentVersion++
		b.ipamPrefixListResolverVersions[id] = append(b.ipamPrefixListResolverVersions[id], resolver.CurrentVersion)
	}

	resolver.State = ipamStateModifyComplete
	resolver.LastVersionCreationStatus = ipamPrefixListResolverVersionStatusSuccess

	return copyIpamPrefixListResolver(resolver), nil
}

// copyIpamPrefixListResolver returns a deep copy of a resolver so callers cannot mutate
// backend state (in particular its Rules slice) through the returned pointer.
func copyIpamPrefixListResolver(r *IpamPrefixListResolver) *IpamPrefixListResolver {
	cp := *r
	cp.Rules = append([]IpamPrefixListResolverRule(nil), r.Rules...)

	for i, rule := range cp.Rules {
		rule.Conditions = append([]IpamPrefixListResolverRuleCondition(nil), rule.Conditions...)
		cp.Rules[i] = rule
	}

	return &cp
}

// GetIpamPrefixListResolverRules returns the current CIDR selection rules of a resolver.
func (b *InMemoryBackend) GetIpamPrefixListResolverRules(resolverID string) ([]IpamPrefixListResolverRule, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverRules")
	defer b.mu.RUnlock()

	resolver, ok := b.ipamPrefixListResolvers.Get(resolverID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	return copyIpamPrefixListResolver(resolver).Rules, nil
}

// GetIpamPrefixListResolverVersions returns the version numbers recorded for a resolver: one
// at creation, plus one for every Modify call that replaced its rules.
func (b *InMemoryBackend) GetIpamPrefixListResolverVersions(resolverID string) ([]int64, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverVersions")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPrefixListResolvers.Get(resolverID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	versions := b.ipamPrefixListResolverVersions[resolverID]
	out := append([]int64(nil), versions...)
	slices.Sort(out)

	return out, nil
}

// GetIpamPrefixListResolverVersionEntries returns the CIDR entries selected by a resolver at a
// given version. This mock does not implement the live CIDR selection pipeline (matching
// resources/rules against real infrastructure), so it always returns an empty (but correctly
// validated and shaped) entry list for any version that was actually created.
func (b *InMemoryBackend) GetIpamPrefixListResolverVersionEntries(resolverID string, version int64) ([]string, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverVersionEntries")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPrefixListResolvers.Get(resolverID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	if slices.Contains(b.ipamPrefixListResolverVersions[resolverID], version) {
		return []string{}, nil
	}

	return nil, fmt.Errorf(
		"%w: version %d of %s does not exist", ErrIpamPrefixListResolverVersionNotFound, version, resolverID,
	)
}

// ---- Prefix List Resolver Targets ----

// CreateIpamPrefixListResolverTarget associates a managed prefix list with a resolver, making
// it an "IPAM managed prefix list" whose CIDRs are kept in sync with the resolver's rules.
func (b *InMemoryBackend) CreateIpamPrefixListResolverTarget(
	resolverID, prefixListID, prefixListRegion string, trackLatestVersion bool, desiredVersion *int64,
) (*IpamPrefixListResolverTarget, error) {
	if resolverID == "" || prefixListID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId and PrefixListId are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers.Get(resolverID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	if prefixListRegion == "" {
		prefixListRegion = b.Region
	}

	id := "ipam-prefix-list-resolver-target-" + uuid.New().String()[:8]
	target := &IpamPrefixListResolverTarget{
		IpamPrefixListResolverTargetID: id,
		IpamPrefixListResolverTargetARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-prefix-list-resolver-target/" + id,
		IpamPrefixListResolverID: resolverID,
		OwnerID:                  b.AccountID,
		PrefixListID:             prefixListID,
		PrefixListRegion:         prefixListRegion,
		TrackLatestVersion:       trackLatestVersion,
		DesiredVersion:           desiredVersion,
		State:                    ipamStateCreateComplete,
	}

	synced := resolver.CurrentVersion
	target.LastSyncedVersion = &synced
	b.ipamPrefixListResolverTargets.Put(target)

	return copyIpamPrefixListResolverTarget(target), nil
}

// DeleteIpamPrefixListResolverTarget removes a resolver target.
func (b *InMemoryBackend) DeleteIpamPrefixListResolverTarget(id string) (*IpamPrefixListResolverTarget, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverTargetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	target, ok := b.ipamPrefixListResolverTargets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverTargetNotFound, id)
	}
	b.ipamPrefixListResolverTargets.Delete(id)

	cp := copyIpamPrefixListResolverTarget(target)
	cp.State = ipamStateDeleteComplete

	return cp, nil
}

// DescribeIpamPrefixListResolverTargets returns resolver targets, optionally filtered by
// resolver ID and/or target IDs.
func (b *InMemoryBackend) DescribeIpamPrefixListResolverTargets(
	resolverID string, ids []string,
) []*IpamPrefixListResolverTarget {
	b.mu.RLock("DescribeIpamPrefixListResolverTargets")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPrefixListResolverTarget, 0, b.ipamPrefixListResolverTargets.Len())

	for _, t := range b.ipamPrefixListResolverTargets.All() {
		if resolverID != "" && t.IpamPrefixListResolverID != resolverID {
			continue
		}

		if len(idSet) > 0 && !idSet[t.IpamPrefixListResolverTargetID] {
			continue
		}

		out = append(out, copyIpamPrefixListResolverTarget(t))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPrefixListResolverTargetID < out[j].IpamPrefixListResolverTargetID
	})

	return out
}

// ModifyIpamPrefixListResolverTarget updates a target's desired version and/or
// track-latest-version flag. Nil pointers leave the corresponding field unchanged.
func (b *InMemoryBackend) ModifyIpamPrefixListResolverTarget(
	id string, desiredVersion *int64, trackLatestVersion *bool,
) (*IpamPrefixListResolverTarget, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverTargetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	target, ok := b.ipamPrefixListResolverTargets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverTargetNotFound, id)
	}

	if desiredVersion != nil {
		v := *desiredVersion
		target.DesiredVersion = &v
	}

	if trackLatestVersion != nil {
		target.TrackLatestVersion = *trackLatestVersion
	}

	target.State = ipamStateModifyComplete

	return copyIpamPrefixListResolverTarget(target), nil
}

// copyIpamPrefixListResolverTarget returns a deep copy of a target so callers cannot mutate
// backend state through the returned pointer's DesiredVersion/LastSyncedVersion pointers.
func copyIpamPrefixListResolverTarget(t *IpamPrefixListResolverTarget) *IpamPrefixListResolverTarget {
	cp := *t

	if t.DesiredVersion != nil {
		v := *t.DesiredVersion
		cp.DesiredVersion = &v
	}

	if t.LastSyncedVersion != nil {
		v := *t.LastSyncedVersion
		cp.LastSyncedVersion = &v
	}

	return &cp
}
