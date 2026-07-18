package wafv2

import (
	"context"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func fillVersionFromRaw(v *ManagedRuleSetVersion, raw any) {
	vMap, ok := raw.(map[string]any)
	if !ok {
		return
	}

	if arnVal, arnOK := vMap["AssociatedRuleGroupArn"].(string); arnOK {
		v.AssociatedRuleGroupArn = arnVal
	}

	if capVal, capOK := toInt64(vMap["Capacity"]); capOK {
		v.Capacity = capVal
	}
}
func (b *InMemoryBackend) buildManagedRuleSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/managedruleset/"+name+"/"+id)
}

// ManagedRuleSetARN builds an ARN for a ManagedRuleSet.
func (b *InMemoryBackend) ManagedRuleSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/managedruleset/"+name+"/"+id)
}

// GetManagedRuleSet returns a ManagedRuleSet by ID.
func (b *InMemoryBackend) GetManagedRuleSet(ctx context.Context, id string) (*ManagedRuleSet, error) {
	b.mu.RLock("GetManagedRuleSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	ms, ok := b.managedRuleSets.Get(regionKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: managed rule set %q not found", ErrManagedRuleSetNotFound, id)
	}

	return cloneManagedRuleSet(ms), nil
}

// ListManagedRuleSets returns all managed rule sets sorted by name, optionally filtered by scope.
func (b *InMemoryBackend) ListManagedRuleSets(ctx context.Context, scope string) []*ManagedRuleSet {
	b.mu.RLock("ListManagedRuleSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionSets := b.managedRuleSetsByRegion.Get(region)
	list := make([]*ManagedRuleSet, 0, len(regionSets))

	for _, ms := range regionSets {
		if scope != "" && ms.Scope != scope {
			continue
		}

		list = append(list, cloneManagedRuleSet(ms))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// PutManagedRuleSetVersions creates or updates a managed rule set with the given versions.
// If the ID does not exist, a new managed rule set is created. If it exists, the lock token
// is verified before updating.
func (b *InMemoryBackend) PutManagedRuleSetVersions(
	ctx context.Context,
	id, name, scope, lockToken, recommendedVersion string,
	versionsToPublish map[string]any,
) (*ManagedRuleSet, error) {
	b.mu.Lock("PutManagedRuleSetVersions")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, exists := b.managedRuleSets.Get(regionKey(region, id))
	if exists && lockToken != "" && lockToken != ms.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for managed rule set %q", ErrOptimisticLock, id)
	}

	if !exists {
		arnStr := b.buildManagedRuleSetARN(name, id, scope, region)
		ms = &ManagedRuleSet{
			ID:                id,
			Name:              name,
			Scope:             scope,
			ARN:               arnStr,
			LockToken:         uuid.NewString(),
			PublishedVersions: make(map[string]ManagedRuleSetVersion),
			Region:            region,
		}
		b.managedRuleSets.Put(ms)
	}

	for versionName, versionRaw := range versionsToPublish {
		version := ManagedRuleSetVersion{}
		fillVersionFromRaw(&version, versionRaw)

		ms.PublishedVersions[versionName] = version
	}

	if recommendedVersion != "" {
		ms.RecommendedVersion = recommendedVersion
	}

	ms.LockToken = uuid.NewString()

	return cloneManagedRuleSet(ms), nil
}

// UpdateManagedRuleSetVersionExpiryDate updates the expiry timestamp on a specific version
// of a managed rule set. Returns the updated managed rule set, the expiring version name,
// and any error.
func (b *InMemoryBackend) UpdateManagedRuleSetVersionExpiryDate(
	ctx context.Context,
	id, lockToken, versionToExpire string,
	expiryTimestamp *int64,
) (*ManagedRuleSet, error) {
	b.mu.Lock("UpdateManagedRuleSetVersionExpiryDate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ms, ok := b.managedRuleSets.Get(regionKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: managed rule set %q not found", ErrManagedRuleSetNotFound, id)
	}

	if lockToken != "" && lockToken != ms.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for managed rule set %q", ErrOptimisticLock, id)
	}

	v, ok := ms.PublishedVersions[versionToExpire]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %q not found in managed rule set %q",
			ErrManagedRuleSetNotFound,
			versionToExpire,
			id,
		)
	}

	v.ExpiryTimestamp = expiryTimestamp
	ms.PublishedVersions[versionToExpire] = v
	ms.LockToken = uuid.NewString()

	return cloneManagedRuleSet(ms), nil
}
func cloneManagedRuleSet(ms *ManagedRuleSet) *ManagedRuleSet {
	cp := *ms

	if ms.PublishedVersions != nil {
		cp.PublishedVersions = make(map[string]ManagedRuleSetVersion, len(ms.PublishedVersions))

		maps.Copy(cp.PublishedVersions, ms.PublishedVersions)
	}

	return &cp
}
