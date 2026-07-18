package wafv2

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildRegexPatternSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/regexpatternset/"+name+"/"+id)
}

// RegexPatternSetARN builds an ARN for a RegexPatternSet.
func (b *InMemoryBackend) RegexPatternSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/regexpatternset/"+name+"/"+id)
}

// validateRegexEntries validates a list of RegexEntry objects.
func validateRegexEntries(entries []RegexEntry) error {
	if len(entries) > maxRegexPatternSetEntries {
		return fmt.Errorf(
			"%w: regex pattern set exceeds maximum of %d entries",
			ErrLimitsExceeded,
			maxRegexPatternSetEntries,
		)
	}

	for _, entry := range entries {
		if _, err := regexp.Compile(entry.RegexString); err != nil {
			return fmt.Errorf(
				"%w: invalid regex %q: %s",
				errInvalidRequest,
				entry.RegexString,
				err.Error(),
			)
		}
	}

	return nil
}

// lookupRegexPatternSetByID finds a RegexPatternSet with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupRegexPatternSetByID(requestRegion, id string) (*RegexPatternSet, bool) {
	if r, ok := b.regexPatternSets.Get(regionKey(requestRegion, id)); ok {
		return r, true
	}

	if requestRegion != "" {
		if r, ok := b.regexPatternSets.Get(regionKey("", id)); ok {
			return r, true
		}
	}

	return nil, false
}

// CreateRegexPatternSet creates a new RegexPatternSet.
func (b *InMemoryBackend) CreateRegexPatternSet(
	ctx context.Context,
	name, scope, description string,
	regularExpressionList []RegexEntry,
	tags map[string]string,
) (*RegexPatternSet, error) {
	b.mu.Lock("CreateRegexPatternSet")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.regexPatternSetsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q already exists in scope %s",
			ErrRegexPatternSetAlreadyExists,
			name,
			scope,
		)
	}

	id := uuid.NewString()
	arnStr := b.buildRegexPatternSetARN(name, id, scope, region)
	rps := &RegexPatternSet{
		ARN:                   arnStr,
		ID:                    id,
		Name:                  name,
		Scope:                 scope,
		Description:           description,
		RegularExpressionList: cloneRegexEntries(regularExpressionList),
		LockToken:             uuid.NewString(),
		Tags:                  cloneTags(tags),
	}
	b.regexPatternSets.Put(rps)

	return cloneRegexPatternSet(rps), nil
}

// DeleteRegexPatternSet deletes a RegexPatternSet by ID.
func (b *InMemoryBackend) DeleteRegexPatternSet(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteRegexPatternSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rps, ok := b.lookupRegexPatternSetByID(region, id)
	if !ok {
		return fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	storeReg := regionFromARN(rps.ARN)

	if lockToken != "" && lockToken != rps.LockToken {
		return fmt.Errorf("%w: lock token mismatch for regex pattern set %q", ErrOptimisticLock, id)
	}

	b.regexPatternSets.Delete(regionKey(storeReg, id))

	return nil
}

// GetRegexPatternSet returns a RegexPatternSet by ID.
func (b *InMemoryBackend) GetRegexPatternSet(ctx context.Context, id string) (*RegexPatternSet, error) {
	b.mu.RLock("GetRegexPatternSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	r, ok := b.lookupRegexPatternSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	return cloneRegexPatternSet(r), nil
}

// ListRegexPatternSets returns all RegexPatternSets sorted by name.
func (b *InMemoryBackend) ListRegexPatternSets(ctx context.Context) []*RegexPatternSet {
	b.mu.RLock("ListRegexPatternSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionSets := b.regexPatternSetsByRegion.Get(region)
	list := make([]*RegexPatternSet, 0, len(regionSets))

	for _, r := range regionSets {
		list = append(list, cloneRegexPatternSet(r))
	}

	if region != "" {
		for _, r := range b.regexPatternSetsByRegion.Get("") {
			list = append(list, cloneRegexPatternSet(r))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRegexPatternSet updates a RegexPatternSet by ID.
func (b *InMemoryBackend) UpdateRegexPatternSet(
	ctx context.Context,
	id, description, lockToken string,
	regularExpressionList []RegexEntry,
) (*RegexPatternSet, error) {
	b.mu.Lock("UpdateRegexPatternSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.lookupRegexPatternSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	if lockToken != "" && lockToken != r.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for regex pattern set %q", ErrOptimisticLock, id)
	}

	if description != "" {
		r.Description = description
	}

	if regularExpressionList != nil {
		r.RegularExpressionList = cloneRegexEntries(regularExpressionList)
	}

	r.LockToken = uuid.NewString()

	return cloneRegexPatternSet(r), nil
}
func cloneRegexPatternSet(r *RegexPatternSet) *RegexPatternSet {
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.RegularExpressionList = cloneRegexEntries(r.RegularExpressionList)

	return &cp
}
func cloneRegexEntries(entries []RegexEntry) []RegexEntry {
	if entries == nil {
		return []RegexEntry{}
	}

	out := make([]RegexEntry, len(entries))
	copy(out, entries)

	return out
}
