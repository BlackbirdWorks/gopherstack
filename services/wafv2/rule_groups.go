package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildRuleGroupARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/rulegroup/"+name+"/"+id)
}

// RuleGroupARN builds an ARN for a RuleGroup.
func (b *InMemoryBackend) RuleGroupARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/rulegroup/"+name+"/"+id)
}

// lookupRuleGroupByID finds a RuleGroup with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupRuleGroupByID(requestRegion, id string) (*RuleGroup, bool) {
	if rg, ok := b.ruleGroups.Get(regionKey(requestRegion, id)); ok {
		return rg, true
	}

	if requestRegion != "" {
		if rg, ok := b.ruleGroups.Get(regionKey("", id)); ok {
			return rg, true
		}
	}

	return nil, false
}

// CreateRuleGroup creates a new RuleGroup.
func (b *InMemoryBackend) CreateRuleGroup(
	ctx context.Context,
	name, scope, description, visibilityConfig string,
	capacity int64,
	rules []map[string]any,
	tags map[string]string,
) (*RuleGroup, error) {
	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.ruleGroupsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: rule group %q already exists in scope %s", ErrRuleGroupAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildRuleGroupARN(name, id, scope, region)
	rg := &RuleGroup{
		ARN:              arnStr,
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		VisibilityConfig: visibilityConfig,
		Capacity:         capacity,
		Rules:            cloneRules(rules),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ruleGroups.Put(rg)

	return cloneRuleGroup(rg), nil
}

// DeleteRuleGroup deletes a RuleGroup by ID, checking for WebACL references.
func (b *InMemoryBackend) DeleteRuleGroup(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
	if !ok {
		return fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	storeReg := regionFromARN(rg.ARN)

	if lockToken != "" && lockToken != rg.LockToken {
		return fmt.Errorf("%w: lock token mismatch for rule group %q", ErrOptimisticLock, id)
	}

	rgARN := rg.ARN

	for _, w := range b.webACLs.All() {
		for _, rule := range w.Rules {
			if b.ruleReferencesARN(rule, rgARN) {
				return fmt.Errorf(
					"%w: rule group %q is referenced by web ACL %q",
					ErrAssociatedItem,
					id,
					w.ID,
				)
			}
		}
	}

	b.ruleGroups.Delete(regionKey(storeReg, id))

	return nil
}

// ruleReferencesARN checks if a rule map references the given ARN.
func (b *InMemoryBackend) ruleReferencesARN(rule map[string]any, arnStr string) bool {
	stmt, isStmt := rule["Statement"].(map[string]any)
	if !isStmt {
		return false
	}

	rgrStmt, isRGR := stmt["RuleGroupReferenceStatement"].(map[string]any)
	if !isRGR {
		return false
	}

	ref, isStr := rgrStmt["ARN"].(string)

	return isStr && ref == arnStr
}

// GetRuleGroup returns a RuleGroup by ID.
func (b *InMemoryBackend) GetRuleGroup(ctx context.Context, id string) (*RuleGroup, error) {
	b.mu.RLock("GetRuleGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	return cloneRuleGroup(rg), nil
}

// ListRuleGroups returns all RuleGroups sorted by name.
func (b *InMemoryBackend) ListRuleGroups(ctx context.Context) []*RuleGroup {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionGroups := b.ruleGroupsByRegion.Get(region)
	list := make([]*RuleGroup, 0, len(regionGroups))

	for _, rg := range regionGroups {
		list = append(list, cloneRuleGroup(rg))
	}

	if region != "" {
		for _, rg := range b.ruleGroupsByRegion.Get("") {
			list = append(list, cloneRuleGroup(rg))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRuleGroup updates a RuleGroup by ID.
func (b *InMemoryBackend) UpdateRuleGroup(
	ctx context.Context,
	id, description, visibilityConfig, lockToken string,
	rules []map[string]any,
) (*RuleGroup, error) {
	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	if lockToken != "" && lockToken != rg.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for rule group %q", ErrOptimisticLock, id)
	}

	if description != "" {
		rg.Description = description
	}

	if visibilityConfig != "" {
		rg.VisibilityConfig = visibilityConfig
	}

	if rules != nil {
		rg.Rules = cloneRules(rules)
	}

	rg.LockToken = uuid.NewString()

	return cloneRuleGroup(rg), nil
}
func cloneRuleGroup(rg *RuleGroup) *RuleGroup {
	cp := *rg
	cp.Tags = maps.Clone(rg.Tags)
	cp.Rules = cloneRules(rg.Rules)

	return &cp
}

// shallowCopyRules returns a shallow copy of each rule map in rules.
// Used as a fallback when JSON round-trip fails in cloneRules.
func shallowCopyRules(rules []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rules))

	for i, r := range rules {
		rm := make(map[string]any, len(r))
		maps.Copy(rm, r)
		out[i] = rm
	}

	return out
}

// cloneRules performs a deep clone of a rules slice. A JSON round-trip is used
// to ensure that nested maps and any json.RawMessage-backed values do not share
// backing arrays with the original, preventing data races and mutation aliasing.
func cloneRules(rules []map[string]any) []map[string]any {
	if rules == nil {
		return []map[string]any{}
	}

	data, marshalErr := json.Marshal(rules)
	if marshalErr != nil {
		return shallowCopyRules(rules)
	}

	var out []map[string]any
	if unmarshalErr := json.Unmarshal(data, &out); unmarshalErr != nil {
		return shallowCopyRules(rules)
	}

	return out
}
