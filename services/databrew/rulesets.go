package databrew

import (
	"context"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) rulesetARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "ruleset/"+name)
}

func (b *InMemoryBackend) CreateRuleset(
	ctx context.Context,
	name, description, targetArn string,
	rules []Rule,
	tags map[string]string,
) (*Ruleset, error) {
	b.mu.Lock("CreateRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	if err := validateRules(rules); err != nil {
		return nil, err
	}
	t := b.rulesetsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	rs := &Ruleset{
		Name: name, Arn: b.rulesetARN(region, name), Description: description,
		TargetArn: targetArn, Rules: cloneRules(rules), RuleCount: len(rules),
		Tags: maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()), AccountID: b.accountID,
	}
	t.Put(rs)

	return b.rulesetCopy(rs), nil
}

func (b *InMemoryBackend) rulesetCopy(rs *Ruleset) *Ruleset {
	cp := *rs
	cp.Tags = maps.Clone(rs.Tags)
	cp.Rules = cloneRules(rs.Rules)

	return &cp
}

// cloneRules deep-copies rules so stored state never aliases caller slices.
func cloneRules(in []Rule) []Rule {
	out := make([]Rule, len(in))
	for i, r := range in {
		out[i] = r
		out[i].SubstitutionMap = maps.Clone(r.SubstitutionMap)
		out[i].ColumnSelectors = append([]ColumnSelector(nil), r.ColumnSelectors...)
		if r.Threshold != nil {
			th := *r.Threshold
			out[i].Threshold = &th
		}
	}

	return out
}

// validateRules checks Threshold enums (types.ThresholdType/ThresholdUnit, databrew@v1.42.4 enums.go).
func validateRules(rules []Rule) error {
	for _, r := range rules {
		th := r.Threshold
		if th == nil {
			continue
		}
		switch th.Type {
		case "", "GREATER_THAN_OR_EQUAL", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "LESS_THAN":
		default:
			return ErrValidation
		}
		switch th.Unit {
		case "", "COUNT", "PERCENTAGE":
		default:
			return ErrValidation
		}
	}

	return nil
}

func (b *InMemoryBackend) DescribeRuleset(ctx context.Context, name string) (*Ruleset, error) {
	b.mu.RLock("DescribeRuleset")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	rs, ok := b.rulesetsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return b.rulesetCopy(rs), nil
}

func (b *InMemoryBackend) ListRulesets(
	ctx context.Context,
	maxResults int,
	nextToken, targetArn string,
) ([]*Ruleset, string) {
	b.mu.RLock("ListRulesets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.rulesetsTable(region)
	keys := snapshotKeys(t, rulesetKeyFn)
	filtered := keys
	if targetArn != "" {
		filtered = make([]string, 0, len(keys))
		for _, k := range keys {
			v, _ := t.Get(k)
			if v.TargetArn == targetArn {
				filtered = append(filtered, k)
			}
		}
	}
	pageKeys, next := paginateKeys(filtered, maxResults, nextToken)
	out := make([]*Ruleset, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		out = append(out, b.rulesetCopy(v))
	}

	return out, next
}

// UpdateRuleset overwrites Rules unconditionally (UpdateRulesetInput marks
// it "This member is required") but only overwrites Description when
// non-empty: Description has no such marker, so a caller updating just
// Rules must not have their existing Description clobbered.
func (b *InMemoryBackend) UpdateRuleset(
	ctx context.Context,
	name, description string,
	rules []Rule,
) error {
	b.mu.Lock("UpdateRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	rs, ok := b.rulesetsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	if err := validateRules(rules); err != nil {
		return err
	}
	if description != "" {
		rs.Description = description
	}
	rs.Rules = cloneRules(rules)
	rs.RuleCount = len(rules)
	rs.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteRuleset(ctx context.Context, name string) error {
	b.mu.Lock("DeleteRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.rulesetsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}
