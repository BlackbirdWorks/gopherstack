package eventbridge

import (
	"context"
	"fmt"
	"sort"
)

// PutTargets adds or updates targets for a rule.
func (b *InMemoryBackend) PutTargets(ctx context.Context,
	ruleName, eventBusName string,
	targets []Target,
) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.Lock("PutTargets")
	defer b.mu.Unlock()

	busRules, exists := b.rulesStore(region)[busKey]
	if !exists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	if !busRules.Has(ruleName) {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	key := b.targetKey(eventBusName, ruleName)
	targetTable := b.targetTableFor(region, key)

	// Reject if adding these targets would exceed the per-rule limit.
	if targetTable.Len()+len(targets) > maxTargetsPerRule {
		return nil, fmt.Errorf(
			"%w: rule %s already has the maximum number of targets (%d)",
			ErrInvalidParameter,
			ruleName,
			maxTargetsPerRule,
		)
	}

	var failed []FailedEntry
	for _, t := range targets {
		if t.ID == "" {
			failed = append(failed, FailedEntry{
				TargetID:     t.ID,
				ErrorCode:    putTargetsFailedEntryErrorCode,
				ErrorMessage: "Target Id is required",
			})

			continue
		}
		if t.InputTransformer != nil {
			if err := validateInputTransformer(t.InputTransformer); err != nil {
				failed = append(failed, FailedEntry{
					TargetID:     t.ID,
					ErrorCode:    putTargetsFailedEntryErrorCode,
					ErrorMessage: err.Error(),
				})

				continue
			}
		}
		if err := validateTargetTypeParameters(&t); err != nil {
			failed = append(failed, FailedEntry{
				TargetID:     t.ID,
				ErrorCode:    putTargetsFailedEntryErrorCode,
				ErrorMessage: err.Error(),
			})

			continue
		}
		// Maintain ARN index: remove old entry if this target ID already exists with a different ARN.
		if existingTarget, targetExists := targetTable.Get(t.ID); targetExists && existingTarget.Arn != t.Arn {
			b.arnIndexRemoveTarget(region, existingTarget.Arn, key)
		}
		cp := t
		targetTable.Put(&cp)
		b.arnIndexAdd(region, t.Arn, key)
	}

	return failed, nil
}

// RemoveTargets removes targets from a rule by their IDs.
func (b *InMemoryBackend) RemoveTargets(ctx context.Context,
	ruleName, eventBusName string,
	ids []string,
) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("RemoveTargets")
	defer b.mu.Unlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targetsStore(region)[key]

	var failed []FailedEntry
	for _, id := range ids {
		var t *Target
		var exists bool
		if ruleTargets != nil {
			t, exists = ruleTargets.Get(id)
		}
		if !exists {
			failed = append(failed, FailedEntry{
				TargetID:     id,
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: fmt.Sprintf("Target %s not found", id),
			})

			continue
		}
		b.arnIndexRemoveTarget(region, t.Arn, key)
		ruleTargets.Delete(id)
	}

	return failed, nil
}

// ListTargetsByRule returns targets for a rule with optional pagination.
// limit caps the page size (0 uses the default); AWS EventBridge honours the
// Limit request parameter, so it is threaded through to pagination here.
func (b *InMemoryBackend) ListTargetsByRule(ctx context.Context,
	ruleName, eventBusName, nextToken string, limit int,
) ([]Target, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListTargetsByRule")
	defer b.mu.RUnlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targetsStore(region)[key]
	var ruleTargetsAll []*Target
	if ruleTargets != nil {
		ruleTargetsAll = ruleTargets.All()
	}
	all := make([]Target, 0, len(ruleTargetsAll))
	for _, t := range ruleTargetsAll {
		all = append(all, *t)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, outToken := paginateN(all, nextToken, limit)

	return page, outToken, nil
}
