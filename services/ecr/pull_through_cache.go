package ecr

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// CreatePullThroughCacheRule creates a new pull-through cache rule.
func (b *InMemoryBackend) CreatePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix, upstreamURL, credentialArn, upstreamRegistry, customRoleArn, upstreamRepositoryPrefix string,
) (*PullThroughCacheRule, error) {
	if prefix == "" {
		return nil, fmt.Errorf("%w: ecrRepositoryPrefix is required", ErrInvalidRepositoryName)
	}

	b.mu.Lock("CreatePullThroughCacheRule")
	defer b.mu.Unlock()

	if b.pullThroughCacheRules.Has(prefix) {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleAlreadyExists, prefix)
	}

	now := time.Now()
	rule := &PullThroughCacheRule{
		EcrRepositoryPrefix:      prefix,
		UpstreamRegistryURL:      upstreamURL,
		CredentialArn:            credentialArn,
		CustomRoleArn:            customRoleArn,
		UpstreamRegistry:         upstreamRegistry,
		UpstreamRepositoryPrefix: upstreamRepositoryPrefix,
		RegistryID:               b.accountID,
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	b.pullThroughCacheRules.Put(rule)

	cp := *rule

	return &cp, nil
}

// DescribePullThroughCacheRules lists pull-through cache rules.
func (b *InMemoryBackend) DescribePullThroughCacheRules(
	ctx context.Context, //nolint:revive // existing issue.
	prefixes []string,
) ([]PullThroughCacheRule, error) {
	b.mu.RLock("DescribePullThroughCacheRules")
	defer b.mu.RUnlock()

	out := make([]PullThroughCacheRule, 0, b.pullThroughCacheRules.Len())
	if len(prefixes) == 0 {
		for _, rule := range b.pullThroughCacheRules.All() {
			out = append(out, *rule)
		}
	} else {
		for _, prefix := range prefixes {
			rule, ok := b.pullThroughCacheRules.Get(prefix)
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
			}

			out = append(out, *rule)
		}
	}

	sort.Slice(
		out,
		func(i, j int) bool { return out[i].EcrRepositoryPrefix < out[j].EcrRepositoryPrefix },
	)

	return out, nil
}

// DeletePullThroughCacheRule deletes a pull-through cache rule by prefix.
func (b *InMemoryBackend) DeletePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix string,
) (*PullThroughCacheRule, error) {
	b.mu.Lock("DeletePullThroughCacheRule")
	defer b.mu.Unlock()

	rule, ok := b.pullThroughCacheRules.Get(prefix)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
	}

	b.pullThroughCacheRules.Delete(prefix)

	cp := *rule

	return &cp, nil
}

// UpdatePullThroughCacheRule updates a pull-through cache rule by prefix.
func (b *InMemoryBackend) UpdatePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix, credentialArn, customRoleArn string,
) (*PullThroughCacheRule, error) {
	b.mu.Lock("UpdatePullThroughCacheRule")
	defer b.mu.Unlock()

	rule, ok := b.pullThroughCacheRules.Get(prefix)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
	}

	if credentialArn != "" {
		rule.CredentialArn = credentialArn
	}

	if customRoleArn != "" {
		rule.CustomRoleArn = customRoleArn
	}

	rule.UpdatedAt = time.Now()
	cp := *rule

	return &cp, nil
}

// ValidatePullThroughCacheRule validates a pull-through cache rule by prefix.
func (b *InMemoryBackend) ValidatePullThroughCacheRule(
	ctx context.Context, //nolint:revive // existing issue.
	prefix string,
) (*ValidatePullThroughCacheRuleResult, error) {
	b.mu.RLock("ValidatePullThroughCacheRule")
	defer b.mu.RUnlock()

	rule, ok := b.pullThroughCacheRules.Get(prefix)
	if !ok {
		return &ValidatePullThroughCacheRuleResult{
			EcrRepositoryPrefix: prefix,
			Failure:             "Pull through cache rule not found",
			IsValid:             false,
			RegistryID:          b.accountID,
		}, nil
	}

	return &ValidatePullThroughCacheRuleResult{
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		IsValid:                  true,
		RegistryID:               rule.RegistryID,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
	}, nil
}
