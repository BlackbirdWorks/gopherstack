package lambdaweb

import (
	"maps"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) functionByARNLocked(resourceARN string) (*Function, error) {
	p, ok := parseWebARN(resourceARN)
	if !ok || p.kind != "" || p.account != b.accountID {
		return nil, validationError("Invalid web function ARN: %s", resourceARN)
	}

	return b.liveFunctionLocked(p.region, p.function)
}

// PutResourcePolicy replaces a function's resource-based policy.
func (b *InMemoryBackend) PutResourcePolicy(resourceARN, policy, revisionID string) (*ResourcePolicy, error) {
	if err := validatePolicyDocument(policy); err != nil {
		return nil, err
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	if revisionID != "" && revisionID != f.PolicyRevID {
		return nil, conflictError(resPolicy, f.Name, "The RevisionId provided does not match the latest RevisionId.")
	}

	f.Policy, f.PolicyRevID = policy, uuid.NewString()

	return &ResourcePolicy{Policy: f.Policy, RevisionID: f.PolicyRevID}, nil
}

// GetResourcePolicy returns a function's resource-based policy.
func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (*ResourcePolicy, error) {
	b.mu.Lock("GetResourcePolicy")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	if f.Policy == "" {
		return nil, notFoundError(resPolicy, f.Name)
	}

	return &ResourcePolicy{Policy: f.Policy, RevisionID: f.PolicyRevID}, nil
}

// DeleteResourcePolicy removes a function's resource-based policy.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN, revisionID string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return err
	}

	if f.Policy == "" {
		return notFoundError(resPolicy, f.Name)
	}

	if revisionID != "" && revisionID != f.PolicyRevID {
		return conflictError(resPolicy, f.Name, "The RevisionId provided does not match the latest RevisionId.")
	}

	f.Policy, f.PolicyRevID = "", ""

	return nil
}

// TagResource adds or overwrites tags on a function.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return err
	}

	merged := maps.Clone(f.Tags)
	maps.Copy(merged, tags)

	if err = validateTags(merged); err != nil {
		return err
	}

	f.Tags = merged

	return nil
}

// UntagResource removes tags from a function.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(f.Tags, k)
	}

	return nil
}

// ListTags returns a function's tags.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.Lock("ListTags")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionByARNLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	return maps.Clone(f.Tags), nil
}
