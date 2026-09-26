package eventbridge

import (
	"context"
	"fmt"
	"strconv"
)

// schemasDefaultRegistryName is the registry a resource policy applies to
// when RegistryName is omitted (schemas@v1.37.4 GetResourcePolicyInput doc:
// RegistryName is optional, defaulting to the account's default registry).
const schemasDefaultRegistryName = "default"

func effectiveResourcePolicyRegistry(name string) string {
	if name == "" {
		return schemasDefaultRegistryName
	}

	return name
}

// GetResourcePolicy returns the resource-based policy attached to a registry.
func (b *InMemoryBackend) GetResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) (*ResourcePolicy, error) {
	name := effectiveResourcePolicyRegistry(registryName)

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	p, ok := b.resourcePolicies[name]
	if !ok {
		return nil, fmt.Errorf("%w: no resource policy attached to registry %s", ErrNotFound, name)
	}

	cp := *p

	return &cp, nil
}

// PutResourcePolicy attaches or updates the resource-based policy on a
// registry. A non-empty input.RevisionID must match the policy's current
// revision, or PutResourcePolicy fails with ErrPreconditionFailed.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	input PutResourcePolicyInput,
) (*ResourcePolicy, error) {
	if input.Policy == "" {
		return nil, fmt.Errorf("%w: Policy is required", ErrInvalidParameter)
	}

	name := effectiveResourcePolicyRegistry(input.RegistryName)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	existing, ok := b.resourcePolicies[name]
	if input.RevisionID != "" && (!ok || existing.RevisionID != input.RevisionID) {
		return nil, fmt.Errorf(
			"%w: revision %s is out of date for registry %s policy",
			ErrPreconditionFailed,
			input.RevisionID,
			name,
		)
	}

	next := "1"
	if ok {
		n, _ := strconv.Atoi(existing.RevisionID)
		next = strconv.Itoa(n + 1)
	}

	p := &ResourcePolicy{Policy: input.Policy, RevisionID: next}
	b.resourcePolicies[name] = p

	cp := *p

	return &cp, nil
}

// DeleteResourcePolicy removes the resource-based policy attached to a registry.
func (b *InMemoryBackend) DeleteResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) error {
	name := effectiveResourcePolicyRegistry(registryName)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[name]; !ok {
		return fmt.Errorf("%w: no resource policy attached to registry %s", ErrNotFound, name)
	}

	delete(b.resourcePolicies, name)

	return nil
}
