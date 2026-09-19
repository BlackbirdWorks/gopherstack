package lambda

import (
	"fmt"

	"github.com/google/uuid"
)

// effectivePolicyLocked returns the current policy JSON/revision for a
// function/qualifier target, preferring a PutResourcePolicy override over the
// statement-based (AddPermission) rendering when one exists -- matching real
// AWS's "PutResourcePolicy replaces any existing policy" semantics. Caller
// must hold b.mu.
func (b *InMemoryBackend) effectivePolicyLocked(name, qualifier string) (string, string, bool) {
	key := permissionMapKey(name, qualifier)
	if override, ok := b.resourcePolicyOverrides[key]; ok {
		return override.Policy, override.RevisionID, true
	}

	return b.statementPolicyLocked(name, qualifier)
}

// GetResourcePolicy returns the resource-based policy attached to a Lambda
// function/version/alias, identified by (qualified or unqualified) ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (*ResourcePolicyOutput, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	name, qualifier := resolvePermissionTarget(resourceArn, "")

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	policy, rev, ok := b.effectivePolicyLocked(name, qualifier)
	if !ok {
		return nil, ErrNoPolicyFound
	}

	return &ResourcePolicyOutput{Policy: policy, RevisionID: rev}, nil
}

// PutResourcePolicy replaces the resource-based policy attached to a Lambda
// resource. When revisionID is non-empty it must match the target's current
// effective revision (statement-based or override), or the call fails with
// ErrPreconditionFailed without mutating anything. Any existing
// AddPermission-created statements for the target are cleared, matching real
// AWS: this operation fully replaces the policy document.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy, revisionID string) (*ResourcePolicyOutput, error) {
	if policy == "" {
		return nil, fmt.Errorf("%w: Policy is required", ErrInvalidParameterValue)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	name, qualifier := resolvePermissionTarget(resourceArn, "")

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	if qualifier != "" && !b.qualifierExistsLocked(name, qualifier) {
		return nil, ErrVersionNotFound
	}

	_, currentRev, exists := b.effectivePolicyLocked(name, qualifier)
	if revisionID != "" && (!exists || currentRev != revisionID) {
		return nil, ErrPreconditionFailed
	}

	b.clearPermissionsForTargetLocked(name, qualifier)

	newRev := uuid.New().String()
	b.resourcePolicyOverrides[permissionMapKey(name, qualifier)] = &ResourcePolicyOverride{
		Policy:     policy,
		RevisionID: newRev,
	}

	return &ResourcePolicyOutput{Policy: policy, RevisionID: newRev}, nil
}

// DeleteResourcePolicy removes the resource-based policy attached to a Lambda
// resource, whether it came from PutResourcePolicy or AddPermission-created
// statements. When revisionID is non-empty it must match the target's
// current effective revision, or the call fails with ErrPreconditionFailed.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn, revisionID string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	name, qualifier := resolvePermissionTarget(resourceArn, "")

	if _, ok := b.functions.Get(name); !ok {
		return ErrFunctionNotFound
	}

	_, currentRev, exists := b.effectivePolicyLocked(name, qualifier)
	if !exists {
		return ErrNoPolicyFound
	}

	if revisionID != "" && currentRev != revisionID {
		return ErrPreconditionFailed
	}

	delete(b.resourcePolicyOverrides, permissionMapKey(name, qualifier))
	b.clearPermissionsForTargetLocked(name, qualifier)

	return nil
}
