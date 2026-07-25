package glue

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

const globalPolicyKey = "__global__"

// ErrResourcePolicyConditionFailed is returned when PutResourcePolicy's
// PolicyExistsCondition or PolicyHashCondition does not match the current backend
// state, mirroring AWS's ConditionCheckFailureException — the optimistic-concurrency
// guard PutResourcePolicy documents to prevent callers from clobbering a policy
// someone else has since changed.
var ErrResourcePolicyConditionFailed = awserr.New(
	"ConditionCheckFailureException",
	awserr.ErrConflict,
)

// PutResourcePolicy creates or updates a resource policy (or the account-level
// policy when resourceARN is empty). existsCondition ("MUST_EXIST"/"NOT_EXIST"/""
// or "NONE") and hashCondition mirror PutResourcePolicyInput's
// PolicyExistsCondition/PolicyHashCondition optimistic-concurrency guards.
// enableHybrid mirrors PutResourcePolicyInput.EnableHybrid ("TRUE"/"FALSE"/"");
// AWS requires it be "TRUE" only when the account already has cross-account
// access granted via the Lake Formation console alongside this policy — this
// backend does not model Lake Formation console grants, so that precondition
// never applies and any well-formed value is accepted and recorded.
func (b *InMemoryBackend) PutResourcePolicy(
	policy, resourceARN, existsCondition, hashCondition, enableHybrid string,
) (string, error) {
	switch enableHybrid {
	case "", "TRUE", "FALSE":
	default:
		return "", fmt.Errorf("%w: EnableHybrid must be TRUE or FALSE", ErrValidation)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}

	existing, hasExisting := b.resourcePolicies[key]

	switch existsCondition {
	case "MUST_EXIST":
		if !hasExisting {
			return "", fmt.Errorf("resource policy not found: %w", ErrNotFound)
		}
	case "NOT_EXIST":
		if hasExisting {
			return "", ErrResourcePolicyConditionFailed
		}
	}

	if hashCondition != "" && (!hasExisting || existing.Hash != hashCondition) {
		return "", ErrResourcePolicyConditionFailed
	}

	hash := uuid.NewString()[:8]
	now := float64(time.Now().Unix())

	createTime := now
	if hasExisting {
		createTime = existing.CreateTime
	}

	b.resourcePolicies[key] = &resourcePolicyEntry{
		Policy:       policy,
		Hash:         hash,
		EnableHybrid: enableHybrid,
		CreateTime:   createTime,
		UpdateTime:   now,
	}

	return hash, nil
}

// ListResourcePolicies returns every stored resource policy (per-resource ARN
// policies plus the account-level policy, if set), sorted by key for a
// deterministic response.
func (b *InMemoryBackend) ListResourcePolicies() []*resourcePolicyEntry {
	b.mu.RLock("ListResourcePolicies")
	defer b.mu.RUnlock()

	keys := collections.SortedKeys(b.resourcePolicies)
	out := make([]*resourcePolicyEntry, 0, len(keys))

	for _, k := range keys {
		cp := *b.resourcePolicies[k]
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (string, string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}
	e, ok := b.resourcePolicies[key]
	if !ok {
		return "", "", fmt.Errorf("resource policy not found: %w", ErrNotFound)
	}

	return e.Policy, e.Hash, nil
}

func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN, policyHash string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	key := resourceARN
	if key == "" {
		key = globalPolicyKey
	}

	e, ok := b.resourcePolicies[key]
	if !ok {
		return fmt.Errorf("resource policy not found: %w", ErrNotFound)
	}

	// PolicyHashCondition, when supplied, must match the stored hash — this was
	// previously accepted and silently ignored, so a stale caller could delete a
	// policy someone else had since updated.
	if policyHash != "" && e.Hash != policyHash {
		return ErrResourcePolicyConditionFailed
	}

	delete(b.resourcePolicies, key)

	return nil
}
