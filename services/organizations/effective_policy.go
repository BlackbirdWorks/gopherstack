package organizations

import (
	"encoding/json"
	"slices"
	"time"
)

// DescribeEffectivePolicy returns the effective policy of a given type for a target.
func (b *InMemoryBackend) DescribeEffectivePolicy(
	policyType, targetID string,
) (*EffectivePolicy, error) {
	b.mu.RLock("DescribeEffectivePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if targetID == "" {
		targetID = b.org.MasterAccountID
	}

	content, policyID := b.findEffectivePolicyLocked(policyType, targetID)
	if content == "" {
		return nil, ErrEffectivePolicyNotFound
	}

	return &EffectivePolicy{
		PolicyContent:        content,
		PolicyID:             policyID,
		PolicyType:           policyType,
		TargetID:             targetID,
		LastUpdatedTimestamp: time.Now(),
	}, nil
}

// findEffectivePolicyLocked walks the full hierarchy from targetID up to root,
// collecting all policies of policyType, then merges them per AWS rules.
// For TAG_POLICY/BACKUP_POLICY/AISERVICES_OPT_OUT_POLICY: deep-merge JSON objects
// (child overrides parent). For all other types (SCP/RCP): child takes precedence
// (last attached at each level wins; root-level policies form the baseline).
// Returns (mergedContent, lastPolicyID) or ("","") if no policies found.
// Must be called with a read lock held.
func (b *InMemoryBackend) findEffectivePolicyLocked(policyType, targetID string) (string, string) {
	chain := b.collectPolicyChainLocked(policyType, targetID)
	if len(chain) == 0 {
		return "", ""
	}

	return b.mergePolicyChain(policyType, chain)
}

// effectivePolicyEntry holds a single policy in the effective-policy chain.
type effectivePolicyEntry struct {
	id      string
	content string
}

// collectPolicyChainLocked walks from targetID up to root, collecting all policies
// of policyType in order (target first, root last).
// Must be called with a read lock held.
func (b *InMemoryBackend) collectPolicyChainLocked(policyType, targetID string) []effectivePolicyEntry {
	var chain []effectivePolicyEntry

	current := targetID

	for current != "" {
		for _, pid := range b.targetPolicies[current] {
			if p, ok := b.policies.Get(pid); ok && p.PolicySummary.Type == policyType {
				chain = append(chain, effectivePolicyEntry{id: p.PolicySummary.ID, content: p.Content})
			}
		}

		if b.root != nil && current == b.root.ID {
			break
		}

		if parentID, ok := b.accountParent[current]; ok {
			current = parentID

			continue
		}

		if parentID, ok := b.ouParent[current]; ok {
			current = parentID

			continue
		}

		break
	}

	return chain
}

// mergePolicyChain merges a chain of policies per AWS effective-policy rules.
// For TAG_POLICY/BACKUP_POLICY/AISERVICES_OPT_OUT_POLICY: deep-merge JSON objects
// (root is base, child overrides). For all other types: child wins (first entry).
func (b *InMemoryBackend) mergePolicyChain(policyType string, chain []effectivePolicyEntry) (string, string) {
	if len(chain) == 1 {
		return chain[0].content, chain[0].id
	}

	switch policyType {
	case "TAG_POLICY", "BACKUP_POLICY", "AISERVICES_OPT_OUT_POLICY":
		return b.mergeTagStyleChain(chain)
	}

	return chain[0].content, chain[0].id
}

// mergeTagStyleChain merges TAG_POLICY-style policy chains: merge from root to target,
// child overrides parent (last write wins per key).
func (b *InMemoryBackend) mergeTagStyleChain(chain []effectivePolicyEntry) (string, string) {
	merged := make(map[string]any)

	for _, entry := range slices.Backward(chain) {
		var obj map[string]any
		if err := json.Unmarshal([]byte(entry.content), &obj); err == nil {
			mergeJSONObjects(merged, obj)
		}
	}

	if data, err := json.Marshal(merged); err == nil {
		return string(data), chain[0].id
	}

	return chain[0].content, chain[0].id
}

// ListEffectivePolicyValidationErrors returns validation errors for an effective policy (always empty for stub).
func (b *InMemoryBackend) ListEffectivePolicyValidationErrors(policyType, _ string) ([]any, error) {
	b.mu.RLock("ListEffectivePolicyValidationErrors")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !slices.Contains(validPolicyTypes(), policyType) {
		return nil, ErrInvalidInput
	}

	return []any{}, nil
}
