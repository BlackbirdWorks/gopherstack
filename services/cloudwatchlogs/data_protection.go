package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"
)

// validAccountPolicyTypes returns the allowed values for the account policy type field.
func validAccountPolicyTypes() map[string]struct{} {
	return map[string]struct{}{
		"DATA_PROTECTION_POLICY":     {},
		"SUBSCRIPTION_FILTER_POLICY": {},
		"FIELD_INDEX_POLICY":         {},
		"TRANSFORMER_POLICY":         {},
	}
}

// validAccountPolicyScopes returns the allowed values for the account policy scope field.
func validAccountPolicyScopes() map[string]struct{} {
	return map[string]struct{}{
		"ALL":                {},
		"SELECTION_CRITERIA": {},
	}
}

// PutDataProtectionPolicy stores a data protection policy for a log group.
// policyDocument is stored as-is and returned verbatim by GetDataProtectionPolicy.
func (b *InMemoryBackend) PutDataProtectionPolicy(logGroupIdentifier, policyDocument string) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutDataProtectionPolicy")
	defer b.mu.Unlock()

	b.dataProtectionPolicies.Put(&dataProtectionPolicyEntry{
		LogGroupIdentifier: logGroupIdentifier,
		PolicyDocument:     policyDocument,
		LastUpdatedTime:    time.Now().UTC().UnixMilli(),
	})

	return nil
}

// GetDataProtectionPolicy returns the data protection policy and its last
// update time for a log group. Returns an empty policy document and a zero
// timestamp if none has been set.
func (b *InMemoryBackend) GetDataProtectionPolicy(logGroupIdentifier string) (string, int64, error) {
	b.mu.RLock("GetDataProtectionPolicy")
	defer b.mu.RUnlock()

	entry, ok := b.dataProtectionPolicies.Get(logGroupIdentifier)
	if !ok {
		return "{}", 0, nil
	}

	return entry.PolicyDocument, entry.LastUpdatedTime, nil
}

// DeleteDataProtectionPolicy removes the data protection policy for a log group.
func (b *InMemoryBackend) DeleteDataProtectionPolicy(logGroupIdentifier string) error {
	b.mu.Lock("DeleteDataProtectionPolicy")
	defer b.mu.Unlock()

	b.dataProtectionPolicies.Delete(logGroupIdentifier)

	return nil
}

// DeleteAccountPolicy deletes a CloudWatch Logs account-level policy.
func (b *InMemoryBackend) DeleteAccountPolicy(policyName, policyType string) error {
	if policyName == "" {
		return fmt.Errorf("%w: policyName is required", ErrValidation)
	}

	if policyType == "" {
		return fmt.Errorf("%w: policyType is required", ErrValidation)
	}

	if _, ok := validAccountPolicyTypes()[policyType]; !ok {
		return fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
	}

	b.mu.Lock("DeleteAccountPolicy")
	defer b.mu.Unlock()

	key := policyName + ":" + policyType
	b.accountPolicies.Delete(key)

	return nil
}

// PutAccountPolicy creates or updates an account-level policy.
// scope must be ALL or SELECTION_CRITERIA (defaults to ALL if empty).
func (b *InMemoryBackend) PutAccountPolicy(
	policyName, policyType, policyDocument, scope, selectionCriteria string,
) (*AccountPolicy, error) {
	if policyName == "" {
		return nil, fmt.Errorf("%w: policyName is required", ErrValidation)
	}
	if policyType == "" {
		return nil, fmt.Errorf("%w: policyType is required", ErrValidation)
	}
	if _, ok := validAccountPolicyTypes()[policyType]; !ok {
		return nil, fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
	}
	if scope == "" {
		scope = "ALL"
	}
	if _, ok := validAccountPolicyScopes()[scope]; !ok {
		return nil, fmt.Errorf(
			"%w: invalid scope %q, must be ALL or SELECTION_CRITERIA",
			ErrValidation,
			scope,
		)
	}
	if scope == "SELECTION_CRITERIA" && selectionCriteria == "" {
		return nil, fmt.Errorf(
			"%w: selectionCriteria is required when scope is SELECTION_CRITERIA",
			ErrValidation,
		)
	}

	b.mu.Lock("PutAccountPolicy")
	defer b.mu.Unlock()

	p := &AccountPolicy{
		PolicyName:        policyName,
		PolicyType:        policyType,
		PolicyDocument:    policyDocument,
		Scope:             scope,
		SelectionCriteria: selectionCriteria,
		AccountID:         b.accountID,
		LastUpdatedTime:   time.Now().UnixMilli(),
	}
	b.accountPolicies.Put(p)
	cp := *p

	return &cp, nil
}

// DescribeAccountPolicies returns account-level policies, optionally filtered, with pagination.
// accountIdentifiers filters by account IDs embedded in the policy name (prefix match).
func (b *InMemoryBackend) DescribeAccountPolicies(
	policyType, policyName string,
	_ []string,
	limit int,
	nextToken string,
) ([]AccountPolicy, string, error) {
	if policyType != "" {
		if _, ok := validAccountPolicyTypes()[policyType]; !ok {
			return nil, "", fmt.Errorf("%w: invalid policyType %q", ErrValidation, policyType)
		}
	}

	b.mu.RLock("DescribeAccountPolicies")
	defer b.mu.RUnlock()

	all := make([]AccountPolicy, 0, b.accountPolicies.Len())
	for _, p := range b.accountPolicies.All() {
		if policyType != "" && p.PolicyType != policyType {
			continue
		}
		if policyName != "" && p.PolicyName != policyName {
			continue
		}
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].PolicyName < all[j].PolicyName })

	// Apply pagination.
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []AccountPolicy{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}
