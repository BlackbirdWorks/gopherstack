package verifiedpermissions

import (
	"fmt"
	"strings"

	cedar "github.com/cedar-policy/cedar-go"
)

// buildCedarPolicySet returns the Cedar PolicySet for a store, rebuilding only when dirty.
// Caller must hold at least a read lock; when dirty, caller must hold a write lock to
// promote the cache entry.  In practice all auth callers drop the read lock before calling
// this, then re-acquire as a write lock when dirty — but since we are under a single
// read lock here, we use a simple always-rebuild-when-dirty approach: build outside the
// lock and let the caller store the result.  The returned value is safe to use after the
// lock is released because it is immutable once built.
func (b *InMemoryBackend) buildCedarPolicySet(policyStoreID string) *cedar.PolicySet {
	if !b.policySetDirty[policyStoreID] {
		if cached, ok := b.policySetCache[policyStoreID]; ok {
			return cached
		}
	}

	ps := cedar.NewPolicySet()

	policies := b.policiesByStore.Get(policyStoreID)
	for _, p := range policies {
		if p.PolicyType != policyTypeStatic || p.Statement == "" {
			continue
		}

		list, err := cedar.NewPolicyListFromBytes("policy.cedar", []byte(p.Statement))
		if err != nil {
			continue
		}

		for i, pol := range list {
			pid := cedar.PolicyID(fmt.Sprintf("%s_p%d", p.PolicyID, i))
			polCopy := pol

			ps.Add(pid, polCopy)
		}
	}

	b.policySetCache[policyStoreID] = ps
	b.policySetDirty[policyStoreID] = false

	return ps
}

// invalidatePolicySetCache marks the compiled Cedar policy set for policyStoreID as dirty.
// Must be called under the write lock whenever STATIC policies change.
func (b *InMemoryBackend) invalidatePolicySetCache(policyStoreID string) {
	b.policySetDirty[policyStoreID] = true
}

// evaluateCedar runs cedar authorization and returns the AuthDecision.
func evaluateCedar(ps *cedar.PolicySet, req AuthorizationRequest) AuthDecision {
	cedarReq := cedar.Request{}

	if req.PrincipalEntityType != "" {
		cedarReq.Principal = cedar.NewEntityUID(
			cedar.EntityType(req.PrincipalEntityType),
			cedar.String(req.PrincipalEntityID),
		)
	}

	if req.ActionType != "" {
		cedarReq.Action = cedar.NewEntityUID(cedar.EntityType(req.ActionType), cedar.String(req.ActionID))
	}

	if req.ResourceEntityType != "" {
		cedarReq.Resource = cedar.NewEntityUID(
			cedar.EntityType(req.ResourceEntityType),
			cedar.String(req.ResourceEntityID),
		)
	}

	decision, diag := cedar.Authorize(ps, nil, cedarReq)

	result := AuthDecision{
		Request:             req,
		DeterminingPolicies: []string{},
		Errors:              []string{},
	}

	if decision == cedar.Allow {
		result.Decision = decisionAllow
	} else {
		result.Decision = decisionDeny
	}

	// Collect determining policy IDs (strip the "_p0" suffix to get original ID).
	for _, r := range diag.Reasons {
		rawID := string(r.PolicyID)
		// Strip the suffix added in buildCedarPolicySet.
		if idx := strings.LastIndex(rawID, "_p"); idx >= 0 {
			rawID = rawID[:idx]
		}

		result.DeterminingPolicies = append(result.DeterminingPolicies, rawID)
	}

	for _, e := range diag.Errors {
		result.Errors = append(result.Errors, e.Message)
	}

	return result
}

const maxBatchRequests = 30

// BatchIsAuthorized evaluates a batch of authorization requests.
func (b *InMemoryBackend) BatchIsAuthorized(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	if len(requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			ErrValidation,
			len(requests),
			maxBatchRequests,
		)
	}

	b.mu.Lock("BatchIsAuthorized")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, evaluateCedar(ps, req))
	}

	return decisions, nil
}

// BatchIsAuthorizedWithToken evaluates a batch of authorization requests using a token.
func (b *InMemoryBackend) BatchIsAuthorizedWithToken(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	if len(requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			ErrValidation,
			len(requests),
			maxBatchRequests,
		)
	}

	b.mu.Lock("BatchIsAuthorizedWithToken")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, evaluateCedar(ps, req))
	}

	return decisions, nil
}

// IsAuthorized evaluates a single authorization request against stored Cedar policies.
func (b *InMemoryBackend) IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error) {
	b.mu.Lock("IsAuthorized")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	result := evaluateCedar(ps, req)

	return &result, nil
}

// IsAuthorizedWithToken evaluates a single authorization request using a token.
func (b *InMemoryBackend) IsAuthorizedWithToken(
	policyStoreID string,
	req AuthorizationRequest,
) (*AuthDecision, error) {
	b.mu.Lock("IsAuthorizedWithToken")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	result := evaluateCedar(ps, req)

	return &result, nil
}
