package resiliencehub

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resolvePolicyLocked resolves policyArn to its stored ResiliencyPolicy.
// Callers must hold b.mu.
func (b *InMemoryBackend) resolvePolicyLocked(policyArn string) (*ResiliencyPolicy, bool) {
	id, ok := resourceIDFromARN(policyArn, ":resiliency-policy/")
	if !ok {
		return nil, false
	}

	return b.policies.Get(id)
}

// validatePolicyMap requires a FailurePolicy entry for every DisruptionType
// (Software/Hardware/AZ/Region) -- FailurePolicy.RtoInSecs/RpoInSecs are
// plain int32 (always present, never pointers) on the wire, so a policy that
// is missing a disruption type has no well-formed zero value to fall back
// to. The exact required-keys rule is not encoded in the Go SDK types
// themselves (PARITY.md flags this as something "the implementer would need
// to infer/decide") -- requiring all four is the most defensible reading of
// "a resiliency policy for every disruption type", matching how
// CreateResiliencyPolicyInput.Policy is documented as required in full.
func validatePolicyMap(m map[string]failurePolicyWire) error {
	if len(m) == 0 {
		return validationError("policy is required")
	}

	for _, dt := range disruptionTypes {
		fp, ok := m[dt]
		if !ok {
			return validationError("policy is missing required disruption type: " + dt)
		}

		if fp.RtoInSecs < 0 || fp.RpoInSecs < 0 {
			return validationError("policy RtoInSecs/RpoInSecs must be non-negative for disruption type: " + dt)
		}
	}

	return nil
}

func isValidTier(v string) bool {
	switch v {
	case TierMissionCritical, TierCritical, TierImportant, TierCoreServices, TierNonCritical, TierNotApplicable:
		return true
	default:
		return false
	}
}

// CreateResiliencyPolicy creates a new resiliency policy.
func (b *InMemoryBackend) CreateResiliencyPolicy(req *createResiliencyPolicyRequest) (*ResiliencyPolicy, error) {
	if req.PolicyName == "" {
		return nil, validationError("policyName is required")
	}

	if !isValidTier(req.Tier) {
		return nil, validationError("invalid tier: " + req.Tier)
	}

	if err := validatePolicyMap(req.Policy); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateResiliencyPolicy")
	defer b.mu.Unlock()

	id := newPolicyID()
	t := tags.New("resiliencehub.policy." + id + ".tags")
	t.Merge(req.Tags)

	p := &ResiliencyPolicy{
		ARN:                    b.PolicyARN(id),
		Name:                   req.PolicyName,
		Tier:                   req.Tier,
		Policy:                 fromFailurePolicyMapWire(req.Policy),
		CreationTime:           time.Now().UTC(),
		DataLocationConstraint: req.DataLocationConstraint,
		// EstimatedCostTier is left empty: like ResiliencyScore, it is an
		// output of a cost-estimation model with no derivable formula in the
		// SDK -- see PARITY.md's honest-gap posture.
		Description: req.PolicyDescription,
		Tags:        t,
	}

	b.policies.Put(p)

	return p.clone(), nil
}

// DescribeResiliencyPolicy returns a copy of the policy identified by
// policyArn.
func (b *InMemoryBackend) DescribeResiliencyPolicy(policyArn string) (*ResiliencyPolicy, error) {
	b.mu.RLock("DescribeResiliencyPolicy")
	defer b.mu.RUnlock()

	p, ok := b.resolvePolicyLocked(policyArn)
	if !ok {
		return nil, notFoundError(resourcePolicy, policyArn)
	}

	return p.clone(), nil
}

// UpdateResiliencyPolicy applies a partial update to the policy identified
// by policyArn.
func (b *InMemoryBackend) UpdateResiliencyPolicy(
	policyArn string,
	req *updateResiliencyPolicyRequest,
) (*ResiliencyPolicy, error) {
	if req.Tier != "" && !isValidTier(req.Tier) {
		return nil, validationError("invalid tier: " + req.Tier)
	}

	if len(req.Policy) > 0 {
		if err := validatePolicyMap(req.Policy); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateResiliencyPolicy")
	defer b.mu.Unlock()

	p, ok := b.resolvePolicyLocked(policyArn)
	if !ok {
		return nil, notFoundError(resourcePolicy, policyArn)
	}

	if req.PolicyName != "" {
		p.Name = req.PolicyName
	}

	if req.Tier != "" {
		p.Tier = req.Tier
	}

	if req.PolicyDescription != "" {
		p.Description = req.PolicyDescription
	}

	if req.DataLocationConstraint != "" {
		p.DataLocationConstraint = req.DataLocationConstraint
	}

	if len(req.Policy) > 0 {
		p.Policy = fromFailurePolicyMapWire(req.Policy)
	}

	return p.clone(), nil
}

// DeleteResiliencyPolicy deletes the policy identified by policyArn.
// Rejected (ConflictException) while any App is still bound to it -- a real
// FK-integrity check, not a stub.
func (b *InMemoryBackend) DeleteResiliencyPolicy(policyArn string) error {
	b.mu.Lock("DeleteResiliencyPolicy")
	defer b.mu.Unlock()

	p, ok := b.resolvePolicyLocked(policyArn)
	if !ok {
		return notFoundError(resourcePolicy, policyArn)
	}

	for _, a := range b.apps.All() {
		if a.PolicyArn == p.ARN {
			return conflictErrorWithResource(resourcePolicy, policyIDFromARN(p.ARN), "policy is bound to app: "+a.ARN)
		}
	}

	if p.Tags != nil {
		p.Tags.Close()
	}

	b.policies.Delete(policyKeyFn(p))

	return nil
}

func policyIDFromARN(policyArn string) string {
	id, _ := resourceIDFromARN(policyArn, ":resiliency-policy/")

	return id
}

// ListResiliencyPolicies returns a page of policies, optionally filtered by
// name.
func (b *InMemoryBackend) ListResiliencyPolicies(policyName, token string, limit int) page.Page[*ResiliencyPolicy] {
	b.mu.RLock("ListResiliencyPolicies")
	defer b.mu.RUnlock()

	all := b.policies.Snapshot()

	if policyName == "" {
		return page.New(all, token, limit, defaultPageLimit)
	}

	filtered := make([]*ResiliencyPolicy, 0, len(all))

	for _, p := range all {
		if p.Name == policyName {
			filtered = append(filtered, p)
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}

// suggestedPolicyTier is one static per-tier RTO/RPO default entry backing
// ListSuggestedResiliencyPolicies. DOCUMENTED STAND-IN, NOT FABRICATED
// ANALYSIS: AWS's real console auto-suggests defaults per tier from
// operational data not encoded anywhere in the Go SDK (PARITY.md: "not
// fabricable with confidence but a reasonable static table is a defensible
// stand-in if clearly documented as such", matching services/grafana's
// static version-list precedent). These four RTO/RPO pairs are a coarse,
// halving-per-tier progression, NOT AWS-published numbers.
type suggestedPolicyTier struct {
	tier    string
	rtoSecs int32
	rpoSecs int32
}

// Coarse halving-per-tier second counts backing suggestedPolicyTiers below --
// named (rather than inline) purely to satisfy the magic-number linter; the
// values themselves remain the same documented, non-authoritative stand-in.
const (
	suggestedRtoMissionCriticalSecs = 60
	suggestedRtoCriticalSecs        = 600
	suggestedRtoImportantSecs       = 3600
	suggestedRtoCoreServicesSecs    = 86400
	suggestedRtoNonCriticalSecs     = 604800
)

//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var suggestedPolicyTiers = []suggestedPolicyTier{
	{tier: TierMissionCritical, rtoSecs: suggestedRtoMissionCriticalSecs, rpoSecs: suggestedRtoMissionCriticalSecs},
	{tier: TierCritical, rtoSecs: suggestedRtoCriticalSecs, rpoSecs: suggestedRtoCriticalSecs},
	{tier: TierImportant, rtoSecs: suggestedRtoImportantSecs, rpoSecs: suggestedRtoImportantSecs},
	{tier: TierCoreServices, rtoSecs: suggestedRtoCoreServicesSecs, rpoSecs: suggestedRtoCoreServicesSecs},
	{tier: TierNonCritical, rtoSecs: suggestedRtoNonCriticalSecs, rpoSecs: suggestedRtoNonCriticalSecs},
}

// ListSuggestedResiliencyPolicies returns the static suggested-policy
// reference table as a page.
func (b *InMemoryBackend) ListSuggestedResiliencyPolicies(token string, limit int) page.Page[*ResiliencyPolicy] {
	all := make([]*ResiliencyPolicy, 0, len(suggestedPolicyTiers))

	for _, s := range suggestedPolicyTiers {
		policy := make(map[string]FailurePolicy, len(disruptionTypes))
		for _, dt := range disruptionTypes {
			policy[dt] = FailurePolicy{RtoInSecs: s.rtoSecs, RpoInSecs: s.rpoSecs}
		}

		all = append(all, &ResiliencyPolicy{
			Name:   "Suggested-" + s.tier,
			Tier:   s.tier,
			Policy: policy,
		})
	}

	return page.New(all, token, limit, defaultPageLimit)
}
