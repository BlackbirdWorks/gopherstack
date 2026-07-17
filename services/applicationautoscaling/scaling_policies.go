package applicationautoscaling

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isValidPolicyType reports whether t is a recognised Application Auto Scaling
// PolicyType value.
func isValidPolicyType(t string) bool {
	switch t {
	case "StepScaling", "TargetTrackingScaling", "PredictiveScaling":
		return true
	default:
		return false
	}
}

// cloneScalingPolicy returns a deep copy of p with config maps cloned.
func cloneScalingPolicy(p *ScalingPolicy) *ScalingPolicy {
	cp := *p
	cp.TargetTrackingConfig = maps.Clone(p.TargetTrackingConfig)
	cp.StepScalingConfig = maps.Clone(p.StepScalingConfig)

	return &cp
}

// PutScalingPolicy upserts a scaling policy (update if policyName matches for resource, create otherwise).
func (b *InMemoryBackend) PutScalingPolicy(
	serviceNamespace, resourceID, scalableDimension, policyName, policyType string,
	targetTrackingConfig, stepScalingConfig map[string]any,
) (*ScalingPolicy, error) {
	if serviceNamespace == "" {
		return nil, fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	if resourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	if scalableDimension == "" {
		return nil, fmt.Errorf("%w: ScalableDimension is required", ErrValidation)
	}

	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	// Validate PolicyType if provided; do not default yet — defaulting only
	// applies when creating a brand-new policy (see below).
	if policyType != "" && !isValidPolicyType(policyType) {
		return nil, fmt.Errorf(
			"%w: invalid PolicyType %q; must be one of StepScaling, TargetTrackingScaling, PredictiveScaling",
			ErrValidation,
			policyType,
		)
	}

	b.mu.Lock("PutScalingPolicy")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	key := policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName)

	if group := b.policiesByName.Get(key); len(group) > 0 {
		p := group[0]
		// Only update PolicyType when the caller explicitly provided one.
		if policyType != "" {
			p.PolicyType = policyType
		}

		p.TargetTrackingConfig = maps.Clone(targetTrackingConfig)
		p.StepScalingConfig = maps.Clone(stepScalingConfig)
		p.LastModifiedTime = now

		return cloneScalingPolicy(p), nil
	}

	// Default PolicyType to StepScaling for new policies only -- this matches
	// real AWS/Terraform behavior (the aws_appautoscaling_policy resource
	// documents "StepScaling" as its default policy_type), not
	// TargetTrackingScaling.
	if policyType == "" {
		policyType = "StepScaling"
	}

	// Real AWS policy ARNs separate the policyName segment from the
	// resource/namespace/resourceId segment with a colon, not a slash:
	// scalingPolicy:{uuid}:resource/{namespace}/{resourceId}:policyName/{name}.
	policyARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scalingPolicy:%s:resource/%s/%s:policyName/%s",
			uuid.NewString(), serviceNamespace, resourceID, policyName))
	p := &ScalingPolicy{
		ServiceNamespace:     serviceNamespace,
		ResourceID:           resourceID,
		ScalableDimension:    scalableDimension,
		PolicyName:           policyName,
		PolicyType:           policyType,
		ARN:                  policyARN,
		TargetTrackingConfig: maps.Clone(targetTrackingConfig),
		StepScalingConfig:    maps.Clone(stepScalingConfig),
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.scalingPolicies.Put(p)
	cp := cloneScalingPolicy(p)

	return cp, nil
}

// DeleteScalingPolicy removes a scaling policy by name.
func (b *InMemoryBackend) DeleteScalingPolicy(
	serviceNamespace, resourceID, scalableDimension, policyName string,
) error {
	if serviceNamespace == "" {
		return fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	if resourceID == "" {
		return fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	if scalableDimension == "" {
		return fmt.Errorf("%w: ScalableDimension is required", ErrValidation)
	}

	if policyName == "" {
		return fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	b.mu.Lock("DeleteScalingPolicy")
	defer b.mu.Unlock()

	key := policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName)

	group := b.policiesByName.Get(key)
	if len(group) == 0 {
		return fmt.Errorf("%w: scaling policy %s not found", ErrNotFound, policyName)
	}

	b.scalingPolicies.Delete(group[0].ARN)

	return nil
}

// DescribeScalingPoliciesFilter carries optional filters for DescribeScalingPolicies.
type DescribeScalingPoliciesFilter struct {
	// ServiceNamespace limits results to this namespace when non-empty.
	ServiceNamespace string
	// ResourceID limits results to this resource when non-empty.
	ResourceID string
	// ScalableDimension limits results to this dimension when non-empty.
	ScalableDimension string
	// PolicyNames, when non-empty, limits results to the named policies.
	PolicyNames []string
	// NextToken is the opaque pagination cursor returned by a prior call.
	NextToken string
	// PolicyARNs, when non-empty, limits results to these ARNs.
	PolicyARNs []string
	// MaxResults, when > 0, limits the number of returned items.
	MaxResults int32
}

// buildStringSet converts ss into an O(1) membership-test set. The returned set
// is nil when ss is empty, which callers can use as a "no filter" sentinel.
func buildStringSet(ss []string) map[string]bool {
	if len(ss) == 0 {
		return nil
	}

	out := make(map[string]bool, len(ss))
	for _, s := range ss {
		out[s] = true
	}

	return out
}

// policyMatchesFilter reports whether p satisfies filter f.
// nameSet and arnSet are pre-built O(1) lookup sets derived from f.PolicyNames
// and f.PolicyARNs respectively; they are passed in to avoid rebuilding them
// inside the inner loop. A nil set means "no filter on that dimension".
func policyMatchesFilter(p *ScalingPolicy, f DescribeScalingPoliciesFilter, nameSet, arnSet map[string]bool) bool {
	if f.ServiceNamespace != "" && p.ServiceNamespace != f.ServiceNamespace {
		return false
	}

	if f.ResourceID != "" && p.ResourceID != f.ResourceID {
		return false
	}

	if f.ScalableDimension != "" && p.ScalableDimension != f.ScalableDimension {
		return false
	}

	if nameSet != nil && !nameSet[p.PolicyName] {
		return false
	}

	if arnSet != nil && !arnSet[p.ARN] {
		return false
	}

	return true
}

// DescribeScalingPolicies lists scaling policies, optionally filtered, and
// returns the NextToken for the following page (empty on the last page).
func (b *InMemoryBackend) DescribeScalingPolicies(f DescribeScalingPoliciesFilter) ([]*ScalingPolicy, string) {
	b.mu.RLock("DescribeScalingPolicies")
	defer b.mu.RUnlock()

	nameSet := buildStringSet(f.PolicyNames)
	arnSet := buildStringSet(f.PolicyARNs)

	list := make([]*ScalingPolicy, 0, b.scalingPolicies.Len())
	for _, p := range b.scalingPolicies.All() {
		if policyMatchesFilter(p, f, nameSet, arnSet) {
			list = append(list, cloneScalingPolicy(p))
		}
	}

	return paginate(list, f.MaxResults, f.NextToken, func(p *ScalingPolicy) string {
		return p.ARN
	})
}
