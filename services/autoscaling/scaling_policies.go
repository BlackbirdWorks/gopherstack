package autoscaling

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// DescribeAdjustmentTypes returns the supported scaling adjustment types.
func (b *InMemoryBackend) DescribeAdjustmentTypes() ([]string, error) {
	return []string{"ChangeInCapacity", "ExactCapacity", "PercentChangeInCapacity"}, nil
}

// ExecutePolicy executes a scaling policy on the ASG.
func (b *InMemoryBackend) ExecutePolicy(input ExecutePolicyInput) error {
	b.mu.Lock("ExecutePolicy")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(input.AutoScalingGroupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	policy, ok := b.scalingPolicies.Get(scopedKey(input.AutoScalingGroupName, input.PolicyName))
	if !ok {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, input.PolicyName)
	}

	// Check cooldown if HonorCooldown is requested
	if input.HonorCooldown && policy.Cooldown > 0 && !g.LastScalingActivity.IsZero() {
		cooldownDur := time.Duration(policy.Cooldown) * time.Second
		if time.Since(g.LastScalingActivity) < cooldownDur {
			return fmt.Errorf("%w: scaling activity in progress (cooldown)", ErrScalingActivityInProgress)
		}
	}

	// StepScaling policies select their ScalingAdjustment from StepAdjustments based
	// on where (MetricValue - BreachThreshold) falls; both are required in that case
	// and unsupported otherwise (matches AWS ExecutePolicy validation).
	scalingAdjustment := policy.ScalingAdjustment

	if policy.PolicyType == "StepScaling" {
		if input.MetricValue == nil || input.BreachThreshold == nil {
			return fmt.Errorf(
				"%w: MetricValue and BreachThreshold are required to execute a StepScaling policy",
				ErrInvalidParameter,
			)
		}

		diff := *input.MetricValue - *input.BreachThreshold

		step, found := findStepAdjustment(policy.StepAdjustments, diff)
		if !found {
			return fmt.Errorf(
				"%w: no step adjustment matches MetricValue %v with BreachThreshold %v",
				ErrInvalidParameter, *input.MetricValue, *input.BreachThreshold,
			)
		}

		scalingAdjustment = step.ScalingAdjustment
	}

	var newDesired int32

	switch policy.AdjustmentType {
	case "ExactCapacity":
		newDesired = scalingAdjustment
	case "PercentChangeInCapacity":
		pct := float64(g.DesiredCapacity) * float64(scalingAdjustment) / percentDivisor
		delta := int32(pct)
		newDesired = g.DesiredCapacity + delta
	default: // ChangeInCapacity
		newDesired = g.DesiredCapacity + scalingAdjustment
	}

	newDesired = max(g.MinSize, min(g.MaxSize, newDesired))
	newDesired = min(newDesired, maxDesiredCapacity)

	if g.DesiredCapacity != newDesired {
		// Route through applyDesiredCapacityChange (shared with SetDesiredCapacity)
		// so ExecutePolicy also respects SuspendedProcesses, scale-in protection,
		// instanceIndex bookkeeping, and launch-hook gating instead of duplicating
		// (and diverging from) that logic.
		b.applyDesiredCapacityChange(g, newDesired)
	}

	return nil
}

// findStepAdjustment returns the StepAdjustment whose [MetricIntervalLowerBound,
// MetricIntervalUpperBound) interval contains diff (MetricValue-BreachThreshold), and
// whether one was found. A nil bound means unbounded in that direction, matching AWS.
func findStepAdjustment(steps []StepAdjustment, diff float64) (StepAdjustment, bool) {
	for _, s := range steps {
		lowerOK := s.MetricIntervalLowerBound == nil || diff >= *s.MetricIntervalLowerBound
		upperOK := s.MetricIntervalUpperBound == nil || diff < *s.MetricIntervalUpperBound

		if lowerOK && upperOK {
			return s, true
		}
	}

	return StepAdjustment{}, false
}

// PutScalingPolicy creates or updates a scaling policy.
func (b *InMemoryBackend) PutScalingPolicy(input ScalingPolicyInput) (*ScalingPolicy, error) {
	b.mu.Lock("PutScalingPolicy")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	arn := "arn:aws:autoscaling:" + config.DefaultRegion + ":" + config.DefaultAccountID + ":scalingPolicy:" +
		uuid.NewString() + ":autoScalingGroupName/" + input.AutoScalingGroupName + ":policyName/" + input.PolicyName

	// Preserve ARN if policy already exists
	if existing, ok := b.scalingPolicies.Get(scopedKey(input.AutoScalingGroupName, input.PolicyName)); ok {
		arn = existing.PolicyARN
	}

	policy := &ScalingPolicy{
		PolicyName:                     input.PolicyName,
		PolicyARN:                      arn,
		AutoScalingGroupName:           input.AutoScalingGroupName,
		PolicyType:                     input.PolicyType,
		AdjustmentType:                 input.AdjustmentType,
		MetricAggregationType:          input.MetricAggregationType,
		CustomizedMetricSpecification:  input.CustomizedMetricSpecification,
		ScalingAdjustment:              input.ScalingAdjustment,
		MinAdjustmentStep:              input.MinAdjustmentStep,
		MinAdjustmentMagnitude:         input.MinAdjustmentMagnitude,
		Cooldown:                       input.Cooldown,
		TargetValue:                    input.TargetValue,
		MetricType:                     input.MetricType,
		ResourceLabel:                  input.ResourceLabel,
		DisableScaleIn:                 input.DisableScaleIn,
		EstimatedWarmup:                input.EstimatedWarmup,
		StepAdjustments:                input.StepAdjustments,
		PredictiveScalingConfiguration: input.PredictiveScalingConfiguration,
	}

	b.scalingPolicies.Put(policy)

	cp := *policy

	return &cp, nil
}

// DeletePolicy removes a scaling policy from the ASG.
func (b *InMemoryBackend) DeletePolicy(groupName, policyNameOrARN string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if groupName != "" {
		// Try by name first
		key := scopedKey(groupName, policyNameOrARN)
		if b.scalingPolicies.Has(key) {
			b.scalingPolicies.Delete(key)

			return nil
		}

		// Try by ARN
		for _, p := range b.scalingPoliciesInGroupLocked(groupName) {
			if p.PolicyARN == policyNameOrARN {
				b.scalingPolicies.Delete(scalingPoliciesKeyFn(p))

				return nil
			}
		}

		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyNameOrARN)
	}

	// Check across all groups if groupName is empty
	for _, p := range b.scalingPolicies.All() {
		if p.PolicyName == policyNameOrARN || p.PolicyARN == policyNameOrARN {
			b.scalingPolicies.Delete(scalingPoliciesKeyFn(p))

			return nil
		}
	}

	return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyNameOrARN)
}

// DescribePolicies returns scaling policies for the given group, optionally
// filtered by name and/or PolicyTypes (api_op_DescribePolicies.go: "The
// valid values are SimpleScaling, StepScaling, TargetTrackingScaling, and
// PredictiveScaling").
func (b *InMemoryBackend) DescribePolicies(
	groupName string, policyNames, policyTypes []string,
) ([]ScalingPolicy, error) {
	b.mu.RLock("DescribePolicies")
	defer b.mu.RUnlock()

	nameFilter := make(map[string]bool, len(policyNames))
	for _, n := range policyNames {
		nameFilter[n] = true
	}

	typeFilter := make(map[string]bool, len(policyTypes))
	for _, t := range policyTypes {
		typeFilter[t] = true
	}

	matches := func(p *ScalingPolicy) bool {
		if len(nameFilter) > 0 && !nameFilter[p.PolicyName] {
			return false
		}

		if len(typeFilter) > 0 && !typeFilter[p.PolicyType] {
			return false
		}

		return true
	}

	var result []ScalingPolicy

	if groupName != "" {
		for _, p := range b.scalingPoliciesByGroup.Get(groupName) {
			if matches(p) {
				result = append(result, *p)
			}
		}
	} else {
		for _, p := range b.scalingPolicies.All() {
			if matches(p) {
				result = append(result, *p)
			}
		}
	}

	// PolicyName is unique only within a group (scalingPolicies is keyed by
	// scopedKey(groupName, PolicyName)), not account-wide -- when groupName is empty this scans
	// every group's policies, so two different groups can share a policy name and need
	// AutoScalingGroupName as a tiebreak for a stable pagination cursor.
	sort.Slice(result, func(i, j int) bool {
		if result[i].PolicyName != result[j].PolicyName {
			return result[i].PolicyName < result[j].PolicyName
		}

		return result[i].AutoScalingGroupName < result[j].AutoScalingGroupName
	})

	return result, nil
}
