package applicationautoscaling

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ValidationException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
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

// maxForecastWindow is the maximum allowed [startTime, endTime) range for
// GetPredictiveScalingForecast, matching the real AWS constraint of 14 days.
const maxForecastWindow = 14 * 24 * time.Hour

// SuspendedState represents the suspension configuration for a scalable target.
// Each field independently suspends a category of scaling activity.
type SuspendedState struct {
	DynamicScalingInSuspended  bool `json:"dynamicScalingInSuspended"`
	DynamicScalingOutSuspended bool `json:"dynamicScalingOutSuspended"`
	ScheduledScalingSuspended  bool `json:"scheduledScalingSuspended"`
}

// ScalableTargetAction holds the capacity bounds for a scheduled action.
type ScalableTargetAction struct {
	MinCapacity *int32 `json:"minCapacity,omitempty"`
	MaxCapacity *int32 `json:"maxCapacity,omitempty"`
}

// ScalableTarget represents a registered Application Auto Scaling scalable target.
type ScalableTarget struct {
	CreationTime      time.Time         `json:"creationTime"`
	LastModifiedTime  time.Time         `json:"lastModifiedTime"`
	SuspendedState    *SuspendedState   `json:"suspendedState,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
	ResourceID        string            `json:"resourceId"`
	ARN               string            `json:"arn"`
	RoleARN           string            `json:"roleArn,omitempty"`
	ScalableDimension string            `json:"scalableDimension"`
	ServiceNamespace  string            `json:"serviceNamespace"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	MinCapacity       int32             `json:"minCapacity"`
	MaxCapacity       int32             `json:"maxCapacity"`
}

// ScalingPolicy represents an Application Auto Scaling scaling policy.
type ScalingPolicy struct {
	CreationTime         time.Time      `json:"creationTime"`
	LastModifiedTime     time.Time      `json:"lastModifiedTime"`
	TargetTrackingConfig map[string]any `json:"targetTrackingConfig,omitempty"`
	StepScalingConfig    map[string]any `json:"stepScalingConfig,omitempty"`
	PolicyType           string         `json:"policyType"`
	PolicyName           string         `json:"policyName"`
	ResourceID           string         `json:"resourceId"`
	ARN                  string         `json:"arn"`
	ScalableDimension    string         `json:"scalableDimension"`
	ServiceNamespace     string         `json:"serviceNamespace"`
}

// ScheduledAction represents an Application Auto Scaling scheduled action.
type ScheduledAction struct {
	CreationTime         time.Time             `json:"creationTime"`
	LastModifiedTime     time.Time             `json:"lastModifiedTime"`
	ScalableTargetAction *ScalableTargetAction `json:"scalableTargetAction,omitempty"`
	ScheduledActionName  string                `json:"scheduledActionName"`
	ResourceID           string                `json:"resourceId"`
	ARN                  string                `json:"arn"`
	Schedule             string                `json:"schedule"`
	ScalableDimension    string                `json:"scalableDimension"`
	ServiceNamespace     string                `json:"serviceNamespace"`
}

// InMemoryBackend stores Application Auto Scaling state in memory.
type InMemoryBackend struct {
	scalableTargets  map[string]*ScalableTarget
	scalingPolicies  map[string]*ScalingPolicy
	scheduledActions map[string]*ScheduledAction
	targetARNIndex   map[string]string // ARN → scalableTargetKey
	policyNameIndex  map[string]string // policyNameKey → policyARN (secondary index for O(1) lookup)
	actionNameIndex  map[string]string // actionNameKey → actionARN (secondary index for O(1) lookup)
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		scalableTargets:  make(map[string]*ScalableTarget),
		scalingPolicies:  make(map[string]*ScalingPolicy),
		scheduledActions: make(map[string]*ScheduledAction),
		targetARNIndex:   make(map[string]string),
		policyNameIndex:  make(map[string]string),
		actionNameIndex:  make(map[string]string),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("applicationautoscaling"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// scalableTargetKey returns the backend key for a scalable target.
func scalableTargetKey(serviceNamespace, resourceID, scalableDimension string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension
}

// policyNameKey returns the secondary-index key for a scaling policy.
func policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension + "/" + policyName
}

// actionNameKey returns the secondary-index key for a scheduled action.
func actionNameKey(serviceNamespace, resourceID, scalableDimension, scheduledActionName string) string {
	return serviceNamespace + "/" + resourceID + "/" + scalableDimension + "/" + scheduledActionName
}

// RegisterScalableTarget upserts a scalable target (creates or updates).
func (b *InMemoryBackend) RegisterScalableTarget(
	serviceNamespace, resourceID, scalableDimension string,
	minCapacity, maxCapacity int32,
	tags map[string]string,
	roleARN string,
) (*ScalableTarget, error) {
	if serviceNamespace == "" {
		return nil, fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	if resourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	if scalableDimension == "" {
		return nil, fmt.Errorf("%w: ScalableDimension is required", ErrValidation)
	}

	if minCapacity > maxCapacity {
		return nil, fmt.Errorf(
			"%w: MinCapacity (%d) must not exceed MaxCapacity (%d)",
			ErrValidation,
			minCapacity,
			maxCapacity,
		)
	}

	b.mu.Lock("RegisterScalableTarget")
	defer b.mu.Unlock()

	key := scalableTargetKey(serviceNamespace, resourceID, scalableDimension)
	now := time.Now().UTC()

	if existing, ok := b.scalableTargets[key]; ok {
		// Update in place, then return a copy to prevent callers from
		// directly mutating backend-owned state.
		existing.MinCapacity = minCapacity
		existing.MaxCapacity = maxCapacity
		existing.LastModifiedTime = now
		if roleARN != "" {
			existing.RoleARN = roleARN
		}

		if len(tags) > 0 {
			if existing.Tags == nil {
				existing.Tags = make(map[string]string)
			}

			maps.Copy(existing.Tags, tags)
		}

		cp := *existing
		cp.Tags = maps.Clone(existing.Tags)

		return &cp, nil
	}

	t := &ScalableTarget{
		ServiceNamespace:  serviceNamespace,
		ResourceID:        resourceID,
		ScalableDimension: scalableDimension,
		MinCapacity:       minCapacity,
		MaxCapacity:       maxCapacity,
		ARN: arn.Build(
			"application-autoscaling",
			b.region,
			b.accountID,
			"scalable-target/"+uuid.NewString(),
		),
		RoleARN:          roleARN,
		AccountID:        b.accountID,
		Region:           b.region,
		Tags:             maps.Clone(tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}

	b.scalableTargets[key] = t
	b.targetARNIndex[t.ARN] = key
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp, nil
}

// DeregisterScalableTarget removes a scalable target.
func (b *InMemoryBackend) DeregisterScalableTarget(serviceNamespace, resourceID, scalableDimension string) error {
	if serviceNamespace == "" {
		return fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	if resourceID == "" {
		return fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	if scalableDimension == "" {
		return fmt.Errorf("%w: ScalableDimension is required", ErrValidation)
	}

	b.mu.Lock("DeregisterScalableTarget")
	defer b.mu.Unlock()

	key := scalableTargetKey(serviceNamespace, resourceID, scalableDimension)

	t, ok := b.scalableTargets[key]
	if !ok {
		return fmt.Errorf("%w: scalable target %s not found", ErrNotFound, key)
	}

	delete(b.targetARNIndex, t.ARN)
	delete(b.scalableTargets, key)

	return nil
}

// DescribeScalableTargetsFilter carries optional filters for DescribeScalableTargets.
type DescribeScalableTargetsFilter struct {
	ServiceNamespace  string
	ScalableDimension string
	ResourceIDs       []string
}

// DescribeScalableTargets lists scalable targets, optionally filtered.
func (b *InMemoryBackend) DescribeScalableTargets(f DescribeScalableTargetsFilter) []*ScalableTarget {
	b.mu.RLock("DescribeScalableTargets")
	defer b.mu.RUnlock()

	var idSet map[string]bool
	if len(f.ResourceIDs) > 0 {
		idSet = make(map[string]bool, len(f.ResourceIDs))
		for _, id := range f.ResourceIDs {
			idSet[id] = true
		}
	}

	list := make([]*ScalableTarget, 0, len(b.scalableTargets))
	for _, t := range b.scalableTargets {
		if f.ServiceNamespace != "" && t.ServiceNamespace != f.ServiceNamespace {
			continue
		}

		if idSet != nil && !idSet[t.ResourceID] {
			continue
		}

		if f.ScalableDimension != "" && t.ScalableDimension != f.ScalableDimension {
			continue
		}

		cp := *t
		cp.Tags = maps.Clone(t.Tags)

		list = append(list, &cp)
	}

	return list
}

// PutScalingPolicy upserts a scaling policy (update if policyName matches for resource, create otherwise).
// cloneScalingPolicy returns a deep copy of p with config maps cloned.
func cloneScalingPolicy(p *ScalingPolicy) *ScalingPolicy {
	cp := *p
	cp.TargetTrackingConfig = maps.Clone(p.TargetTrackingConfig)
	cp.StepScalingConfig = maps.Clone(p.StepScalingConfig)

	return &cp
}

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

	if policyType == "" {
		policyType = "TargetTrackingScaling"
	} else if !isValidPolicyType(policyType) {
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

	if existingARN, ok := b.policyNameIndex[key]; ok {
		p := b.scalingPolicies[existingARN]
		p.TargetTrackingConfig = maps.Clone(targetTrackingConfig)
		p.StepScalingConfig = maps.Clone(stepScalingConfig)
		p.LastModifiedTime = now

		return cloneScalingPolicy(p), nil
	}

	policyARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scalingPolicy:%s:resource/%s/%s/policyName/%s",
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
	b.scalingPolicies[policyARN] = p
	b.policyNameIndex[key] = policyARN
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

	existingARN, ok := b.policyNameIndex[key]
	if !ok {
		return fmt.Errorf("%w: scaling policy %s not found", ErrNotFound, policyName)
	}

	delete(b.scalingPolicies, existingARN)
	delete(b.policyNameIndex, key)

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
}

// DescribeScalingPolicies lists scaling policies, optionally filtered.
func (b *InMemoryBackend) DescribeScalingPolicies(f DescribeScalingPoliciesFilter) []*ScalingPolicy {
	b.mu.RLock("DescribeScalingPolicies")
	defer b.mu.RUnlock()

	var nameSet map[string]bool
	if len(f.PolicyNames) > 0 {
		nameSet = make(map[string]bool, len(f.PolicyNames))
		for _, n := range f.PolicyNames {
			nameSet[n] = true
		}
	}

	list := make([]*ScalingPolicy, 0, len(b.scalingPolicies))
	for _, p := range b.scalingPolicies {
		if f.ServiceNamespace != "" && p.ServiceNamespace != f.ServiceNamespace {
			continue
		}

		if f.ResourceID != "" && p.ResourceID != f.ResourceID {
			continue
		}

		if f.ScalableDimension != "" && p.ScalableDimension != f.ScalableDimension {
			continue
		}

		if nameSet != nil && !nameSet[p.PolicyName] {
			continue
		}

		list = append(list, cloneScalingPolicy(p))
	}

	return list
}

// PutScheduledAction upserts a scheduled action.
func (b *InMemoryBackend) PutScheduledAction(
	serviceNamespace, resourceID, scalableDimension, scheduledActionName, schedule string,
	scalableTargetAction *ScalableTargetAction,
) (*ScheduledAction, error) {
	if serviceNamespace == "" {
		return nil, fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	if resourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	if scalableDimension == "" {
		return nil, fmt.Errorf("%w: ScalableDimension is required", ErrValidation)
	}

	if scheduledActionName == "" {
		return nil, fmt.Errorf("%w: ScheduledActionName is required", ErrValidation)
	}

	b.mu.Lock("PutScheduledAction")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	key := actionNameKey(serviceNamespace, resourceID, scalableDimension, scheduledActionName)

	if existingARN, ok := b.actionNameIndex[key]; ok {
		a := b.scheduledActions[existingARN]
		a.Schedule = schedule
		a.LastModifiedTime = now
		if scalableTargetAction != nil {
			a.ScalableTargetAction = scalableTargetAction
		}

		cp := *a

		return &cp, nil
	}

	actionARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scheduledAction:%s:resource/%s/%s/scheduledActionName/%s",
			uuid.NewString(), serviceNamespace, resourceID, scheduledActionName))
	a := &ScheduledAction{
		ServiceNamespace:     serviceNamespace,
		ResourceID:           resourceID,
		ScalableDimension:    scalableDimension,
		ScheduledActionName:  scheduledActionName,
		Schedule:             schedule,
		ScalableTargetAction: scalableTargetAction,
		ARN:                  actionARN,
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.scheduledActions[actionARN] = a
	b.actionNameIndex[key] = actionARN
	cp := *a

	return &cp, nil
}

// DeleteScheduledAction removes a scheduled action.
func (b *InMemoryBackend) DeleteScheduledAction(
	serviceNamespace, resourceID, scalableDimension, scheduledActionName string,
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

	if scheduledActionName == "" {
		return fmt.Errorf("%w: ScheduledActionName is required", ErrValidation)
	}

	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	key := actionNameKey(serviceNamespace, resourceID, scalableDimension, scheduledActionName)

	existingARN, ok := b.actionNameIndex[key]
	if !ok {
		return fmt.Errorf("%w: scheduled action %s not found", ErrNotFound, scheduledActionName)
	}

	delete(b.scheduledActions, existingARN)
	delete(b.actionNameIndex, key)

	return nil
}

// DescribeScheduledActionsFilter carries optional filters for DescribeScheduledActions.
type DescribeScheduledActionsFilter struct {
	// ServiceNamespace limits results to this namespace when non-empty.
	ServiceNamespace string
	// ResourceID limits results to this resource when non-empty.
	ResourceID string
	// ScalableDimension limits results to this dimension when non-empty.
	ScalableDimension string
	// ScheduledActionNames, when non-empty, limits results to the named actions.
	ScheduledActionNames []string
}

// DescribeScheduledActions lists scheduled actions, optionally filtered.
func (b *InMemoryBackend) DescribeScheduledActions(f DescribeScheduledActionsFilter) []*ScheduledAction {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	var nameSet map[string]bool
	if len(f.ScheduledActionNames) > 0 {
		nameSet = make(map[string]bool, len(f.ScheduledActionNames))
		for _, n := range f.ScheduledActionNames {
			nameSet[n] = true
		}
	}

	list := make([]*ScheduledAction, 0, len(b.scheduledActions))
	for _, a := range b.scheduledActions {
		if f.ServiceNamespace != "" && a.ServiceNamespace != f.ServiceNamespace {
			continue
		}

		if f.ResourceID != "" && a.ResourceID != f.ResourceID {
			continue
		}

		if f.ScalableDimension != "" && a.ScalableDimension != f.ScalableDimension {
			continue
		}

		if nameSet != nil && !nameSet[a.ScheduledActionName] {
			continue
		}

		cp := *a
		list = append(list, &cp)
	}

	return list
}

// TagResource adds or updates tags on a scalable target identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	key, ok := b.targetARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := b.scalableTargets[key]

	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}

	maps.Copy(t.Tags, kv)

	return nil
}

// ListTagsForResource returns tags for a scalable target identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	key, ok := b.targetARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := b.scalableTargets[key]
	out := make(map[string]string, len(t.Tags))
	maps.Copy(out, t.Tags)

	return out, nil
}

// UntagResource removes tags from a scalable target identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	key, ok := b.targetARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := b.scalableTargets[key]

	for _, k := range tagKeys {
		delete(t.Tags, k)
	}

	return nil
}

// CapacityForecastData holds the timestamps and capacity values for a forecast.
type CapacityForecastData struct {
	Timestamps []time.Time
	Values     []float64
}

// LoadForecastData holds the timestamps, values, and a metric specification label for a load forecast.
type LoadForecastData struct {
	MetricSpecification string
	Timestamps          []time.Time
	Values              []float64
}

// GetPredictiveScalingForecast returns simulated hourly forecast data for the requested
// policy over the given time window. It verifies the associated scaling policy exists.
func (b *InMemoryBackend) GetPredictiveScalingForecast(
	serviceNamespace, resourceID, scalableDimension, policyName string,
	startTime, endTime time.Time,
) (*CapacityForecastData, []LoadForecastData, time.Time, error) {
	if !endTime.After(startTime) {
		return nil, nil, time.Time{}, fmt.Errorf("%w: EndTime must be after StartTime", ErrValidation)
	}

	if endTime.Sub(startTime) > maxForecastWindow {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: forecast window must not exceed 14 days",
			ErrValidation,
		)
	}

	b.mu.RLock("GetPredictiveScalingForecast")
	defer b.mu.RUnlock()

	key := policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName)
	if _, ok := b.policyNameIndex[key]; !ok {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: scaling policy %s not found for %s/%s/%s",
			ErrNotFound, policyName, serviceNamespace, resourceID, scalableDimension,
		)
	}

	// Build hourly data points in [startTime, endTime).
	// Start from the first complete hour boundary >= startTime to avoid
	// emitting timestamps that precede the requested window.
	start := startTime.Truncate(time.Hour)
	if start.Before(startTime) {
		start = start.Add(time.Hour)
	}

	timestamps := make([]time.Time, 0)

	for t := start; t.Before(endTime); t = t.Add(time.Hour) {
		timestamps = append(timestamps, t)
	}

	values := make([]float64, len(timestamps))
	for i := range values {
		values[i] = 10.0
	}

	capacity := &CapacityForecastData{
		Timestamps: timestamps,
		Values:     values,
	}

	load := []LoadForecastData{
		{
			Timestamps:          timestamps,
			Values:              values,
			MetricSpecification: fmt.Sprintf("%s/%s/%s", serviceNamespace, resourceID, scalableDimension),
		},
	}

	return capacity, load, time.Now().UTC(), nil
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.scalableTargets = make(map[string]*ScalableTarget)
	b.scalingPolicies = make(map[string]*ScalingPolicy)
	b.scheduledActions = make(map[string]*ScheduledAction)
	b.targetARNIndex = make(map[string]string)
	b.policyNameIndex = make(map[string]string)
	b.actionNameIndex = make(map[string]string)
}
