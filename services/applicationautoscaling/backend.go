package applicationautoscaling

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

// maxTagsPerResource is the AWS limit on the number of tags per resource.
const maxTagsPerResource = 50

// maxDescribeResults is the upper bound for MaxResults on Describe* operations.
const maxDescribeResults = 100

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
	StartTime            *time.Time            `json:"startTime,omitempty"`
	EndTime              *time.Time            `json:"endTime,omitempty"`
	CreationTime         time.Time             `json:"creationTime"`
	LastModifiedTime     time.Time             `json:"lastModifiedTime"`
	ScalableTargetAction *ScalableTargetAction `json:"scalableTargetAction,omitempty"`
	ScheduledActionName  string                `json:"scheduledActionName"`
	ResourceID           string                `json:"resourceId"`
	ARN                  string                `json:"arn"`
	Schedule             string                `json:"schedule"`
	ScalableDimension    string                `json:"scalableDimension"`
	ServiceNamespace     string                `json:"serviceNamespace"`
	Timezone             string                `json:"timezone,omitempty"`
}

// InMemoryBackend stores Application Auto Scaling state in memory.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment.
	registry        *store.Registry
	scalableTargets *store.Table[ScalableTarget]
	// targetsByARN is a secondary index over scalableTargets grouping by ARN,
	// answering the "target for resource ARN X" lookups TagResource,
	// ListTagsForResource, and UntagResource need. It replaces the previous
	// map[string]string targetARNIndex reverse-lookup map.
	targetsByARN    *store.Index[ScalableTarget]
	scalingPolicies *store.Table[ScalingPolicy]
	// policiesByName is a secondary index over scalingPolicies grouping by
	// the (serviceNamespace,resourceId,scalableDimension,policyName)
	// composite key built by policyNameKey. It replaces the previous
	// map[string]string policyNameIndex reverse-lookup map.
	policiesByName   *store.Index[ScalingPolicy]
	scheduledActions *store.Table[ScheduledAction]
	// actionsByName is a secondary index over scheduledActions grouping by
	// the (serviceNamespace,resourceId,scalableDimension,scheduledActionName)
	// composite key built by actionNameKey. It replaces the previous
	// map[string]string actionNameIndex reverse-lookup map.
	actionsByName *store.Index[ScheduledAction]
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
	// scalingActivities is append-order-sensitive: DescribeScalingActivities
	// returns entries most-recent-first via slices.Backward over this exact
	// slice. store.Table has no defined insertion order (see pkgs/store's
	// package doc), so this is intentionally left as a raw slice rather than
	// converted.
	scalingActivities []*ScalingActivity
}

// ScalingActivity records a capacity-changing activity on a scalable target,
// returned by DescribeScalingActivities.
type ScalingActivity struct {
	StartTime         time.Time `json:"StartTime"`
	EndTime           time.Time `json:"EndTime"`
	ActivityID        string    `json:"ActivityId"`
	ServiceNamespace  string    `json:"ServiceNamespace"`
	ResourceID        string    `json:"ResourceId"`
	ScalableDimension string    `json:"ScalableDimension"`
	Description       string    `json:"Description"`
	Cause             string    `json:"Cause"`
	StatusCode        string    `json:"StatusCode"`
	StatusMessage     string    `json:"StatusMessage"`
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("applicationautoscaling"),
	}
	registerAllTables(b)

	return b
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
	suspendedState *SuspendedState,
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

	if len(tags) > maxTagsPerResource {
		return nil, fmt.Errorf(
			"%w: too many tags; maximum allowed is %d",
			ErrValidation,
			maxTagsPerResource,
		)
	}

	b.mu.Lock("RegisterScalableTarget")
	defer b.mu.Unlock()

	key := scalableTargetKey(serviceNamespace, resourceID, scalableDimension)
	now := time.Now().UTC()

	if existing, ok := b.scalableTargets.Get(key); ok {
		return b.updateExistingTarget(existing, minCapacity, maxCapacity, tags, roleARN, suspendedState, now)
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
		SuspendedState:   suspendedState,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}

	b.scalableTargets.Put(t)

	b.recordActivityLocked(
		serviceNamespace, resourceID, scalableDimension,
		"Setting min/max capacity",
		"target registered via RegisterScalableTarget",
		now,
	)

	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp, nil
}

// recordActivityLocked appends a completed scaling activity. The caller must
// hold the write lock.
func (b *InMemoryBackend) recordActivityLocked(
	serviceNamespace, resourceID, scalableDimension, description, cause string,
	when time.Time,
) {
	b.scalingActivities = append(b.scalingActivities, &ScalingActivity{
		ActivityID:        uuid.NewString(),
		ServiceNamespace:  serviceNamespace,
		ResourceID:        resourceID,
		ScalableDimension: scalableDimension,
		Description:       description,
		Cause:             cause,
		StartTime:         when,
		EndTime:           when,
		StatusCode:        "Successful",
		StatusMessage:     "Successfully set desired capacity.",
	})
}

// DescribeScalingActivitiesFilter carries optional filters for DescribeScalingActivities.
type DescribeScalingActivitiesFilter struct {
	// ServiceNamespace limits results to this namespace when non-empty.
	ServiceNamespace string
	// ResourceID limits results to this resource when non-empty.
	ResourceID string
	// ScalableDimension limits results to this dimension when non-empty.
	ScalableDimension string
	// NextToken is the opaque pagination cursor returned by a prior call.
	NextToken string
	// MaxResults, when > 0, limits the number of returned items. Capped at maxDescribeResults.
	MaxResults int32
}

// DescribeScalingActivities returns recorded scaling activities filtered by the
// optional fields in f, most recent first, with pagination.
func (b *InMemoryBackend) DescribeScalingActivities(f DescribeScalingActivitiesFilter) ([]*ScalingActivity, string) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	out := make([]*ScalingActivity, 0, len(b.scalingActivities))

	for _, a := range slices.Backward(b.scalingActivities) {
		if f.ServiceNamespace != "" && a.ServiceNamespace != f.ServiceNamespace {
			continue
		}

		if f.ResourceID != "" && a.ResourceID != f.ResourceID {
			continue
		}

		if f.ScalableDimension != "" && a.ScalableDimension != f.ScalableDimension {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	return paginate(out, f.MaxResults, f.NextToken, func(a *ScalingActivity) string {
		return a.ActivityID
	})
}

// mergeTags merges src into dst enforcing the per-resource tag limit.
// dst must be non-nil; callers are responsible for initialising it before the call.
// Returns an error if the merge would exceed the limit.
func mergeTags(dst map[string]string, src map[string]string) error {
	if len(src) == 0 {
		return nil
	}

	// Count net-new keys (keys that do not already exist in dst).
	netNew := 0
	for k := range src {
		if _, exists := dst[k]; !exists {
			netNew++
		}
	}

	if len(dst)+netNew > maxTagsPerResource {
		return fmt.Errorf(
			"%w: tag count would exceed maximum allowed (%d)",
			ErrValidation,
			maxTagsPerResource,
		)
	}

	maps.Copy(dst, src)

	return nil
}

// updateExistingTarget updates an existing scalable target in-place.
// Caller must hold the write lock.
func (b *InMemoryBackend) updateExistingTarget(
	existing *ScalableTarget,
	minCapacity, maxCapacity int32,
	tags map[string]string,
	roleARN string,
	suspendedState *SuspendedState,
	now time.Time,
) (*ScalableTarget, error) {
	existing.MinCapacity = minCapacity
	existing.MaxCapacity = maxCapacity
	existing.LastModifiedTime = now

	if roleARN != "" {
		existing.RoleARN = roleARN
	}

	if suspendedState != nil {
		existing.SuspendedState = suspendedState
	}

	if len(tags) > 0 {
		if existing.Tags == nil {
			existing.Tags = make(map[string]string)
		}

		if err := mergeTags(existing.Tags, tags); err != nil {
			return nil, err
		}
	}

	b.recordActivityLocked(
		existing.ServiceNamespace, existing.ResourceID, existing.ScalableDimension,
		"Setting min/max capacity",
		"target updated via RegisterScalableTarget",
		now,
	)

	cp := *existing
	cp.Tags = maps.Clone(existing.Tags)

	return &cp, nil
}

// DeregisterScalableTarget removes a scalable target.
// DeregisterScalableTarget removes a scalable target and cascades the deletion
// to all scaling policies and scheduled actions that belong to the same
// resource (AWS behaviour).
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

	if !b.scalableTargets.Has(key) {
		return fmt.Errorf("%w: scalable target %s not found", ErrNotFound, key)
	}

	b.scalableTargets.Delete(key)

	// Cascade: remove all scaling policies that belong to this target.
	// b.scalingPolicies.All() returns a fresh slice, so deleting by key while
	// ranging over it is safe (unlike ranging directly over the table's
	// internal map).
	for _, p := range b.scalingPolicies.All() {
		if p.ServiceNamespace == serviceNamespace && p.ResourceID == resourceID &&
			p.ScalableDimension == scalableDimension {
			b.scalingPolicies.Delete(p.ARN)
		}
	}

	// Cascade: remove all scheduled actions that belong to this target.
	for _, a := range b.scheduledActions.All() {
		if a.ServiceNamespace == serviceNamespace && a.ResourceID == resourceID &&
			a.ScalableDimension == scalableDimension {
			b.scheduledActions.Delete(a.ARN)
		}
	}

	return nil
}

// DescribeScalableTargetsFilter carries optional filters for DescribeScalableTargets.
type DescribeScalableTargetsFilter struct {
	ServiceNamespace  string
	ScalableDimension string
	// NextToken is the opaque pagination cursor returned by a prior call.
	NextToken   string
	ResourceIDs []string
	// MaxResults, when > 0, limits the number of returned items. Capped at maxDescribeResults.
	MaxResults int32
}

// paginate sorts list by keyFn, applies the opaque nextToken cursor, and returns
// at most maxResults items plus the token for the following page (empty when the
// page is the last). The token is the sort key of the first item of the next
// page, which is a stable cursor as long as keyFn is unique and ordering is
// deterministic. This is what lets Application Auto Scaling Describe* ops report
// a real NextToken rather than always-empty.
func paginate[T any](list []T, maxResults int32, nextToken string, keyFn func(T) string) ([]T, string) {
	sort.Slice(list, func(i, j int) bool {
		return keyFn(list[i]) < keyFn(list[j])
	})

	start := 0

	if nextToken != "" {
		for i := range list {
			if keyFn(list[i]) >= nextToken {
				start = i

				break
			}

			start = i + 1
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > int(maxDescribeResults) {
		limit = int(maxDescribeResults)
	}

	end := min(start+limit, len(list))

	page := list[start:end]

	next := ""
	if end < len(list) {
		next = keyFn(list[end])
	}

	return page, next
}

// DescribeScalableTargets lists scalable targets, optionally filtered, and
// returns the NextToken for the following page (empty on the last page).
func (b *InMemoryBackend) DescribeScalableTargets(f DescribeScalableTargetsFilter) ([]*ScalableTarget, string) {
	b.mu.RLock("DescribeScalableTargets")
	defer b.mu.RUnlock()

	var idSet map[string]bool
	if len(f.ResourceIDs) > 0 {
		idSet = make(map[string]bool, len(f.ResourceIDs))
		for _, id := range f.ResourceIDs {
			idSet[id] = true
		}
	}

	list := make([]*ScalableTarget, 0, b.scalableTargets.Len())
	for _, t := range b.scalableTargets.All() {
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

	return paginate(list, f.MaxResults, f.NextToken, func(t *ScalableTarget) string {
		return t.ResourceID + "|" + t.ScalableDimension
	})
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

// PutScheduledAction upserts a scheduled action.
func (b *InMemoryBackend) PutScheduledAction(
	serviceNamespace, resourceID, scalableDimension, scheduledActionName, schedule, timezone string,
	startTime, endTime *time.Time,
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

	if schedule == "" {
		return nil, fmt.Errorf("%w: Schedule is required", ErrValidation)
	}

	b.mu.Lock("PutScheduledAction")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	key := actionNameKey(serviceNamespace, resourceID, scalableDimension, scheduledActionName)

	if group := b.actionsByName.Get(key); len(group) > 0 {
		a := group[0]
		a.Schedule = schedule
		a.LastModifiedTime = now
		if scalableTargetAction != nil {
			a.ScalableTargetAction = scalableTargetAction
		}

		if startTime != nil {
			a.StartTime = startTime
		}

		if endTime != nil {
			a.EndTime = endTime
		}

		if timezone != "" {
			a.Timezone = timezone
		}

		cp := *a

		return &cp, nil
	}

	// Real AWS scheduled-action ARNs separate the scheduledActionName segment
	// from the resource/namespace/resourceId segment with a colon, not a
	// slash: scheduledAction:{uuid}:resource/{namespace}/{resourceId}:scheduledActionName/{name}.
	actionARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scheduledAction:%s:resource/%s/%s:scheduledActionName/%s",
			uuid.NewString(), serviceNamespace, resourceID, scheduledActionName))
	a := &ScheduledAction{
		ServiceNamespace:     serviceNamespace,
		ResourceID:           resourceID,
		ScalableDimension:    scalableDimension,
		ScheduledActionName:  scheduledActionName,
		Schedule:             schedule,
		ScalableTargetAction: scalableTargetAction,
		StartTime:            startTime,
		EndTime:              endTime,
		Timezone:             timezone,
		ARN:                  actionARN,
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.scheduledActions.Put(a)
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

	group := b.actionsByName.Get(key)
	if len(group) == 0 {
		return fmt.Errorf("%w: scheduled action %s not found", ErrNotFound, scheduledActionName)
	}

	b.scheduledActions.Delete(group[0].ARN)

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
	// NextToken is the opaque pagination cursor returned by a prior call.
	NextToken string
	// ScheduledActionNames, when non-empty, limits results to the named actions.
	ScheduledActionNames []string
	// MaxResults, when > 0, limits the number of returned items.
	MaxResults int32
}

// DescribeScheduledActions lists scheduled actions, optionally filtered, and
// returns the NextToken for the following page (empty on the last page).
func (b *InMemoryBackend) DescribeScheduledActions(f DescribeScheduledActionsFilter) ([]*ScheduledAction, string) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	var nameSet map[string]bool
	if len(f.ScheduledActionNames) > 0 {
		nameSet = make(map[string]bool, len(f.ScheduledActionNames))
		for _, n := range f.ScheduledActionNames {
			nameSet[n] = true
		}
	}

	list := make([]*ScheduledAction, 0, b.scheduledActions.Len())
	for _, a := range b.scheduledActions.All() {
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

	return paginate(list, f.MaxResults, f.NextToken, func(a *ScheduledAction) string {
		return a.ServiceNamespace + "|" + a.ResourceID + "|" + a.ScalableDimension + "|" + a.ScheduledActionName
	})
}

// TagResource adds or updates tags on a scalable target identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := group[0]

	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}

	return mergeTags(t.Tags, kv)
}

// ListTagsForResource returns tags for a scalable target identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := group[0]
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

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t := group[0]

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

	group := b.policiesByName.Get(key)
	if len(group) == 0 {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: scaling policy %s not found for %s/%s/%s",
			ErrNotFound, policyName, serviceNamespace, resourceID, scalableDimension,
		)
	}

	p := group[0]
	if p.PolicyType != "PredictiveScaling" {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: GetPredictiveScalingForecast is only supported for PredictiveScaling policies; policy %s has type %s",
			ErrValidation, policyName, p.PolicyType,
		)
	}

	// Build hourly data points in [startTime, endTime).
	// Truncate always rounds down; if startTime is not on an exact hour boundary
	// the truncated value precedes startTime, so we advance by one hour.
	// When startTime is exactly on an hour boundary, truncation is a no-op and
	// the condition is false, keeping the boundary as the first point.
	start := startTime.Truncate(time.Hour)
	if start.Before(startTime) {
		start = start.Add(time.Hour)
	}

	// Preallocate with the exact known capacity to avoid slice growth.
	numPoints := max(0, int(endTime.Sub(start)/time.Hour))

	timestamps := make([]time.Time, 0, numPoints)

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

	b.registry.ResetAll()
	b.scalingActivities = nil
}

// Purge removes all resources from the backend. It is safe to call concurrently.
func (b *InMemoryBackend) Purge() {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.scalingActivities = nil
}
