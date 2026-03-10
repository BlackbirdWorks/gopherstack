package applicationautoscaling

import (
	"fmt"
	"maps"

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
)

// ScalableTarget represents a registered Application Auto Scaling scalable target.
type ScalableTarget struct {
	Tags              map[string]string `json:"tags,omitempty"`
	ResourceID        string            `json:"resourceId"`
	ARN               string            `json:"arn"`
	ScalableDimension string            `json:"scalableDimension"`
	ServiceNamespace  string            `json:"serviceNamespace"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	MinCapacity       int32             `json:"minCapacity"`
	MaxCapacity       int32             `json:"maxCapacity"`
}

// ScalingPolicy represents an Application Auto Scaling scaling policy.
type ScalingPolicy struct {
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
	ScheduledActionName string `json:"scheduledActionName"`
	ResourceID          string `json:"resourceId"`
	ARN                 string `json:"arn"`
	Schedule            string `json:"schedule"`
	ScalableDimension   string `json:"scalableDimension"`
	ServiceNamespace    string `json:"serviceNamespace"`
}

// InMemoryBackend stores Application Auto Scaling state in memory.
type InMemoryBackend struct {
	scalableTargets  map[string]*ScalableTarget
	scalingPolicies  map[string]*ScalingPolicy
	scheduledActions map[string]*ScheduledAction
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

// RegisterScalableTarget upserts a scalable target (creates or updates).
func (b *InMemoryBackend) RegisterScalableTarget(
	serviceNamespace, resourceID, scalableDimension string,
	minCapacity, maxCapacity int32,
) (*ScalableTarget, error) {
	b.mu.Lock("RegisterScalableTarget")
	defer b.mu.Unlock()

	key := scalableTargetKey(serviceNamespace, resourceID, scalableDimension)

	if existing, ok := b.scalableTargets[key]; ok {
		// Update in place, then return a copy to prevent callers from
		// directly mutating backend-owned state.
		existing.MinCapacity = minCapacity
		existing.MaxCapacity = maxCapacity
		cp := *existing

		return &cp, nil
	}

	targetARN := arn.Build("application-autoscaling", b.region, b.accountID, "scalable-target/"+uuid.NewString())
	t := &ScalableTarget{
		ServiceNamespace:  serviceNamespace,
		ResourceID:        resourceID,
		ScalableDimension: scalableDimension,
		MinCapacity:       minCapacity,
		MaxCapacity:       maxCapacity,
		ARN:               targetARN,
		AccountID:         b.accountID,
		Region:            b.region,
		Tags:              make(map[string]string),
	}
	b.scalableTargets[key] = t
	cp := *t

	return &cp, nil
}

// DeregisterScalableTarget removes a scalable target.
func (b *InMemoryBackend) DeregisterScalableTarget(serviceNamespace, resourceID, scalableDimension string) error {
	b.mu.Lock("DeregisterScalableTarget")
	defer b.mu.Unlock()

	key := scalableTargetKey(serviceNamespace, resourceID, scalableDimension)
	if _, ok := b.scalableTargets[key]; !ok {
		return fmt.Errorf("%w: scalable target %s not found", ErrNotFound, key)
	}
	delete(b.scalableTargets, key)

	return nil
}

// DescribeScalableTargets lists scalable targets, optionally filtered by service namespace.
func (b *InMemoryBackend) DescribeScalableTargets(serviceNamespace string) []*ScalableTarget {
	b.mu.RLock("DescribeScalableTargets")
	defer b.mu.RUnlock()

	list := make([]*ScalableTarget, 0, len(b.scalableTargets))
	for _, t := range b.scalableTargets {
		if serviceNamespace != "" && t.ServiceNamespace != serviceNamespace {
			continue
		}
		cp := *t

		list = append(list, &cp)
	}

	return list
}

// PutScalingPolicy upserts a scaling policy (update if policyName matches for resource, create otherwise).
func (b *InMemoryBackend) PutScalingPolicy(
	serviceNamespace, resourceID, scalableDimension, policyName, policyType string,
) (*ScalingPolicy, error) {
	b.mu.Lock("PutScalingPolicy")
	defer b.mu.Unlock()

	for _, p := range b.scalingPolicies {
		if p.ServiceNamespace == serviceNamespace &&
			p.ResourceID == resourceID &&
			p.ScalableDimension == scalableDimension &&
			p.PolicyName == policyName {
			cp := *p

			return &cp, nil
		}
	}

	policyARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scalingPolicy:%s:resource/%s/%s/policyName/%s",
			uuid.NewString(), serviceNamespace, resourceID, policyName))
	p := &ScalingPolicy{
		ServiceNamespace:  serviceNamespace,
		ResourceID:        resourceID,
		ScalableDimension: scalableDimension,
		PolicyName:        policyName,
		PolicyType:        policyType,
		ARN:               policyARN,
	}
	b.scalingPolicies[policyARN] = p
	cp := *p

	return &cp, nil
}

// DeleteScalingPolicy removes a scaling policy by ARN.
func (b *InMemoryBackend) DeleteScalingPolicy(
	serviceNamespace, resourceID, scalableDimension, policyName string,
) error {
	b.mu.Lock("DeleteScalingPolicy")
	defer b.mu.Unlock()

	for k, p := range b.scalingPolicies {
		if p.ServiceNamespace == serviceNamespace &&
			p.ResourceID == resourceID &&
			p.ScalableDimension == scalableDimension &&
			p.PolicyName == policyName {
			delete(b.scalingPolicies, k)

			return nil
		}
	}

	return fmt.Errorf("%w: scaling policy %s not found", ErrNotFound, policyName)
}

// DescribeScalingPolicies lists scaling policies, optionally filtered by service namespace.
func (b *InMemoryBackend) DescribeScalingPolicies(serviceNamespace string) []*ScalingPolicy {
	b.mu.RLock("DescribeScalingPolicies")
	defer b.mu.RUnlock()

	list := make([]*ScalingPolicy, 0, len(b.scalingPolicies))
	for _, p := range b.scalingPolicies {
		if serviceNamespace != "" && p.ServiceNamespace != serviceNamespace {
			continue
		}
		cp := *p
		list = append(list, &cp)
	}

	return list
}

// PutScheduledAction upserts a scheduled action.
func (b *InMemoryBackend) PutScheduledAction(
	serviceNamespace, resourceID, scalableDimension, scheduledActionName, schedule string,
) (*ScheduledAction, error) {
	b.mu.Lock("PutScheduledAction")
	defer b.mu.Unlock()

	for _, a := range b.scheduledActions {
		if a.ServiceNamespace == serviceNamespace &&
			a.ResourceID == resourceID &&
			a.ScalableDimension == scalableDimension &&
			a.ScheduledActionName == scheduledActionName {
			a.Schedule = schedule
			cp := *a

			return &cp, nil
		}
	}

	actionARN := arn.Build("autoscaling", b.region, b.accountID,
		fmt.Sprintf("scheduledAction:%s:resource/%s/%s/scheduledActionName/%s",
			uuid.NewString(), serviceNamespace, resourceID, scheduledActionName))
	a := &ScheduledAction{
		ServiceNamespace:    serviceNamespace,
		ResourceID:          resourceID,
		ScalableDimension:   scalableDimension,
		ScheduledActionName: scheduledActionName,
		Schedule:            schedule,
		ARN:                 actionARN,
	}
	b.scheduledActions[actionARN] = a
	cp := *a

	return &cp, nil
}

// DeleteScheduledAction removes a scheduled action.
func (b *InMemoryBackend) DeleteScheduledAction(
	serviceNamespace, resourceID, scalableDimension, scheduledActionName string,
) error {
	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	for k, a := range b.scheduledActions {
		if a.ServiceNamespace == serviceNamespace &&
			a.ResourceID == resourceID &&
			a.ScalableDimension == scalableDimension &&
			a.ScheduledActionName == scheduledActionName {
			delete(b.scheduledActions, k)

			return nil
		}
	}

	return fmt.Errorf("%w: scheduled action %s not found", ErrNotFound, scheduledActionName)
}

// DescribeScheduledActions lists scheduled actions, optionally filtered by service namespace.
func (b *InMemoryBackend) DescribeScheduledActions(serviceNamespace string) []*ScheduledAction {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	list := make([]*ScheduledAction, 0, len(b.scheduledActions))
	for _, a := range b.scheduledActions {
		if serviceNamespace != "" && a.ServiceNamespace != serviceNamespace {
			continue
		}
		cp := *a
		list = append(list, &cp)
	}

	return list
}

// TagResource adds or updates tags on a scalable target identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, t := range b.scalableTargets {
		if t.ARN == resourceARN {
			if t.Tags == nil {
				t.Tags = make(map[string]string)
			}
			maps.Copy(t.Tags, kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a scalable target identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, t := range b.scalableTargets {
		if t.ARN == resourceARN {
			out := make(map[string]string, len(t.Tags))
			maps.Copy(out, t.Tags)

			return out, nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a scalable target identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, t := range b.scalableTargets {
		if t.ARN == resourceARN {
			for _, k := range tagKeys {
				delete(t.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
