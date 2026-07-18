package applicationautoscaling

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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
