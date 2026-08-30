package autoscaling

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// BatchDeleteScheduledAction removes the named scheduled actions from the group.
// Actions that cannot be found are returned as failures.
func (b *InMemoryBackend) BatchDeleteScheduledAction(
	groupName string,
	scheduledActionNames []string,
) ([]FailedScheduledAction, error) {
	b.mu.Lock("BatchDeleteScheduledAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	failed := make([]FailedScheduledAction, 0, len(scheduledActionNames))

	for _, name := range scheduledActionNames {
		key := scopedKey(groupName, name)
		if !b.scheduledActions.Has(key) {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: name,
				ErrorCode:           errValidationError,
				ErrorMessage:        fmt.Sprintf("scheduled action %q not found", name),
			})

			continue
		}

		b.scheduledActions.Delete(key)
	}

	return failed, nil
}

// BatchPutScheduledUpdateGroupAction creates or updates scheduled actions for the group.
func (b *InMemoryBackend) BatchPutScheduledUpdateGroupAction(
	groupName string,
	actions []ScheduledUpdateGroupAction,
) ([]FailedScheduledAction, error) {
	b.mu.Lock("BatchPutScheduledUpdateGroupAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	failed := make([]FailedScheduledAction, 0, len(actions))

	for _, a := range actions {
		if a.ScheduledActionName == "" {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: a.ScheduledActionName,
				ErrorCode:           errValidationError,
				ErrorMessage:        "ScheduledActionName is required",
			})

			continue
		}

		b.scheduledActions.Put(&ScheduledAction{
			ScheduledActionName:  a.ScheduledActionName,
			AutoScalingGroupName: groupName,
			Recurrence:           a.Recurrence,
			TimeZone:             a.TimeZone,
			StartTime:            a.StartTime,
			EndTime:              a.EndTime,
			DesiredCapacity:      a.DesiredCapacity,
			MinSize:              a.MinSize,
			MaxSize:              a.MaxSize,
		})
	}

	return failed, nil
}

// DescribeScheduledActions returns scheduled actions for the given group,
// optionally filtered by name, or by [startTime, endTime] against each
// action's StartTime (api_op_DescribeScheduledActions.go: "If scheduled
// action names are provided, this property is ignored" -- so the time range
// only applies when actionNames is empty, matching the branch below). A
// zero startTime/endTime means that bound is not documented/not supplied.
func (b *InMemoryBackend) DescribeScheduledActions(
	groupName string,
	actionNames []string,
	startTime, endTime time.Time,
) ([]ScheduledAction, error) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	if groupName != "" && !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	if len(actionNames) > 0 && groupName != "" {
		return b.scheduledActionsByNamesLocked(groupName, actionNames), nil
	}

	result := b.scheduledActionsInTimeRangeLocked(groupName, startTime, endTime)

	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledActionName < result[j].ScheduledActionName
	})

	return result, nil
}

// scheduledActionsByNamesLocked looks up each named scheduled action for
// groupName, skipping unknown names. The caller must hold at least a read
// lock.
func (b *InMemoryBackend) scheduledActionsByNamesLocked(groupName string, actionNames []string) []ScheduledAction {
	result := make([]ScheduledAction, 0, len(actionNames))

	for _, name := range actionNames {
		a, exists := b.scheduledActions.Get(scopedKey(groupName, name))
		if !exists {
			continue
		}

		result = append(result, *a)
	}

	return result
}

// scheduledActionsInTimeRangeLocked returns every scheduled action for
// groupName (or account-wide when empty) whose StartTime falls within
// [startTime, endTime]; a zero bound is unset. The caller must hold at least
// a read lock.
func (b *InMemoryBackend) scheduledActionsInTimeRangeLocked(
	groupName string, startTime, endTime time.Time,
) []ScheduledAction {
	matchesTimeRange := func(a *ScheduledAction) bool {
		if !startTime.IsZero() && a.StartTime.Before(startTime) {
			return false
		}

		return endTime.IsZero() || !a.StartTime.After(endTime)
	}

	var result []ScheduledAction

	actions := b.scheduledActions.All()
	if groupName != "" {
		actions = b.scheduledActionsByGroup.Get(groupName)
	}

	for _, a := range actions {
		if matchesTimeRange(a) {
			result = append(result, *a)
		}
	}

	return result
}

// PutScheduledUpdateGroupAction creates or updates a single scheduled action.
func (b *InMemoryBackend) PutScheduledUpdateGroupAction(groupName string, action ScheduledUpdateGroupAction) error {
	b.mu.Lock("PutScheduledUpdateGroupAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	scheduledARN := fmt.Sprintf(
		"arn:aws:autoscaling:%s:%s:scheduledUpdateGroupAction:%s:autoScalingGroupName/%s:scheduledActionName/%s",
		config.DefaultRegion, config.DefaultAccountID, uuid.NewString(), groupName, action.ScheduledActionName,
	)

	b.scheduledActions.Put(&ScheduledAction{
		ScheduledActionName:  action.ScheduledActionName,
		ScheduledActionARN:   scheduledARN,
		AutoScalingGroupName: groupName,
		Recurrence:           action.Recurrence,
		TimeZone:             action.TimeZone,
		StartTime:            action.StartTime,
		EndTime:              action.EndTime,
		DesiredCapacity:      action.DesiredCapacity,
		MinSize:              action.MinSize,
		MaxSize:              action.MaxSize,
	})

	return nil
}

// DeleteScheduledAction removes a single scheduled action from the ASG.
func (b *InMemoryBackend) DeleteScheduledAction(groupName, scheduledActionName string) error {
	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	key := scopedKey(groupName, scheduledActionName)
	if !b.scheduledActions.Has(key) {
		return fmt.Errorf("%w: scheduled action %q not found", ErrInvalidParameter, scheduledActionName)
	}

	b.scheduledActions.Delete(key)

	return nil
}
