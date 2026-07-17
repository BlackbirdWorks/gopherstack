package autoscaling

import (
	"fmt"
	"sort"

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

// DescribeScheduledActions returns scheduled actions for the given group, optionally filtered by name.
func (b *InMemoryBackend) DescribeScheduledActions(
	groupName string,
	actionNames []string,
) ([]ScheduledAction, error) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}
	}

	if len(actionNames) > 0 && groupName != "" {
		result := make([]ScheduledAction, 0, len(actionNames))

		for _, name := range actionNames {
			a, exists := b.scheduledActions.Get(scopedKey(groupName, name))
			if !exists {
				continue
			}

			result = append(result, *a)
		}

		return result, nil
	}

	var result []ScheduledAction

	if groupName != "" {
		for _, a := range b.scheduledActionsByGroup.Get(groupName) {
			result = append(result, *a)
		}
	} else {
		for _, a := range b.scheduledActions.All() {
			result = append(result, *a)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledActionName < result[j].ScheduledActionName
	})

	return result, nil
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
