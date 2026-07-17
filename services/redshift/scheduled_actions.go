package redshift

import "fmt"

// CreateScheduledAction creates a new Redshift scheduled action.
func (b *InMemoryBackend) CreateScheduledAction(
	name, schedule, iamRole, description, targetAction string,
) (*ScheduledAction, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateScheduledAction")
	defer b.mu.Unlock()

	if _, exists := b.scheduledActions.Get(name); exists {
		return nil, fmt.Errorf("%w: scheduled action %s already exists", ErrScheduledActionAlreadyExists, name)
	}

	action := &ScheduledAction{
		ScheduledActionName:        name,
		Schedule:                   schedule,
		IamRole:                    iamRole,
		ScheduledActionDescription: description,
		State:                      "ACTIVE",
		TargetAction:               targetAction,
	}
	b.scheduledActions.Put(action)

	cp := *action

	return &cp, nil
}

// DeleteScheduledAction deletes the named scheduled action.
func (b *InMemoryBackend) DeleteScheduledAction(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	if _, exists := b.scheduledActions.Get(name); !exists {
		return fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
	}

	b.scheduledActions.Delete(name)

	return nil
}

// DescribeScheduledActions returns scheduled actions, optionally filtered by name.
func (b *InMemoryBackend) DescribeScheduledActions(name string) ([]ScheduledAction, error) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	if name != "" {
		a, exists := b.scheduledActions.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
		}

		cp := *a

		return []ScheduledAction{cp}, nil
	}

	result := make([]ScheduledAction, 0, b.scheduledActions.Len())

	for _, a := range b.scheduledActions.All() {
		result = append(result, *a)
	}

	return result, nil
}

// ModifyScheduledAction updates a scheduled action's schedule, IAM role, or description.
func (b *InMemoryBackend) ModifyScheduledAction(
	name, schedule, iamRole, description string,
) (*ScheduledAction, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ScheduledActionName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyScheduledAction")
	defer b.mu.Unlock()

	a, exists := b.scheduledActions.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: scheduled action %s not found", ErrScheduledActionNotFound, name)
	}

	if schedule != "" {
		a.Schedule = schedule
	}
	if iamRole != "" {
		a.IamRole = iamRole
	}
	if description != "" {
		a.ScheduledActionDescription = description
	}

	cp := *a

	return &cp, nil
}
