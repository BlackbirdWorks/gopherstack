package redshift

import "fmt"

// scheduledActionStateActiveValue is the wire State value scheduledActionState
// returns for an enabled scheduled action. Named locally (rather than reusing
// dataShareStatusActive, which shares the "ACTIVE" string by coincidence) so a
// future change to either enum can't silently desync the other.
const scheduledActionStateActiveValue = "ACTIVE"

// scheduledActionState returns the wire State ("ACTIVE"/"DISABLED") for the given
// Enable input. A nil enable (unspecified) defaults to enabled, matching this
// backend's prior always-ACTIVE behavior for callers that don't pass Enable.
func scheduledActionState(enable *bool) string {
	if enable != nil && !*enable {
		return "DISABLED"
	}

	return scheduledActionStateActiveValue
}

// CreateScheduledAction creates a new Redshift scheduled action.
func (b *InMemoryBackend) CreateScheduledAction(
	name, schedule, iamRole, description string,
	target *ScheduledActionTarget,
	enable *bool,
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
		State:                      scheduledActionState(enable),
		TargetAction:               target,
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

// ModifyScheduledAction updates a scheduled action's schedule, IAM role,
// description, target action, and/or enabled state.
func (b *InMemoryBackend) ModifyScheduledAction(
	name, schedule, iamRole, description string,
	target *ScheduledActionTarget,
	enable *bool,
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
	if target != nil {
		a.TargetAction = target
	}
	if enable != nil {
		a.State = scheduledActionState(enable)
	}

	cp := *a

	return &cp, nil
}
