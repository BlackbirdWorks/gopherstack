package medialive

import "fmt"

// BatchUpdateSchedule adds/removes schedule actions for a channel.
func (b *InMemoryBackend) BatchUpdateSchedule(
	channelID string,
	creates []ScheduleAction,
	deleteActionNames []string,
) (*BatchUpdateScheduleResult, error) {
	b.mu.Lock("BatchUpdateSchedule")
	defer b.mu.Unlock()
	if !b.channels.Has(channelID) {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}
	actions := b.scheduleActions[channelID]
	// Remove deleted actions.
	toDelete := make(map[string]bool, len(deleteActionNames))
	for _, n := range deleteActionNames {
		toDelete[n] = true
	}
	filtered := actions[:0]
	for _, a := range actions {
		if !toDelete[a.ActionName] {
			filtered = append(filtered, a)
		}
	}
	// Add new actions.
	var created []ScheduleAction
	for _, c := range creates {
		filtered = append(
			filtered,
			&storedScheduleAction{ActionName: c.ActionName, ActionType: c.ActionType},
		)
		created = append(created, c)
	}
	b.scheduleActions[channelID] = filtered
	// Build deleted list from intersection of requested deletes and what actually existed.
	var deleted []ScheduleAction
	for _, n := range deleteActionNames {
		deleted = append(deleted, ScheduleAction{ActionName: n})
	}

	return &BatchUpdateScheduleResult{Creates: created, Deletes: deleted}, nil
}

// --- Schedule operations ---

// DescribeSchedule returns the schedule actions for a channel.
func (b *InMemoryBackend) DescribeSchedule(channelID string) ([]ScheduleAction, error) {
	b.mu.RLock("DescribeSchedule")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelID) {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	stored := b.scheduleActions[channelID]
	out := make([]ScheduleAction, 0, len(stored))
	for _, a := range stored {
		out = append(out, ScheduleAction{ActionName: a.ActionName, ActionType: a.ActionType})
	}

	return out, nil
}

// DeleteSchedule removes all schedule actions for a channel.
func (b *InMemoryBackend) DeleteSchedule(channelID string) error {
	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()

	if !b.channels.Has(channelID) {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	delete(b.scheduleActions, channelID)

	return nil
}
