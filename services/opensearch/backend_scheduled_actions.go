package opensearch

// ListScheduledActions returns scheduled actions for a domain.
func (b *InMemoryBackend) ListScheduledActions(domainName string) []*ScheduledAction {
	b.mu.RLock("ListScheduledActions")
	defer b.mu.RUnlock()

	src := b.scheduledActions[domainName]
	out := make([]*ScheduledAction, len(src))

	for i, sa := range src {
		cp := *sa
		out[i] = &cp
	}

	return out
}

// UpdateScheduledAction updates or adds a scheduled action for a domain.
func (b *InMemoryBackend) UpdateScheduledAction(
	domainName string,
	action *ScheduledAction,
) (*ScheduledAction, error) {
	b.mu.Lock("UpdateScheduledAction")
	defer b.mu.Unlock()

	actions := b.scheduledActions[domainName]
	for i, sa := range actions {
		if sa.ID == action.ID {
			*sa = *action
			cp := *actions[i]

			return &cp, nil
		}
	}

	cp := *action
	b.scheduledActions[domainName] = append(b.scheduledActions[domainName], &cp)

	ret := *action

	return &ret, nil
}
