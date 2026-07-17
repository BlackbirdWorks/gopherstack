package elasticbeanstalk

import "context"

func (b *InMemoryBackend) managedActionHistoryStore(region string) map[string][]*ManagedActionHistory {
	if b.managedActionHistory[region] == nil {
		b.managedActionHistory[region] = make(map[string][]*ManagedActionHistory)
	}

	return b.managedActionHistory[region]
}

// ApplyEnvironmentManagedAction applies a scheduled managed action immediately.
// Records the action in the managed action history (improvement #4).
func (b *InMemoryBackend) ApplyEnvironmentManagedAction(ctx context.Context, envName, actionID string) error {
	b.mu.Lock("ApplyEnvironmentManagedAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        "InstanceRefresh",
		ActionDescription: "Managed action applied",
		Status:            "Succeeded",
		FinishedTime:      managedActionFinishedTime,
	}
	store := b.managedActionHistoryStore(region)
	store[envName] = append(store[envName], item)

	return nil
}

// AddManagedActionHistory records a managed action history item for an environment (improvement #4).
func (b *InMemoryBackend) AddManagedActionHistory(
	ctx context.Context,
	envName, actionID, actionType, actionDesc, status string,
) {
	b.mu.Lock("AddManagedActionHistory")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        actionType,
		ActionDescription: actionDesc,
		Status:            status,
		FinishedTime:      managedActionFinishedTime,
	}
	store := b.managedActionHistoryStore(region)
	store[envName] = append(store[envName], item)
}

// DescribeEnvironmentManagedActionHistory returns stored managed action history for an environment (improvement #4).
func (b *InMemoryBackend) DescribeEnvironmentManagedActionHistory(
	ctx context.Context,
	envName string,
) []*ManagedActionHistory {
	b.mu.RLock("DescribeEnvironmentManagedActionHistory")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.managedActionHistoryStore(region)[envName]

	if len(items) == 0 {
		return []*ManagedActionHistory{}
	}

	out := make([]*ManagedActionHistory, len(items))
	for i, item := range items {
		cp := *item
		out[i] = &cp
	}

	return out
}
