package autoscaling

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CancelInstanceRefresh cancels an active instance refresh for the group.
// It returns the ID of the cancelled refresh.
func (b *InMemoryBackend) CancelInstanceRefresh(groupName string) (string, error) {
	b.mu.Lock("CancelInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return "", fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, r := range b.instanceRefreshes[groupName] {
		if r.Status == statusInProgress || r.Status == statusPending {
			r.Status = "Cancelling"

			return r.InstanceRefreshID, nil
		}
	}

	return "", fmt.Errorf("%w: no active instance refresh for group %q",
		ErrActiveInstanceRefreshNotFound, groupName)
}

// AddInstanceRefresh stores an instance refresh for the given group (used for testing CancelInstanceRefresh).
func (b *InMemoryBackend) AddInstanceRefresh(refresh InstanceRefresh) error {
	b.mu.Lock("AddInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(refresh.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, refresh.AutoScalingGroupName)
	}

	cp := refresh
	b.instanceRefreshes[refresh.AutoScalingGroupName] = append(
		b.instanceRefreshes[refresh.AutoScalingGroupName],
		&cp,
	)

	return nil
}

// StartInstanceRefresh creates a new instance refresh for the group.
func (b *InMemoryBackend) StartInstanceRefresh(groupName string) (*InstanceRefresh, error) {
	return b.StartInstanceRefreshWithInput(StartInstanceRefreshInput{AutoScalingGroupName: groupName})
}

// StartInstanceRefreshWithInput creates a new instance refresh for the group with full input.
func (b *InMemoryBackend) StartInstanceRefreshWithInput(input StartInstanceRefreshInput) (*InstanceRefresh, error) {
	b.mu.Lock("StartInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	for _, r := range b.instanceRefreshes[input.AutoScalingGroupName] {
		if r.Status == statusInProgress || r.Status == statusPending {
			return nil, fmt.Errorf(
				"%w: an instance refresh is already in progress for group %q",
				ErrInstanceRefreshInProgress, input.AutoScalingGroupName,
			)
		}
	}

	strategy := input.Strategy
	if strategy == "" {
		strategy = "Rolling"
	}

	prefs := input.Preferences
	if prefs.MinHealthyPercentage == 0 {
		prefs.MinHealthyPercentage = 90
	}

	refresh := &InstanceRefresh{
		InstanceRefreshID:    uuid.NewString(),
		AutoScalingGroupName: input.AutoScalingGroupName,
		Status:               statusInProgress,
		StartTime:            time.Now(),
		Strategy:             strategy,
		Preferences:          prefs,
	}

	b.instanceRefreshes[input.AutoScalingGroupName] = append(b.instanceRefreshes[input.AutoScalingGroupName], refresh)

	cp := *refresh

	return &cp, nil
}

// RollbackInstanceRefresh rolls back an in-progress instance refresh for the group.
func (b *InMemoryBackend) RollbackInstanceRefresh(groupName string) (string, error) {
	b.mu.Lock("RollbackInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return "", fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, r := range b.instanceRefreshes[groupName] {
		if r.Status == statusInProgress || r.Status == statusPending {
			r.Status = "RollbackInProgress"

			return r.InstanceRefreshID, nil
		}
	}

	return "", fmt.Errorf("%w: no active instance refresh for group %q",
		ErrActiveInstanceRefreshNotFound, groupName)
}

// DescribeInstanceRefreshes returns instance refreshes for the group, optionally filtered by ID.
func (b *InMemoryBackend) DescribeInstanceRefreshes(groupName string, refreshIDs []string) ([]InstanceRefresh, error) {
	b.mu.RLock("DescribeInstanceRefreshes")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}
	}

	idFilter := make(map[string]bool, len(refreshIDs))
	for _, id := range refreshIDs {
		idFilter[id] = true
	}

	var result []InstanceRefresh

	groups := b.instanceRefreshes
	if groupName != "" {
		groups = map[string][]*InstanceRefresh{groupName: b.instanceRefreshes[groupName]}
	}

	for _, refreshes := range groups {
		for _, r := range refreshes {
			if len(idFilter) > 0 && !idFilter[r.InstanceRefreshID] {
				continue
			}
			result = append(result, *r)
		}
	}

	// groups is b.instanceRefreshes (a map) when groupName is empty, so account-wide iteration
	// order is randomized run to run; a stable total order is required for pagination to not
	// drop or duplicate records across a page boundary. InstanceRefreshID is a uuid.NewString()
	// value (see StartInstanceRefresh below) -- globally unique, so no tiebreak is needed.
	sort.Slice(result, func(i, j int) bool { return result[i].InstanceRefreshID < result[j].InstanceRefreshID })

	return result, nil
}
