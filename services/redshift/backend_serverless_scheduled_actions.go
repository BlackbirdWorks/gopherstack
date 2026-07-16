package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless Scheduled Actions
// ---------------------------------------------------------------------------

// CreateServerlessScheduledAction creates a serverless scheduled action.
func (b *InMemoryBackend) CreateServerlessScheduledAction(
	scheduledActionName, namespaceName, schedule, targetAction string,
	startTime, endTime time.Time,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("CreateServerlessScheduledAction")
	defer b.mu.Unlock()

	if _, ok := b.slScheduledActions.Get(scheduledActionName); ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q already exists",
			ErrServerlessConflict,
			scheduledActionName,
		)
	}

	saArn := arn.Build(
		"redshift-serverless",
		b.region,
		b.accountID,
		"scheduledaction/"+scheduledActionName,
	)

	sa := &ServerlessScheduledAction{
		ScheduledActionArn:  saArn,
		ScheduledActionName: scheduledActionName,
		NamespaceName:       namespaceName,
		Schedule:            schedule,
		StartTime:           startTime,
		EndTime:             endTime,
		Status:              slStatusActive,
		TargetAction:        targetAction,
	}
	b.slScheduledActions.Put(sa)
	b.slScheduledActionIdx.insert(scheduledActionName)

	return cloneServerlessScheduledAction(sa), nil
}

// GetServerlessScheduledAction returns a serverless scheduled action by name.
func (b *InMemoryBackend) GetServerlessScheduledAction(
	scheduledActionName string,
) (*ServerlessScheduledAction, error) {
	b.mu.RLock("GetServerlessScheduledAction")
	defer b.mu.RUnlock()

	sa, ok := b.slScheduledActions.Get(scheduledActionName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	return cloneServerlessScheduledAction(sa), nil
}

// ListServerlessScheduledActions returns all serverless scheduled actions.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessScheduledActions(
	namespaceName string,
	maxResults int,
	nextToken string,
) ([]*ServerlessScheduledAction, string) {
	b.mu.RLock("ListServerlessScheduledActions")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slScheduledActionIdx.ordered()
	list := make([]*ServerlessScheduledAction, 0, len(keys))

	for _, name := range keys {
		sa, ok := b.slScheduledActions.Get(name)
		if !ok {
			continue
		}

		if namespaceName == "" || sa.NamespaceName == namespaceName {
			list = append(list, cloneServerlessScheduledAction(sa))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessScheduledAction{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateServerlessScheduledAction updates a serverless scheduled action.
func (b *InMemoryBackend) UpdateServerlessScheduledAction(
	scheduledActionName, schedule, targetAction string,
	startTime, endTime time.Time,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("UpdateServerlessScheduledAction")
	defer b.mu.Unlock()

	sa, ok := b.slScheduledActions.Get(scheduledActionName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	if schedule != "" {
		sa.Schedule = schedule
	}

	if targetAction != "" {
		sa.TargetAction = targetAction
	}

	if !startTime.IsZero() {
		sa.StartTime = startTime
	}

	if !endTime.IsZero() {
		sa.EndTime = endTime
	}

	return cloneServerlessScheduledAction(sa), nil
}

// DeleteServerlessScheduledAction deletes a serverless scheduled action.
func (b *InMemoryBackend) DeleteServerlessScheduledAction(
	scheduledActionName string,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("DeleteServerlessScheduledAction")
	defer b.mu.Unlock()

	sa, ok := b.slScheduledActions.Get(scheduledActionName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q not found",
			ErrScheduledActionSLNotFound,
			scheduledActionName,
		)
	}

	cp := cloneServerlessScheduledAction(sa)
	b.slScheduledActions.Delete(scheduledActionName)
	b.slScheduledActionIdx.remove(scheduledActionName)

	return cp, nil
}

func cloneServerlessScheduledAction(sa *ServerlessScheduledAction) *ServerlessScheduledAction {
	cp := *sa

	return &cp
}
