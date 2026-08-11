package redshift

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Serverless Scheduled Actions
// ---------------------------------------------------------------------------

// CreateScheduledActionParams holds the fields accepted by
// CreateScheduledAction. RoleArn/Schedule/ScheduledActionName/TargetAction are
// required by the real API (CreateScheduledActionInput); Schedule/TargetAction
// are raw JSON, see ServerlessScheduledAction's doc comment.
type CreateScheduledActionParams struct {
	StartTime                  time.Time
	EndTime                    time.Time
	Enabled                    *bool
	ScheduledActionName        string
	NamespaceName              string
	RoleArn                    string
	ScheduledActionDescription string
	Schedule                   json.RawMessage
	TargetAction               json.RawMessage
}

// UpdateScheduledActionParams holds the mutable fields accepted by
// UpdateScheduledAction.
type UpdateScheduledActionParams struct {
	StartTime                  time.Time
	EndTime                    time.Time
	ScheduledActionDescription *string
	Enabled                    *bool
	RoleArn                    string
	Schedule                   json.RawMessage
	TargetAction               json.RawMessage
}

// CreateServerlessScheduledAction creates a serverless scheduled action.
func (b *InMemoryBackend) CreateServerlessScheduledAction(
	p CreateScheduledActionParams,
) (*ServerlessScheduledAction, error) {
	b.mu.Lock("CreateServerlessScheduledAction")
	defer b.mu.Unlock()

	if _, ok := b.slScheduledActions.Get(p.ScheduledActionName); ok {
		return nil, fmt.Errorf(
			"%w: scheduled action %q already exists",
			ErrServerlessConflict,
			p.ScheduledActionName,
		)
	}

	state := slStateActive
	if p.Enabled != nil && !*p.Enabled {
		state = slStateDisabled
	}

	sa := &ServerlessScheduledAction{
		ScheduledActionName:        p.ScheduledActionName,
		NamespaceName:              p.NamespaceName,
		RoleArn:                    p.RoleArn,
		Schedule:                   p.Schedule,
		TargetAction:               p.TargetAction,
		ScheduledActionDescription: p.ScheduledActionDescription,
		ScheduledActionUUID:        randomUUID(),
		StartTime:                  p.StartTime,
		EndTime:                    p.EndTime,
		State:                      state,
	}
	b.slScheduledActions.Put(sa)
	b.slScheduledActionIdx.insert(p.ScheduledActionName)

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
	scheduledActionName string,
	p UpdateScheduledActionParams,
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

	if len(p.Schedule) > 0 {
		sa.Schedule = p.Schedule
	}

	if len(p.TargetAction) > 0 {
		sa.TargetAction = p.TargetAction
	}

	if p.RoleArn != "" {
		sa.RoleArn = p.RoleArn
	}

	if p.ScheduledActionDescription != nil {
		sa.ScheduledActionDescription = *p.ScheduledActionDescription
	}

	if !p.StartTime.IsZero() {
		sa.StartTime = p.StartTime
	}

	if !p.EndTime.IsZero() {
		sa.EndTime = p.EndTime
	}

	if p.Enabled != nil {
		if *p.Enabled {
			sa.State = slStateActive
		} else {
			sa.State = slStateDisabled
		}
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
	if sa.Schedule != nil {
		cp.Schedule = append(json.RawMessage(nil), sa.Schedule...)
	}

	if sa.TargetAction != nil {
		cp.TargetAction = append(json.RawMessage(nil), sa.TargetAction...)
	}

	return &cp
}

// randomUUID produces a UUID-v4-shaped string (8-4-4-4-12 hex digits). Not
// cryptographically significant here -- ScheduledActionUUID just needs to
// look like the real API's identifier, which this backend does not otherwise
// track.
func randomUUID() string {
	h := randomHex(slUUIDHexBytes) // 16 bytes -> 32 hex chars

	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
