package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// validEvaluationFrequencies returns the allowed values for the anomaly detector
// evaluation frequency field, matching the AWS CloudWatch Logs API enum.
func validEvaluationFrequencies() map[string]struct{} {
	return map[string]struct{}{
		"ONE_MIN":     {},
		"FIVE_MIN":    {},
		"TEN_MIN":     {},
		"FIFTEEN_MIN": {},
		"THIRTY_MIN":  {},
		"ONE_HOUR":    {},
	}
}

// validScheduledQueryStates returns the allowed values for the scheduled query state field.
func validScheduledQueryStates() map[string]struct{} {
	return map[string]struct{}{
		statusEnabled: {},
		"DISABLED":    {},
	}
}

// CreateScheduledQuery creates a scheduled CloudWatch Logs Insights query.
// Returns the ARN of the created scheduled query.
func (b *InMemoryBackend) CreateScheduledQuery(
	name, queryString, scheduleExpression, _, state string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrValidation)
	}

	if queryString == "" {
		return "", fmt.Errorf("%w: queryString is required", ErrValidation)
	}

	if state != "" {
		if _, ok := validScheduledQueryStates()[state]; !ok {
			return "", fmt.Errorf(
				"%w: invalid state %q, must be ENABLED or DISABLED",
				ErrValidation,
				state,
			)
		}
	} else {
		state = statusEnabled
	}

	id := uuid.New().String()
	queryARN := arn.Build("logs", b.region, b.accountID, "scheduled-query:"+id)

	sq := &ScheduledQuery{
		ScheduledQueryArn:  queryARN,
		Name:               name,
		QueryString:        queryString,
		ScheduleExpression: scheduleExpression,
		State:              state,
		CreationTime:       time.Now().UnixMilli(),
	}

	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	if b.scheduledQueries.Len() >= maxScheduledQueries {
		return "", fmt.Errorf("%w: scheduled query limit exceeded", ErrValidation)
	}

	b.scheduledQueries.Put(sq)

	// Seed an initial SUCCEEDED run so history is non-empty from creation.
	now := time.Now().UnixMilli()
	b.scheduledQueryRuns.Put(&scheduledQueryRunHistory{
		Arn: queryARN,
		Runs: []*ScheduledQueryRunSummary{
			{
				Arn:            queryARN,
				RunStatus:      "SUCCEEDED",
				ExecutionTime:  now,
				InvocationTime: now,
			},
		},
	})

	return queryARN, nil
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
func (b *InMemoryBackend) DeleteScheduledQuery(scheduledQueryArn string) error {
	if scheduledQueryArn == "" {
		return fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	if !b.scheduledQueries.Delete(scheduledQueryArn) {
		return fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}

	return nil
}

// ListScheduledQueries lists all scheduled queries with pagination.
func (b *InMemoryBackend) ListScheduledQueries(
	limit int,
	nextToken string,
) ([]ScheduledQuery, string, error) {
	b.mu.RLock("ListScheduledQueries")
	defer b.mu.RUnlock()

	all := make([]ScheduledQuery, 0, b.scheduledQueries.Len())
	for _, sq := range b.scheduledQueries.All() {
		all = append(all, *sq)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ScheduledQuery{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateScheduledQuery updates the state of a scheduled query.
func (b *InMemoryBackend) UpdateScheduledQuery(scheduledQueryArn, state string) error {
	if scheduledQueryArn == "" {
		return fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}
	if state == "" {
		return fmt.Errorf("%w: state is required", ErrValidation)
	}
	if _, ok := validScheduledQueryStates()[state]; !ok {
		return fmt.Errorf("%w: invalid state %q, must be ENABLED or DISABLED", ErrValidation, state)
	}

	b.mu.Lock("UpdateScheduledQuery")
	defer b.mu.Unlock()

	sq, ok := b.scheduledQueries.Get(scheduledQueryArn)
	if !ok {
		return fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}
	sq.State = state

	return nil
}

// AddScheduledQueryRunInternal seeds a ScheduledQueryRunSummary for testing.
func (b *InMemoryBackend) AddScheduledQueryRunInternal(
	scheduledQueryArn string,
	run ScheduledQueryRunSummary,
) {
	b.mu.Lock("AddScheduledQueryRunInternal")
	defer b.mu.Unlock()

	r := run
	history, ok := b.scheduledQueryRuns.Get(scheduledQueryArn)
	if !ok {
		history = &scheduledQueryRunHistory{Arn: scheduledQueryArn}
	}
	history.Runs = append(history.Runs, &r)
	b.scheduledQueryRuns.Put(history)
}

// GetScheduledQuery returns the scheduled query with the given ARN.
func (b *InMemoryBackend) GetScheduledQuery(scheduledQueryArn string) (*ScheduledQuery, error) {
	if scheduledQueryArn == "" {
		return nil, fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.RLock("GetScheduledQuery")
	defer b.mu.RUnlock()

	sq, ok := b.scheduledQueries.Get(scheduledQueryArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}
	cp := *sq

	return &cp, nil
}

// GetScheduledQueryHistory returns the execution history for a scheduled query.
func (b *InMemoryBackend) GetScheduledQueryHistory(
	scheduledQueryArn string,
	nextToken string,
	maxResults int,
) ([]ScheduledQueryRunSummary, string, error) {
	if scheduledQueryArn == "" {
		return nil, "", fmt.Errorf("%w: scheduledQueryArn is required", ErrValidation)
	}

	b.mu.RLock("GetScheduledQueryHistory")
	defer b.mu.RUnlock()

	if !b.scheduledQueries.Has(scheduledQueryArn) {
		return nil, "", fmt.Errorf(
			"%w: scheduled query %s not found",
			ErrScheduledQueryNotFound,
			scheduledQueryArn,
		)
	}

	var runs []*ScheduledQueryRunSummary
	if history, ok := b.scheduledQueryRuns.Get(scheduledQueryArn); ok {
		runs = history.Runs
	}
	all := make([]ScheduledQueryRunSummary, 0, len(runs))
	for _, r := range runs {
		all = append(all, *r)
	}
	// Most recent invocations first.
	sort.Slice(all, func(i, j int) bool { return all[i].InvocationTime > all[j].InvocationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []ScheduledQueryRunSummary{}, "", nil
	}

	if maxResults <= 0 {
		maxResults = defaultDescribeLimit
	}

	end := startIdx + maxResults
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}
