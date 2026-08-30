package cloudwatchlogs

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// validScheduledQueryLanguages returns the allowed values for the scheduled
// query queryLanguage field (aws-sdk-go-v2 types.QueryLanguage: CWLI/PPL/SQL,
// per CreateScheduledQueryInput's doc comment).
func validScheduledQueryLanguages() map[string]struct{} {
	return map[string]struct{}{
		"CWLI": {},
		"PPL":  {},
		"SQL":  {},
	}
}

// ScheduledQueryCreateParams bundles CreateScheduledQuery's real-API request
// fields (field-diffed against CreateScheduledQueryInput) to avoid an
// unwieldy positional parameter list. Name/QueryString/QueryLanguage/
// ScheduleExpression/ExecutionRoleArn are required, matching the real input's
// "This member is required" members; State defaults to ENABLED when empty,
// also matching the real default.
type ScheduledQueryCreateParams struct {
	DestinationConfiguration *ScheduledQueryDestinationConfig
	ScheduleExpression       string
	QueryLanguage            string
	Name                     string
	ExecutionRoleArn         string
	State                    string
	Description              string
	Timezone                 string
	QueryString              string
	LogGroupIdentifiers      []string
	EndTimeOffset            int64
	StartTimeOffset          int64
	ScheduleStartTime        int64
	ScheduleEndTime          int64
}

// CreateScheduledQuery creates a scheduled CloudWatch Logs Insights query.
// Returns the ARN of the created scheduled query. Field-diffed against
// CreateScheduledQueryInput: p.ExecutionRoleArn and p.QueryLanguage are both
// real required members -- a previous revision accepted executionRoleArn
// from the wire but silently discarded it (never stored, never returned),
// and never modeled queryLanguage at all, so a real client's required
// queryLanguage was accepted without validation and dropped.
func (b *InMemoryBackend) CreateScheduledQuery(p ScheduledQueryCreateParams) (string, error) {
	if p.Name == "" {
		return "", fmt.Errorf("%w: name is required", ErrValidation)
	}

	if p.QueryString == "" {
		return "", fmt.Errorf("%w: queryString is required", ErrValidation)
	}

	if p.ScheduleExpression == "" {
		return "", fmt.Errorf("%w: scheduleExpression is required", ErrValidation)
	}

	if p.ExecutionRoleArn == "" {
		return "", fmt.Errorf("%w: executionRoleArn is required", ErrValidation)
	}

	if p.QueryLanguage == "" {
		return "", fmt.Errorf("%w: queryLanguage is required", ErrValidation)
	}

	if _, ok := validScheduledQueryLanguages()[p.QueryLanguage]; !ok {
		return "", fmt.Errorf(
			"%w: invalid queryLanguage %q, must be one of CWLI, PPL, SQL",
			ErrValidation, p.QueryLanguage,
		)
	}

	state := p.State
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
	now := time.Now().UnixMilli()

	sq := &ScheduledQuery{
		ScheduledQueryArn:        queryARN,
		Name:                     p.Name,
		Description:              p.Description,
		QueryString:              p.QueryString,
		QueryLanguage:            p.QueryLanguage,
		ScheduleExpression:       p.ScheduleExpression,
		ScheduleType:             "CUSTOMER_MANAGED",
		ExecutionRoleArn:         p.ExecutionRoleArn,
		State:                    state,
		Timezone:                 p.Timezone,
		LogGroupIdentifiers:      p.LogGroupIdentifiers,
		DestinationConfiguration: p.DestinationConfiguration,
		EndTimeOffset:            p.EndTimeOffset,
		StartTimeOffset:          p.StartTimeOffset,
		ScheduleStartTime:        p.ScheduleStartTime,
		ScheduleEndTime:          p.ScheduleEndTime,
		CreationTime:             now,
		LastUpdatedTime:          now,
	}

	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	if b.scheduledQueries.Len() >= maxScheduledQueries {
		return "", fmt.Errorf("%w: scheduled query limit exceeded", ErrValidation)
	}

	b.scheduledQueries.Put(sq)

	// Seed an initial SUCCEEDED run so history is non-empty from creation, and
	// reflect it on the ScheduledQuery row itself: real GetScheduledQueryOutput's
	// lastExecutionStatus/lastTriggeredTime describe the most recent execution.
	sq.LastExecutionStatus = "SUCCEEDED"
	sq.LastTriggeredTime = now
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
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreationTime != all[j].CreationTime {
			return all[i].CreationTime < all[j].CreationTime
		}

		return all[i].ScheduledQueryArn < all[j].ScheduledQueryArn
	})

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
	sq.LastUpdatedTime = time.Now().UnixMilli()

	return nil
}

// AddScheduledQueryInternal seeds a ScheduledQuery directly into the store
// for testing. It overwrites any existing query with the same ARN.
func (b *InMemoryBackend) AddScheduledQueryInternal(query ScheduledQuery) {
	b.mu.Lock("AddScheduledQueryInternal")
	defer b.mu.Unlock()

	q := query
	q.LogGroupIdentifiers = slices.Clone(query.LogGroupIdentifiers)
	b.scheduledQueries.Put(&q)
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
