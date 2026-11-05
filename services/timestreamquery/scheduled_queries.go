package timestreamquery

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	scheduledQueryArnFormat     = "scheduled-query/%s"
	scheduledQueryStateEnabled  = "ENABLED"
	scheduledQueryStateDisabled = "DISABLED"
)

// CreateScheduledQuery creates a new scheduled query.
//
// clientToken supports the idempotency contract documented on
// CreateScheduledQueryInput.ClientToken: the aws-sdk-go-v2 client
// auto-generates a ClientToken on every call via its idempotency-token-autofill
// middleware, so a retried request (e.g. after a network blip following a
// successful create) must replay the original success rather than surface a
// spurious "already exists" conflict.
func (b *InMemoryBackend) CreateScheduledQuery(
	ctx context.Context,
	name, queryString, scheduleExpression, executionRoleArn,
	notificationTopicArn, errorReportS3BucketName, targetDatabase, targetTable, clientToken, kmsKeyID string,
	errorReportEncryptionOption, errorReportObjectKeyPrefix string,
	tags map[string]string,
	targetDetail *ScheduledQueryTargetDetail,
) (*ScheduledQuery, error) {
	if err := validateScheduleExpression(scheduleExpression); err != nil {
		return nil, err
	}

	if err := b.checkTableRefs(ctx, queryString); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)
	now := b.clock()

	created := false

	defer func() {
		if !created {
			return
		}

		n, _ := b.deps()
		b.publishTo(ctx, n, notificationTopicArn, map[string]string{
			notifyKeyType: notifyCreating,
			notifyKeyArn:  arn.Build("timestream", region, b.accountID, fmt.Sprintf(scheduledQueryArnFormat, name)),
		})
	}()

	b.mu.Lock("CreateScheduledQuery")
	defer b.mu.Unlock()

	if clientToken != "" {
		b.scheduledQueryTokens.sweep()

		if cachedArn := b.scheduledQueryTokens.get(clientToken); cachedArn != "" {
			if existing, ok := b.scheduledQueries.Get(cachedArn); ok {
				return cloneScheduledQuery(existing), nil
			}
			// Cached entry outlived the resource (e.g. deleted); fall through
			// and treat this as a fresh request.
		}
	}

	arnStr := arn.Build("timestream", region, b.accountID, fmt.Sprintf(scheduledQueryArnFormat, name))
	if b.scheduledQueries.Has(arnStr) {
		return nil, fmt.Errorf("%w: scheduled query %q already exists", ErrAlreadyExists, name)
	}

	sq := &ScheduledQuery{
		Arn:                         arnStr,
		Name:                        name,
		QueryString:                 queryString,
		ScheduleExpression:          scheduleExpression,
		ExecutionRoleArn:            executionRoleArn,
		NotificationTopicArn:        notificationTopicArn,
		ErrorReportS3BucketName:     errorReportS3BucketName,
		ErrorReportEncryptionOption: errorReportEncryptionOption,
		ErrorReportObjectKeyPrefix:  errorReportObjectKeyPrefix,
		TargetDatabase:              targetDatabase,
		TargetTable:                 targetTable,
		TargetDetail:                targetDetail,
		KmsKeyID:                    kmsKeyID,
		State:                       scheduledQueryStateEnabled,
		CreationTime:                now,
		NextRun:                     nextInvocationTime(scheduleExpression, now),
		Tags:                        make(map[string]string),
	}

	if tags != nil {
		maps.Copy(sq.Tags, tags)
	}

	b.scheduledQueries.Put(sq)

	created = true

	if clientToken != "" {
		b.scheduledQueryTokens.set(clientToken, arnStr)
	}

	if len(tags) > 0 && b.tagWriteBackend != nil {
		// Best-effort: TimestreamWrite is the shared tag store real clients'
		// TagResource/ListTagsForResource calls hit (writeServiceTagOps); a
		// failure here would only affect that cross-service view, not this
		// scheduled query's own creation.
		_ = b.tagWriteBackend.TagResource(arnStr, tags)
	}

	return cloneScheduledQuery(sq), nil
}

// DescribeScheduledQuery returns details of a scheduled query by ARN.
// The ARN self-encodes the region, so no separate region resolution is needed.
func (b *InMemoryBackend) DescribeScheduledQuery(_ context.Context, arnStr string) (*ScheduledQuery, error) {
	b.mu.RLock("DescribeScheduledQuery")
	defer b.mu.RUnlock()

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		return nil, err
	}

	return cloneScheduledQuery(sq), nil
}

// DeleteScheduledQuery deletes a scheduled query by ARN.
// The ARN self-encodes the region, so no separate region resolution is needed.
// Any tags CreateScheduledQuery mirrored into the shared TimestreamWrite tag
// store (tagWriteBackend) are removed too, so a deleted ARN doesn't leave
// ghost tags behind for a later ListTagsForResource to surface.
func (b *InMemoryBackend) DeleteScheduledQuery(ctx context.Context, arnStr string) error {
	topic, err := b.removeScheduledQuery(arnStr)
	if err != nil {
		return err
	}

	n, _ := b.deps()
	b.publishTo(ctx, n, topic, map[string]string{notifyKeyType: notifyDeleted, notifyKeyArn: arnStr})

	return nil
}

func (b *InMemoryBackend) removeScheduledQuery(arnStr string) (string, error) {
	b.mu.Lock("DeleteScheduledQuery")
	defer b.mu.Unlock()

	sq, ok := b.scheduledQueries.Get(arnStr)
	if !ok {
		return "", fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	tagKeys := make([]string, 0, len(sq.Tags))
	for k := range sq.Tags {
		tagKeys = append(tagKeys, k)
	}

	b.scheduledQueries.Delete(arnStr)

	if len(tagKeys) > 0 && b.tagWriteBackend != nil {
		_ = b.tagWriteBackend.UntagResource(arnStr, tagKeys)
	}

	return sq.NotificationTopicArn, nil
}

// ListScheduledQueries returns all scheduled queries for the request region sorted by name.
func (b *InMemoryBackend) ListScheduledQueries(ctx context.Context) []ScheduledQuerySummary {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueries")
	defer b.mu.RUnlock()

	sorted := sortedScheduledQueriesByRegion(b.scheduledQueriesByRegion.Get(region))

	out := make([]ScheduledQuerySummary, 0, len(sorted))

	for _, sq := range sorted {
		out = append(out, ScheduledQuerySummary{
			Arn:   sq.Arn,
			Name:  sq.Name,
			State: sq.State,
		})
	}

	return out
}

// UpdateScheduledQuery updates the state of a scheduled query by ARN.
// Only ENABLED and DISABLED are valid states. The ARN self-encodes the
// region, so no separate region resolution is needed.
func (b *InMemoryBackend) UpdateScheduledQuery(ctx context.Context, arnStr, state string) error {
	if state != scheduledQueryStateEnabled && state != scheduledQueryStateDisabled {
		return fmt.Errorf("%w: State must be %s or %s",
			ErrValidation, scheduledQueryStateEnabled, scheduledQueryStateDisabled)
	}

	now := b.clock()

	b.mu.Lock("UpdateScheduledQuery")

	sq, err := b.lookupByARN(arnStr)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	changed := sq.State != state
	sq.State = state

	if state == scheduledQueryStateEnabled && changed {
		sq.NextRun = nextInvocationTime(sq.ScheduleExpression, now)
	}

	topic := sq.NotificationTopicArn
	b.mu.Unlock()

	n, _ := b.deps()
	b.publishTo(ctx, n, topic, map[string]string{notifyKeyType: notifyUpdate, notifyKeyArn: arnStr, "state": state})

	return nil
}

// lookupByARN finds a scheduled query by its (globally unique) ARN.
// Must be called with the lock held.
func (b *InMemoryBackend) lookupByARN(arnStr string) (*ScheduledQuery, error) {
	sq, ok := b.scheduledQueries.Get(arnStr)
	if !ok {
		return nil, fmt.Errorf("%w: scheduled query %q not found", ErrNotFound, arnStr)
	}

	return sq, nil
}

// cloneScheduledQuery returns a deep copy of a scheduled query.
func cloneScheduledQuery(sq *ScheduledQuery) *ScheduledQuery {
	if sq == nil {
		return nil
	}

	cp := *sq

	if sq.Tags != nil {
		cp.Tags = make(map[string]string, len(sq.Tags))

		maps.Copy(cp.Tags, sq.Tags)
	}

	return &cp
}

// sortedScheduledQueriesByRegion returns a copy of group (a byRegion index
// result) sorted by Name ascending. store.Index.Get's returned slice is
// insertion-ordered, not sorted, and owned by the index -- so the result must
// be copied before sorting, matching the sorted-by-name output the old
// collections.SortedKeys(map[name]*ScheduledQuery) call produced.
func sortedScheduledQueriesByRegion(group []*ScheduledQuery) []*ScheduledQuery {
	sorted := make([]*ScheduledQuery, len(group))
	copy(sorted, group)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	return sorted
}

// ListScheduledQueriesFull returns all scheduled queries for the request region with full details, sorted by name.
func (b *InMemoryBackend) ListScheduledQueriesFull(ctx context.Context) []*ScheduledQuery {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueriesFull")
	defer b.mu.RUnlock()

	sorted := sortedScheduledQueriesByRegion(b.scheduledQueriesByRegion.Get(region))

	out := make([]*ScheduledQuery, 0, len(sorted))

	for _, sq := range sorted {
		out = append(out, cloneScheduledQuery(sq))
	}

	return out
}

// ListScheduledQueriesEnriched returns paged enriched scheduled query summaries for the request region.
func (b *InMemoryBackend) ListScheduledQueriesEnriched(
	ctx context.Context, nextToken string, maxResults int32,
) ListScheduledQueriesResult {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListScheduledQueriesEnriched")
	group := b.scheduledQueriesByRegion.Get(region)
	all := make([]*ScheduledQuery, 0, len(group))
	for _, sq := range group {
		all = append(all, cloneScheduledQuery(sq))
	}
	b.mu.RUnlock()

	return listScheduledQueriesPaged(all, nextToken, maxResults)
}

// AddScheduledQueryInternal is a test-only seed helper that stores a scheduled query directly,
// bypassing normal validation. It is used to pre-populate backend state in tests.
func (b *InMemoryBackend) AddScheduledQueryInternal(sq *ScheduledQuery) {
	b.mu.Lock("AddScheduledQueryInternal")
	defer b.mu.Unlock()

	if sq.Tags == nil {
		sq.Tags = make(map[string]string)
	}

	b.scheduledQueries.Put(sq)
}

// ---------------------------------------------------------------------------
// ScheduleExpression validation (gap #23)
// ---------------------------------------------------------------------------

const cronFieldCount = 6 // cron expressions require exactly 6 fields

var (
	rateExpr  = regexp.MustCompile(`^rate\(\d+\s+(minute|minutes|hour|hours|day|days)\)$`)
	cronParts = regexp.MustCompile(`^cron\((.+)\)$`)
	// rateCapture captures the count and unit of a rate() schedule expression.
	rateCapture = regexp.MustCompile(`^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$`)
)

// validateScheduleExpression checks that expr is a valid rate(...) or cron(...) expression.
func validateScheduleExpression(expr string) error {
	if expr == "" {
		return fmt.Errorf("%w: ScheduleExpression is required", ErrValidation)
	}
	if rateExpr.MatchString(expr) {
		return nil
	}
	if m := cronParts.FindStringSubmatch(expr); m != nil {
		// cron must have exactly 6 space-separated fields.
		fields := strings.Fields(m[1])
		if len(fields) != cronFieldCount {
			return fmt.Errorf(
				"%w: cron expression must have 6 fields (got %d): %s",
				ErrValidation, len(fields), expr,
			)
		}

		return nil
	}

	return fmt.Errorf(
		"%w: invalid ScheduleExpression %q — must be rate(N unit) or cron(f1 f2 f3 f4 f5 f6)",
		ErrValidation, expr,
	)
}

// ---------------------------------------------------------------------------
// Next/PreviousInvocationTime from schedule expression (gap #20)
// ---------------------------------------------------------------------------

// nextInvocationTime returns the next scheduled invocation after from, or the zero time when none exists.
func nextInvocationTime(expr string, from time.Time) time.Time {
	sch, ok := parseSchedule(expr)
	if !ok {
		return time.Time{}
	}

	return sch.next(from)
}

// previousInvocationTime returns the most recent invocation before from for rate schedules; cron
// schedules report the start of the hour.
func previousInvocationTime(expr string, from time.Time) time.Time {
	if sch, ok := parseSchedule(expr); ok && sch.rate > 0 {
		return from.Add(-sch.rate)
	}

	return from.Truncate(time.Hour)
}

// ---------------------------------------------------------------------------
// Full ScheduledQuery view helpers (gap #19, #21)
// ---------------------------------------------------------------------------

// scheduledQueryRunStatus values (types.ScheduledQueryRunStatus has a 4th,
// MANUAL_TRIGGER_FAILURE, omitted here: see PARITY.md gaps). AUTO_TRIGGER_*
// are unreachable in this emulator -- there is no scheduler goroutine, so
// every run is produced by an explicit ExecuteScheduledQuery call.
const (
	runStatusAutoTriggerSuccess   = "AUTO_TRIGGER_SUCCESS"
	runStatusAutoTriggerFailure   = "AUTO_TRIGGER_FAILURE"
	runStatusManualTriggerSuccess = "MANUAL_TRIGGER_SUCCESS"
)

// buildScheduledQueryListEntry converts a ScheduledQuery to an enriched list entry.
func buildScheduledQueryListEntry(sq *ScheduledQuery) ScheduledQueryListEntry {
	entry := ScheduledQueryListEntry{
		Arn:          sq.Arn,
		Name:         sq.Name,
		State:        sq.State,
		CreationTime: epochSeconds(sq.CreationTime),
	}
	if sq.TargetDatabase != "" || sq.TargetTable != "" {
		entry.TargetDestination = &TargetDestinationForList{
			TimestreamDestination: &TimestreamDestinationForList{
				DatabaseName: sq.TargetDatabase,
				TableName:    sq.TargetTable,
			},
		}
	}
	now := time.Now()
	if !sq.LastRunTime.IsZero() {
		entry.LastRunStatus = sq.LastRunStatus
		if entry.LastRunStatus == "" {
			entry.LastRunStatus = runStatusManualTriggerSuccess
		}
		entry.PreviousInvocationTime = epochSeconds(sq.LastRunTime)
	} else if sq.ScheduleExpression != "" {
		entry.PreviousInvocationTime = epochSeconds(previousInvocationTime(sq.ScheduleExpression, now))
	}
	switch {
	case sq.State != scheduledQueryStateEnabled:
	case !sq.NextRun.IsZero():
		entry.NextInvocationTime = epochSeconds(sq.NextRun)
	default:
		entry.NextInvocationTime = epochSeconds(nextInvocationTime(sq.ScheduleExpression, now))
	}

	return entry
}

// buildLastRunSummary constructs the LastRunSummary from the recorded outcome of the latest run.
func buildLastRunSummary(sq *ScheduledQuery) *LastRunSummary {
	if sq.LastRunTime.IsZero() {
		return nil
	}

	status := sq.LastRunStatus
	if status == "" {
		status = runStatusManualTriggerSuccess
	}

	stats := ExecutionStats{}
	if sq.LastRunStats != nil {
		stats = *sq.LastRunStats
	}

	trigger := sq.LastRunTriggerTime
	if trigger.IsZero() {
		trigger = sq.LastRunTime.Add(-time.Second)
	}

	return &LastRunSummary{
		RunStatus:           status,
		FailureReason:       sq.LastRunFailure,
		InvocationTime:      epochSeconds(sq.LastRunTime),
		TriggerTime:         epochSeconds(trigger),
		ExecutionStats:      &stats,
		ErrorReportLocation: reportLocation(sq.LastRunErrorReport),
	}
}

func reportLocation(l *S3ReportLocation) *ErrorReportLocation {
	if l == nil {
		return nil
	}

	return &ErrorReportLocation{S3ReportLocation: l}
}

func buildRecentlyFailedRuns(sq *ScheduledQuery) []LastRunSummary {
	out := make([]LastRunSummary, 0, len(sq.RecentlyFailedRuns))

	for _, r := range sq.RecentlyFailedRuns {
		stats := r.Stats
		out = append(out, LastRunSummary{
			RunStatus: r.Status, FailureReason: r.Failure, ExecutionStats: &stats,
			InvocationTime: epochSeconds(r.InvocationTime), TriggerTime: epochSeconds(r.TriggerTime),
			ErrorReportLocation: reportLocation(r.ErrorReport),
		})
	}

	return out
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// ---------------------------------------------------------------------------
// enriched ListScheduledQueries with pagination (gap #18)
// ---------------------------------------------------------------------------

// listScheduledQueriesPaged returns a paged slice of scheduled query entries.
func listScheduledQueriesPaged(
	queries []*ScheduledQuery,
	nextToken string,
	maxResults int32,
) ListScheduledQueriesResult {
	if maxResults <= 0 {
		maxResults = 100
	}

	// Sort by name for stable ordering.
	sorted := make([]*ScheduledQuery, len(queries))
	copy(sorted, queries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	start := 0
	if nextToken != "" {
		name := decodeScheduledQueryToken(nextToken)
		for i, sq := range sorted {
			if sq.Name == name {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var outNextToken string
	if end < len(sorted) {
		outNextToken = base64.RawURLEncoding.EncodeToString([]byte(sorted[end].Name))
	} else {
		end = len(sorted)
	}

	items := make([]ScheduledQueryListEntry, 0, end-start)
	for _, sq := range sorted[start:end] {
		items = append(items, buildScheduledQueryListEntry(sq))
	}

	return ListScheduledQueriesResult{Items: items, NextToken: outNextToken}
}

func decodeScheduledQueryToken(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}

	return string(b)
}

func validateScheduledQueryPaging(token string, maxResults int32) error {
	if maxResults < 0 || maxResults > maxScheduledQueryPage {
		return fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxScheduledQueryPage)
	}

	if token != "" {
		if _, err := base64.RawURLEncoding.DecodeString(token); err != nil {
			return fmt.Errorf("%w: invalid NextToken", ErrValidation)
		}
	}

	return nil
}

const (
	maxScheduledQueryPage = 1000
	maxScheduledQueryName = 64
)

var scheduledQueryNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

func validateScheduledQueryName(name string) error {
	if len(name) > maxScheduledQueryName || !scheduledQueryNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: Name %q must be 1-%d characters of letters, digits, '_', '.' or '-'",
			ErrValidation, name, maxScheduledQueryName,
		)
	}

	return nil
}
