package cloudwatchlogs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// validLogGroupClasses returns the allowed values for the log group class field.
func validLogGroupClasses() map[string]struct{} {
	return map[string]struct{}{
		LogGroupClassStandard:         {},
		LogGroupClassInfrequentAccess: {},
	}
}

func (b *InMemoryBackend) groupARN(region, name string) string {
	return arn.Build("logs", region, b.accountID, "log-group:"+name)
}

// CreateLogGroup creates a new log group with the given class and optional KMS key.
// logGroupClass must be STANDARD or INFREQUENT_ACCESS (defaults to STANDARD if empty).
func (b *InMemoryBackend) CreateLogGroup(
	ctx context.Context,
	name, logGroupClass, kmsKeyID string,
) (*LogGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if !validLogGroupName(name) {
		return nil, fmt.Errorf(
			"%w: logGroupName contains invalid characters (allowed: [a-zA-Z0-9._-/#], length 1-512)",
			ErrValidation,
		)
	}

	if logGroupClass == "" {
		logGroupClass = LogGroupClassStandard
	}

	if _, ok := validLogGroupClasses()[logGroupClass]; !ok {
		return nil, fmt.Errorf(
			"%w: invalid logGroupClass %q, must be STANDARD or INFREQUENT_ACCESS",
			ErrValidation,
			logGroupClass,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateLogGroup")
	defer b.mu.Unlock()

	if b.groupHas(region, name) {
		return nil, fmt.Errorf("%w: Log group %s already exists", ErrLogGroupAlreadyExists, name)
	}

	g := &LogGroup{
		CreationTime:  time.Now().UnixMilli(),
		LogGroupName:  name,
		Arn:           b.groupARN(region, name),
		LogGroupClass: logGroupClass,
		KmsKeyID:      kmsKeyID,
		region:        region,
	}
	b.groupPut(g)

	if kmsKeyID != "" {
		b.kmsKeys.Put(&kmsKeyEntry{Key: name, KmsKeyID: kmsKeyID})
	}

	cp := *g

	return &cp, nil
}

// DeleteLogGroup deletes a log group and all its streams/events.
func (b *InMemoryBackend) DeleteLogGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteLogGroup")
	defer b.mu.Unlock()

	if !b.groupHas(region, name) {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
	}

	b.groupDelete(region, name)
	b.deleteStreamsInGroup(region, name)
	b.deleteSubscriptionFiltersInGroup(region, name)
	b.deleteMetricFiltersInGroup(region, name)

	return nil
}

// SetRetentionPolicy sets or clears the retention policy for a log group.
// A nil days value removes any existing retention policy.
func (b *InMemoryBackend) SetRetentionPolicy(
	ctx context.Context,
	groupName string,
	days *int32,
) error {
	if days != nil {
		if _, ok := validRetentionDays()[*days]; !ok {
			return fmt.Errorf(
				"%w: invalid retentionInDays %d, must be one of the allowed values",
				ErrValidation,
				*days,
			)
		}
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("SetRetentionPolicy")
	defer b.mu.Unlock()

	g, exists := b.groupGet(region, groupName)
	if !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	g.RetentionInDays = days

	return nil
}

// DescribeLogGroups returns log groups optionally filtered by prefix, with pagination.
func (b *InMemoryBackend) DescribeLogGroups(
	ctx context.Context, prefix, nextToken string, limit int,
) ([]LogGroup, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeLogGroups")
	defer b.mu.RUnlock()

	if limit > defaultDescribeLimit {
		limit = defaultDescribeLimit
	}

	regionGroups := b.groupsInRegion(region)
	all := make([]LogGroup, 0, len(regionGroups))
	for _, g := range regionGroups {
		if prefix == "" || strings.HasPrefix(g.LogGroupName, prefix) {
			all = append(all, *g)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogGroupName < all[j].LogGroupName })

	groups, token := paginateGroups(all, nextToken, limit)

	return groups, token, nil
}

func paginateGroups(all []LogGroup, nextToken string, limit int) ([]LogGroup, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogGroup{}, ""
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

	return all[startIdx:end], outToken
}

// AssociateKmsKey associates a KMS key with a log group or query results resource.
// Exactly one of logGroupName or resourceIdentifier must be non-empty.
func (b *InMemoryBackend) AssociateKmsKey(logGroupName, resourceIdentifier, kmsKeyID string) error {
	if kmsKeyID == "" {
		return fmt.Errorf("%w: kmsKeyId is required", ErrValidation)
	}

	if logGroupName == "" && resourceIdentifier == "" {
		return fmt.Errorf(
			"%w: one of logGroupName or resourceIdentifier is required",
			ErrValidation,
		)
	}

	b.mu.Lock("AssociateKmsKey")
	defer b.mu.Unlock()

	key := logGroupName
	if key == "" {
		key = resourceIdentifier
	}

	b.kmsKeys.Put(&kmsKeyEntry{Key: key, KmsKeyID: kmsKeyID})

	return nil
}

// DisassociateKmsKey removes the KMS key association from a log group or resource.
func (b *InMemoryBackend) DisassociateKmsKey(logGroupName, resourceIdentifier string) error {
	if logGroupName == "" && resourceIdentifier == "" {
		return fmt.Errorf(
			"%w: one of logGroupName or resourceIdentifier is required",
			ErrValidation,
		)
	}

	b.mu.Lock("DisassociateKmsKey")
	defer b.mu.Unlock()

	key := logGroupName
	if key == "" {
		key = resourceIdentifier
	}
	b.kmsKeys.Delete(key)

	return nil
}

// validLogGroupName returns true if name matches the AWS CloudWatch Logs allowed character set.
// Pattern: [.\\-_/#A-Za-z0-9]+, length 1-512.
func validLogGroupName(name string) bool {
	if len(name) == 0 || len(name) > 512 {
		return false
	}
	for _, r := range name {
		if !isValidLogGroupChar(r) {
			return false
		}
	}

	return true
}

func isValidLogGroupChar(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') ||
		r == '.' || r == '_' || r == '-' || r == '/' || r == '#'
}

// validRetentionDays returns the full set of retention values accepted by AWS CloudWatch Logs.
func validRetentionDays() map[int32]struct{} {
	return map[int32]struct{}{
		1: {}, 3: {}, 5: {}, 7: {}, 14: {}, 30: {}, 60: {}, 90: {}, 120: {}, 150: {}, 180: {},
		365: {}, 400: {}, 545: {}, 731: {}, 1096: {}, 1827: {}, 2192: {}, 2557: {}, 2922: {}, 3288: {}, 3653: {},
	}
}

// standardLogGroupFields returns the standard AWS CloudWatch Logs fields present in all log events.
// All standard fields are present in 100% of events.
func standardLogGroupFields() []LogGroupField {
	const pct int32 = 100

	return []LogGroupField{
		{Name: keyMessageField, Percent: pct},
		{Name: keyTimestamp, Percent: pct},
		{Name: keyIngestionTime, Percent: pct},
		{Name: keyLogStream, Percent: pct},
	}
}

func (b *InMemoryBackend) GetLogGroupFields(
	ctx context.Context,
	logGroupName string,
) ([]LogGroupField, error) {
	if logGroupName == "" {
		return nil, fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLogGroupFields")
	defer b.mu.RUnlock()

	if !b.groupHas(region, logGroupName) {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	return standardLogGroupFields(), nil
}

// ListLogGroups is the newer paginated list operation, equivalent to DescribeLogGroups.
func (b *InMemoryBackend) ListLogGroups(
	ctx context.Context, namePrefix, nextToken string, limit int,
) ([]LogGroup, string, error) {
	return b.DescribeLogGroups(ctx, namePrefix, nextToken, limit)
}

// SetLogGroupDeletionProtection enables or disables deletion protection for a log group.
func (b *InMemoryBackend) SetLogGroupDeletionProtection(logGroupIdentifier string, protected bool) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("SetLogGroupDeletionProtection")
	defer b.mu.Unlock()

	b.deletionProtected.Put(&deletionProtectionEntry{
		LogGroupIdentifier: logGroupIdentifier,
		Protected:          protected,
	})

	return nil
}

// IsLogGroupDeletionProtected returns whether deletion protection is enabled.
func (b *InMemoryBackend) IsLogGroupDeletionProtected(logGroupIdentifier string) bool {
	b.mu.RLock("IsLogGroupDeletionProtected")
	defer b.mu.RUnlock()

	entry, ok := b.deletionProtected.Get(logGroupIdentifier)

	return ok && entry.Protected
}

// ListAggregateLogGroupSummaries returns aggregate summaries derived from the
// real log groups and their stored events for the current region. Summaries are
// sorted by log group name for deterministic output.
func (b *InMemoryBackend) ListAggregateLogGroupSummaries(
	ctx context.Context,
) []AggregateLogGroupSummary {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAggregateLogGroupSummaries")
	defer b.mu.RUnlock()

	groups := b.groupsInRegion(region)

	summaries := make([]AggregateLogGroupSummary, 0, len(groups))
	for _, group := range groups {
		var count int64
		for _, stream := range b.streamsInGroup(region, group.LogGroupName) {
			count += int64(len(stream.events))
		}

		summaries = append(summaries, AggregateLogGroupSummary{
			LogGroupName:  group.LogGroupName,
			LogGroupArn:   group.Arn,
			LogGroupClass: group.LogGroupClass,
			StoredBytes:   group.StoredBytes,
			LogEventCount: count,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].LogGroupName < summaries[j].LogGroupName
	})

	return summaries
}

// ValidateLiveTailLogGroups validates that every supplied log group identifier
// resolves to an existing log group. StartLiveTail is a streaming (HTTP/2
// event-stream) operation that cannot be meaningfully emulated over the standard
// JSON response, so the backend only performs input validation and returns
// ResourceNotFoundException for any unknown log group.
func (b *InMemoryBackend) ValidateLiveTailLogGroups(
	ctx context.Context,
	logGroupIdentifiers []string,
) error {
	if len(logGroupIdentifiers) == 0 {
		return fmt.Errorf("%w: logGroupIdentifiers is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ValidateLiveTailLogGroups")
	defer b.mu.RUnlock()

	for _, id := range logGroupIdentifiers {
		name := normalizeLogGroupIdentifier(id)
		if !b.groupHas(region, name) {
			return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
		}
	}

	return nil
}
