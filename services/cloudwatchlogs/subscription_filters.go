package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/google/uuid"
)

// validDistributions returns the allowed values for subscription filter distribution.
func validDistributions() map[string]struct{} {
	return map[string]struct{}{
		DistributionRandom:      {},
		DistributionByLogStream: {},
	}
}

// PutSubscriptionFilter creates or updates a subscription filter for a log group.
// roleArn is required by AWS when delivering to Kinesis streams; distribution defaults to Random.
func (b *InMemoryBackend) PutSubscriptionFilter(
	ctx context.Context,
	groupName, filterName, filterPattern, destinationArn, roleArn, distribution string,
) error {
	if groupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}

	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}

	if destinationArn == "" {
		return fmt.Errorf("%w: destinationArn is required", ErrValidation)
	}

	if distribution == "" {
		distribution = DistributionRandom
	}

	if _, ok := validDistributions()[distribution]; !ok {
		return fmt.Errorf(
			"%w: invalid distribution %q, must be Random or ByLogStream",
			ErrValidation,
			distribution,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutSubscriptionFilter")
	defer b.mu.Unlock()

	if !b.groupHas(region, groupName) {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	existing := b.subscriptionFiltersInGroup(region, groupName)

	// Check for a filter with the same name (update).
	for _, f := range existing {
		if f.FilterName == filterName {
			f.FilterPattern = filterPattern
			f.DestinationArn = destinationArn
			f.RoleArn = roleArn
			f.Distribution = distribution

			return nil
		}
	}

	// Enforce AWS limit of 2 subscription filters per log group.
	if len(existing) >= maxSubscriptionFilters {
		return fmt.Errorf("%w: log group %s already has the maximum number of subscription filters",
			ErrSubscriptionFilterLimitExceed, groupName)
	}

	b.subscriptionFilters.Put(&SubscriptionFilter{
		FilterName:     filterName,
		FilterPattern:  filterPattern,
		LogGroupName:   groupName,
		DestinationArn: destinationArn,
		RoleArn:        roleArn,
		Distribution:   distribution,
		CreationTime:   time.Now().UnixMilli(),
		region:         region,
	})

	return nil
}

// DescribeSubscriptionFilters returns subscription filters for a log group with optional prefix and pagination.
func (b *InMemoryBackend) DescribeSubscriptionFilters(
	ctx context.Context, groupName, filterNamePrefix, nextToken string, limit int,
) (
	[]SubscriptionFilter, string, error,
) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSubscriptionFilters")
	defer b.mu.RUnlock()

	if !b.groupHas(region, groupName) {
		return nil, "", fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	groupFilters := b.subscriptionFiltersInGroup(region, groupName)
	all := make([]SubscriptionFilter, 0, len(groupFilters))
	for _, f := range groupFilters {
		if filterNamePrefix == "" || strings.HasPrefix(f.FilterName, filterNamePrefix) {
			all = append(all, *f)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].FilterName < all[j].FilterName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []SubscriptionFilter{}, "", nil
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

// DeleteSubscriptionFilter removes a subscription filter from a log group.
func (b *InMemoryBackend) DeleteSubscriptionFilter(
	ctx context.Context,
	groupName, filterName string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSubscriptionFilter")
	defer b.mu.Unlock()

	if !b.groupHas(region, groupName) {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, groupName)
	}

	if b.subscriptionFilters.Delete(subFilterTableKey(region, groupName, filterName)) {
		return nil
	}

	return fmt.Errorf("%w: subscription filter %s not found in log group %s",
		ErrSubscriptionFilterNotFound, filterName, groupName)
}

// matchingFilters returns subscription filters whose pattern matches any of the given events.
// Must be called with the write lock held (called from PutLogEvents before Unlock).
func (b *InMemoryBackend) matchingFilters(
	region, groupName string,
	events []InputLogEvent,
) []*SubscriptionFilter {
	filters := b.subscriptionFiltersInGroup(region, groupName)
	if len(filters) == 0 {
		return nil
	}

	var matched []*SubscriptionFilter
	for _, f := range filters {
		compiled := b.getCompiledPattern(f.FilterPattern)
		if filterMatchesCompiled(compiled, events) {
			matched = append(matched, f)
		}
	}

	return matched
}

// deliverToFilters builds the subscription payload and delivers it to each matched filter destination.
func (b *InMemoryBackend) deliverToFilters(
	ctx context.Context,
	groupName, streamName, accountID string,
	events []InputLogEvent,
	filters []*SubscriptionFilter,
	deliverer SubscriptionDeliverer,
	timeout time.Duration,
) {
	filterNames := make([]string, len(filters))
	for i, f := range filters {
		filterNames[i] = f.FilterName
	}

	logEvts := make([]subscriptionLogEvent, len(events))
	for i, ev := range events {
		logEvts[i] = subscriptionLogEvent{
			ID:        uuid.New().String(),
			Timestamp: ev.Timestamp,
			Message:   ev.Message,
		}
	}

	payload := subscriptionPayload{
		MessageType:         "DATA_MESSAGE",
		Owner:               accountID,
		LogGroup:            groupName,
		LogStream:           streamName,
		SubscriptionFilters: filterNames,
		LogEvents:           logEvts,
	}

	encoded, err := encodeSubscriptionPayload(payload)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: failed to encode subscription payload",
			"logGroup", groupName, "error", err)

		return
	}

	for _, f := range filters {
		deliverCtx := ctx
		var cancel context.CancelFunc
		if timeout > 0 {
			deliverCtx, cancel = context.WithTimeout(ctx, timeout)
		}

		deliverErr := deliverer.DeliverLogEvents(deliverCtx, f.DestinationArn, encoded)
		if cancel != nil {
			cancel()
		}

		if deliverErr != nil {
			logger.Load(ctx).
				WarnContext(ctx, "cloudwatchlogs: failed to deliver log events to subscription filter",
					"logGroup", groupName, "filterName", f.FilterName, "destination", f.DestinationArn, "error", deliverErr)
		}
	}
}

// encodeSubscriptionPayload gzips the JSON payload and base64-encodes it.
func encodeSubscriptionPayload(payload subscriptionPayload) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err = gz.Write(raw); err != nil {
		return nil, err
	}

	if err = gz.Close(); err != nil {
		return nil, err
	}

	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	return []byte(encoded), nil
}

func cloneSubscriptionFilters(filters []*SubscriptionFilter) []*SubscriptionFilter {
	if len(filters) == 0 {
		return nil
	}

	out := make([]*SubscriptionFilter, len(filters))
	for i, f := range filters {
		cp := *f
		out[i] = &cp
	}

	return out
}
