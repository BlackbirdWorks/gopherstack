package cloudwatchlogs

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// metricFilterMatch holds a metric filter and the count of events that matched it.
// metricFilterMatch pairs a metric filter with the raw messages (in event
// order) of every log event that matched it. The messages -- not just a count
// -- are needed because MetricTransformation.MetricValue may reference a named
// field ("$size", "$.bytes") that must be extracted per-event rather than a
// single fixed literal applied matchCount times.
type metricFilterMatch struct {
	filter   *MetricFilter
	messages []string
}

// matchingMetricFilters returns metric filters for groupName whose pattern matches at least one
// of the given events, along with the matched messages themselves (see metricFilterMatch).
// Events outer, filters inner: each event is visited once regardless of filter count,
// cutting allocations from O(filters×events) repeated scans to a single pass.
// Must be called while holding the write lock.
func (b *InMemoryBackend) matchingMetricFilters(
	region, groupName string,
	events []InputLogEvent,
) []metricFilterMatch {
	mfMap := b.metricFiltersInGroup(region, groupName)
	if len(mfMap) == 0 {
		return nil
	}

	// Pre-compile all patterns once, keyed by filter name.
	type filterEntry struct {
		filter   *MetricFilter
		compiled *compiledFilterPattern
	}
	entries := make([]filterEntry, 0, len(mfMap))
	for _, f := range mfMap {
		entries = append(
			entries,
			filterEntry{filter: f, compiled: b.getCompiledPattern(f.FilterPattern)},
		)
	}

	matchedMsgs := make([][]string, len(entries))
	for _, ev := range events {
		for i, e := range entries {
			if e.compiled == nil || e.compiled.matches(ev.Message) {
				matchedMsgs[i] = append(matchedMsgs[i], ev.Message)
			}
		}
	}

	var matched []metricFilterMatch
	for i, e := range entries {
		if len(matchedMsgs[i]) > 0 {
			cp := *e.filter
			matched = append(matched, metricFilterMatch{filter: &cp, messages: matchedMsgs[i]})
		}
	}

	return matched
}

// emitMetricFilterMatches calls the MetricEmitter for each matched metric filter transformation.
// One data point is emitted per matched event per transformation, with the value resolved by
// metricTransformationValue (a literal MetricValue, or a per-event field extraction).
func (b *InMemoryBackend) emitMetricFilterMatches(
	emitter MetricEmitter,
	matches []metricFilterMatch,
) {
	for _, m := range matches {
		compiled := b.getCompiledPattern(m.filter.FilterPattern)
		for _, t := range m.filter.MetricTransformations {
			for _, msg := range m.messages {
				val, ok := metricTransformationValue(compiled, msg, t)
				if !ok {
					continue
				}
				if emitErr := emitter.EmitMetric(t.MetricNamespace, t.MetricName, val, t.Unit); emitErr != nil {
					logger.Load(b.ctx).Warn(
						"cloudwatchlogs: metric filter emit failed",
						"namespace", t.MetricNamespace,
						"metric", t.MetricName,
						"err", emitErr,
					)
				}
			}
		}
	}
}

// metricTransformationValue resolves the numeric value to publish for one matched log event.
// A literal numeric MetricValue (e.g. "1") publishes as-is for every match. A field reference
// ("$size" for space-delimited patterns, "$.bytes" for JSON patterns) extracts that field from
// this specific matched message.
//
// AWS's DefaultValue is documented as "the value to emit when a filter pattern does NOT match a
// log event" (a periodic/no-data-point substitute), not a fallback for failed per-event field
// extraction -- so a missing or non-numeric field on a MATCHED event silently emits no data point
// for that event, matching real CloudWatch Logs metric filter behavior, rather than fabricating a
// value.
func metricTransformationValue(compiled *compiledFilterPattern, msg string, t MetricTransformation) (float64, bool) {
	if f, err := strconv.ParseFloat(t.MetricValue, 64); err == nil {
		return f, true
	}

	return compiled.extractValue(msg, t.MetricValue)
}

// PutMetricFilter creates or updates a metric filter for a log group.
func (b *InMemoryBackend) PutMetricFilter(
	ctx context.Context,
	logGroupName, filterName, filterPattern string,
	transformations []MetricTransformation,
) error {
	if logGroupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}
	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}
	if len(transformations) == 0 {
		return fmt.Errorf("%w: at least one metricTransformation is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutMetricFilter")
	defer b.mu.Unlock()

	group, exists := b.groupGet(region, logGroupName)
	if !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	creationTime := time.Now().UnixMilli()
	if existing, ok := b.metricFilterGet(region, logGroupName, filterName); ok {
		creationTime = existing.CreationTime
	}

	mf := &MetricFilter{
		FilterName:            filterName,
		LogGroupName:          logGroupName,
		FilterPattern:         filterPattern,
		MetricTransformations: append([]MetricTransformation(nil), transformations...),
		CreationTime:          creationTime,
		region:                region,
	}
	b.metricFilters.Put(mf)
	count := len(b.metricFiltersInGroup(region, logGroupName))
	group.MetricFilterCount = int32(
		count,
	) // #nosec G115 -- count bounded by AWS API limit

	return nil
}

// DescribeMetricFilters lists metric filters with optional filters.
func (b *InMemoryBackend) DescribeMetricFilters(
	ctx context.Context,
	logGroupName, filterNamePrefix, metricName, metricNamespace, nextToken string,
	limit int,
) ([]MetricFilter, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMetricFilters")
	defer b.mu.RUnlock()

	var filterSet []*MetricFilter
	if logGroupName != "" {
		filterSet = b.metricFiltersInGroup(region, logGroupName)
	} else {
		filterSet = b.metricFilters.All()
	}

	var all []MetricFilter
	for _, mf := range filterSet {
		if mf.region != region {
			continue
		}
		if !metricFilterMatches(mf, filterNamePrefix, metricName, metricNamespace) {
			continue
		}
		cp := *mf
		cp.MetricTransformations = append(
			[]MetricTransformation(nil),
			mf.MetricTransformations...)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].LogGroupName != all[j].LogGroupName {
			return all[i].LogGroupName < all[j].LogGroupName
		}

		return all[i].FilterName < all[j].FilterName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []MetricFilter{}, "", nil
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

// metricFilterMatches returns true if mf passes the given filter criteria.
func metricFilterMatches(
	mf *MetricFilter,
	filterNamePrefix, metricName, metricNamespace string,
) bool {
	if filterNamePrefix != "" && !strings.HasPrefix(mf.FilterName, filterNamePrefix) {
		return false
	}
	if metricName == "" && metricNamespace == "" {
		return true
	}
	for _, t := range mf.MetricTransformations {
		if (metricName == "" || t.MetricName == metricName) &&
			(metricNamespace == "" || t.MetricNamespace == metricNamespace) {
			return true
		}
	}

	return false
}

// DeleteMetricFilter deletes a metric filter from a log group.
func (b *InMemoryBackend) DeleteMetricFilter(
	ctx context.Context,
	logGroupName, filterName string,
) error {
	if logGroupName == "" {
		return fmt.Errorf("%w: logGroupName is required", ErrValidation)
	}
	if filterName == "" {
		return fmt.Errorf("%w: filterName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMetricFilter")
	defer b.mu.Unlock()

	group, exists := b.groupGet(region, logGroupName)
	if !exists {
		return fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, logGroupName)
	}

	if !b.metricFilters.Delete(metricFilterTableKey(region, logGroupName, filterName)) {
		return fmt.Errorf(
			"%w: metric filter %s not found in log group %s",
			ErrMetricFilterNotFound,
			filterName,
			logGroupName,
		)
	}
	count := len(b.metricFiltersInGroup(region, logGroupName))
	group.MetricFilterCount = int32(
		count,
	) // #nosec G115 -- count bounded by AWS API limit

	return nil
}

// TestMetricFilter tests a metric filter pattern against provided log event messages.
func (b *InMemoryBackend) TestMetricFilter(
	filterPattern string,
	logEventMessages []string,
) ([]MetricFilterMatchRecord, error) {
	if filterPattern == "" {
		return nil, fmt.Errorf("%w: filterPattern is required", ErrValidation)
	}

	compiled := compileFilterPattern(filterPattern)
	fieldRefs := patternFieldRefs(filterPattern)
	matches := make([]MetricFilterMatchRecord, 0, len(logEventMessages))
	for i, msg := range logEventMessages {
		if !compiled.matches(msg) {
			continue
		}

		extracted := make(map[string]string, len(fieldRefs))
		for _, ref := range fieldRefs {
			if val, ok := compiled.extractString(msg, ref); ok {
				extracted[ref] = val
			}
		}

		matches = append(matches, MetricFilterMatchRecord{
			EventMessage:    msg,
			EventNumber:     int64(i + 1),
			ExtractedValues: extracted,
		})
	}

	return matches, nil
}
