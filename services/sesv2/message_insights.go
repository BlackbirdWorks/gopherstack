package sesv2

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// MetricDataQuery represents a single query for BatchGetMetricData, matching
// types.BatchGetMetricDataQuery.
type MetricDataQuery struct {
	StartDate  time.Time         `json:"startDate"`
	EndDate    time.Time         `json:"endDate"`
	Dimensions map[string]string `json:"dimensions"`
	ID         string            `json:"id"`
	Namespace  string            `json:"namespace"`
	Metric     string            `json:"metric"`
}

// MetricDataResult is the result for a single metric query, matching
// types.MetricDataResult (Values is a cumulative/sum count, hence int64).
type MetricDataResult struct {
	ID         string      `json:"id"`
	Timestamps []time.Time `json:"timestamps"`
	Values     []int64     `json:"values"`
}

// metricSend is the only types.Metric value gopherstack has real backing
// data for; see BatchGetMetricData.
const metricSend = "SEND"

// dimensionEmailIdentity is the only types.MetricDimensionName gopherstack
// can filter on for real (via each Email's From address); see
// BatchGetMetricData.
const dimensionEmailIdentity = "EMAIL_IDENTITY"

// BatchGetMetricData returns real per-day SEND counts derived from
// gopherstack's actual send history (b.emails) when a query's Metric is SEND
// and its only dimension (if any) is EMAIL_IDENTITY -- gopherstack records
// every SendEmail call with its From address and timestamp, so that
// combination is genuine aggregated data, not a placeholder. Every other
// Metric (COMPLAINT/PERMANENT_BOUNCE/TRANSIENT_BOUNCE/OPEN/CLICK/DELIVERY/
// DELIVERY_OPEN/DELIVERY_CLICK/DELIVERY_COMPLAINT) requires a
// delivery/bounce/complaint/engagement-tracking pipeline gopherstack doesn't
// have, and the CONFIGURATION_SET/ISP dimensions require a per-email
// association gopherstack doesn't track either -- those combinations (and
// any SEND query matching zero real sends) fall back to a single
// zero-valued datapoint rather than a fabricated non-zero count.
func (b *InMemoryBackend) BatchGetMetricData(
	queries []MetricDataQuery,
) ([]MetricDataResult, error) {
	b.mu.RLock("BatchGetMetricData")
	emails := make([]Email, len(b.emails))
	copy(emails, b.emails)
	b.mu.RUnlock()

	results := make([]MetricDataResult, 0, len(queries))
	for _, q := range queries {
		results = append(results, metricDataResultForQuery(q, emails))
	}

	return results, nil
}

// metricDataResultForQuery computes the real result for a single query when
// possible, falling back to the honest single zero-valued datapoint
// otherwise.
func metricDataResultForQuery(q MetricDataQuery, emails []Email) MetricDataResult {
	if supported, identityFilter := sendMetricSupported(q); supported {
		timestamps, values := sendMetricCounts(emails, q.StartDate, q.EndDate, identityFilter)
		if len(timestamps) > 0 {
			return MetricDataResult{ID: q.ID, Timestamps: timestamps, Values: values}
		}
	}

	now := time.Now().UTC().Truncate(time.Hour)

	return MetricDataResult{ID: q.ID, Timestamps: []time.Time{now}, Values: []int64{0}}
}

// sendMetricSupported reports whether q is a combination gopherstack can
// compute real SEND data for (Metric SEND, and either no dimensions or only
// EMAIL_IDENTITY), returning the EMAIL_IDENTITY filter value when present.
func sendMetricSupported(q MetricDataQuery) (bool, string) {
	if q.Metric != metricSend {
		return false, ""
	}

	switch len(q.Dimensions) {
	case 0:
		return true, ""
	case 1:
		if v, ok := q.Dimensions[dimensionEmailIdentity]; ok {
			return true, v
		}
	}

	return false, ""
}

// matchesIdentityFilter reports whether from was sent from identity, either
// as an exact address match or via its domain portion -- the same
// resolution checkFromIdentityLocked uses for SendEmail's from-identity check.
func matchesIdentityFilter(from, identity string) bool {
	if from == identity {
		return true
	}

	if at := strings.LastIndex(from, "@"); at >= 0 {
		return from[at+1:] == identity
	}

	return false
}

// sendMetricCounts buckets SEND events from emails by UTC calendar day
// within [start, end] (an unset bound means "no filter on that side"),
// optionally restricted to identityFilter, and returns them as parallel
// sorted-by-day Timestamps/Values slices.
func sendMetricCounts(emails []Email, start, end time.Time, identityFilter string) ([]time.Time, []int64) {
	buckets := make(map[string]int64)
	order := make([]string, 0)

	for _, e := range emails {
		if !start.IsZero() && e.Timestamp.Before(start) {
			continue
		}

		if !end.IsZero() && e.Timestamp.After(end) {
			continue
		}

		if identityFilter != "" && !matchesIdentityFilter(e.From, identityFilter) {
			continue
		}

		day := e.Timestamp.UTC().Format(time.DateOnly)
		if _, seen := buckets[day]; !seen {
			order = append(order, day)
		}

		buckets[day]++
	}

	sort.Strings(order)

	timestamps := make([]time.Time, 0, len(order))
	values := make([]int64, 0, len(order))

	for _, day := range order {
		t, _ := time.Parse(time.DateOnly, day)
		timestamps = append(timestamps, t)
		values = append(values, buckets[day])
	}

	return timestamps, values
}

// GetMessageInsights returns per-destination insights for a previously sent
// email, matching GetMessageInsightsOutput/types.EmailInsights. Real SES v2
// returns NotFoundException for a MessageId it doesn't recognize (confirmed
// against the op's documented error list); gopherstack's message history
// (SendEmail's b.emails, capped at maxRetainedEmails) is the closest
// equivalent to AWS's message tracking store. Only a synthetic SEND event is
// reported per destination -- gopherstack has no delivery/bounce/complaint
// pipeline to generate DELIVERY/BOUNCE/COMPLAINT events from.
func (b *InMemoryBackend) GetMessageInsights(messageID string) (map[string]any, error) {
	b.mu.RLock("GetMessageInsights")
	defer b.mu.RUnlock()

	for _, e := range b.emails {
		if e.MessageID != messageID {
			continue
		}

		insights := make([]map[string]any, 0, len(e.To))
		for _, dest := range e.To {
			insights = append(insights, map[string]any{
				"Destination": dest,
				"Events": []map[string]any{
					{"Type": "SEND", "Timestamp": awstime.Epoch(e.Timestamp)},
				},
			})
		}

		return map[string]any{
			keyMessageID:       e.MessageID,
			"FromEmailAddress": e.From,
			keySubject:         e.Subject,
			"Insights":         insights,
		}, nil
	}

	return nil, fmt.Errorf("%w: message %s not found", ErrNotFound, messageID)
}
