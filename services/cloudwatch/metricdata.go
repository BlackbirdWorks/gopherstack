package cloudwatch

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"time"
)

const (
	// metricDataStatusPartial indicates a result contains partial data (e.g. a
	// metric-math expression produced NaN/Inf for one or more points).
	metricDataStatusPartial = "PartialData"

	// defaultMaxDatapoints is the AWS default cap on the number of data points a
	// single GetMetricData call returns before it paginates.
	defaultMaxDatapoints = 100800

	// messageCodeArithmeticError flags metric-math points that could not be computed.
	messageCodeArithmeticError = "ArithmeticError"
	// messageCodeMaxDatapoints flags a response truncated to honour MaxDatapoints.
	messageCodeMaxDatapoints = "MaxDatapointsExceeded"
)

// metricDataCursor is the decoded form of a GetMetricData NextToken. It records
// where to resume paginating: ResultIndex is the index into the ordered result
// slice, PointOffset is the offset of the first unreturned datapoint within that
// result.
type metricDataCursor struct {
	ResultIndex int `json:"r"`
	PointOffset int `json:"o"`
}

// encodeMetricDataToken encodes a pagination cursor into an opaque base64 token.
func encodeMetricDataToken(c metricDataCursor) string {
	raw, err := json.Marshal(c)
	if err != nil {
		return ""
	}

	return base64.StdEncoding.EncodeToString(raw)
}

// decodeMetricDataToken decodes an opaque GetMetricData NextToken. A malformed or
// empty token resolves to the start of the result set.
func decodeMetricDataToken(token string) metricDataCursor {
	if token == "" {
		return metricDataCursor{}
	}

	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return metricDataCursor{}
	}

	var c metricDataCursor
	if err = json.Unmarshal(raw, &c); err != nil {
		return metricDataCursor{}
	}

	if c.ResultIndex < 0 {
		c.ResultIndex = 0
	}

	if c.PointOffset < 0 {
		c.PointOffset = 0
	}

	return c
}

// aggregateValuesCounts reduces a PutMetricData Values/Counts array pair (each
// Values[i] occurred Counts[i] times during the period) into the same
// Sum/SampleCount/Min/Max summary that a StatisticSet carries, so downstream
// bucket aggregation (populateBuckets) can treat every MetricDatum shape
// uniformly. Assumes len(values) == len(counts) and len(values) > 0; callers
// must validate the datum (via validateMetricDatum) before calling this.
func aggregateValuesCounts(values, counts []float64) (float64, float64, float64, float64) {
	var sum, count float64

	minV := math.MaxFloat64
	maxV := -math.MaxFloat64

	for i, v := range values {
		c := counts[i]
		sum += v * c
		count += c

		if v < minV {
			minV = v
		}

		if v > maxV {
			maxV = v
		}
	}

	return sum, count, minV, maxV
}

// annotateArithmeticMessages inspects a resolved metric-math result for NaN/Inf
// values. Any non-finite point demotes the result to PartialData and records a
// single ArithmeticError message, matching AWS behaviour for expressions that hit
// division-by-zero or similar undefined operations.
func annotateArithmeticMessages(r *MetricDataResult) {
	for _, v := range r.Values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			r.StatusCode = metricDataStatusPartial
			r.Messages = append(r.Messages, MetricDataMessage{
				Code:  messageCodeArithmeticError,
				Value: "One or more data points could not be computed (NaN or Infinity).",
			})

			return
		}
	}
}

// GetMetricDataPaged resolves the queries and applies MaxDatapoints pagination.
// maxDatapoints <= 0 uses the AWS default. The returned page carries a NextToken
// when the response was truncated, plus a top-level informational message so
// callers know more data is available.
func (b *InMemoryBackend) GetMetricDataPaged(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
	scanBy, nextToken string,
	maxDatapoints int,
) (GetMetricDataPage, error) {
	if maxDatapoints <= 0 {
		maxDatapoints = defaultMaxDatapoints
	}

	b.mu.RLock("GetMetricDataPaged")
	all := b.resolveMetricDataQueries(queries, startTime, endTime, scanBy)
	b.mu.RUnlock()

	return paginateMetricData(all, maxDatapoints, nextToken), nil
}

// paginateMetricData slices a fully-resolved result set into a single page that
// contains at most maxDatapoints data points, resuming from the supplied token.
// Empty results (no datapoints) are always emitted so callers still observe the
// result IDs and any per-result messages.
func paginateMetricData(
	all []MetricDataResult,
	maxDatapoints int,
	nextToken string,
) GetMetricDataPage {
	cursor := decodeMetricDataToken(nextToken)

	page := GetMetricDataPage{Results: make([]MetricDataResult, 0, len(all))}
	budget := maxDatapoints

	for i := cursor.ResultIndex; i < len(all); i++ {
		src := all[i]

		start := 0
		if i == cursor.ResultIndex {
			start = cursor.PointOffset
		}

		if start > len(src.Values) {
			start = len(src.Values)
		}

		remaining := len(src.Values) - start

		// Empty result (or fully consumed): emit metadata-only entry.
		if remaining == 0 {
			page.Results = append(page.Results, emptyPointsResult(src))

			continue
		}

		take := min(remaining, budget)

		page.Results = append(page.Results, sliceResult(src, start, start+take))
		budget -= take

		if take < remaining {
			// Ran out of budget partway through this result: page ends here.
			page.NextToken = encodeMetricDataToken(metricDataCursor{
				ResultIndex: i,
				PointOffset: start + take,
			})

			break
		}

		if budget == 0 && i+1 < len(all) {
			// Exhausted the budget exactly at a result boundary and more remain.
			page.NextToken = encodeMetricDataToken(metricDataCursor{
				ResultIndex: i + 1,
				PointOffset: 0,
			})

			break
		}
	}

	if page.NextToken != "" {
		page.Messages = append(page.Messages, MetricDataMessage{
			Code: messageCodeMaxDatapoints,
			Value: "The response was truncated to the requested MaxDatapoints. " +
				"Use the returned NextToken to retrieve the remaining data points.",
		})
	}

	return page
}

// sliceResult returns a copy of r restricted to points [lo, hi).
func sliceResult(r MetricDataResult, lo, hi int) MetricDataResult {
	out := r
	out.Timestamps = append([]time.Time(nil), r.Timestamps[lo:hi]...)
	out.Values = append([]float64(nil), r.Values[lo:hi]...)

	return out
}

// emptyPointsResult returns a copy of r carrying no data points but preserving
// the ID, Label, StatusCode, and Messages.
func emptyPointsResult(r MetricDataResult) MetricDataResult {
	out := r
	out.Timestamps = []time.Time{}
	out.Values = []float64{}

	return out
}
