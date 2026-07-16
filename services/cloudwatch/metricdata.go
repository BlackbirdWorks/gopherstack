package cloudwatch

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
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

// GetMetricData executes multiple metric queries and returns results.
// Queries with a MetricStat are resolved first; expression queries are evaluated
// after all metric-stat results are available so expressions can reference them.
// scanBy controls sort order: "TimestampDescending" reverses each result; default ascending.
func (b *InMemoryBackend) GetMetricData(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
) ([]MetricDataResult, error) {
	return b.GetMetricDataWithOptions(queries, startTime, endTime, "")
}

// GetMetricDataWithOptions is GetMetricData extended with scan order control.
// scanBy may be "TimestampDescending" or "" / "TimestampAscending" (default).
func (b *InMemoryBackend) GetMetricDataWithOptions(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
	scanBy string,
) ([]MetricDataResult, error) {
	b.mu.RLock("GetMetricData")
	defer b.mu.RUnlock()

	return b.resolveMetricDataQueries(queries, startTime, endTime, scanBy), nil
}

// resolveMetricDataQueries resolves every query into a MetricDataResult, preserving
// query order and honouring ReturnData / ScanBy. Per-result diagnostic Messages and
// the PartialData status are attached where metric math produced NaN/Inf.
// Caller must hold b.mu (at least a read lock).
func (b *InMemoryBackend) resolveMetricDataQueries(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
	scanBy string,
) []MetricDataResult {
	resolved := make(map[string]MetricDataResult, len(queries))
	// Preserve original query order for the returned slice.
	ordered := make([]string, 0, len(queries))

	// First pass: resolve MetricStat queries.
	for _, q := range queries {
		ordered = append(ordered, q.ID)
		if q.Expression != "" {
			continue
		}
		resolved[q.ID] = b.resolveMetricStat(q, startTime, endTime)
	}

	// Second pass: evaluate expression queries in topological order so forward
	// references work regardless of declaration order.
	exprOrder, _ := topoSortExpressions(queries)

	exprByID := make(map[string]MetricDataQuery, len(queries))
	for _, q := range queries {
		if q.Expression != "" {
			exprByID[q.ID] = q
		}
	}

	for _, id := range exprOrder {
		q, ok := exprByID[id]
		if !ok {
			continue
		}
		r := evalExpression(q, resolved)
		annotateArithmeticMessages(&r)
		resolved[id] = r
	}

	descending := strings.EqualFold(scanBy, "TimestampDescending")

	results := make([]MetricDataResult, 0, len(queries))
	for i, id := range ordered {
		q := queries[i]
		r := resolved[id]
		// ReturnData defaults to true when the field is its zero value (false) AND it's not
		// an expression-only query. AWS semantics: omitting ReturnData means return it.
		// We model ReturnData=false as the caller explicitly setting it; since Go zero is false,
		// we cannot distinguish "not set" from "set false" without a pointer. We treat false as
		// "return data" for MetricStat queries, consistent with AWS SDK defaults.
		// For expression queries, ReturnData=false suppresses output.
		if q.Expression != "" && !q.ReturnData {
			continue
		}
		if descending && len(r.Timestamps) > 1 {
			reverseMetricDataResult(&r)
		}
		results = append(results, r)
	}

	return results
}

// reverseMetricDataResult reverses the timestamp and value slices in-place.
func reverseMetricDataResult(r *MetricDataResult) {
	n := len(r.Timestamps)
	for i := range n / 2 {
		j := n - 1 - i
		r.Timestamps[i], r.Timestamps[j] = r.Timestamps[j], r.Timestamps[i]
		r.Values[i], r.Values[j] = r.Values[j], r.Values[i]
	}
}

// resolveMetricStat fetches and aggregates data for a single MetricStat query.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) resolveMetricStat(
	q MetricDataQuery,
	startTime, endTime time.Time,
) MetricDataResult {
	// Cross-account queries reference metrics from another AWS account.
	// Return empty data gracefully — cross-account is not supported locally.
	if q.AccountID != "" && q.AccountID != b.accountID {
		label := q.Label
		if label == "" {
			label = q.MetricStat.MetricName
		}

		return MetricDataResult{
			ID:         q.ID,
			Label:      label,
			Timestamps: []time.Time{},
			Values:     []float64{},
			StatusCode: metricDataStatusComplete,
		}
	}

	ns := q.MetricStat.Namespace
	metricName := q.MetricStat.MetricName
	period := q.MetricStat.Period
	stat := q.MetricStat.Stat

	var all []MetricDatum
	if nsMap, ok := b.metrics[ns]; ok {
		key := metricStorageKey(metricName, q.MetricStat.Dimensions)
		if rec, found := nsMap[key]; found {
			all = rec.Points
		}
	}

	buckets := populateBuckets(all, startTime, endTime, period)

	statSet := map[string]bool{stat: true}
	var timestamps []time.Time
	var values []float64

	for _, bk := range buckets {
		if bk.count == 0 {
			continue
		}

		dp := buildDatapoint(bk, statSet)
		v := statValue(dp, stat)
		timestamps = append(timestamps, dp.Timestamp)
		values = append(values, v)
	}

	// Sort by timestamp ascending — AWS guarantees ascending order for GetMetricData.
	if len(timestamps) > 1 {
		type tsv struct {
			ts  time.Time
			val float64
		}
		pairs := make([]tsv, len(timestamps))
		for i := range timestamps {
			pairs[i] = tsv{timestamps[i], values[i]}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].ts.Before(pairs[j].ts) })
		for i := range pairs {
			timestamps[i] = pairs[i].ts
			values[i] = pairs[i].val
		}
	}

	label := q.Label
	if label == "" {
		label = metricName
	}

	return MetricDataResult{
		ID:         q.ID,
		Label:      label,
		Timestamps: timestamps,
		Values:     values,
		StatusCode: metricDataStatusComplete,
	}
}

// exprNode represents a node in the expression dependency graph for topological sorting.
type exprNode struct {
	id   string
	deps []string // IDs of metrics/expressions this one references
}

// topoSortExpressions performs a topological sort on expression queries so forward
// references are resolved before the expression that uses them.
// Returns the sorted list of expression IDs, or an error if a cycle is detected.
// MetricStat queries (non-expression) are not included in the output.
func topoSortExpressions(queries []MetricDataQuery) ([]string, error) {
	_, nodes := buildExprNodes(queries)
	if len(nodes) == 0 {
		return nil, nil
	}

	adjList, inDegree := buildExprGraph(nodes)

	return kahnSort(nodes, adjList, inDegree)
}

// buildExprNodes indexes expression queries and builds exprNode dependency lists.
func buildExprNodes(queries []MetricDataQuery) (map[string]bool, map[string]*exprNode) {
	isExpr := make(map[string]bool, len(queries))
	exprByID := make(map[string]MetricDataQuery, len(queries))

	for _, q := range queries {
		if q.Expression != "" {
			isExpr[q.ID] = true
			exprByID[q.ID] = q
		}
	}

	nodes := make(map[string]*exprNode, len(exprByID))
	for id, q := range exprByID {
		deps := extractExprDeps(q.Expression, isExpr)
		nodes[id] = &exprNode{id: id, deps: deps}
	}

	return isExpr, nodes
}

// buildExprGraph builds the adjacency list and in-degree map for Kahn's algorithm.
func buildExprGraph(nodes map[string]*exprNode) (map[string][]string, map[string]int) {
	adjList := make(map[string][]string, len(nodes))
	inDegree := make(map[string]int, len(nodes))

	for id := range nodes {
		inDegree[id] = 0
	}

	for id, node := range nodes {
		for _, dep := range node.deps {
			if _, ok := nodes[dep]; ok {
				adjList[dep] = append(adjList[dep], id)
				inDegree[id]++
			}
		}
	}

	return adjList, inDegree
}

// kahnSort executes Kahn's BFS topological sort and returns ordered IDs.
// Returns ErrValidation when a cycle is detected.
func kahnSort(nodes map[string]*exprNode, adjList map[string][]string, inDegree map[string]int) ([]string, error) {
	queue := make([]string, 0, len(nodes))

	for id := range nodes {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	stableSort(queue)

	sorted := make([]string, 0, len(nodes))

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		sorted = append(sorted, cur)

		neighbors := adjList[cur]
		stableSort(neighbors)

		for _, next := range neighbors {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if len(sorted) != len(nodes) {
		return nil, fmt.Errorf("%w: expression cycle detected", ErrValidation)
	}

	return sorted, nil
}

// stableSort sorts a string slice in-place (insertion sort for small slices).
func stableSort(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// extractExprDeps returns the set of IDs referenced in an expression that are in the known set.
// This is a simple scan: any word token that matches a known expression/metric ID.
func extractExprDeps(expr string, known map[string]bool) []string {
	// Tokenize: split on non-alphanumeric and non-underscore characters.
	tokens := make([]string, 0)
	cur := strings.Builder{}

	for _, r := range expr {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			cur.WriteRune(r)
		} else if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}

	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}

	seen := make(map[string]bool, len(tokens))
	deps := make([]string, 0)

	for _, tok := range tokens {
		if known[tok] && !seen[tok] {
			deps = append(deps, tok)
			seen[tok] = true
		}
	}

	return deps
}
