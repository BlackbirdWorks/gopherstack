package cloudwatch

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

// ErrInvalidNextToken is returned when a pagination NextToken is invalid or expired.
var ErrInvalidNextToken = errors.New("InvalidParameterValue: NextToken is invalid or expired")

// ErrValueAndStatisticSet is returned when both Value and StatisticSet are set in a MetricDatum.
var ErrValueAndStatisticSet = errors.New("InvalidParameterCombination: Value and StatisticSet are mutually exclusive")

// tokenSecret is used to HMAC-sign pagination tokens so we can reject spoofed ones.
// In a real system this would be loaded from config; here we use a deterministic value.
var tokenSecret = []byte("gopherstack-cw-token-v1") //nolint:gochecknoglobals // package-level signing key

// signToken signs a raw token string and returns a "sig.payload" format string.
func signToken(raw string) string {
	mac := hmac.New(sha256.New, tokenSecret)
	mac.Write([]byte(raw))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return sig + "." + base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// verifyToken checks the signature on a signed token and returns the original payload.
// Returns ErrInvalidNextToken if the token is malformed or the signature does not match.
func verifyToken(signed string) (string, error) {
	sigB64, payB64, ok := strings.Cut(signed, ".")
	if !ok {
		return "", ErrInvalidNextToken
	}

	payload, err := base64.RawURLEncoding.DecodeString(payB64)
	if err != nil {
		return "", ErrInvalidNextToken
	}

	mac := hmac.New(sha256.New, tokenSecret)
	mac.Write(payload)
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sigB64), []byte(expected)) {
		return "", ErrInvalidNextToken
	}

	return string(payload), nil
}

// tokenPayloadLen is the byte length of a serialized page token offset.
const tokenPayloadLen = 8

// signPageToken wraps a raw integer-offset pagination token with HMAC signing.
// A value of 0 (start) is returned as empty string so callers can omit it.
func signPageToken(offset int) string {
	if offset == 0 {
		return ""
	}

	buf := make([]byte, tokenPayloadLen)
	binary.BigEndian.PutUint64(buf, uint64(offset)) //nolint:gosec // safe: offset is always non-negative

	return signToken(string(buf))
}

// parseSignedPageToken parses a signed page token and returns the integer offset.
// An empty token means offset 0 (start). Invalid tokens return ErrInvalidNextToken.
func parseSignedPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	raw, err := verifyToken(token)
	if err != nil {
		return 0, err
	}

	if len(raw) != tokenPayloadLen {
		return 0, ErrInvalidNextToken
	}

	u := binary.BigEndian.Uint64([]byte(raw))
	if u > uint64(^uint(0)>>1) {
		return 0, ErrInvalidNextToken
	}

	return int(u), nil
}

// validateMetricDatum returns an error when a MetricDatum has both Value and a StatisticSet.
// AWS rejects this combination with InvalidParameterCombination.
// This must be called before the handler normalizes the datum (before Count/Sum/Min/Max are
// derived from Value for single-value points).
func validateMetricDatum(d MetricDatum) error {
	if !d.HasStatisticSet {
		return nil
	}

	if d.Value != 0 {
		return fmt.Errorf("%w: MetricName=%s", ErrValueAndStatisticSet, d.MetricName)
	}

	return nil
}

// storageResolutionStandard is the standard (60-second) storage resolution.
const storageResolutionStandard int32 = 60

// storageResolutionHighRes is the high-resolution (1-second) storage resolution.
const storageResolutionHighRes int32 = 1

// validateStorageResolution returns an error when StorageResolution is not 1 or 60 (or 0 = default 60).
func validateStorageResolution(res int32) error {
	switch res {
	case 0, storageResolutionHighRes, storageResolutionStandard:
		return nil
	default:
		return fmt.Errorf("%w: StorageResolution must be %d or %d, got %d",
			ErrValidation, storageResolutionHighRes, storageResolutionStandard, res)
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

// anomalyBandWidth returns the half-width of an anomaly band given stddev and an
// optional detector configuration band width. Default is 2 standard deviations.
const defaultAnomalyBandStdDevs = 2.0

// computeAnomalyBand calculates an anomaly band (lower, upper) using rolling
// Z-score statistics over a window of data points.
// Returns (lower values, upper values) aligned to the input timestamps.
func computeAnomalyBand(values []float64) ([]float64, []float64) {
	if len(values) == 0 {
		return nil, nil
	}

	mean, stddev := rollingStats(values)
	lower := make([]float64, len(values))
	upper := make([]float64, len(values))
	halfWidth := defaultAnomalyBandStdDevs * stddev

	for i := range values {
		lower[i] = mean - halfWidth
		upper[i] = mean + halfWidth
	}

	return lower, upper
}

// rollingStats computes mean and population standard deviation of a float slice.
func rollingStats(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}

	sum := 0.0
	for _, v := range vals {
		sum += v
	}

	mean := sum / float64(len(vals))

	variance := 0.0
	for _, v := range vals {
		diff := v - mean
		variance += diff * diff
	}

	variance /= float64(len(vals))

	return mean, math.Sqrt(variance)
}

// evalAnomalyDetectionBand evaluates an ANOMALY_DETECTION_BAND(id, [stdDevs]) expression.
// Returns the MetricDataResult for the band (values are the upper bound; a companion
// lower-band result is computed separately by the caller).
// Returns ok=false if the expression does not match the ANOMALY_DETECTION_BAND pattern.
func evalAnomalyDetectionBand(
	expr string,
	resolved map[string]MetricDataResult,
) (MetricDataResult, MetricDataResult, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))

	const prefix = "ANOMALY_DETECTION_BAND("

	if !strings.HasPrefix(upper, prefix) || !strings.HasSuffix(upper, ")") {
		return MetricDataResult{}, MetricDataResult{}, false
	}

	inner := strings.TrimSpace(expr[len(prefix) : len(expr)-1])
	refID, stdDevsStr, hasStdDevs := strings.Cut(inner, ",")
	refID = strings.TrimSpace(refID)

	nStdDevs := defaultAnomalyBandStdDevs
	if hasStdDevs {
		s := strings.TrimSpace(stdDevsStr)
		if v, err := parseFloat(s); err == nil && v > 0 {
			nStdDevs = v
		}
	}

	base, baseOK := resolved[refID]
	if !baseOK || len(base.Values) == 0 {
		return MetricDataResult{}, MetricDataResult{}, true
	}

	mean, stddev := rollingStats(base.Values)
	halfWidth := nStdDevs * stddev

	uVals := make([]float64, len(base.Values))
	lVals := make([]float64, len(base.Values))

	for i := range base.Values {
		uVals[i] = mean + halfWidth
		lVals[i] = mean - halfWidth
	}

	upperResult := MetricDataResult{
		Timestamps: base.Timestamps,
		Values:     uVals,
		StatusCode: metricDataStatusComplete,
	}
	lowerResult := MetricDataResult{
		Timestamps: base.Timestamps,
		Values:     lVals,
		StatusCode: metricDataStatusComplete,
	}

	return upperResult, lowerResult, true
}

// parseFloat is a thin wrapper for strconv.ParseFloat to avoid importing strconv in this file.
func parseFloat(s string) (float64, error) {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}
