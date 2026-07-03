package firehose

import (
	"encoding/json"
	"strconv"
	"strings"
)

// partitionQueryNamespace is the Firehose namespace for jq-derived partition keys.
const partitionQueryNamespace = "partitionKeyFromQuery"

// partitionLambdaNamespace is the Firehose namespace for Lambda-derived partition keys.
const partitionLambdaNamespace = "partitionKeyFromLambda"

// partitionGroup holds the records that resolve to a single dynamic-partitioning prefix.
type partitionGroup struct {
	prefix  string
	records [][]byte
}

// resolvePartitions groups records by the dynamic-partitioning prefix they resolve to.
//
// When dynamic partitioning is disabled or the prefix contains no partition-key
// expressions, all records map to a single group with the literal prefix. Otherwise each
// record's JSON body is evaluated against every !{partitionKeyFromQuery:<jq>} expression in
// the prefix; records whose keys all resolve are grouped under the substituted prefix, and
// records that cannot be partitioned (non-JSON body or a missing key) are returned as
// failures so the caller routes them to the error output — matching AWS behaviour.
func resolvePartitions(
	records [][]byte,
	prefix string,
	dp *DynamicPartitioningConfiguration,
) ([]partitionGroup, [][]byte) {
	exprs := extractPartitionExpressions(prefix)
	if dp == nil || !dp.Enabled || len(exprs) == 0 {
		return []partitionGroup{{prefix: prefix, records: records}}, nil
	}

	var failed [][]byte
	byPrefix := make(map[string][][]byte)
	order := make([]string, 0)

	for _, rec := range records {
		resolved, ok := resolveRecordPrefix(rec, prefix, exprs)
		if !ok {
			failed = append(failed, rec)

			continue
		}

		if _, seen := byPrefix[resolved]; !seen {
			order = append(order, resolved)
		}
		byPrefix[resolved] = append(byPrefix[resolved], rec)
	}

	groups := make([]partitionGroup, 0, len(order))
	for _, p := range order {
		groups = append(groups, partitionGroup{prefix: p, records: byPrefix[p]})
	}

	return groups, failed
}

// partitionExpression is a single !{namespace:jqPath} token found in a prefix.
type partitionExpression struct {
	token     string // full token including the !{...} wrapper
	namespace string
	jqPath    string
}

// extractPartitionExpressions parses all !{partitionKeyFromQuery:<path>} and
// !{partitionKeyFromLambda:<path>} tokens from a prefix.
func extractPartitionExpressions(prefix string) []partitionExpression {
	var exprs []partitionExpression

	rest := prefix
	for {
		start := strings.Index(rest, "!{")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], "}")
		if end < 0 {
			break
		}
		token := rest[start : start+end+1]
		inner := token[2 : len(token)-1] // strip !{ and }
		rest = rest[start+end+1:]

		ns, path, ok := strings.Cut(inner, ":")
		if !ok {
			continue
		}
		if ns != partitionQueryNamespace && ns != partitionLambdaNamespace {
			continue
		}
		exprs = append(exprs, partitionExpression{token: token, namespace: ns, jqPath: path})
	}

	return exprs
}

// resolveRecordPrefix substitutes every partition expression in prefix using values
// extracted from the record's JSON body. It returns ok=false when the record is not a
// JSON object or when any referenced key is missing.
func resolveRecordPrefix(rec []byte, prefix string, exprs []partitionExpression) (string, bool) {
	var obj map[string]any
	if err := json.Unmarshal(rec, &obj); err != nil {
		return "", false
	}

	resolved := prefix
	for _, expr := range exprs {
		val, ok := evalJQPath(obj, expr.jqPath)
		if !ok {
			return "", false
		}
		resolved = strings.ReplaceAll(resolved, expr.token, val)
	}

	return resolved, true
}

// evalJQPath evaluates a simple jq-style path (e.g. ".a.b") against a decoded JSON object
// and returns the value rendered as a string. Only object traversal with dotted keys is
// supported, which covers the common Firehose partitioning expressions.
func evalJQPath(obj map[string]any, path string) (string, bool) {
	trimmed := strings.TrimSpace(path)
	trimmed = strings.TrimPrefix(trimmed, ".")
	if trimmed == "" {
		return "", false
	}

	segments := strings.Split(trimmed, ".")

	var cur any = obj
	for _, seg := range segments {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		next, ok := m[seg]
		if !ok {
			return "", false
		}
		cur = next
	}

	return scalarToString(cur)
}

// scalarToString renders a scalar JSON value as a partition-key string. Objects, arrays,
// and null are rejected because they cannot form a valid partition key.
func scalarToString(v any) (string, bool) {
	switch val := v.(type) {
	case string:
		return val, true
	case bool:
		if val {
			return "true", true
		}

		return "false", true
	case json.Number:
		return val.String(), true
	case float64:
		// Render integers without a trailing ".0" to match typical partition values.
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10), true
		}

		return strconv.FormatFloat(val, 'f', -1, 64), true
	default:
		return "", false
	}
}
