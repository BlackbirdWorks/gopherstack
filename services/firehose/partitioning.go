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
// lambdaKeys (parallel to records, may be nil) are processor partitionKeys and query maps keyID to
// its jq path; records with unresolvable keys are returned as failures.
func resolvePartitions(
	records [][]byte,
	lambdaKeys []map[string]string,
	prefix string,
	dp *DynamicPartitioningConfiguration,
	query map[string]string,
) ([]partitionGroup, [][]byte) {
	exprs := extractPartitionExpressions(prefix)
	if dp == nil || !dp.Enabled || len(exprs) == 0 {
		return []partitionGroup{{prefix: prefix, records: records}}, nil
	}

	var failed [][]byte
	byPrefix := make(map[string][][]byte)
	order := make([]string, 0)

	for i, rec := range records {
		var keys map[string]string
		if i < len(lambdaKeys) {
			keys = lambdaKeys[i]
		}

		resolved, ok := resolveRecordPrefix(rec, keys, prefix, exprs, query)
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

// partitionValue resolves one expression, decoding the record's JSON at most once.
func partitionValue(
	rec []byte,
	obj *map[string]any,
	parsed *bool,
	lambdaKeys map[string]string,
	expr partitionExpression,
	query map[string]string,
) (string, bool) {
	if expr.namespace == partitionLambdaNamespace {
		val, ok := lambdaKeys[expr.jqPath]

		return val, ok
	}

	if !*parsed {
		*parsed = true
		if err := json.Unmarshal(rec, obj); err != nil {
			return "", false
		}
	}

	path, found := query[expr.jqPath]
	if !found && strings.HasPrefix(expr.jqPath, ".") {
		path, found = expr.jqPath, true
	}

	if !found || *obj == nil {
		return "", false
	}

	return evalJQPath(*obj, path)
}

// metadataExtractionQuery parses the MetadataExtraction processor's {key: .path, ...} query
// into keyID -> path. Only plain object construction over simple paths is understood.
func metadataExtractionQuery(pc *ProcessingConfiguration) map[string]string {
	if pc == nil {
		return nil
	}

	for _, proc := range pc.Processors {
		if proc.Type != "MetadataExtraction" {
			continue
		}

		for _, p := range proc.Parameters {
			if p.ParameterName == "MetadataExtractionQuery" {
				return parseExtractionQuery(p.ParameterValue)
			}
		}
	}

	return nil
}

func parseExtractionQuery(q string) map[string]string {
	q = strings.TrimSpace(q)
	q = strings.TrimSuffix(strings.TrimPrefix(q, "{"), "}")

	out := make(map[string]string)

	for _, part := range splitTopLevel(q) {
		key, path, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}

		out[strings.Trim(strings.TrimSpace(key), `"`)] = strings.TrimSpace(path)
	}

	return out
}

func splitTopLevel(s string) []string {
	var parts []string

	depth, start := 0, 0

	for i, r := range s {
		switch r {
		case '[', '(', '{':
			depth++
		case ']', ')', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}

	return append(parts, s[start:])
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

// resolveRecordPrefix substitutes every partition expression in prefix. It returns ok=false
// when a referenced key cannot be resolved for the record.
func resolveRecordPrefix(
	rec []byte,
	lambdaKeys map[string]string,
	prefix string,
	exprs []partitionExpression,
	query map[string]string,
) (string, bool) {
	var obj map[string]any

	parsed := false
	resolved := prefix

	for _, expr := range exprs {
		val, ok := partitionValue(rec, &obj, &parsed, lambdaKeys, expr, query)
		if !ok || val == "" {
			return "", false
		}

		resolved = strings.ReplaceAll(resolved, expr.token, val)
	}

	return resolved, true
}

// evalJQPath evaluates a simple jq path (".a.b", ".a[0].b") against a decoded JSON object
// and returns the scalar rendered as a string.
func evalJQPath(obj map[string]any, path string) (string, bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(path), ".")
	if trimmed == "" {
		return "", false
	}

	var cur any = obj

	for seg := range strings.SplitSeq(trimmed, ".") {
		name, rest, _ := strings.Cut(seg, "[")

		if name != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return "", false
			}

			if cur, ok = m[name]; !ok {
				return "", false
			}
		}

		var ok bool
		if cur, ok = indexInto(cur, rest); !ok {
			return "", false
		}
	}

	return scalarToString(cur)
}

// indexInto applies the "0][1]" index suffix left after a segment's name.
func indexInto(cur any, rest string) (any, bool) {
	for rest != "" {
		num, tail, found := strings.Cut(rest, "]")
		if !found {
			return nil, false
		}

		idx, err := strconv.Atoi(num)
		arr, isArr := cur.([]any)

		if err != nil || !isArr || idx < 0 || idx >= len(arr) {
			return nil, false
		}

		cur = arr[idx]
		rest = strings.TrimPrefix(tail, "[")
	}

	return cur, true
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
