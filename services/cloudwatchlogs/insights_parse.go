package cloudwatchlogs

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// parseParseCommand parses "parse [source] <"glob"|/regex/> [as f1, f2, ...]".
func parseParseCommand(q *insightsQuery, rest string) error {
	source, patternPart := splitParseSource(rest)

	pat, after, err := readPatternLiteral(patternPart)
	if err != nil {
		return err
	}

	names := parseAsNames(after)

	stage, err := buildParseStage(source, pat, names)
	if err != nil {
		return err
	}

	q.stages = append(q.stages, stage)

	return nil
}

// splitParseSource extracts an optional leading source field. When the remainder
// begins with a quote or regex literal, the source defaults to @message.
func splitParseSource(rest string) (string, string) {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return keyMessageField, ""
	}

	if rest[0] == '"' || rest[0] == '\'' || rest[0] == '/' {
		return keyMessageField, rest
	}

	idx := strings.IndexFunc(rest, func(r rune) bool { return r == ' ' || r == '\t' })
	if idx < 0 {
		return keyMessageField, rest
	}

	return rest[:idx], strings.TrimSpace(rest[idx+1:])
}

// readPatternLiteral reads a leading quoted glob or /regex/ literal, returning the
// token, the trailing text, and any error.
func readPatternLiteral(s string) (exprToken, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return exprToken{}, "", fmt.Errorf("%w: parse requires a pattern", ErrParseExpr)
	}

	switch s[0] {
	case '"', '\'':
		tok, next, err := lexString(s, 0)

		return tok, strings.TrimSpace(s[next:]), err
	case '/':
		tok, next, err := lexRegex(s, 0)

		return tok, strings.TrimSpace(s[next:]), err
	default:
		return exprToken{}, "", fmt.Errorf("%w: parse pattern must be quoted or /regex/", ErrParseExpr)
	}
}

// parseAsNames parses an optional "as f1, f2, ..." clause.
func parseAsNames(after string) []string {
	after = strings.TrimSpace(after)
	lower := strings.ToLower(after)
	if !strings.HasPrefix(lower, "as ") {
		return nil
	}

	var names []string
	for _, n := range splitTopLevelCommas(after[len("as "):]) {
		n = strings.Trim(strings.TrimSpace(n), "`\"'")
		if n != "" {
			names = append(names, n)
		}
	}

	return names
}

// buildParseStage compiles the parse pattern into an executable stage.
func buildParseStage(source string, pat exprToken, names []string) (queryStage, error) {
	if pat.kind == tkRegex {
		// Logs Insights uses the (?<name>...) named-group syntax; translate it to
		// Go's regexp (?P<name>...) form.
		re, err := regexp.Compile(normalizeNamedGroups(pat.text))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrParseExpr, err)
		}

		return &parseStage{source: source, re: re, names: names}, nil
	}

	re, err := globToRegex(pat.text)
	if err != nil {
		return nil, err
	}

	return &parseStage{source: source, re: re, names: names}, nil
}

// normalizeNamedGroups rewrites "(?<name>" to Go's "(?P<name>" named-group form.
func normalizeNamedGroups(pattern string) string {
	re := regexp.MustCompile(`\(\?<([a-zA-Z_][a-zA-Z0-9_]*)>`)

	return re.ReplaceAllString(pattern, `(?P<$1>`)
}

// globToRegex converts a Logs Insights glob (literals plus "*" wildcards) into a
// capturing regular expression. Each "*" becomes a non-greedy capture group.
func globToRegex(glob string) (*regexp.Regexp, error) {
	var sb strings.Builder

	segments := strings.Split(glob, "*")
	for i, seg := range segments {
		sb.WriteString(regexp.QuoteMeta(seg))
		if i < len(segments)-1 {
			sb.WriteString("(.*?)")
		}
	}

	re, err := regexp.Compile(sb.String())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParseExpr, err)
	}

	return re, nil
}

// parseStage extracts fields from a source field using a regular expression.
// Named capture groups populate fields by name; the optional names slice renames
// (or, for glob patterns, names) the positional capture groups.
type parseStage struct {
	re     *regexp.Regexp
	source string
	names  []string
}

func (s *parseStage) apply(recs []*insightsRecord) []*insightsRecord {
	subexpNames := s.re.SubexpNames()

	for _, rec := range recs {
		m := s.re.FindStringSubmatch(rec.resolveString(s.source))
		if m == nil {
			continue
		}

		s.assignGroups(rec, m, subexpNames)
	}

	return recs
}

// assignGroups writes capture-group values into record fields.
func (s *parseStage) assignGroups(rec *insightsRecord, m, subexpNames []string) {
	for gi := 1; gi < len(m); gi++ {
		if name := s.groupName(gi, subexpNames); name != "" {
			rec.setField(name, stringValue(m[gi]))
		}
	}
}

// groupName resolves the output field name for capture group gi (1-based). The
// explicit names list takes precedence, then any named subexpression.
func (s *parseStage) groupName(gi int, subexpNames []string) string {
	if idx := gi - 1; idx < len(s.names) {
		return s.names[idx]
	}

	if gi < len(subexpNames) {
		return subexpNames[gi]
	}

	return ""
}

// resolveJSONField extracts a dotted/bracketed field path from a JSON message.
// It returns an unset value when the message is not JSON or the path is absent.
func resolveJSONField(message, name string) cwValue {
	message = strings.TrimSpace(message)
	if message == "" || (message[0] != '{' && message[0] != '[') {
		return cwValue{}
	}

	var root any
	if err := json.Unmarshal([]byte(message), &root); err != nil {
		return cwValue{}
	}

	return lookupJSONPath(root, name)
}

// lookupJSONPath navigates a parsed JSON value using a dotted/bracketed path.
func lookupJSONPath(root any, path string) cwValue {
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")

	cur := root
	for _, seg := range splitJSONPath(path) {
		next, ok := jsonStep(cur, seg)
		if !ok {
			return cwValue{}
		}

		cur = next
	}

	return jsonScalar(cur)
}

// splitJSONPath breaks a path like "a.b[0].c" into segments ["a","b","0","c"].
func splitJSONPath(path string) []string {
	replaced := strings.NewReplacer("[", ".", "]", "").Replace(path)

	var segs []string
	for s := range strings.SplitSeq(replaced, ".") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// jsonStep descends one segment into a JSON object or array.
func jsonStep(cur any, seg string) (any, bool) {
	switch node := cur.(type) {
	case map[string]any:
		v, ok := node[seg]

		return v, ok
	case []any:
		idx, err := strconv.Atoi(seg)
		if err != nil || idx < 0 || idx >= len(node) {
			return nil, false
		}

		return node[idx], true
	default:
		return nil, false
	}
}

// jsonScalar converts a leaf JSON value into a cwValue.
func jsonScalar(v any) cwValue {
	switch t := v.(type) {
	case float64:
		return numFromFloat(t)
	case string:
		return stringValue(t)
	case bool:
		if t {
			return literalString("1")
		}

		return literalString("0")
	case nil:
		return cwValue{}
	default:
		if b, err := json.Marshal(t); err == nil {
			return literalString(string(b))
		}

		return cwValue{}
	}
}
