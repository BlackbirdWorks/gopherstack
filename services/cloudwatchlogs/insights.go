package cloudwatchlogs

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// keyPtr is the built-in field name for the record pointer returned by Logs Insights.
const keyPtr = "@ptr"

// Boolean/null literal keywords shared by the expression and JSON-pattern parsers.
const (
	literalTrue  = "true"
	literalFalse = "false"
	literalNull  = "null"
)

// twoCharToken is the byte length of a two-character operator token.
const twoCharToken = 2

// decimalPlaces is the number of fractional digits used when formatting
// non-integral aggregate results (avg, pct, stddev), matching AWS's typical
// Logs Insights output precision.
const decimalPlaces = 4

// insightsQuery is the parsed representation of a CloudWatch Logs Insights query.
// A query is a pipeline of stages executed left-to-right, exactly as AWS applies
// the pipe-separated commands. outputFields records the ordered set of columns the
// final result rows should contain; it is updated by fields/display/stats stages.
type insightsQuery struct {
	stages       []queryStage
	outputFields []string
	limit        int
	hasStats     bool
}

// queryStage is a single command in the Insights pipeline.
type queryStage interface {
	apply(recs []*insightsRecord) []*insightsRecord
}

// insightsRecord is a single log record flowing through the pipeline. It carries
// the underlying event plus any fields materialised by parse/fields/stats stages.
type insightsRecord struct {
	ev    *OutputLogEvent
	extra map[string]cwValue
	order []string
}

func newInsightsRecord(ev *OutputLogEvent) *insightsRecord {
	return &insightsRecord{ev: ev, extra: nil, order: nil}
}

// setField stores a materialised field value, preserving first-seen order.
func (r *insightsRecord) setField(name string, v cwValue) {
	if r.extra == nil {
		r.extra = make(map[string]cwValue)
	}

	if _, seen := r.extra[name]; !seen {
		r.order = append(r.order, name)
	}

	r.extra[name] = v
}

// resolve returns the value of a field for this record, checking materialised
// fields first, then Insights built-ins, then JSON extraction from @message.
func (r *insightsRecord) resolve(name string) cwValue {
	if r.extra != nil {
		if v, ok := r.extra[name]; ok {
			return v
		}
	}

	if v, ok := r.resolveBuiltin(name); ok {
		return v
	}

	return resolveJSONField(r.ev.Message, name)
}

// resolveBuiltin resolves the @-prefixed system fields.
func (r *insightsRecord) resolveBuiltin(name string) (cwValue, bool) {
	switch name {
	case keyMessageField:
		return stringValue(r.ev.Message), true
	case keyTimestamp:
		return numFromInt(r.ev.Timestamp), true
	case keyIngestionTime:
		return numFromInt(r.ev.IngestionTime), true
	case keyPtr:
		return stringValue(r.ev.Ptr), true
	default:
		return cwValue{}, false
	}
}

// resolveString returns the string form of a resolved field.
func (r *insightsRecord) resolveString(name string) string {
	return r.resolve(name).String()
}

// defaultInsightsFields are returned when no explicit fields/display command is given.
func defaultInsightsFields() []string {
	return []string{keyTimestamp, keyMessageField, keyPtr}
}

// parseInsightsQuery parses a CloudWatch Logs Insights query string into an
// insightsQuery. The query is a sequence of pipe-separated commands.
func parseInsightsQuery(query string) (*insightsQuery, error) {
	q := &insightsQuery{
		stages:       nil,
		outputFields: defaultInsightsFields(),
		limit:        0,
		hasStats:     false,
	}

	for _, cmd := range splitPipes(query) {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		if err := parseCommand(q, cmd); err != nil {
			return nil, err
		}
	}

	return q, nil
}

// cloneInsightsQuery returns a shallow copy safe for concurrent execution. Stages
// are immutable after parsing, so the slice header copy is sufficient.
func cloneInsightsQuery(q *insightsQuery) *insightsQuery {
	if q == nil {
		return nil
	}

	cp := *q
	cp.stages = slices.Clone(q.stages)
	cp.outputFields = slices.Clone(q.outputFields)

	return &cp
}

// parseCommand parses a single pipeline command and appends the corresponding stage.
func parseCommand(q *insightsQuery, cmd string) error {
	keyword, rest := splitKeyword(cmd)

	switch strings.ToLower(keyword) {
	case "fields", "display":
		return parseFieldsCommand(q, rest)
	case "filter", "where":
		return parseFilterCommand(q, rest)
	case "parse":
		return parseParseCommand(q, rest)
	case "sort":
		return parseSortCommand(q, rest)
	case "limit":
		return parseLimitCommand(q, rest)
	case "stats":
		return parseStatsCommand(q, rest)
	case "dedup":
		return parseDedupCommand(q, rest)
	default:
		// Unknown commands are ignored for forward compatibility.
		return nil
	}
}

// splitKeyword splits a command into its leading keyword and the remainder.
func splitKeyword(cmd string) (string, string) {
	cmd = strings.TrimSpace(cmd)
	idx := strings.IndexFunc(cmd, func(r rune) bool { return r == ' ' || r == '\t' })
	if idx < 0 {
		return cmd, ""
	}

	return cmd[:idx], strings.TrimSpace(cmd[idx+1:])
}

// executeQuery executes a parsed query against events and returns result rows.
func executeQuery(q *insightsQuery, events []*OutputLogEvent) [][]ResultField {
	recs := make([]*insightsRecord, len(events))
	for i, ev := range events {
		recs[i] = newInsightsRecord(ev)
	}

	for _, st := range q.stages {
		recs = st.apply(recs)
	}

	return projectRows(q, recs)
}

// projectRows converts the final record set into result rows using outputFields.
func projectRows(q *insightsQuery, recs []*insightsRecord) [][]ResultField {
	fields := q.outputFields
	if len(fields) == 0 {
		fields = defaultInsightsFields()
	}

	rows := make([][]ResultField, 0, len(recs))
	for _, rec := range recs {
		row := make([]ResultField, 0, len(fields))
		for _, f := range fields {
			v := rec.resolve(f)
			if !v.isSet {
				// AWS omits fields that do not resolve for a given record; keep
				// the column but with an empty value for stable row width.
				row = append(row, ResultField{Field: f, Value: ""})

				continue
			}

			row = append(row, ResultField{Field: f, Value: v.String()})
		}

		rows = append(rows, row)
	}

	return rows
}

// cwValue is a dynamically-typed value used during query evaluation. Numeric
// values carry both the parsed float and the original text so comparisons and
// output preserve AWS-accurate formatting.
type cwValue struct {
	text  string
	num   float64
	isNum bool
	isSet bool
}

// stringValue builds a string-typed value, inferring a numeric interpretation
// when the text parses cleanly as a number (so JSON/plain numeric strings sort
// and compare numerically, matching Logs Insights).
func stringValue(s string) cwValue {
	v := cwValue{text: s, num: 0, isNum: false, isSet: true}
	if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil && s != "" {
		v.num = f
		v.isNum = true
	}

	return v
}

// literalString builds a pure string value that is never treated as numeric,
// used for quoted string literals in queries.
func literalString(s string) cwValue {
	return cwValue{text: s, num: 0, isNum: false, isSet: true}
}

// numFromFloat builds a numeric value.
func numFromFloat(f float64) cwValue {
	return cwValue{text: formatFloat(f), num: f, isNum: true, isSet: true}
}

// numFromInt builds a numeric value from an integer.
func numFromInt(i int64) cwValue {
	return cwValue{text: strconv.FormatInt(i, 10), num: float64(i), isNum: true, isSet: true}
}

// String returns the display form of the value.
func (v cwValue) String() string {
	return v.text
}

// truthy reports whether the value is logically true for filter evaluation.
func (v cwValue) truthy() bool {
	if !v.isSet {
		return false
	}

	if v.isNum {
		return v.num != 0
	}

	return v.text != "" && !strings.EqualFold(v.text, "false")
}

// formatFloat renders a float the way Logs Insights does: integers without a
// decimal point, and fractional values rounded to decimalPlaces with trailing
// zeros trimmed.
func formatFloat(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}

	s := strconv.FormatFloat(f, 'f', decimalPlaces, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")

	return s
}

// compareValues returns -1, 0, or 1 comparing a and b. If both values are
// numeric the comparison is numeric; otherwise it is lexical on the text form.
func compareValues(a, b cwValue) int {
	if a.isNum && b.isNum {
		switch {
		case a.num < b.num:
			return -1
		case a.num > b.num:
			return 1
		default:
			return 0
		}
	}

	return strings.Compare(a.text, b.text)
}

// valuesEqual reports value equality using numeric comparison when possible.
func valuesEqual(a, b cwValue) bool {
	return compareValues(a, b) == 0
}

// regexState tracks parser state while inside a regex literal /.../.
type regexState struct {
	inCharClass bool
	escaped     bool
}

// advanceRegex processes one byte inside a regex literal, returning the updated
// state and whether the regex literal has ended (closing '/').
func advanceRegex(ch byte, st regexState) (regexState, bool) {
	if st.escaped {
		return regexState{inCharClass: st.inCharClass, escaped: false}, false
	}

	switch ch {
	case '\\':
		return regexState{inCharClass: st.inCharClass, escaped: true}, false
	case '[':
		return regexState{inCharClass: true, escaped: false}, false
	case ']':
		return regexState{inCharClass: false, escaped: false}, false
	case '/':
		if !st.inCharClass {
			return regexState{inCharClass: false, escaped: false}, true
		}
	}

	return regexState{inCharClass: st.inCharClass, escaped: false}, false
}

// splitPipes splits a query on '|' but not within regex literals /.../,
// correctly handling escaped characters and character classes.
func splitPipes(query string) []string {
	var parts []string
	var cur strings.Builder
	inRegex := false
	var rs regexState

	for i := range len(query) {
		ch := query[i]

		if inRegex {
			cur.WriteByte(ch)

			var ended bool
			rs, ended = advanceRegex(ch, rs)
			if ended {
				inRegex = false
			}

			continue
		}

		switch ch {
		case '/':
			inRegex = true
			rs = regexState{inCharClass: false, escaped: false}
			cur.WriteByte(ch)
		case '|':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}

	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}

	return parts
}

// splitTopLevelCommas splits s on commas that are not nested inside parentheses,
// brackets, or quotes/regex literals — used for fields and stats argument lists.
func splitTopLevelCommas(s string) []string {
	var parts []string
	var cur strings.Builder
	depth := 0
	inStr := byte(0)

	for i := range len(s) {
		ch := s[i]
		switch {
		case inStr != 0:
			cur.WriteByte(ch)
			if ch == inStr {
				inStr = 0
			}
		case ch == '"' || ch == '\'' || ch == '/':
			inStr = ch
			cur.WriteByte(ch)
		case ch == '(' || ch == '[':
			depth++
			cur.WriteByte(ch)
		case ch == ')' || ch == ']':
			depth--
			cur.WriteByte(ch)
		case ch == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}

	if strings.TrimSpace(cur.String()) != "" {
		parts = append(parts, strings.TrimSpace(cur.String()))
	}

	return parts
}

// parseLimitCommand parses "limit N".
func parseLimitCommand(q *insightsQuery, rest string) error {
	n, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil {
		return fmt.Errorf("invalid limit %q: %w", rest, err)
	}

	q.limit = n
	q.stages = append(q.stages, &limitStage{n: n})

	return nil
}

// limitStage truncates the record set to at most n records.
type limitStage struct {
	n int
}

func (s *limitStage) apply(recs []*insightsRecord) []*insightsRecord {
	if s.n > 0 && len(recs) > s.n {
		return recs[:s.n]
	}

	return recs
}

// parseDedupCommand parses "dedup field1, field2".
func parseDedupCommand(q *insightsQuery, rest string) error {
	var fields []string
	for _, f := range splitTopLevelCommas(rest) {
		if f != "" {
			fields = append(fields, f)
		}
	}

	if len(fields) == 0 {
		return nil
	}

	q.stages = append(q.stages, &dedupStage{fields: fields})

	return nil
}

// dedupStage keeps only the first record for each distinct combination of the
// named field values, mirroring the Logs Insights dedup command.
type dedupStage struct {
	fields []string
}

func (s *dedupStage) apply(recs []*insightsRecord) []*insightsRecord {
	seen := make(map[string]struct{}, len(recs))
	out := make([]*insightsRecord, 0, len(recs))

	for _, rec := range recs {
		parts := make([]string, len(s.fields))
		for i, f := range s.fields {
			parts[i] = rec.resolveString(f)
		}

		key := strings.Join(parts, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, rec)
	}

	return out
}
