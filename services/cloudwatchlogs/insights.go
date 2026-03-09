package cloudwatchlogs

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// insightsQuery holds the parsed representation of a Logs Insights query string.
type insightsQuery struct {
	statsBy   string
	sortField string
	fields    []string
	filters   []*regexp.Regexp
	limit     int
	sortDesc  bool
	hasStats  bool
}

// defaultInsightsFields are returned when no explicit fields command is given.
func defaultInsightsFields() []string {
	return []string{"@timestamp", "@message", "@ingestionTime"}
}

// parseInsightsQuery parses a CloudWatch Logs Insights query string into an insightsQuery.
// The query is a sequence of pipe-separated commands.
func parseInsightsQuery(query string) (*insightsQuery, error) {
	q := &insightsQuery{
		sortField: "@timestamp",
		sortDesc:  false,
		limit:     0,
	}

	commands := splitPipes(query)
	for _, cmd := range commands {
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

// splitPipes splits query string on '|' but not within regex literals /.../.
func splitPipes(query string) []string {
	var parts []string
	var cur strings.Builder
	inRegex := false
	for i := range len(query) {
		ch := query[i]
		switch {
		case ch == '/' && !inRegex:
			inRegex = true
			cur.WriteByte(ch)
		case ch == '/' && inRegex:
			inRegex = false
			cur.WriteByte(ch)
		case ch == '|' && !inRegex:
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

// parseCommand parses a single command token and updates the query.
func parseCommand(q *insightsQuery, cmd string) error {
	lower := strings.ToLower(cmd)

	switch {
	case strings.HasPrefix(lower, "fields "):
		return parseFields(q, cmd[len("fields "):])
	case strings.HasPrefix(lower, "filter "):
		return parseFilter(q, cmd[len("filter "):])
	case strings.HasPrefix(lower, "sort "):
		return parseSort(q, cmd[len("sort "):])
	case strings.HasPrefix(lower, "limit "):
		return parseLimit(q, cmd[len("limit "):])
	case strings.HasPrefix(lower, "stats "):
		return parseStats(q, cmd[len("stats "):])
	}

	// Unknown commands are silently ignored (forward compatibility).
	return nil
}

func parseFields(q *insightsQuery, rest string) error {
	parts := strings.SplitSeq(rest, ",")
	for p := range parts {
		f := strings.TrimSpace(p)
		if f != "" {
			q.fields = append(q.fields, f)
		}
	}

	return nil
}

func parseFilter(q *insightsQuery, rest string) error {
	rest = strings.TrimSpace(rest)
	// Support: filter @message like /pattern/ or filter @field like /pattern/
	// Also support simple: filter @message like "string"
	lower := strings.ToLower(rest)
	likeIdx := strings.Index(lower, " like ")
	if likeIdx < 0 {
		// Unknown filter form — skip.
		return nil
	}

	pattern := strings.TrimSpace(rest[likeIdx+len(" like "):])
	re, err := extractRegexPattern(pattern)
	if err != nil {
		return fmt.Errorf("invalid filter pattern %q: %w", pattern, err)
	}
	q.filters = append(q.filters, re)

	return nil
}

// extractRegexPattern extracts the pattern from /pattern/ or "string" syntax.
func extractRegexPattern(s string) (*regexp.Regexp, error) {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "/") && strings.HasSuffix(s, "/") {
		inner := s[1 : len(s)-1]

		return regexp.Compile(inner)
	}

	if (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		inner := s[1 : len(s)-1]

		return regexp.Compile(regexp.QuoteMeta(inner))
	}

	// Treat as literal string.
	return regexp.Compile(regexp.QuoteMeta(s))
}

const sortDirectionParts = 2

func parseSort(q *insightsQuery, rest string) error {
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return nil
	}
	q.sortField = parts[0]
	if len(parts) >= sortDirectionParts {
		q.sortDesc = strings.EqualFold(parts[1], "desc")
	}

	return nil
}

func parseLimit(q *insightsQuery, rest string) error {
	rest = strings.TrimSpace(rest)
	n, err := strconv.Atoi(rest)
	if err != nil {
		return fmt.Errorf("invalid limit %q: %w", rest, err)
	}
	q.limit = n

	return nil
}

func parseStats(q *insightsQuery, rest string) error {
	// Support: stats count(*) by field
	q.hasStats = true
	lower := strings.ToLower(rest)
	byIdx := strings.Index(lower, " by ")
	if byIdx >= 0 {
		q.statsBy = strings.TrimSpace(rest[byIdx+4:])
	}

	return nil
}

// executeQuery executes a parsed insights query against the provided events and returns result rows.
func executeQuery(q *insightsQuery, events []*OutputLogEvent) [][]ResultField {
	// Apply filters.
	filtered := applyFilters(q.filters, events)

	// Handle stats aggregation.
	if q.hasStats {
		return executeStats(q, filtered)
	}

	// Sort.
	sorted := sortEvents(filtered, q.sortField, q.sortDesc)

	// Apply limit.
	if q.limit > 0 && len(sorted) > q.limit {
		sorted = sorted[:q.limit]
	}

	// Project fields.
	fields := q.fields
	if len(fields) == 0 {
		fields = defaultInsightsFields()
	}

	rows := make([][]ResultField, len(sorted))
	for i, ev := range sorted {
		rows[i] = projectFields(ev, fields)
	}

	return rows
}

func applyFilters(filters []*regexp.Regexp, events []*OutputLogEvent) []*OutputLogEvent {
	if len(filters) == 0 {
		return events
	}

	out := make([]*OutputLogEvent, 0, len(events))
	for _, ev := range events {
		match := true
		for _, re := range filters {
			if !re.MatchString(ev.Message) {
				match = false

				break
			}
		}
		if match {
			out = append(out, ev)
		}
	}

	return out
}

func sortEvents(events []*OutputLogEvent, field string, desc bool) []*OutputLogEvent {
	cp := make([]*OutputLogEvent, len(events))
	copy(cp, events)

	sort.SliceStable(cp, func(i, j int) bool {
		vi := fieldValue(cp[i], field)
		vj := fieldValue(cp[j], field)
		if desc {
			return vi > vj
		}

		return vi < vj
	})

	return cp
}

// fieldValue returns the sort key for a field. For numeric fields, zero-padded string comparison works
// because timestamps are int64 represented as decimal strings of equal length.
func fieldValue(ev *OutputLogEvent, field string) string {
	switch field {
	case "@timestamp":
		return fmt.Sprintf("%020d", ev.Timestamp)
	case "@ingestionTime":
		return fmt.Sprintf("%020d", ev.IngestionTime)
	case "@message":
		return ev.Message
	}

	return ""
}

// projectFields maps an event to a slice of ResultField for the requested field names.
func projectFields(ev *OutputLogEvent, fields []string) []ResultField {
	row := make([]ResultField, 0, len(fields))
	for _, f := range fields {
		row = append(row, ResultField{
			Field: f,
			Value: eventFieldAsString(ev, f),
		})
	}

	return row
}

func eventFieldAsString(ev *OutputLogEvent, field string) string {
	switch field {
	case "@timestamp":
		return strconv.FormatInt(ev.Timestamp, 10)
	case "@ingestionTime":
		return strconv.FormatInt(ev.IngestionTime, 10)
	case "@message":
		return ev.Message
	}

	return ""
}

// executeStats performs a basic aggregation (count(*) by field).
func executeStats(q *insightsQuery, events []*OutputLogEvent) [][]ResultField {
	if q.statsBy == "" {
		// count(*) with no group-by: one row.
		return [][]ResultField{
			{
				{Field: "count(*)", Value: strconv.Itoa(len(events))},
			},
		}
	}

	counts := make(map[string]int)
	order := make([]string, 0)
	for _, ev := range events {
		key := eventFieldAsString(ev, q.statsBy)
		if _, seen := counts[key]; !seen {
			order = append(order, key)
		}
		counts[key]++
	}

	rows := make([][]ResultField, 0, len(counts))
	for _, key := range order {
		rows = append(rows, []ResultField{
			{Field: q.statsBy, Value: key},
			{Field: "count(*)", Value: strconv.Itoa(counts[key])},
		})
	}

	if q.limit > 0 && len(rows) > q.limit {
		rows = rows[:q.limit]
	}

	return rows
}
