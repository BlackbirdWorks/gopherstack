package cloudtrail

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// This file implements a bounded, honest subset of CloudTrail Lake SQL
// execution for StartQuery/GetQueryResults/DescribeQuery. CloudTrail Lake
// queries run against an event data store's schema (eventTime, eventName,
// eventSource, ..., mirroring the CloudTrailEvent JSON detail). This backend
// does not model per-event-data-store ingestion (there is a single, shared,
// account/region-wide recorded-events log, b.events -- the same log
// LookupEvents already reads), so every query executes against that shared
// log regardless of which event data store its FROM clause names.
//
// Supported grammar (case-insensitive, single statement, no trailing
// semicolon required):
//
//	SELECT <* | col[, col...]> FROM <event-data-store>
//	  [WHERE <col> [!]= <'value'|value> [AND <col> [!]= <'value'|value>]...]
//	  [LIMIT <n>]
//
// Anything outside that subset (joins, aggregates, GROUP BY, subqueries,
// OR, LIKE, ...) is not an error -- real Lake SQL is a large surface, and
// StartQuery/GetQueryResults must not reject syntactically valid queries
// just because this emulator can't interpret them. Such statements still
// "run" (QueryStatus reaches FINISHED) but yield zero rows, which is the
// same documented-simplification pattern PARITY.md already used for
// "GetQueryResults always empty" before this pass -- now narrowed to only
// the genuinely-unsupported subset instead of every query.
const defaultQueryRowLimit = 1000

var (
	queryFromRe = regexp.MustCompile(`(?is)\bFROM\s+([^\s,;()]+)`)
	queryFullRe = regexp.MustCompile(
		`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+([^\s;]+)(?:\s+WHERE\s+(.+?))?(?:\s+LIMIT\s+(\d+))?\s*;?\s*$`,
	)
	queryAndRe  = regexp.MustCompile(`(?i)\s+AND\s+`)
	queryCondRe = regexp.MustCompile(`^\s*([A-Za-z0-9_.]+)\s*(!=|<>|=)\s*(?:'([^']*)'|"([^"]*)"|(\S+))\s*$`)
)

// queryTrimSet is the set of characters trimmed off a bare identifier or
// value token (quotes/backticks/trailing semicolon).
const queryTrimSet = "\"'`;"

// extractQueryFromTarget returns the identifier following FROM in a
// CloudTrail Lake SQL statement (case-insensitive), or "" if none is found.
// Used by StartQuery to resolve which event data store a query targets
// without relying on a gopherstack-invented "EventDataStore" wire field (the
// real StartQueryInput has none -- the target is embedded in the SQL itself).
func extractQueryFromTarget(stmt string) string {
	m := queryFromRe.FindStringSubmatch(stmt)
	if m == nil {
		return ""
	}

	return strings.Trim(m[1], queryTrimSet)
}

// queryCondition is a single WHERE comparison: column (=|!=) value.
type queryCondition struct {
	column string
	value  string
	negate bool
}

// parsedLakeQuery is a successfully parsed statement in the supported subset.
type parsedLakeQuery struct {
	columns []string // nil means "*" (all columns)
	where   []queryCondition
	limit   int // 0 means "use defaultQueryRowLimit"
}

// parseLakeQuery attempts to parse stmt against the supported grammar. The
// second return is false for anything outside that subset.
func parseLakeQuery(stmt string) (parsedLakeQuery, bool) {
	m := queryFullRe.FindStringSubmatch(stmt)
	if m == nil {
		return parsedLakeQuery{}, false
	}

	where, whereOK := parseWhereClause(m[3])
	if !whereOK {
		return parsedLakeQuery{}, false
	}

	pq := parsedLakeQuery{
		columns: parseSelectColumns(m[1]),
		where:   where,
		limit:   parseLimitClause(m[4]),
	}

	return pq, true
}

// parseSelectColumns splits a SELECT column list ("*" or "col1, col2, ...")
// into individual (trimmed, unquoted) column names. Returns nil for "*".
func parseSelectColumns(colsStr string) []string {
	cols := strings.TrimSpace(colsStr)
	if cols == "*" {
		return nil
	}

	var columns []string
	for c := range strings.SplitSeq(cols, ",") {
		columns = append(columns, strings.Trim(strings.TrimSpace(c), queryTrimSet))
	}

	return columns
}

// parseWhereClause splits a WHERE clause's ANDed conditions into
// queryConditions. The second return is false if any clause falls outside
// the supported single-quoted/bare-value equality subset (e.g. LIKE, OR,
// nested parens) -- the caller treats the whole statement as unsupported
// rather than guessing.
func parseWhereClause(whereStr string) ([]queryCondition, bool) {
	where := strings.TrimSpace(whereStr)
	if where == "" {
		return nil, true
	}

	var conds []queryCondition

	for _, clause := range queryAndRe.Split(where, -1) {
		cond, condOK := parseWhereCondition(clause)
		if !condOK {
			return nil, false
		}

		conds = append(conds, cond)
	}

	return conds, true
}

// parseWhereCondition parses a single "<col> [!]= <'value'|value>" clause.
func parseWhereCondition(clause string) (queryCondition, bool) {
	cm := queryCondRe.FindStringSubmatch(clause)
	if cm == nil {
		return queryCondition{}, false
	}

	value := cm[3]
	if value == "" && cm[4] != "" {
		value = cm[4]
	}
	if value == "" && cm[5] != "" {
		value = cm[5]
	}

	return queryCondition{
		column: strings.ToLower(cm[1]),
		negate: cm[2] == "!=" || cm[2] == "<>",
		value:  value,
	}, true
}

// parseLimitClause parses an optional "LIMIT n" capture group. Returns 0
// (meaning "use defaultQueryRowLimit") if absent or invalid.
func parseLimitClause(limitStr string) int {
	if limitStr == "" {
		return 0
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n <= 0 {
		return 0
	}

	return n
}

// eventToRow flattens an Event (its top-level fields plus the parsed
// CloudTrailEvent JSON detail, when present) into the lowercase-keyed column
// map a WHERE clause and projection match against -- mirroring the real
// CloudTrail Lake event schema (eventTime, eventName, eventSource,
// eventCategory, userIdentity.type, ...).
func eventToRow(ev Event) map[string]string {
	row := map[string]string{
		"eventid":     ev.EventID,
		"eventname":   ev.EventName,
		"eventsource": ev.EventSource,
		"eventtime":   ev.EventTime.UTC().Format("2006-01-02T15:04:05Z"),
		"username":    ev.Username,
		"readonly":    ev.ReadOnly,
		"accesskeyid": ev.AccessKeyID,
	}
	if ev.EventCategory != "" {
		row["eventcategory"] = ev.EventCategory
	}

	if ev.CloudTrailEvent != "" {
		var detail map[string]any
		if err := json.Unmarshal([]byte(ev.CloudTrailEvent), &detail); err == nil {
			flattenJSONInto(row, "", detail)
		}
	}

	return row
}

// flattenJSONInto flattens a decoded JSON object into row using dot-notation
// keys (e.g. "userIdentity.type"), lowercased to match eventToRow's other
// keys, without overwriting keys already set from the top-level Event fields.
func flattenJSONInto(row map[string]string, prefix string, obj map[string]any) {
	for k, v := range obj {
		key := strings.ToLower(k)
		if prefix != "" {
			key = prefix + "." + key
		}

		switch tv := v.(type) {
		case map[string]any:
			flattenJSONInto(row, key, tv)
		case string:
			if _, exists := row[key]; !exists {
				row[key] = tv
			}
		case nil:
			// omit -- absent columns read as "" via rowValue, matching a
			// missing/null field.
		default:
			if _, exists := row[key]; !exists {
				if b, err := json.Marshal(tv); err == nil {
					row[key] = string(b)
				}
			}
		}
	}
}

// rowMatchesWhere reports whether row satisfies every (ANDed) condition.
func rowMatchesWhere(row map[string]string, conds []queryCondition) bool {
	for _, c := range conds {
		match := row[c.column] == c.value
		if match == c.negate {
			return false
		}
	}

	return true
}

// projectRow renders row as the AWS QueryResultRows shape: a slice of
// single-key {columnName: value} maps, one per selected column. cols nil
// means "*" -- every column present on the row, in a deterministic order.
func projectRow(row map[string]string, cols []string) []map[string]string {
	names := cols
	if names == nil {
		names = make([]string, 0, len(row))
		for k := range row {
			names = append(names, k)
		}

		sortStrings(names)
	}

	out := make([]map[string]string, 0, len(names))
	for _, name := range names {
		out = append(out, map[string]string{name: row[strings.ToLower(name)]})
	}

	return out
}

// sortStrings is a tiny insertion sort to avoid importing "sort" solely for
// a handful of column names (called once per result row's "*" projection).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// queryExecStats summarizes an executed query's scan/match counts, mirroring
// QueryStatisticsForDescribeQuery / QueryStatistics.
type queryExecStats struct {
	eventsScanned int64
	eventsMatched int64
	bytesScanned  int64
}

// executeLakeQuery runs stmt against events, returning the full (unpaginated,
// un-LIMIT-truncated beyond defaultQueryRowLimit as a safety cap) result set
// and scan statistics. Called with the backend lock held (see
// materializeQueryLocked).
func executeLakeQuery(stmt string, events []Event) ([][]map[string]string, queryExecStats) {
	stats := queryExecStats{eventsScanned: int64(len(events))}
	for _, ev := range events {
		stats.bytesScanned += int64(len(ev.CloudTrailEvent))
	}

	pq, ok := parseLakeQuery(stmt)
	if !ok {
		// Outside the supported grammar: the query still "ran" (FINISHED),
		// it simply matched nothing this emulator can interpret.
		return [][]map[string]string{}, stats
	}

	limit := pq.limit
	if limit <= 0 || limit > defaultQueryRowLimit {
		limit = defaultQueryRowLimit
	}

	rows := make([][]map[string]string, 0, len(events))
	for _, ev := range events {
		row := eventToRow(ev)
		if !rowMatchesWhere(row, pq.where) {
			continue
		}

		stats.eventsMatched++
		if len(rows) < limit {
			rows = append(rows, projectRow(row, pq.columns))
		}
	}

	return rows, stats
}
