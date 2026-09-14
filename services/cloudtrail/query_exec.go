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
// semicolon required; see query_lex.go/query_parse.go/query_where.go):
//
//	SELECT <* | item[, item...]> FROM <event-data-store> [AS alias]
//	  [WHERE <bool-expr>]
//	  [GROUP BY col[, col...]]
//	  [LIMIT <n>]
//
//	item      := col [AS alias] | COUNT(* | col) [AS alias]
//	bool-expr := bool-expr OR bool-expr
//	           | bool-expr AND bool-expr
//	           | NOT bool-expr
//	           | ( bool-expr )
//	           | col (=|!=|<>) value
//	           | col [NOT] LIKE 'pattern'
//	           | col [NOT] IN (value[, value...])
//
// Anything outside that subset -- joins/set operations across event data
// stores (real CloudTrail Lake feature, genuinely large: see
// query_parse.go's parseFromTarget), SUM/AVG/MIN/MAX, subqueries, HAVING,
// ORDER BY, DISTINCT, and any other syntactically-valid-but-unhandled SQL --
// is a genuine query failure: the query reaches QueryStatus FAILED with a
// populated ErrorMessage (DescribeQueryOutput.ErrorMessage /
// GetQueryResultsOutput.ErrorMessage; QueryStatus has a documented FAILED
// value -- cloudtrail@v1.58.4 api_op_DescribeQuery.go:69,
// types/enums.go:384), never a silent empty FINISHED result.
const defaultQueryRowLimit = 1000

var queryFromRe = regexp.MustCompile(`(?is)\bFROM\s+([^\s,;()]+)`)

// queryTrimSet is the set of characters trimmed off a bare identifier or
// value token (quotes/backticks/trailing semicolon).
const queryTrimSet = "\"'`;"

// extractQueryFromTarget returns the identifier following FROM in a
// CloudTrail Lake SQL statement (case-insensitive), or "" if none is found.
// Used by StartQuery to resolve which event data store a query targets
// without relying on a gopherstack-invented "EventDataStore" wire field (the
// real StartQueryInput has none -- the target is embedded in the SQL
// itself). Deliberately independent of the stricter grammar parser below:
// StartQuery must still resolve a target event data store for a query this
// emulator can't otherwise execute (e.g. a JOIN), since that query is only
// discovered to be unsupported later, lazily, at first read.
func extractQueryFromTarget(stmt string) string {
	m := queryFromRe.FindStringSubmatch(stmt)
	if m == nil {
		return ""
	}

	return strings.Trim(m[1], queryTrimSet)
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

// sortStrings is a tiny insertion sort to avoid importing "sort" solely for
// a handful of column/group-key names (called once per query execution's
// output ordering, not per row).
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

// executeLakeQuery runs stmt against events, returning the result rows,
// scan statistics, the resulting QueryStatus ("FINISHED" or "FAILED"), and
// an ErrorMessage (non-empty only when FAILED). Called with the backend
// lock held (see materializeQueryLocked).
func executeLakeQuery(stmt string, events []Event) ([][]map[string]string, queryExecStats, string, string) {
	stats := queryExecStats{eventsScanned: int64(len(events))}
	for _, ev := range events {
		stats.bytesScanned += int64(len(ev.CloudTrailEvent))
	}

	pq, parseErr := parseLakeQuery(stmt)
	if parseErr != "" {
		return nil, stats, "FAILED", parseErr
	}

	matched := make([]map[string]string, 0, len(events))

	for _, ev := range events {
		row := eventToRow(ev)
		if pq.where == nil || pq.where.eval(row) {
			matched = append(matched, row)
		}
	}

	stats.eventsMatched = int64(len(matched))

	limit := effectiveQueryLimit(pq.limit)

	rows := projectRows(matched, pq, limit)

	return rows, stats, "FINISHED", ""
}

func effectiveQueryLimit(limit int) int {
	if limit <= 0 || limit > defaultQueryRowLimit {
		return defaultQueryRowLimit
	}

	return limit
}

func projectRows(matched []map[string]string, pq parsedLakeQuery, limit int) [][]map[string]string {
	if pq.hasAgg {
		return aggregateRows(matched, pq, limit)
	}

	rows := make([][]map[string]string, 0, min(len(matched), limit))

	for _, row := range matched {
		if len(rows) >= limit {
			break
		}

		rows = append(rows, projectRow(row, pq.items))
	}

	return rows
}

// projectRow renders row as the AWS QueryResultRows shape: a slice of
// single-key {columnName: value} maps, one per selected item. items nil
// means "*" -- every column present on the row, in a deterministic order.
func projectRow(row map[string]string, items []selectItem) []map[string]string {
	if items == nil {
		names := make([]string, 0, len(row))
		for k := range row {
			names = append(names, k)
		}

		sortStrings(names)

		out := make([]map[string]string, 0, len(names))
		for _, name := range names {
			out = append(out, map[string]string{name: row[name]})
		}

		return out
	}

	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]string{item.outName: row[item.column]})
	}

	return out
}

// aggState accumulates one GROUP BY bucket: the group-by columns' values
// (identical across every row in the bucket, by definition of grouping,
// captured via the first row seen) and a running COUNT.
type aggState struct {
	values map[string]string
	count  int64
}

// aggregateRows evaluates COUNT(*)/COUNT(col), with or without GROUP BY (no
// GROUP BY means a single implicit group over every matched row). Output
// order is sorted by group key so it's deterministic across Go's randomized
// map iteration.
func aggregateRows(matched []map[string]string, pq parsedLakeQuery, limit int) [][]map[string]string {
	groups := map[string]*aggState{}

	for _, row := range matched {
		key := groupKey(row, pq.groupBy)

		st, ok := groups[key]
		if !ok {
			st = &aggState{values: row}
			groups[key] = st
		}

		st.count++
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}

	sortStrings(keys)

	rows := make([][]map[string]string, 0, min(len(keys), limit))

	for _, k := range keys {
		if len(rows) >= limit {
			break
		}

		rows = append(rows, renderAggRow(pq.items, groups[k]))
	}

	return rows
}

// groupKeyFieldSep separates GROUP BY column values in a composite group
// key. \x1f (ASCII unit separator) can't appear in a recorded event's
// string-valued fields, so it can't collide with real column content.
const groupKeyFieldSep = "\x1f"

func groupKey(row map[string]string, groupBy []string) string {
	if len(groupBy) == 0 {
		return ""
	}

	parts := make([]string, len(groupBy))
	for i, col := range groupBy {
		parts[i] = row[col]
	}

	return strings.Join(parts, groupKeyFieldSep)
}

func renderAggRow(items []selectItem, st *aggState) []map[string]string {
	out := make([]map[string]string, 0, len(items))

	for _, item := range items {
		if item.kind == itemCount || item.kind == itemCountStar {
			out = append(out, map[string]string{item.outName: strconv.FormatInt(st.count, 10)})

			continue
		}

		out = append(out, map[string]string{item.outName: st.values[item.column]})
	}

	return out
}
