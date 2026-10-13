package securityhub

import (
	"fmt"
	"sort"
	"time"
)

const (
	keyGroupByField = "GroupByField"
	keyFieldValue   = "FieldValue"
	keyGroupByRules = "GroupByRules"

	granularityDaily   = "Daily"
	granularityWeekly  = "Weekly"
	granularityMonthly = "Monthly"

	trendGranularityDailySpan = 2 * 24 * time.Hour
	trendGranularityWeekSpan  = 31 * 24 * time.Hour
)

// GroupByRule is one entry of the real GroupByRules/ResourceGroupByRules wire shape
// ({GroupByField, Filters}): the field to aggregate by and the optional filter that
// narrows the items counted for it.
type GroupByRule struct {
	Filters map[string]any
	Field   string
}

// groupByRulesFromBody parses GroupByRules/ResourceGroupByRules, skipping entries
// without a GroupByField.
func groupByRulesFromBody(raw any) []GroupByRule {
	rules, ok := raw.([]any)
	if !ok {
		return nil
	}

	out := make([]GroupByRule, 0, len(rules))

	for _, r := range rules {
		rule, ruleOK := r.(map[string]any)
		if !ruleOK {
			continue
		}

		field, fieldOK := rule[keyGroupByField].(string)
		if !fieldOK || field == "" {
			continue
		}

		out = append(out, GroupByRule{Field: field, Filters: filterRuleFilters(rule)})
	}

	return out
}

// groupByResults aggregates items by each requested field into the
// GroupByResult shape shared by GetFindingStatisticsV2 and
// GetResourcesStatisticsV2 (types.GroupByResult: GroupByField plus a
// GroupByValues list of {FieldValue, Count} -- securityhub@v1.75.4
// types/types.go:15698-15707,15724-15735). GroupByResult.GroupByField echoes
// back the field exactly as the client requested it (the documented OCSF
// wire vocabulary); fieldMap optionally translates that requested name to
// this backend's internal storage key for the actual lookup, so findings
// storing "SeverityLabel" can still be grouped by the client's "severity".
// Pass nil to look items up by the requested name verbatim.
func groupByResults(
	items []map[string]any, rules []GroupByRule, fieldMap map[string]string, sortOrder string,
	matches func(item, filters map[string]any) bool,
) []map[string]any {
	results := make([]map[string]any, 0, len(rules))

	for _, rule := range rules {
		field := rule.Field
		lookupField := field
		if mapped, found := fieldMap[field]; found {
			lookupField = mapped
		}

		counts := make(map[string]int)

		var order []string

		for _, item := range items {
			if len(rule.Filters) > 0 && !matches(item, rule.Filters) {
				continue
			}

			val := ""
			if v, ok := item[lookupField]; ok {
				val = fmt.Sprintf("%v", v)
			}

			if _, seen := counts[val]; !seen {
				order = append(order, val)
			}

			counts[val]++
		}

		// GetFindingStatisticsV2/GetResourcesStatisticsV2's own doc comment:
		// "Orders the aggregation count in descending or ascending order.
		// Descending order is the default" (api_op_GetFindingStatisticsV2.go).
		ascending := sortOrder == "asc"
		sort.SliceStable(order, func(i, j int) bool {
			if ascending {
				return counts[order[i]] < counts[order[j]]
			}

			return counts[order[i]] > counts[order[j]]
		})

		values := make([]map[string]any, 0, len(order))
		for _, val := range order {
			values = append(values, map[string]any{
				keyFieldValue: val,
				keyCount:      counts[val],
			})
		}

		results = append(results, map[string]any{
			keyGroupByField: field,
			"GroupByValues": values,
		})
	}

	return results
}

// trendGranularity buckets a trend request's time span into one of the real
// GranularityField values (Daily/Weekly/Monthly -- securityhub@v1.75.4
// types/enums.go:652-659). Neither GetFindingsTrendsV2Input nor
// GetResourcesTrendsV2Input actually carries a Granularity input member --
// the real API derives it server-side from the requested range -- so this is
// this backend's own documented bucketing heuristic, not a value read off
// the wire.
func trendGranularity(startTime, endTime string) string {
	start, sErr := time.Parse(time.RFC3339, startTime)
	end, eErr := time.Parse(time.RFC3339, endTime)

	if sErr != nil || eErr != nil {
		return granularityDaily
	}

	switch span := end.Sub(start); {
	case span <= trendGranularityDailySpan:
		return granularityDaily
	case span <= trendGranularityWeekSpan:
		return granularityWeekly
	default:
		return granularityMonthly
	}
}
