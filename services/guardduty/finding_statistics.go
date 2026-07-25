package guardduty

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// defaultGroupedStatisticsLimit matches GetFindingsStatisticsInput.MaxResults'
// documented default ("You can use this parameter only with the groupBy
// parameter... The default value is 25.").
const defaultGroupedStatisticsLimit = 25

// Wire-key constants shared by every groupByX helper below (each bucket
// always carries lastGeneratedAt/totalFindings, and accountId is shared by
// groupByAccount/groupByResource).
const (
	keyAccountIDField     = "accountId"
	keyLastGeneratedAt    = "lastGeneratedAt"
	keyTotalFindingsField = "totalFindings"
)

// groupStatFields builds the lastGeneratedAt/totalFindings fields every
// groupedByX entry shares, merged with the bucket-specific identity
// field(s) in extra.
func groupStatFields(g *findingGroup, extra map[string]any) map[string]any {
	out := map[string]any{
		keyLastGeneratedAt:    awstime.Epoch(g.lastGeneratedAt),
		keyTotalFindingsField: g.total,
	}

	maps.Copy(out, extra)

	return out
}

// findingStatisticsFor computes the findingStatistics document for
// GetFindingsStatistics: countBySeverity when groupBy is unset, or the
// single groupedByX list matching groupBy otherwise (see
// types.GetFindingsStatisticsInput.GroupBy / types.FindingStatistics).
func findingStatisticsFor(findings []*Finding, groupBy, orderBy string, maxResults int32) map[string]any {
	limit := int(maxResults)
	if limit <= 0 {
		limit = defaultGroupedStatisticsLimit
	}

	switch strings.ToUpper(groupBy) {
	case "ACCOUNT":
		return map[string]any{"groupedByAccount": limitGroups(groupByAccount(findings, orderBy), limit)}
	case "DATE":
		return map[string]any{"groupedByDate": limitGroups(groupByDate(findings, orderBy), limit)}
	case "FINDING_TYPE":
		return map[string]any{"groupedByFindingType": limitGroups(groupByFindingType(findings, orderBy), limit)}
	case "RESOURCE":
		return map[string]any{"groupedByResource": limitGroups(groupByResource(findings, orderBy), limit)}
	case "SEVERITY":
		return map[string]any{"groupedBySeverity": limitGroups(groupBySeverity(findings, orderBy), limit)}
	default:
		return map[string]any{"countBySeverity": countBySeverity(findings)}
	}
}

func limitGroups(groups []map[string]any, limit int) []map[string]any {
	if limit >= 0 && limit < len(groups) {
		return groups[:limit]
	}

	return groups
}

func countBySeverity(findings []*Finding) map[string]int {
	out := map[string]int{}

	for _, f := range findings {
		key := fmt.Sprintf("%.1f", f.Severity)
		out[key]++
	}

	return out
}

// findingGroup accumulates the total/last-seen bookkeeping every
// groupedByX bucket shares, keyed by whatever attribute that bucket groups
// on (account ID, date, finding type, ...).
type findingGroup struct {
	lastGeneratedAt time.Time
	key             string
	secondaryKey    string // resourceType for groupByResource's composite key
	total           int32
}

func bucketFindings(findings []*Finding, keyFn func(*Finding) (string, string)) []*findingGroup {
	byKey := map[string]*findingGroup{}
	order := make([]string, 0, len(findings))

	for _, f := range findings {
		key, secondary := keyFn(f)

		g, ok := byKey[key]
		if !ok {
			g = &findingGroup{key: key, secondaryKey: secondary}
			byKey[key] = g
			order = append(order, key)
		}

		g.total++

		if t, err := time.Parse(time.RFC3339, f.UpdatedAt); err == nil && t.After(g.lastGeneratedAt) {
			g.lastGeneratedAt = t
		}
	}

	groups := make([]*findingGroup, len(order))
	for i, k := range order {
		groups[i] = byKey[k]
	}

	return groups
}

// sortGroups orders groups by total findings, descending unless orderBy is
// "ASC" -- matching the documented default ("Based on the orderBy
// parameter, this request returns either the most occurring finding types
// or the least occurring finding types ... The default value of orderBy is
// DESC.").
func sortGroups(groups []*findingGroup, orderBy string) {
	asc := strings.EqualFold(orderBy, "ASC")

	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].total == groups[j].total {
			return groups[i].key < groups[j].key
		}

		if asc {
			return groups[i].total < groups[j].total
		}

		return groups[i].total > groups[j].total
	})
}

func groupByAccount(findings []*Finding, orderBy string) []map[string]any {
	groups := bucketFindings(findings, func(f *Finding) (string, string) { return f.AccountID, "" })
	sortGroups(groups, orderBy)

	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupStatFields(g, map[string]any{keyAccountIDField: g.key}))
	}

	return out
}

// groupByDate buckets by the calendar day (UTC) each finding's createdAt
// falls on, matching DateStatistics.Date's documented example format
// ("2024-09-05T17:00:00-07:00" -- the start of the observed day).
func groupByDate(findings []*Finding, orderBy string) []map[string]any {
	groups := bucketFindings(findings, func(f *Finding) (string, string) {
		t, err := time.Parse(time.RFC3339, f.CreatedAt)
		if err != nil {
			return "", ""
		}

		return t.UTC().Format("2006-01-02"), ""
	})
	sortGroups(groups, orderBy)

	out := make([]map[string]any, 0, len(groups))

	for _, g := range groups {
		day, err := time.Parse("2006-01-02", g.key)
		if err != nil {
			day = time.Time{}
		}

		out = append(out, groupStatFields(g, map[string]any{"date": awstime.Epoch(day)}))
	}

	return out
}

func groupByFindingType(findings []*Finding, orderBy string) []map[string]any {
	groups := bucketFindings(findings, func(f *Finding) (string, string) { return f.Type, "" })
	sortGroups(groups, orderBy)

	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupStatFields(g, map[string]any{"findingType": g.key}))
	}

	return out
}

// groupByResource buckets by (accountId, resourceType). This backend only
// tracks FindingResource.ResourceType (no per-type resource identifier such
// as instanceId/functionName), so resourceId is always emitted empty --
// documented as a known limitation rather than fabricated.
func groupByResource(findings []*Finding, orderBy string) []map[string]any {
	groups := bucketFindings(findings, func(f *Finding) (string, string) {
		return f.AccountID, f.Resource.ResourceType
	})
	sortGroups(groups, orderBy)

	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupStatFields(g, map[string]any{
			keyAccountIDField: g.key,
			"resourceId":      "",
			"resourceType":    g.secondaryKey,
		}))
	}

	return out
}

func groupBySeverity(findings []*Finding, orderBy string) []map[string]any {
	groups := bucketFindings(findings, func(f *Finding) (string, string) {
		return fmt.Sprintf("%.1f", f.Severity), ""
	})
	sortGroups(groups, orderBy)

	out := make([]map[string]any, 0, len(groups))

	for _, g := range groups {
		var sev float64

		_, _ = fmt.Sscanf(g.key, "%g", &sev)

		out = append(out, groupStatFields(g, map[string]any{"severity": sev}))
	}

	return out
}
