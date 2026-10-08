package backup

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	summaryAny          = "ANY"
	summaryAggregateAll = "AGGREGATE_ALL"

	periodOneDay      = "ONE_DAY"
	periodSevenDays   = "SEVEN_DAYS"
	periodFourteenDay = "FOURTEEN_DAYS"

	summaryDay      = 24 * time.Hour
	summaryDayCount = 14

	keyScanResultStatus = "ScanResultStatus"
	summaryTimeCeiling  = 1e12

	scanResultNoThreats    = "NO_THREATS_FOUND"
	scanResultThreatsFound = "THREATS_FOUND"
	scanResultUnknown      = "UNKNOWN"
)

// JobSummaryFilter holds the List*JobSummaries query filters. Empty and "ANY"
// return one row per distinct value; "AGGREGATE_ALL" sums that dimension into
// a single row (api_op_List*JobSummaries.go).
type JobSummaryFilter struct {
	AccountID         string
	MessageCategory   string
	ResourceType      string
	State             string
	MalwareScanner    string
	ScanResultStatus  string
	AggregationPeriod string
}

// NewJobSummaryFilter reads the filter members from a List*JobSummaries query string.
func NewJobSummaryFilter(q url.Values) JobSummaryFilter {
	return JobSummaryFilter{
		AccountID:         q.Get("AccountId"),
		MessageCategory:   q.Get("MessageCategory"),
		ResourceType:      q.Get("ResourceType"),
		State:             q.Get("State"),
		MalwareScanner:    q.Get("MalwareScanner"),
		ScanResultStatus:  q.Get("ScanResultStatus"),
		AggregationPeriod: q.Get("AggregationPeriod"),
	}
}

// Validate rejects an AggregationPeriod outside the SDK enum.
func (f JobSummaryFilter) Validate() error {
	switch f.AggregationPeriod {
	case "", periodOneDay, periodSevenDays, periodFourteenDay:
		return nil
	}

	return fmt.Errorf("%w: AggregationPeriod must be one of ONE_DAY, SEVEN_DAYS, FOURTEEN_DAYS", ErrValidation)
}

func (b *InMemoryBackend) summaryAccount(jobAccount string) string {
	if jobAccount == "" {
		return b.accountID
	}

	return jobAccount
}

// summaryJob is the projection of a backup, copy, restore or scan job that
// the summary ops aggregate.
type summaryJob struct {
	at               time.Time
	account          string
	resourceType     string
	state            string
	messageCategory  string
	malwareScanner   string
	scanResultStatus string
}

type summaryWindow struct{ start, end time.Time }

func summaryWindows(period string, now time.Time) []summaryWindow {
	switch period {
	case periodSevenDays:
		return []summaryWindow{{start: now.Add(-7 * summaryDay), end: now}}
	case periodFourteenDay:
		return []summaryWindow{{start: now.Add(-summaryDayCount * summaryDay), end: now}}
	}

	today := now.UTC().Truncate(summaryDay)
	windows := make([]summaryWindow, 0, summaryDayCount)

	for i := range summaryDayCount {
		start := today.Add(-time.Duration(i) * summaryDay)
		windows = append(windows, summaryWindow{start: start, end: start.Add(summaryDay)})
	}

	return windows
}

func windowIndex(windows []summaryWindow, t time.Time, period string) int {
	for i, w := range windows {
		if period == periodSevenDays || period == periodFourteenDay {
			if !t.Before(w.start) && !t.After(w.end) {
				return i
			}

			continue
		}

		if !t.Before(w.start) && t.Before(w.end) {
			return i
		}
	}

	return -1
}

// summaryDim resolves one dimension of a summary row: want is the filter
// value, got the job's value.
func summaryDim(want, got string) (string, bool) {
	switch want {
	case "", summaryAny:
		return got, true
	case summaryAggregateAll:
		return summaryAggregateAll, true
	}

	return got, want == got
}

type summaryKey struct {
	account, resourceType, state, messageCategory, scanner, scanResult string
	window                                                             int
}

func (f JobSummaryFilter) keyFor(j summaryJob, window int) (summaryKey, bool) {
	var (
		k  = summaryKey{window: window}
		ok = true
	)

	resolve := func(dst *string, want, got string) {
		v, match := summaryDim(want, got)
		*dst = v
		ok = ok && match
	}

	resolve(&k.account, f.AccountID, j.account)
	resolve(&k.resourceType, f.ResourceType, j.resourceType)
	resolve(&k.state, f.State, j.state)
	resolve(&k.messageCategory, f.MessageCategory, j.messageCategory)
	resolve(&k.scanner, f.MalwareScanner, j.malwareScanner)
	resolve(&k.scanResult, f.ScanResultStatus, j.scanResultStatus)

	return k, ok
}

// summaryShape selects the optional members a summary type carries.
type summaryShape struct {
	messageCategory bool
	scan            bool
}

func (b *InMemoryBackend) buildSummaries(f JobSummaryFilter, jobs []summaryJob, shape summaryShape) []map[string]any {
	now := time.Now().UTC()
	windows := summaryWindows(f.AggregationPeriod, now)
	counts := map[summaryKey]int{}

	for _, j := range jobs {
		idx := windowIndex(windows, j.at, f.AggregationPeriod)
		if idx < 0 {
			continue
		}

		if k, ok := f.keyFor(j, idx); ok {
			counts[k]++
		}
	}

	rows := make([]map[string]any, 0, len(counts))

	for k, n := range counts {
		w := windows[k.window]
		end := w.end

		if end.After(now) {
			end = now
		}

		row := map[string]any{
			keyState:         k.state,
			keySummaryCount:  n,
			keySummaryRegion: b.region,
			keyAccountID:     k.account,
			"StartTime":      epochSeconds(w.start),
			"EndTime":        epochSeconds(end),
		}

		setOptionalStr(row, "ResourceType", k.resourceType)

		if shape.messageCategory {
			setOptionalStr(row, "MessageCategory", k.messageCategory)
		}

		if shape.scan {
			setOptionalStr(row, "MalwareScanner", k.scanner)
			setOptionalStr(row, keyScanResultStatus, k.scanResult)
		}

		rows = append(rows, row)
	}

	sortSummaries(rows)

	return rows
}

func summaryRowKey(m map[string]any) string {
	var sb strings.Builder

	keys := []string{keyState, "ResourceType", "MessageCategory", keyAccountID, "MalwareScanner", keyScanResultStatus}

	for _, k := range keys {
		s, _ := m[k].(string)
		sb.WriteString(s)
		sb.WriteByte(0)
	}

	start, _ := m["StartTime"].(float64)
	fmt.Fprintf(&sb, "%020.0f", summaryTimeCeiling-start)

	return sb.String()
}

func sortSummaries(s []map[string]any) {
	sort.Slice(s, func(i, j int) bool { return summaryRowKey(s[i]) < summaryRowKey(s[j]) })
}

func scanResultStatusFor(state string) string {
	switch state {
	case statusCompleted:
		return scanResultNoThreats
	case "COMPLETED_WITH_ISSUES":
		return scanResultThreatsFound
	default:
		return scanResultUnknown
	}
}

func copyMessageCategory(state string) string {
	if state == statusCompleted {
		return "SUCCESS"
	}

	return ""
}
