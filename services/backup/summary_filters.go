package backup

import "net/url"

// JobSummaryFilter holds the List*JobSummaries query filters; empty, "ANY" and
// "AGGREGATE_ALL" apply no filter (api_op_List*JobSummaries.go).
type JobSummaryFilter struct {
	AccountID       string
	MessageCategory string
	ResourceType    string
	State           string
	MalwareScanner  string
}

// NewJobSummaryFilter reads the filter members from a List*JobSummaries query string.
func NewJobSummaryFilter(q url.Values) JobSummaryFilter {
	return JobSummaryFilter{
		AccountID:       q.Get("AccountId"),
		MessageCategory: q.Get("MessageCategory"),
		ResourceType:    q.Get("ResourceType"),
		State:           q.Get("State"),
		MalwareScanner:  q.Get("MalwareScanner"),
	}
}

func summaryFieldMatches(want, got string) bool {
	return want == "" || want == "ANY" || want == "AGGREGATE_ALL" || want == got
}

func (f JobSummaryFilter) matches(account, resourceType, state, messageCategory string) bool {
	return summaryFieldMatches(f.AccountID, account) &&
		summaryFieldMatches(f.ResourceType, resourceType) &&
		summaryFieldMatches(f.State, state) &&
		summaryFieldMatches(f.MessageCategory, messageCategory)
}

func (b *InMemoryBackend) summaryAccount(jobAccount string) string {
	if jobAccount == "" {
		return b.accountID
	}

	return jobAccount
}
