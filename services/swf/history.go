package swf

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// GetWorkflowExecutionHistory returns history events for a workflow execution,
// supporting pagination and reverse ordering.
func (b *InMemoryBackend) GetWorkflowExecutionHistory(
	domain, workflowID string,
	maxPageSize int,
	nextPageToken string,
	reverseOrder bool,
) ([]HistoryEvent, string) {
	b.mu.RLock("GetWorkflowExecutionHistory")
	defer b.mu.RUnlock()

	events := b.history[domain+":"+workflowID]
	if len(events) == 0 {
		return []HistoryEvent{}, ""
	}

	cp := make([]HistoryEvent, len(events))
	copy(cp, events)

	if reverseOrder {
		for i, j := 0, len(cp)-1; i < j; i, j = i+1, j-1 {
			cp[i], cp[j] = cp[j], cp[i]
		}
	}

	const maxDefault = 1000
	p := page.New(cp, nextPageToken, maxPageSize, maxDefault)

	return p.Data, p.Next
}
