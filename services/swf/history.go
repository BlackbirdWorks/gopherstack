package swf

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// GetWorkflowExecutionHistory returns history events for one specific run of
// a workflow execution, supporting pagination and reverse ordering. runID is
// optional; if empty, targets the currently open run. Real AWS marks the
// wire equivalent (Execution.RunId) as required, but this backend stays
// lenient -- see resolveExecutionLocked.
func (b *InMemoryBackend) GetWorkflowExecutionHistory(
	domain, workflowID, runID string,
	maxPageSize int,
	nextPageToken string,
	reverseOrder bool,
) ([]HistoryEvent, string) {
	b.mu.Lock("GetWorkflowExecutionHistory")
	defer b.mu.Unlock()

	b.sweepTimedOutExecutionsLocked(time.Now())

	exec, ok := b.resolveExecutionLocked(domain, workflowID, runID)
	if !ok {
		return []HistoryEvent{}, ""
	}

	events := b.history[executionKey(domain, workflowID, exec.RunID)]
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

// RequireExecution returns ErrNotFound unless the domain is registered and the execution (or run) exists.
func (b *InMemoryBackend) RequireExecution(domain, workflowID, runID string) error {
	b.mu.RLock("RequireExecution")
	defer b.mu.RUnlock()

	if _, ok := b.domains.Get(domain); !ok {
		return fmt.Errorf("%w: domain %s not found", ErrNotFound, domain)
	}

	if _, ok := b.resolveExecutionLocked(domain, workflowID, runID); !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}

	return nil
}

// RequireDomain returns ErrNotFound unless the domain is registered.
func (b *InMemoryBackend) RequireDomain(domain string) error {
	b.mu.RLock("RequireDomain")
	defer b.mu.RUnlock()

	if _, ok := b.domains.Get(domain); !ok {
		return fmt.Errorf("%w: domain %s not found", ErrNotFound, domain)
	}

	return nil
}
