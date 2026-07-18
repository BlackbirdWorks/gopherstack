package cloudcontrol

import (
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// validOperations is the set of valid CloudControl operation strings.
//
//nolint:gochecknoglobals // lookup set
var validOperations = map[string]struct{}{
	"CREATE": {},
	"DELETE": {},
	"UPDATE": {},
}

// validOperationStatuses is the set of valid CloudControl operation status strings.
//
//nolint:gochecknoglobals // lookup set
var validOperationStatuses = map[string]struct{}{
	"PENDING":              {},
	"IN_PROGRESS":          {},
	opStatusSuccess:        {},
	"FAILED":               {},
	"CANCEL_IN_PROGRESS":   {},
	opStatusCancelComplete: {},
}

// GetResourceRequestStatus returns a copy of the ProgressEvent for the given request token.
// Events are retained in the map until Reset() is called.
// An unrecognized requestToken returns ErrRequestTokenNotFound (RequestTokenNotFoundException),
// the only error this operation declares -- not ErrNotFound (ResourceNotFoundException),
// which describes a missing *resource*, not a missing *request token*.
func (b *InMemoryBackend) GetResourceRequestStatus(requestToken string) (*ProgressEvent, error) {
	b.mu.RLock("GetResourceRequestStatus")
	defer b.mu.RUnlock()

	event, ok := b.requests.Get(requestToken)
	if !ok {
		return nil, ErrRequestTokenNotFound
	}

	return copyEvent(event), nil
}

// CancelResourceRequest cancels the request identified by requestToken.
// An unrecognized requestToken returns ErrRequestTokenNotFound (RequestTokenNotFoundException).
// Cancelling an already-terminal request (SUCCESS, FAILED, CANCEL_COMPLETE, CANCEL_IN_PROGRESS)
// returns ErrConcurrentModification (ConcurrentModificationException), matching the real AWS
// API reference for this operation -- not a validation error.
func (b *InMemoryBackend) CancelResourceRequest(requestToken string) (*ProgressEvent, error) {
	b.mu.Lock("CancelResourceRequest")
	defer b.mu.Unlock()

	event, ok := b.requests.Get(requestToken)
	if !ok {
		return nil, ErrRequestTokenNotFound
	}

	if event.OperationStatus != "IN_PROGRESS" {
		return nil, ErrConcurrentModification
	}

	cancelled := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        event.TypeName,
		Identifier:      event.Identifier,
		RequestToken:    requestToken,
		Operation:       event.Operation,
		OperationStatus: opStatusCancelComplete,
		ResourceModel:   event.ResourceModel,
	}
	b.requests.Put(cancelled)

	return copyEvent(cancelled), nil
}

// ResourceRequestFilter holds optional filter criteria for ListResourceRequests.
type ResourceRequestFilter struct {
	TypeName          string
	Operations        []string
	OperationStatuses []string
}

// validateFilter returns ErrValidation if the filter contains unknown operation or status strings.
func validateFilter(filter *ResourceRequestFilter) error {
	if filter == nil {
		return nil
	}

	for _, op := range filter.Operations {
		if _, ok := validOperations[op]; !ok {
			return ErrValidation
		}
	}

	for _, st := range filter.OperationStatuses {
		if _, ok := validOperationStatuses[st]; !ok {
			return ErrValidation
		}
	}

	return nil
}

// eventMatchesFilter reports whether event passes the given filter.
// A nil filter matches every event.
func eventMatchesFilter(event *ProgressEvent, filter *ResourceRequestFilter) bool {
	if filter == nil {
		return true
	}

	if len(filter.Operations) > 0 && !slices.Contains(filter.Operations, event.Operation) {
		return false
	}

	if len(filter.OperationStatuses) > 0 && !slices.Contains(filter.OperationStatuses, event.OperationStatus) {
		return false
	}

	if filter.TypeName != "" && event.TypeName != filter.TypeName {
		return false
	}

	return true
}

// ListResourceRequests returns all tracked resource requests, optionally filtered
// by operation type, operation status, and/or resource type name. Results are sorted
// by EventTime descending (most recent first) for deterministic output.
// Returns ErrValidation if the filter contains unknown operation or status strings.
func (b *InMemoryBackend) ListResourceRequests(
	filter *ResourceRequestFilter, maxResults int, nextToken string,
) ([]*ProgressEvent, string, error) {
	if err := validateFilter(filter); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListResourceRequests")
	defer b.mu.RUnlock()

	var out []*ProgressEvent

	b.requests.Range(func(event *ProgressEvent) bool {
		if eventMatchesFilter(event, filter) {
			out = append(out, event)
		}

		return true
	})

	// Sort by EventTime descending so the most-recent request appears first.
	sort.Slice(out, func(i, j int) bool {
		return out[i].EventTime.After(out[j].EventTime.Time)
	})

	pg := page.New(out, nextToken, maxResults, defaultListMaxResults)

	// Deep-copy the page items so callers cannot mutate backend state.
	result := make([]*ProgressEvent, len(pg.Data))
	for i, e := range pg.Data {
		result[i] = copyEvent(e)
	}

	return result, pg.Next, nil
}

// copyEvent returns a shallow copy of a ProgressEvent so callers cannot mutate backend state.
func copyEvent(e *ProgressEvent) *ProgressEvent {
	if e == nil {
		return nil
	}

	cp := *e

	return &cp
}

// AddProgressEvent inserts a ProgressEvent directly into the requests map.
// This is intended for use in tests to set up specific request states that
// cannot be reached through the normal API (e.g. IN_PROGRESS).
func (b *InMemoryBackend) AddProgressEvent(event *ProgressEvent) {
	b.mu.Lock("AddProgressEvent")
	defer b.mu.Unlock()

	b.requests.Put(event)
}
