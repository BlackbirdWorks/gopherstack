package pinpoint

import "net/http"

const (
	// maxAppEvents caps the number of ingested events retained per application so
	// PutEvents cannot grow the appEvents slice without bound on a long-lived app.
	maxAppEvents = 10000
)

// PutEvents records events for an application and returns per-endpoint per-event results.
func (b *InMemoryBackend) PutEvents(appID string, req putEventsRequest) (*eventsResponse, error) {
	b.mu.Lock("PutEvents")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	results := make(map[string]endpointItemResponse, len(req.BatchItem))

	for epID, epEvents := range req.BatchItem {
		evResults := make(map[string]itemEventResponse, len(epEvents.Events))

		for evID, ev := range epEvents.Events {
			b.appEvents[appID] = append(b.appEvents[appID], storedPinpointEvent(ev))
			evResults[evID] = itemEventResponse{
				Message:    "Accepted",
				StatusCode: http.StatusAccepted,
			}
		}

		// Bound retained events to the most recent maxAppEvents. Copy into a
		// fresh slice so the trimmed-off prefix can be garbage collected rather
		// than pinned by the original backing array.
		if events := b.appEvents[appID]; len(events) > maxAppEvents {
			trimmed := make([]storedPinpointEvent, maxAppEvents)
			copy(trimmed, events[len(events)-maxAppEvents:])
			b.appEvents[appID] = trimmed
		}

		results[epID] = endpointItemResponse{EventsItemResponse: evResults}
	}

	return &eventsResponse{Results: results}, nil
}
