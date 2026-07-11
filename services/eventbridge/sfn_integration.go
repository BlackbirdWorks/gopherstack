package eventbridge

import "context"

// SFNPutEvents implements the Step Functions EventBridge PutEvents service integration.
// It maps the SFN entries (each a map with Source, DetailType, Detail, EventBusName)
// to EventEntry values and calls PutEvents, returning the count of failed entries.
func (b *InMemoryBackend) SFNPutEvents(ctx context.Context, entries []map[string]any) (int, error) {
	evtEntries := make([]EventEntry, 0, len(entries))

	for _, e := range entries {
		entry := EventEntry{}
		entry.Source, _ = e["Source"].(string)
		entry.DetailType, _ = e["DetailType"].(string)
		entry.Detail, _ = e["Detail"].(string)
		entry.EventBusName, _ = e["EventBusName"].(string)
		evtEntries = append(evtEntries, entry)
	}

	results, err := b.PutEvents(ctx, evtEntries)
	if err != nil {
		return 0, err
	}

	failedCount := 0
	for _, r := range results {
		if r.ErrorCode != "" {
			failedCount++
		}
	}

	return failedCount, nil
}
