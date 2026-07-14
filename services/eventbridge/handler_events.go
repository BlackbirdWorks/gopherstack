package eventbridge

import (
	"context"
	"encoding/json"
)

type putEventsInput struct {
	Entries []EventEntry `json:"Entries"`
}

type putEventsOutput struct {
	Entries          []EventResultEntry `json:"Entries"`
	FailedEntryCount int                `json:"FailedEntryCount"`
}

// countFailedEntries returns the number of PutEvents/PutPartnerEvents result
// entries that carry an ErrorCode. AWS reports this in the FailedEntryCount
// field so callers can tell how many events in a batch were rejected without
// scanning every entry.
func countFailedEntries(entries []EventResultEntry) int {
	failed := 0
	for i := range entries {
		if entries[i].ErrorCode != "" {
			failed++
		}
	}

	return failed
}

func (h *Handler) eventsActions() map[string]actionFn {
	return map[string]actionFn{
		"PutEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries, err := h.Backend.PutEvents(ctx, input.Entries)
			if err != nil {
				return nil, err
			}

			return &putEventsOutput{
				FailedEntryCount: countFailedEntries(entries),
				Entries:          entries,
			}, nil
		},
	}
}
