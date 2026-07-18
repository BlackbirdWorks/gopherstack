package eventbridge

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// maxPutEventsEntries is AWS's per-request cap on PutEvents/PutPartnerEvents
// entries (Entries: Array Members Minimum 1, Maximum 10).
const maxPutEventsEntries = 10

// anyPutEventsEntryComplete reports whether at least one entry in the batch
// carries all three of Source, DetailType, and Detail. AWS requires these on
// every entry to deliver it; an entry missing one fails individually, but if
// NONE of the entries have all three, AWS fails the entire request instead
// of returning an all-failed per-entry result (see PutEventsRequestEntry.Detail
// doc in aws-sdk-go-v2/service/eventbridge/types).
func anyPutEventsEntryComplete(entries []EventEntry) bool {
	for _, e := range entries {
		if e.Source != "" && e.DetailType != "" && e.Detail != "" {
			return true
		}
	}

	return false
}

// putEventsInvalidArgumentErrorCode is the EventResultEntry.ErrorCode AWS
// uses for a PutEvents entry missing a required field (Source, DetailType,
// or Detail).
const putEventsInvalidArgumentErrorCode = "InvalidArgument"

// validatePutEventsEntry checks the three fields AWS requires on every
// PutEvents entry (Source, DetailType, Detail). It returns
// (errorCode, errorMessage, ok=false) with the AWS "InvalidArgument" error
// code/message for the first missing field, or ("", "", true) when valid.
func validatePutEventsEntry(e EventEntry) (string, string, bool) {
	switch {
	case e.Source == "":
		return putEventsInvalidArgumentErrorCode,
			"Parameter Source is not valid. Reason: Source is a required argument.", false
	case e.DetailType == "":
		return putEventsInvalidArgumentErrorCode,
			"Parameter DetailType is not valid. Reason: DetailType is a required argument.", false
	case e.Detail == "":
		return putEventsInvalidArgumentErrorCode,
			"Parameter Detail is not valid. Reason: Detail is a required argument.", false
	default:
		return "", "", true
	}
}

// PutEvents records events in the event log and returns result entries.
//
// AWS EventBridge constrains PutEvents requests to 256 KiB of total entry
// payload (sum of Source, DetailType, Detail, Resources, Time across every
// entry). Entries that, combined with what's been accepted so far, would
// exceed the cap are rejected individually with the AWS error code
// `EventSizeLimitExceeded`. The remaining entries continue to be accepted.
//
// AWS also requires between 1 and 10 entries per request (a whole-request
// ValidationException-equivalent otherwise) and requires Source, DetailType,
// and Detail on every entry (a per-entry InvalidArgument failure, or a
// whole-request failure if no entry in the batch has all three).
func (b *InMemoryBackend) PutEvents(ctx context.Context, entries []EventEntry) ([]EventResultEntry, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: at least 1 entry is required", ErrInvalidParameter)
	}

	if len(entries) > maxPutEventsEntries {
		return nil, fmt.Errorf(
			"%w: entries must not have more than %d items",
			ErrInvalidParameter,
			maxPutEventsEntries,
		)
	}

	if !anyPutEventsEntryComplete(entries) {
		return nil, fmt.Errorf(
			"%w: at least one entry must include Source, DetailType, and Detail",
			ErrInvalidParameter,
		)
	}

	const maxBatchBytes = 256 * 1024

	region := getRegionFromContext(ctx, b.region)

	results, accepted, dt, workerSem, svcCtx, delivTimeout := b.putEventsLocked(region, entries, maxBatchBytes)

	// Trigger async fan-out delivery after releasing the lock.
	// Skip if the backend is already closing to prevent wg.Add concurrent with
	// wg.Wait (which would panic per sync.WaitGroup semantics).
	if dt != nil && !b.closing.Load() && len(accepted) > 0 {
		entriesCopy := make([]EventEntry, len(accepted))
		copy(entriesCopy, accepted)
		dtCopy := *dt
		b.wg.Go(func() {
			// Acquire a worker slot or abort if the backend is shutting down.
			select {
			case workerSem <- struct{}{}:
				defer func() { <-workerSem }()
			case <-svcCtx.Done():
				return
			}
			b.deliverEvents(svcCtx, region, entriesCopy, dtCopy, delivTimeout)
		})
	}

	return results, nil
}

// putEventsLocked validates and records entries into the event log (and matching
// archives) under b.mu, returning the per-entry results, the accepted entries, and
// the delivery configuration snapshot needed by PutEvents to fan out delivery after
// releasing the lock. Extracted from PutEvents so the locked region is a plain
// method body rather than a function literal.
func (b *InMemoryBackend) putEventsLocked(
	region string,
	entries []EventEntry,
	maxBatchBytes int,
) (
	[]EventResultEntry,
	[]EventEntry,
	*DeliveryTargets,
	chan struct{},
	context.Context,
	time.Duration,
) {
	b.mu.Lock("PutEvents")
	defer b.mu.Unlock()

	results := make([]EventResultEntry, 0, len(entries))
	accepted := make([]EventEntry, 0, len(entries))
	totalBytes := 0
	for _, entry := range entries {
		if errCode, msg, ok := validatePutEventsEntry(entry); !ok {
			results = append(results, EventResultEntry{ErrorCode: errCode, ErrorMessage: msg})

			continue
		}

		entryBytes := putEventsEntryBytes(entry)
		if totalBytes+entryBytes > maxBatchBytes {
			results = append(results, EventResultEntry{
				ErrorCode:    "EventSizeLimitExceeded",
				ErrorMessage: "Event size exceeds 256 KB total batch limit",
			})

			continue
		}

		totalBytes += entryBytes
		eventID := uuid.New().String()
		busName := entry.EventBusName
		if busName == "" {
			busName = defaultEventBusName
		}
		eventTime := time.Now()
		if entry.Time != nil {
			eventTime = *entry.Time
		}
		logEntry := EventLogEntry{
			ID:           eventID,
			Source:       entry.Source,
			DetailType:   entry.DetailType,
			Detail:       entry.Detail,
			EventBusName: busName,
			Time:         eventTime,
		}
		b.eventLog = append(b.eventLog, logEntry)
		// Trim event log to last 1000 entries.
		if len(b.eventLog) > maxEventLogSize {
			b.eventLog = b.eventLog[len(b.eventLog)-maxEventLogSize:]
		}
		// Capture event into matching archives.
		b.captureEventInArchives(region, entry, busName)
		accepted = append(accepted, entry)
		results = append(results, EventResultEntry{EventID: eventID})
	}

	return results, accepted, b.deliveryTargets, b.workerSem, b.ctx, b.deliveryTimeout
}

// GetEventLog returns a copy of the current event log.
func (b *InMemoryBackend) GetEventLog(ctx context.Context) []EventLogEntry { //nolint:revive // existing issue.
	b.mu.RLock("GetEventLog")
	defer b.mu.RUnlock()

	log := make([]EventLogEntry, len(b.eventLog))
	copy(log, b.eventLog)

	return log
}

// putEventsEntryBytes returns the byte size of an event entry as it counts
// against the 256 KiB PutEvents request cap. Per AWS documentation, the
// counted fields are Source, DetailType, Detail, Time, and each Resource ARN
// (14 bytes for Time when present). EventBusName is not counted.
func putEventsEntryBytes(e EventEntry) int {
	const timeBytes = 14

	total := len(e.Source) + len(e.DetailType) + len(e.Detail)
	if e.Time != nil {
		total += timeBytes
	}

	for _, r := range e.Resources {
		total += len(r)
	}

	return total
}
