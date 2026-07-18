package personalize

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// --- EventTracker ---

// CreateEventTracker creates a new event tracker.
func (b *InMemoryBackend) CreateEventTracker(
	name, datasetGroupArn string,
	tags map[string]string,
) (*EventTracker, error) {
	b.mu.Lock("CreateEventTracker")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.eventTrackers.Has(name) {
		return nil, fmt.Errorf("%w: event tracker %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	et := &EventTracker{
		EventTrackerArn:     b.personalizeARN("event-tracker", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		TrackingID:          uuid.New().String(),
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.eventTrackers.Put(et)
	if len(tags) > 0 {
		b.tags[et.EventTrackerArn] = copyStringMap(tags)
	}

	return et, nil
}

// DescribeEventTracker returns an event tracker by name or ARN.
func (b *InMemoryBackend) DescribeEventTracker(nameOrArn string) (*EventTracker, error) {
	b.mu.RLock("DescribeEventTracker")
	defer b.mu.RUnlock()

	if et := b.findEventTracker(nameOrArn); et != nil {
		return et, nil
	}

	return nil, fmt.Errorf("%w: event tracker %q not found", ErrNotFound, nameOrArn)
}

// DeleteEventTracker removes an event tracker.
func (b *InMemoryBackend) DeleteEventTracker(nameOrArn string) error {
	b.mu.Lock("DeleteEventTracker")
	defer b.mu.Unlock()

	et := b.findEventTracker(nameOrArn)
	if et == nil {
		return fmt.Errorf("%w: event tracker %q not found", ErrNotFound, nameOrArn)
	}
	b.eventTrackers.Delete(et.Name)
	delete(b.tags, et.EventTrackerArn)

	return nil
}

// ListEventTrackers returns event trackers, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListEventTrackers(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*EventTracker, string) {
	b.mu.RLock("ListEventTrackers")
	defer b.mu.RUnlock()

	all := b.eventTrackers.Snapshot()
	filtered := make([]*EventTracker, 0, len(all))
	for _, et := range all {
		if datasetGroupArn == "" || et.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, et)
		}
	}

	return paginateItems(filtered, eventTrackerKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findEventTracker(nameOrArn string) *EventTracker {
	if et, ok := b.eventTrackers.Get(nameOrArn); ok {
		return et
	}
	for _, et := range b.eventTrackers.All() {
		if et.EventTrackerArn == nameOrArn {
			return et
		}
	}

	return nil
}
