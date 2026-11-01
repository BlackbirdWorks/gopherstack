package gcpstorage

import (
	"encoding/json"
	"time"
)

// ObjectEventType names a change to an object, matching the suffix of the google.cloud.storage.object.v1 event types.
type ObjectEventType string

const (
	// ObjectFinalized is a new object (or generation) becoming readable.
	ObjectFinalized ObjectEventType = "finalized"
	// ObjectDeleted is an object generation being permanently removed.
	ObjectDeleted ObjectEventType = "deleted"
	// ObjectArchived is the live generation of a versioned bucket becoming noncurrent.
	ObjectArchived ObjectEventType = "archived"
	// ObjectMetadataUpdated is a metadata change to an existing generation.
	ObjectMetadataUpdated ObjectEventType = "metadataUpdated"
)

// ObjectEvent describes one object change; Object is a private copy.
type ObjectEvent struct {
	Time   time.Time
	Object *Object
	Type   ObjectEventType
	// OverwroteGen is the replaced generation on a finalize; OverwrittenByGen the replacing one on delete/archive.
	OverwroteGen     int64
	OverwrittenByGen int64
}

// Data is the event payload: the object resource, as the Cloud Storage JSON API renders it.
func (e ObjectEvent) Data() json.RawMessage {
	raw, err := json.Marshal(objectResource("", e.Object))
	if err != nil {
		return nil
	}

	return raw
}

// ObjectObserver is called after an object change is committed, outside the backend lock.
type ObjectObserver func(ObjectEvent)

// ObserveObjects registers fn to see every object change from now on.
func (b *InMemoryBackend) ObserveObjects(fn ObjectObserver) {
	b.mu.Lock("ObserveObjects")
	defer b.mu.Unlock()

	b.observers = append(b.observers, fn)
}

// queueLocked records an event for flushEvents; it is a no-op without observers.
func (b *InMemoryBackend) queueLocked(t ObjectEventType, o *Object) {
	b.queueLinkedLocked(t, o, 0, 0)
}

func (b *InMemoryBackend) queueLinkedLocked(t ObjectEventType, o *Object, overwrote, overwrittenBy int64) {
	if (len(b.observers) == 0 && b.notifier == nil) || o == nil {
		return
	}

	b.pending = append(b.pending, ObjectEvent{
		Type: t, Object: o.clone(), Time: b.now().UTC(), OverwroteGen: overwrote, OverwrittenByGen: overwrittenBy,
	})
}

// flushEvents delivers queued events. Callers defer it before taking b.mu so it runs after the unlock.
func (b *InMemoryBackend) flushEvents() {
	b.mu.Lock("flushEvents")
	events, observers := b.pending, b.observers
	b.pending = nil
	b.mu.Unlock()

	for _, ev := range events {
		for _, fn := range observers {
			fn(ev)
		}

		b.dispatchNotifications(ev)
	}
}

// replacedLocked queues the events for a live version that a new write displaced.
func (b *InMemoryBackend) replacedLocked(bk *Bucket, prev *Object, newGen int64) {
	if prev == nil {
		return
	}

	if bk.Versioning {
		b.queueLinkedLocked(ObjectArchived, prev, 0, newGen)
	} else {
		b.queueLinkedLocked(ObjectDeleted, prev, 0, newGen)
	}
}
