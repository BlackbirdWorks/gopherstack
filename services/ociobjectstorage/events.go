package ociobjectstorage

import "time"

// Object event types (https://docs.oracle.com/en-us/iaas/Content/Events/Reference/eventsproducers.htm).
const (
	EventCreateObject = "com.oraclecloud.objectstorage.createobject"
	EventUpdateObject = "com.oraclecloud.objectstorage.updateobject"
	EventDeleteObject = "com.oraclecloud.objectstorage.deleteobject"
)

// ObjectEvent describes an object state change in a bucket with objectEventsEnabled.
type ObjectEvent struct {
	Time          time.Time
	EventType     string
	CompartmentID string
	Namespace     string
	BucketName    string
	BucketID      string
	ObjectName    string
	ETag          string
}

// SetObjectEventSink installs the receiver of object events; it is called outside the backend lock.
func (b *InMemoryBackend) SetObjectEventSink(fn func(ObjectEvent)) {
	b.mu.Lock("SetObjectEventSink")
	b.sink = fn
	b.mu.Unlock()
}

// recordEvent queues an event while the lock is held; flushEvents delivers it after unlock.
func (b *InMemoryBackend) recordEvent(bk *Bucket, eventType, object, etag string) {
	if b.sink == nil || !bk.ObjectEventsEnabled {
		return
	}

	b.pending = append(b.pending, ObjectEvent{
		Time: b.nowUTC(), EventType: eventType, CompartmentID: bk.CompartmentID, Namespace: b.namespace,
		BucketName: bk.Name, BucketID: bk.ID, ObjectName: object, ETag: etag,
	})
}

func (b *InMemoryBackend) flushEvents() {
	b.mu.Lock("flushEvents")
	evs, sink := b.pending, b.sink
	b.pending = nil
	b.mu.Unlock()

	if sink == nil {
		return
	}

	for _, e := range evs {
		sink(e)
	}
}

func writeEventType(replaced bool) string {
	if replaced {
		return EventUpdateObject
	}

	return EventCreateObject
}
