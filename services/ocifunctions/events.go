package ocifunctions

import "time"

// Event types (https://docs.oracle.com/en-us/iaas/Content/Events/Reference/eventsproducers.htm).
const (
	EventCreateApplication = "com.oraclecloud.functions.createapplication"
	EventDeleteApplication = "com.oraclecloud.functions.deleteapplication"
	EventCreateFunction    = "com.oraclecloud.functions.createfunction"
	EventDeleteFunction    = "com.oraclecloud.functions.deletefunction"
)

// ResourceEvent describes an application or function lifecycle change.
type ResourceEvent struct {
	Time          time.Time
	EventType     string
	CompartmentID string
	ResourceName  string
	ResourceID    string
}

// SetEventSink installs the receiver of resource events; it is called outside the backend lock.
func (b *InMemoryBackend) SetEventSink(fn func(ResourceEvent)) {
	b.mu.Lock("SetEventSink")
	b.sink = fn
	b.mu.Unlock()
}

// recordEvent queues an event while the lock is held; flushEvents delivers it after unlock.
func (b *InMemoryBackend) recordEvent(eventType, compartment, name, id string) {
	if b.sink == nil {
		return
	}

	b.pending = append(b.pending, ResourceEvent{
		Time: b.nowUTC(), EventType: eventType, CompartmentID: compartment, ResourceName: name, ResourceID: id,
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

// FunctionActive reports whether id is a function that is not deleted or being deleted.
func (b *InMemoryBackend) FunctionActive(id string) bool {
	b.mu.RLock("FunctionActive")
	defer b.mu.RUnlock()

	_, aerr := b.activeFn(id)

	return aerr == nil
}
