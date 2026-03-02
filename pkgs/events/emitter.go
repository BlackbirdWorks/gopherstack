package events

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// listenerEntry wraps a listener with a unique ID for tracking.
type listenerEntry[T Event] struct {
	listener EventListener[T]
	id       uint64
}

// InMemoryEmitter is a simple in-memory implementation of the EventEmitter interface.
// It stores listeners and emits events synchronously to all subscribers.
type InMemoryEmitter[T Event] struct {
	mu        *lockmetrics.RWMutex
	listeners []listenerEntry[T]
	nextID    uint64
}

// NewInMemoryEmitter creates a new in-memory event emitter.
func NewInMemoryEmitter[T Event]() *InMemoryEmitter[T] {
	return &InMemoryEmitter[T]{
		listeners: make([]listenerEntry[T], 0),
		nextID:    1,
		mu:        lockmetrics.New("events.emitter"),
	}
}

// Emit broadcasts an event to all subscribers synchronously.
// Returns the first non-nil error encountered, if any.
func (e *InMemoryEmitter[T]) Emit(ctx context.Context, event T) error {
	e.mu.RLock("Emit")
	listeners := make([]listenerEntry[T], len(e.listeners))
	copy(listeners, e.listeners)
	e.mu.RUnlock()

	for _, entry := range listeners {
		if err := entry.listener(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

// Subscribe adds a listener to the emitter and returns an unsubscribe function.
func (e *InMemoryEmitter[T]) Subscribe(listener EventListener[T]) func() {
	e.mu.Lock("Subscribe")
	id := e.nextID
	e.nextID++
	e.listeners = append(e.listeners, listenerEntry[T]{id: id, listener: listener})
	e.mu.Unlock()

	return func() {
		e.mu.Lock("Unsubscribe")
		defer e.mu.Unlock()
		e.removeByID(id)
	}
}

// removeByID removes the listener with the given ID from the emitter.
func (e *InMemoryEmitter[T]) removeByID(id uint64) {
	for i, entry := range e.listeners {
		if entry.id == id {
			e.listeners = append(e.listeners[:i], e.listeners[i+1:]...)

			return
		}
	}
}

// ListenerCount returns the current number of registered listeners.
func (e *InMemoryEmitter[T]) ListenerCount() int {
	e.mu.RLock("ListenerCount")
	defer e.mu.RUnlock()

	return len(e.listeners)
}

// Clear removes all listeners from the emitter.
// Useful for testing.
func (e *InMemoryEmitter[T]) Clear() {
	e.mu.Lock("Clear")
	defer e.mu.Unlock()
	e.listeners = e.listeners[:0]
}
