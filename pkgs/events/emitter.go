package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ErrListenerPanicked is returned by Emit when a listener function panics.
// Callers can use [errors.Is] to detect this condition.
var ErrListenerPanicked = errors.New("listener panicked")

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
// If a listener panics, the panic is recovered and returned as an error.
func (e *InMemoryEmitter[T]) Emit(ctx context.Context, event T) error {
	e.mu.RLock("Emit")
	listeners := make([]listenerEntry[T], len(e.listeners))
	copy(listeners, e.listeners)
	e.mu.RUnlock()

	for _, entry := range listeners {
		if err := safeCall(ctx, event, entry.listener); err != nil {
			return err
		}
	}

	return nil
}

// safeCall invokes fn with ctx and event, recovering any panic as an error.
func safeCall[T Event](ctx context.Context, event T, fn EventListener[T]) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("%w: %v", ErrListenerPanicked, r)
		}
	}()

	return fn(ctx, event)
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
// It uses a swap-with-last strategy (O(1) removal, order not preserved)
// to avoid shifting all subsequent elements.
func (e *InMemoryEmitter[T]) removeByID(id uint64) {
	for i, entry := range e.listeners {
		if entry.id == id {
			last := len(e.listeners) - 1
			if i != last {
				e.listeners[i] = e.listeners[last]
			}
			e.listeners[last] = listenerEntry[T]{} // clear for GC
			e.listeners = e.listeners[:last]

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
