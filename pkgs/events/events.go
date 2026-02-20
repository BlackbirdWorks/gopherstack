// Package events defines the event system for Gopherstack.
// Services can emit typed events and listeners can subscribe to them.
package events

import "context"

// Event is a marker interface for all events in the system.
// All concrete event types must implement this interface.
type Event interface {
	EventType() string
}

// EventListener is a callback function that handles a specific event type.
type EventListener[T Event] func(ctx context.Context, event T) error

// EventEmitter manages event subscriptions and delivery.
type EventEmitter[T Event] interface {
	Emit(ctx context.Context, event T) error
	Subscribe(listener EventListener[T]) func()
	ListenerCount() int
}
