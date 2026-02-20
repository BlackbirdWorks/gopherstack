package events_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
)

var errListenerTest = errors.New("listener error")

func TestInMemoryEmitter_EventTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		event    events.Event
		expected string
	}{
		{"TableCreatedEvent", &events.TableCreatedEvent{Table: "users"}, "dynamodb.table.created"},
		{"TableDeletedEvent", &events.TableDeletedEvent{Table: "users"}, "dynamodb.table.deleted"},
		{"ItemCreatedEvent", &events.ItemCreatedEvent{Table: "users"}, "dynamodb.item.created"},
		{"ItemUpdatedEvent", &events.ItemUpdatedEvent{Table: "users"}, "dynamodb.item.updated"},
		{"ItemDeletedEvent", &events.ItemDeletedEvent{Table: "users"}, "dynamodb.item.deleted"},
		{"BucketCreatedEvent", &events.BucketCreatedEvent{BucketName: "my-bucket"}, "s3.bucket.created"},
		{"BucketDeletedEvent", &events.BucketDeletedEvent{BucketName: "my-bucket"}, "s3.bucket.deleted"},
		{"ObjectCreatedEvent", &events.ObjectCreatedEvent{BucketName: "my-bucket", Key: "file.txt"}, "s3.object.created"},
		{"ObjectDeletedEvent", &events.ObjectDeletedEvent{BucketName: "my-bucket", Key: "file.txt"}, "s3.object.deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.event.EventType(); got != tt.expected {
				t.Errorf("EventType() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestInMemoryEmitter_Emit(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.ItemCreatedEvent]()
	event := &events.ItemCreatedEvent{Table: "users", Key: map[string]any{"id": "123"}}
	ctx := context.Background()

	var received []*events.ItemCreatedEvent
	emitter.Subscribe(func(_ context.Context, e *events.ItemCreatedEvent) error {
		received = append(received, e)

		return nil
	})

	if err := emitter.Emit(ctx, event); err != nil {
		t.Fatalf("Emit() error: %v", err)
	}

	if len(received) != 1 {
		t.Errorf("Expected 1 event received, got %d", len(received))
	}
	if received[0].Table != "users" {
		t.Errorf("Expected table 'users', got %q", received[0].Table)
	}
}

func TestInMemoryEmitter_MultipleListeners(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "orders"}
	ctx := context.Background()

	var count int
	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})
	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})

	if err := emitter.Emit(ctx, event); err != nil {
		t.Fatalf("Emit() error: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected both listeners to be called, got count = %d", count)
	}
}

func TestInMemoryEmitter_ListenerCount(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()

	if got := emitter.ListenerCount(); got != 0 {
		t.Errorf("ListenerCount() = %d, want 0", got)
	}

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})

	if got := emitter.ListenerCount(); got != 1 {
		t.Errorf("ListenerCount() = %d, want 1", got)
	}

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})

	if got := emitter.ListenerCount(); got != 2 {
		t.Errorf("ListenerCount() = %d, want 2", got)
	}
}

func TestInMemoryEmitter_Unsubscribe(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "products"}
	ctx := context.Background()

	var count int

	unsubscribe := emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})

	// Emit once, count should be 1
	emitter.Emit(ctx, event)
	if count != 1 {
		t.Errorf("After first emit, count = %d, want 1", count)
	}

	// Unsubscribe and emit again, count should still be 1
	unsubscribe()
	emitter.Emit(ctx, event)
	if count != 1 {
		t.Errorf("After unsubscribe and emit, count = %d, want 1", count)
	}
}

func TestInMemoryEmitter_ErrorHandling(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "failed"}
	ctx := context.Background()

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return errListenerTest
	})

	if err := emitter.Emit(ctx, event); !errors.Is(err, errListenerTest) {
		t.Errorf("Emit() error = %v, want %v", err, errListenerTest)
	}
}

func TestInMemoryEmitter_Clear(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})
	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})

	if got := emitter.ListenerCount(); got != 2 {
		t.Errorf("Before clear: ListenerCount() = %d, want 2", got)
	}

	emitter.Clear()

	if got := emitter.ListenerCount(); got != 0 {
		t.Errorf("After clear: ListenerCount() = %d, want 0", got)
	}
}
