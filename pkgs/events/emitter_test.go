package events_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/events"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			"ObjectCreatedEvent",
			&events.ObjectCreatedEvent{BucketName: "my-bucket", Key: "file.txt"},
			"s3.object.created",
		},
		{
			"ObjectDeletedEvent",
			&events.ObjectDeletedEvent{BucketName: "my-bucket", Key: "file.txt"},
			"s3.object.deleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.event.EventType())
		})
	}
}

func TestInMemoryEmitter_Emit(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.ItemCreatedEvent]()
	event := &events.ItemCreatedEvent{Table: "users", Key: map[string]any{"id": "123"}}
	ctx := t.Context()

	var received []*events.ItemCreatedEvent
	emitter.Subscribe(func(_ context.Context, e *events.ItemCreatedEvent) error {
		received = append(received, e)

		return nil
	})

	require.NoError(t, emitter.Emit(ctx, event), "Emit() error")

	assert.Len(t, received, 1, "Expected 1 event received")
	if len(received) > 0 {
		assert.Equal(t, "users", received[0].Table)
	}
}

func TestInMemoryEmitter_MultipleListeners(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "orders"}
	ctx := t.Context()

	var count int
	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})
	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})

	require.NoError(t, emitter.Emit(ctx, event), "Emit() error")

	assert.Equal(t, 2, count, "Expected both listeners to be called")
}

func TestInMemoryEmitter_ListenerCount(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()

	assert.Equal(t, 0, emitter.ListenerCount(), "ListenerCount() should be 0")

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})

	assert.Equal(t, 1, emitter.ListenerCount())

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return nil
	})

	assert.Equal(t, 2, emitter.ListenerCount())
}

func TestInMemoryEmitter_Unsubscribe(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "products"}
	ctx := t.Context()

	var count int

	unsubscribe := emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		count++

		return nil
	})

	// Emit once, count should be 1
	emitter.Emit(ctx, event)
	assert.Equal(t, 1, count, "After first emit")

	// Unsubscribe and emit again, count should still be 1
	unsubscribe()
	emitter.Emit(ctx, event)
	assert.Equal(t, 1, count, "After unsubscribe and emit")
}

func TestInMemoryEmitter_ErrorHandling(t *testing.T) {
	t.Parallel()

	emitter := events.NewInMemoryEmitter[*events.TableCreatedEvent]()
	event := &events.TableCreatedEvent{Table: "failed"}
	ctx := t.Context()

	emitter.Subscribe(func(_ context.Context, _ *events.TableCreatedEvent) error {
		return errListenerTest
	})

	assert.ErrorIs(t, emitter.Emit(ctx, event), errListenerTest)
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

	assert.Equal(t, 2, emitter.ListenerCount(), "Before clear")

	emitter.Clear()

	assert.Equal(t, 0, emitter.ListenerCount(), "After clear")
}
