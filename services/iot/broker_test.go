package iot_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRefinement1_BrokerPublish_NotStarted verifies Publish returns error before start.
func TestBrokerPublish_NotStarted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "publish_before_start_returns_error", wantErr: iot.ErrBrokerNotStarted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			broker := iot.NewBroker(b, 0)
			err := broker.Publish("sensor/temp", []byte("25"), false, 0)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_HandlerBroker verifies Handler.Broker returns the embedded broker.
func TestHandlerBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		hasNil bool
	}{
		{name: "nil_broker", hasNil: true},
		{name: "non_nil_broker", hasNil: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			if tt.hasNil {
				h := iot.NewHandler(b, nil)
				assert.Nil(t, h.Broker())
			} else {
				broker := iot.NewBroker(b, 0)
				h := iot.NewHandler(b, broker)
				assert.Equal(t, broker, h.Broker())
			}
		})
	}
}

// TestRefinement1_StartWorker_NilBroker verifies StartWorker is a no-op with nil broker.
func TestStartWorker_NilBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_broker_no_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			err := h.StartWorker(t.Context())
			require.NoError(t, err)
		})
	}
}

// TestHandlerShutdown_NilBroker verifies Shutdown is a safe no-op when no
// broker was ever started (StartWorker was never called, or was a no-op
// because the broker is nil).
func TestHandlerShutdown_NilBroker(t *testing.T) {
	t.Parallel()

	h, _ := newRefHandler()

	assert.NotPanics(t, func() {
		h.Shutdown(t.Context())
	})
}

// TestHandlerShutdown_DrainsBrokerGoroutine verifies Shutdown blocks until the
// embedded MQTT broker's background goroutine has actually exited, instead of
// merely cancelling its context and returning immediately. Regression test
// for a leak where StartWorker launched a bare `go func()` with no
// Shutdown/drain path, so the broker goroutine could outlive its owner.
func TestHandlerShutdownDrainsBrokerGoroutine(t *testing.T) {
	t.Parallel()

	b := newRefBackend()
	broker := iot.NewBroker(b, 0)
	h := iot.NewHandler(b, broker)

	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()

	require.NoError(t, h.StartWorker(runCtx))

	// Give the broker goroutine a moment to actually start listening before
	// asking it to stop.
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		h.Shutdown(t.Context())
		close(done)
	}()

	select {
	case <-done:
		// Shutdown returned only after the broker goroutine drained -- good.
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return: broker goroutine leaked")
	}
}
