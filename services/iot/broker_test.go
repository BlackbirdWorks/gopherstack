package iot_test

import (
	"testing"

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
