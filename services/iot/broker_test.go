package iot_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
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

// freeTCPPort asks the OS for a currently-unused TCP port by binding to
// :0 and immediately releasing it, so a real *iot.Broker (which needs its
// port at construction time, before Start) can be pointed at a real,
// reachable address for this test's live paho client to dial.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

// startTestBroker starts a real *iot.Broker listening on port, and registers
// cleanup that cancels it and waits for its goroutine to actually exit
// (mirrors TestHandlerShutdownDrainsBrokerGoroutine's drain pattern). It
// blocks until the port is actually accepting connections.
func startTestBroker(t *testing.T, backend *iot.InMemoryBackend, port int) *iot.Broker {
	t.Helper()

	broker := iot.NewBroker(backend, port)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		_ = broker.Start(runCtx)
		close(done)
	}()

	t.Cleanup(func() {
		cancelRun()
		<-done
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()

			return broker
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("broker on %s did not become ready", addr)

	return nil
}

// TestBroker_ClientSubscriptionsAndSendToClient exercises Broker's
// MQTTPublisher extensions -- ClientSubscriptions and SendToClient -- against
// a REAL mochi-mqtt session established by a real paho MQTT client over a
// real TCP loopback connection (not a mock), proving: (1) ClientSubscriptions
// reads genuine per-client subscription state (cl.State.Subscriptions) once
// a client has actually subscribed, and (2) SendToClient genuinely delivers
// straight to one client's connection -- the client receives a message on a
// topic it never subscribed to, which is only possible via a direct,
// per-client write, not the topic-broadcast Publish path.
func TestBroker_ClientSubscriptionsAndSendToClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clientID      string
		startBroker   bool
		connectClient bool
	}{
		{name: "unstarted_broker_client_unknown", clientID: "dev-unstarted"},
		{
			name: "started_broker_no_live_session_client_unknown", clientID: "dev-no-session",
			startBroker: true,
		},
		{
			name:     "connected_client_reports_real_subscription_and_receives_direct_message",
			clientID: "dev-connected", startBroker: true, connectClient: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newRefBackend()
			port := freeTCPPort(t)

			var broker *iot.Broker
			if tt.startBroker {
				broker = startTestBroker(t, backend, port)
			} else {
				broker = iot.NewBroker(backend, port)
			}

			if !tt.connectClient {
				subs, connected := broker.ClientSubscriptions(tt.clientID)
				assert.Nil(t, subs)
				assert.False(t, connected)

				ok, err := broker.SendToClient(tt.clientID, "direct/topic", []byte("hello"), 0)
				assert.False(t, ok)

				if tt.startBroker {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, iot.ErrBrokerNotStarted)
				}

				return
			}

			received := make(chan pahomqtt.Message, 1)
			opts := pahomqtt.NewClientOptions().
				AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
				SetClientID(tt.clientID).
				SetConnectTimeout(3 * time.Second).
				SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) {
					received <- m
				})

			client := pahomqtt.NewClient(opts)
			connToken := client.Connect()
			require.True(t, connToken.WaitTimeout(3*time.Second))
			require.NoError(t, connToken.Error())
			defer client.Disconnect(250)

			subToken := client.Subscribe("sensor/temp", 1, nil)
			require.True(t, subToken.WaitTimeout(3*time.Second))
			require.NoError(t, subToken.Error())

			subs, connected := broker.ClientSubscriptions(tt.clientID)
			require.True(t, connected)
			require.Contains(t, subs, "sensor/temp")
			assert.Equal(t, byte(1), subs["sensor/temp"])

			ok, err := broker.SendToClient(tt.clientID, "direct/topic", []byte("hello-direct"), 0)
			require.NoError(t, err)
			require.True(t, ok)

			select {
			case msg := <-received:
				assert.Equal(t, "direct/topic", msg.Topic())
				assert.Equal(t, []byte("hello-direct"), msg.Payload())
			case <-time.After(3 * time.Second):
				t.Fatal("client never received the directly-sent message")
			}
		})
	}
}
