package iot_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func connectPaho(t *testing.T, port int, clientID string) pahomqtt.Client {
	t.Helper()

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
		SetClientID(clientID).
		SetConnectTimeout(3 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(pahomqtt.Client, pahomqtt.Message) {})

	client := pahomqtt.NewClient(opts)
	token := client.Connect()
	require.True(t, token.WaitTimeout(3*time.Second))
	require.NoError(t, token.Error())
	t.Cleanup(func() { client.Disconnect(0) })

	return client
}

// rawConnect opens an MQTT 3.1.1 session by hand so the test controls how (and whether) it ends and acks.
func rawConnect(t *testing.T, port int, clientID string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	body := append([]byte{0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 60, 0, byte(len(clientID))}, clientID...)
	_, err = conn.Write(append([]byte{0x10, byte(len(body))}, body...))
	require.NoError(t, err)

	connack := make([]byte, 4)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = conn.Read(connack)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Time{}))

	return conn
}

func TestBroker_ReportsLifecycleToDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start      func(t *testing.T, port int, clientID string) (end func())
		name       string
		wantReason string
	}{
		{
			name: "client_initiated",
			start: func(t *testing.T, port int, clientID string) func() {
				t.Helper()

				client := connectPaho(t, port, clientID)

				return func() { client.Disconnect(100) }
			},
			wantReason: "CLIENT_INITIATED_DISCONNECT",
		},
		{
			name: "connection_lost",
			start: func(t *testing.T, port int, clientID string) func() {
				t.Helper()

				conn := rawConnect(t, port, clientID)

				return func() { _ = conn.Close() }
			},
			wantReason: "CONNECTION_LOST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port := freeTCPPort(t)
			broker := startTestBroker(t, newRefBackend(), port)
			dp := iotdataplane.NewInMemoryBackend()
			dp.SetBroker(broker)

			clientID := "life-" + tt.name

			end := tt.start(t, port, clientID)

			require.Eventually(t, func() bool {
				c, err := dp.GetConnection(clientID)

				return err == nil && c.DisconnectedAt.IsZero()
			}, 3*time.Second, 5*time.Millisecond)

			conn, err := dp.GetConnection(clientID)
			require.NoError(t, err)
			assert.Equal(t, "127.0.0.1", conn.SourceIP)

			end()

			require.Eventually(t, func() bool {
				c, getErr := dp.GetConnection(clientID)

				return getErr == nil && !c.DisconnectedAt.IsZero()
			}, 3*time.Second, 5*time.Millisecond)

			conn, err = dp.GetConnection(clientID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantReason, conn.DisconnectReason)
		})
	}
}

func TestBroker_SendDirectMessageAwaitAck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		acks    bool
	}{
		{name: "acked", acks: true},
		{name: "no_puback_times_out", acks: false, wantErr: iotdataplane.ErrDeliveryTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port := freeTCPPort(t)
			broker := startTestBroker(t, newRefBackend(), port)
			dp := iotdataplane.NewInMemoryBackend()
			dp.SetBroker(broker)

			clientID := "ack-" + tt.name

			if tt.acks {
				_ = connectPaho(t, port, clientID)
			} else {
				_ = rawConnect(t, port, clientID)
			}

			require.Eventually(t, func() bool {
				_, err := dp.GetConnection(clientID)

				return err == nil
			}, 3*time.Second, 5*time.Millisecond)

			err := dp.SendDirectMessageAwaitAck(
				clientID, "direct/t", []byte("x"), iotdataplane.MQTT5Properties{}, time.Second,
			)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
