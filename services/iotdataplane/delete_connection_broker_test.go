package iotdataplane_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// awaitTracked waits for the broker's asynchronous session-established hook to
// register clientID; the broker must be wired to b before the client connects.
func awaitTracked(t *testing.T, b *iotdataplane.InMemoryBackend, clientID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, err := b.GetConnection(clientID)

		return err == nil
	}, 3*time.Second, 5*time.Millisecond)
}

// TestDeleteConnection_DisconnectsLiveBrokerSession drives the typed SDK
// DeleteConnection against a real broker and a persistent-session MQTT client.
func TestDeleteConnection_DisconnectsLiveBrokerSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cleanSession bool
		preventWill  bool
	}{
		{name: "defaults_keep_session_and_send_will"},
		{name: "clean_session", cleanSession: true},
		{name: "prevent_will", preventWill: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			broker, port := startRealBroker(t)
			url := fmt.Sprintf("tcp://127.0.0.1:%d", port)

			willSeen := make(chan struct{}, 1)
			watcher := pahomqtt.NewClient(pahomqtt.NewClientOptions().
				AddBroker(url).SetClientID("will-watcher").SetAutoReconnect(false).
				SetDefaultPublishHandler(func(pahomqtt.Client, pahomqtt.Message) { willSeen <- struct{}{} }))
			require.True(t, watcher.Connect().WaitTimeout(3*time.Second))
			t.Cleanup(func() { watcher.Disconnect(100) })
			require.True(t, watcher.Subscribe("lwt/topic", 0, nil).WaitTimeout(3*time.Second))

			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(broker)

			lost := make(chan struct{})
			victim := pahomqtt.NewClient(pahomqtt.NewClientOptions().
				AddBroker(url).SetClientID("victim").
				SetCleanSession(false).SetAutoReconnect(false).
				SetWill("lwt/topic", "gone", 0, false).
				SetConnectionLostHandler(func(pahomqtt.Client, error) { close(lost) }))
			require.True(t, victim.Connect().WaitTimeout(3*time.Second))
			require.True(t, victim.Subscribe("victim/in", 1, nil).WaitTimeout(3*time.Second))

			awaitTracked(t, b, "victim")
			client, _ := newTestIoTDataPlaneSDKClient(t, iotdataplane.NewHandler(b))

			_, err := client.DeleteConnection(t.Context(), &iotdataplanesdk.DeleteConnectionInput{
				ClientId:           aws.String("victim"),
				CleanSession:       tt.cleanSession,
				PreventWillMessage: tt.preventWill,
			})
			require.NoError(t, err)

			select {
			case <-lost:
			case <-time.After(3 * time.Second):
				t.Fatal("broker session was not closed by DeleteConnection")
			}

			if tt.preventWill {
				select {
				case <-willSeen:
					t.Fatal("last will published despite preventWillMessage")
				case <-time.After(500 * time.Millisecond):
				}
			} else {
				select {
				case <-willSeen:
				case <-time.After(3 * time.Second):
					t.Fatal("last will not published on DeleteConnection")
				}
			}

			require.Eventually(t, func() bool {
				subs, known := broker.ClientSubscriptions("victim")
				if tt.cleanSession {
					return !known
				}

				return known && subs["victim/in"] == 1
			}, 3*time.Second, 20*time.Millisecond)

			assert.False(t, victim.IsConnected())
		})
	}
}

// TestGetConnection_ReportsLiveBrokerSession reads the real session fields
// back through the typed SDK GetConnection.
func TestGetConnection_ReportsLiveBrokerSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cleanSession  bool
		includeSocket bool
	}{
		{name: "persistent_no_socket"},
		{name: "clean_with_socket", cleanSession: true, includeSocket: true},
		{name: "persistent_with_socket", includeSocket: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			broker, port := startRealBroker(t)
			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(broker)

			c := pahomqtt.NewClient(pahomqtt.NewClientOptions().
				AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).SetClientID("dev").SetAutoReconnect(false).
				SetCleanSession(tt.cleanSession).SetKeepAlive(30 * time.Second))
			require.True(t, c.Connect().WaitTimeout(3*time.Second))
			t.Cleanup(func() { c.Disconnect(100) })

			awaitTracked(t, b, "dev")
			client, _ := newTestIoTDataPlaneSDKClient(t, iotdataplane.NewHandler(b))

			out, err := client.GetConnection(t.Context(), &iotdataplanesdk.GetConnectionInput{
				ClientId:                 aws.String("dev"),
				IncludeSocketInformation: tt.includeSocket,
			})
			require.NoError(t, err)

			assert.Equal(t, tt.cleanSession, out.CleanSession)
			assert.EqualValues(t, 30, out.KeepAliveDuration)

			if tt.includeSocket {
				assert.Equal(t, "127.0.0.1", aws.ToString(out.SourceIp))
				assert.Equal(t, "127.0.0.1", aws.ToString(out.TargetIp))
				assert.EqualValues(t, port, out.TargetPort)
				assert.Positive(t, out.SourcePort)
			} else {
				assert.Nil(t, out.TargetIp)
				assert.Zero(t, out.SourcePort)
				assert.Zero(t, out.TargetPort)
			}
		})
	}
}
