package iotdataplane_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// freeTCPPort asks the OS for a currently-unused TCP port, mirroring
// services/iot/broker_test.go's helper of the same purpose (duplicated here:
// external test packages can't share unexported test helpers across
// package boundaries).
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

// startRealBroker starts a real *iot.Broker on port and blocks until it is
// accepting connections, registering cleanup to drain its goroutine.
func startRealBroker(t *testing.T, port int) *iot.Broker {
	t.Helper()

	backend := iot.NewInMemoryBackend()
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

// Test_Publish_DeliversThroughRealBroker proves PublishWithProperties -- the
// new broker call InMemoryBackend.Publish makes to forward MQTT5 properties
// (see Test_Publish_MQTT5Fields_ForwardedToBroker for the properties-content
// proof against a mock) -- still delivers topic/payload through a REAL
// mochi-mqtt broker exactly like the old Publish call did, i.e. the new wire
// path is not a regression on basic delivery. The connected client uses
// paho.mqtt.golang, an MQTT 3.1.1 client with no MQTT5 property support in
// its Message interface, so this deliberately cannot observe the properties
// themselves -- see broker.go's PublishWithProperties doc comment (properties
// are encode-gated on the *receiving* client's own negotiated protocol
// version, packets.Packet.PublishEncode in mochi-mqtt@v2.7.9) and
// Test_Publish_MQTT5Fields_ForwardedToBroker for that coverage instead.
func Test_Publish_DeliversThroughRealBroker(t *testing.T) {
	t.Parallel()

	port := freeTCPPort(t)
	broker := startRealBroker(t, port)

	dpBackend := iotdataplane.NewInMemoryBackend()
	dpBackend.SetBroker(broker)
	h := iotdataplane.NewHandler(dpBackend)

	received := make(chan pahomqtt.Message, 1)
	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
		SetClientID("real-broker-sub").
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

	rec := doRequestHeaders(t, h,
		"/topics/sensor/temp?contentType=text%2Fplain&messageExpiry=60&responseTopic=reply/topic",
		[]byte("25"),
		map[string]string{
			"X-Amz-Mqtt5-Correlation-Data":         "aGVsbG8=",
			"X-Amz-Mqtt5-Payload-Format-Indicator": "UTF8_DATA",
		})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	select {
	case msg := <-received:
		assert.Equal(t, "sensor/temp", msg.Topic())
		assert.Equal(t, []byte("25"), msg.Payload())
	case <-time.After(3 * time.Second):
		t.Fatal("subscriber never received the published message")
	}
}

// Test_SendDirectMessage_DeliversThroughRealBroker proves
// SendToClientWithProperties still addresses one real, live broker client
// directly (bypassing subscription matching, per real AWS's documented
// SendDirectMessage semantics) through a REAL mochi-mqtt broker, exactly
// like the old SendToClient call did -- see
// Test_SendDirectMessage_MQTT5Fields_ForwardedToBroker for the
// properties-content proof against a mock.
func Test_SendDirectMessage_DeliversThroughRealBroker(t *testing.T) {
	t.Parallel()

	port := freeTCPPort(t)
	broker := startRealBroker(t, port)

	const clientID = "real-broker-direct"

	dpBackend := iotdataplane.NewInMemoryBackend()
	dpBackend.SetBroker(broker)
	dpBackend.AddConnectionInternal(clientID)
	h := iotdataplane.NewHandler(dpBackend)

	received := make(chan pahomqtt.Message, 1)
	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
		SetClientID(clientID).
		SetConnectTimeout(3 * time.Second).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) {
			received <- m
		})

	client := pahomqtt.NewClient(opts)
	connToken := client.Connect()
	require.True(t, connToken.WaitTimeout(3*time.Second))
	require.NoError(t, connToken.Error())
	defer client.Disconnect(250)

	// Deliberately not subscribed to "direct/topic" -- SendDirectMessage must
	// still reach this client (real AWS: "the receiving client does not need
	// to subscribe to the topic").
	rec := doRequestHeaders(t, h,
		"/connections/"+clientID+"/messages?topic=direct/topic&contentType=text%2Fplain",
		[]byte("hello-direct"), nil)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	select {
	case msg := <-received:
		assert.Equal(t, "direct/topic", msg.Topic())
		assert.Equal(t, []byte("hello-direct"), msg.Payload())
	case <-time.After(3 * time.Second):
		t.Fatal("client never received the directly-sent message")
	}
}
