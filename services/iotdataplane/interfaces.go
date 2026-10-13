package iotdataplane

import (
	"context"
	"time"
)

// MQTT5UserProperty is one key/value pair decoded from PublishInput's or
// SendDirectMessageInput's userProperties JSON array, where each array
// element is a single-key JSON object -- e.g. [{"deviceName": "alpha"}]
// (see PublishInput.UserProperties doc comment,
// aws-sdk-go-v2/service/iotdataplane@v1.35.4/api_op_Publish.go).
type MQTT5UserProperty struct {
	Key   string
	Value string
}

// MQTT5Properties carries the optional MQTT5 packet properties that AWS's
// Publish and SendDirectMessage operations accept (contentType,
// correlationData, responseTopic, payloadFormatIndicator, userProperties --
// plus messageExpiry, Publish-only) so an MQTTPublisher implementation can
// forward them to live subscribers as real MQTT5 packet properties. A zero
// value means the corresponding request field was not supplied.
type MQTT5Properties struct {
	ContentType            string
	ResponseTopic          string
	PayloadFormatIndicator string
	CorrelationData        []byte
	UserProperties         []MQTT5UserProperty
	MessageExpiry          int64
}

// SessionInfo describes a live broker session as GetConnection reports it.
// ExpiryKnown is false when the client stated no expiry (MQTT 3.1.1 never does).
type SessionInfo struct {
	RemoteAddr    string
	LocalAddr     string
	SessionExpiry uint32
	KeepAlive     uint16
	Clean         bool
	ExpiryKnown   bool
}

// MQTTPublisher publishes messages to the MQTT broker and can inspect or
// target the broker's individually connected clients.
type MQTTPublisher interface {
	// Publish delivers a message to an MQTT topic; any connected subscriber
	// of that topic observes it.
	Publish(topic string, payload []byte, retain bool, qos byte) error

	// PublishWithProperties behaves like Publish but also attaches props as
	// real MQTT5 packet properties. Only a subscriber connected with MQTT
	// protocol version 5 observes them -- MQTT 3.1.1 has no wire
	// representation for packet properties, which AWS documents explicitly
	// for userProperties ("For MQTT 3.1.1 clients, user properties are
	// silently dropped", SendDirectMessageInput.UserProperties doc) and
	// which applies to every MQTT5 property by protocol design.
	PublishWithProperties(
		topic string,
		payload []byte,
		retain bool,
		qos byte,
		props MQTT5Properties,
	) error

	// ClientSubscriptions returns the topic-filter -> QoS map of every
	// subscription clientID currently holds on the broker, and whether the
	// broker currently has a live client connected with that ID. A connected
	// client with zero subscriptions returns a non-nil empty map and true; a
	// client the broker doesn't currently know about -- never connected, or
	// (in gopherstack) only ever registered through iotdataplane's
	// admin-only RegisterConnection extension without a real broker session
	// -- returns (nil, false).
	ClientSubscriptions(clientID string) (subs map[string]byte, connected bool)

	// SendToClient delivers payload on topic directly to clientID's live
	// broker connection, bypassing topic subscription matching entirely --
	// mirroring real AWS SendDirectMessage's documented "the receiving
	// client does not need to subscribe to the topic" semantics. ok is false
	// when the broker has no live client with that ID: nothing was sent and
	// err is nil in that case, so callers can distinguish "no live
	// per-client route" from a genuine delivery failure.
	SendToClient(clientID, topic string, payload []byte, qos byte) (ok bool, err error)

	// ClientSession reports the live (not closed) session for clientID, or
	// ok=false when the broker has none.
	ClientSession(clientID string) (info SessionInfo, ok bool)

	// DisconnectClient closes clientID's live connection, optionally dropping session state and
	// its Last Will; ok is false, with nil err, when no such live client exists.
	DisconnectClient(clientID string, cleanSession, preventWill bool) (ok bool, err error)

	// SendToClientWithProperties behaves like SendToClient but also attaches
	// props as real MQTT5 packet properties (see PublishWithProperties).
	SendToClientWithProperties(
		clientID, topic string, payload []byte, qos byte, props MQTT5Properties,
	) (ok bool, err error)
}

// AckingPublisher is an optional MQTTPublisher extension that waits for the client's PUBACK.
type AckingPublisher interface {
	// SendToClientAwaitAck delivers at QoS 1 and blocks until the PUBACK arrives or timeout elapses;
	// delivered is false, with nil err, when no live client exists, and a missing PUBACK is ErrDeliveryTimeout.
	SendToClientAwaitAck(
		clientID, topic string, payload []byte, props MQTT5Properties, timeout time.Duration,
	) (delivered bool, err error)
}

// ConnectionObserver receives broker-originated client lifecycle events.
type ConnectionObserver interface {
	// ClientConnected reports a newly established MQTT session; remoteAddr is the socket's "host:port".
	ClientConnected(clientID, remoteAddr string)
	// ClientDisconnected reports a closed session with its life-cycle-event disconnect reason.
	ClientDisconnected(clientID, reason string)
}

// ConnectionNotifier is an optional MQTTPublisher extension that reports broker-originated lifecycle events.
type ConnectionNotifier interface {
	SetConnectionObserver(observer ConnectionObserver)
}

// StorageBackend defines the interface for the IoT Data Plane backend.
type StorageBackend interface {
	Publish(topic string, payload []byte, qos int32, retain bool, props MQTT5Properties) error
	SetBroker(broker MQTTPublisher)
	GetThingShadow(thingName, shadowName string) ([]byte, error)
	UpdateThingShadow(thingName, shadowName string, document []byte) ([]byte, error)
	DeleteThingShadow(thingName, shadowName string) ([]byte, error)
	ListNamedShadowsForThing(thingName string) ([]string, error)
	ListThingsWithShadows() []string
	RegisterConnection(clientID, sourceIP string) error
	DeleteConnection(clientID string) error
	DeleteConnectionWithOptions(clientID string, cleanSession, preventWill bool) error
	ListConnections() []*Connection
	GetConnection(clientID string) (*Connection, error)
	ListSubscriptions(clientID string) ([]SubscriptionSummary, error)
	SendDirectMessage(
		clientID, topic string,
		payload []byte,
		qos int32,
		props MQTT5Properties,
	) error
	SendDirectMessageAwaitAck(
		clientID, topic string,
		payload []byte,
		props MQTT5Properties,
		timeout time.Duration,
	) error
	StoreRetainedMessage(topic string, payload []byte, qos int32, userProperties []byte) error
	GetRetainedMessage(topic string) (*RetainedMessage, error)
	ListRetainedMessages() ([]*RetainedMessage, error)
	Reset()
}

// Snapshottable is an optional interface a StorageBackend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// Resettable is an optional interface a StorageBackend may implement to
// support full state reset.
type Resettable interface {
	Reset()
}
