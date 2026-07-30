package iotdataplane

import (
	"context"
)

// MQTTPublisher publishes messages to the MQTT broker and can inspect or
// target the broker's individually connected clients.
type MQTTPublisher interface {
	// Publish delivers a message to an MQTT topic; any connected subscriber
	// of that topic observes it.
	Publish(topic string, payload []byte, retain bool, qos byte) error

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
}

// StorageBackend defines the interface for the IoT Data Plane backend.
type StorageBackend interface {
	Publish(topic string, payload []byte, qos int32, retain bool) error
	SetBroker(broker MQTTPublisher)
	GetThingShadow(thingName, shadowName string) ([]byte, error)
	UpdateThingShadow(thingName, shadowName string, document []byte) ([]byte, error)
	DeleteThingShadow(thingName, shadowName string) ([]byte, error)
	ListNamedShadowsForThing(thingName string) ([]string, error)
	ListThingsWithShadows() []string
	RegisterConnection(clientID, sourceIP string) error
	DeleteConnection(clientID string) error
	ListConnections() []*Connection
	GetConnection(clientID string) (*Connection, error)
	ListSubscriptions(clientID string) ([]SubscriptionSummary, error)
	SendDirectMessage(clientID, topic string, payload []byte, qos int32) error
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
