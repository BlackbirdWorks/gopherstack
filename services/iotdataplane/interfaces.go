package iotdataplane

import (
	"context"
)

// MQTTPublisher publishes a message to an MQTT topic.
type MQTTPublisher interface {
	Publish(topic string, payload []byte, retain bool, qos byte) error
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
	ListSubscriptions(clientID string) error
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
