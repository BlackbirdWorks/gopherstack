package iotdataplane

// MQTTPublisher publishes a message to an MQTT topic.
type MQTTPublisher interface {
	Publish(topic string, payload []byte, retain bool, qos byte) error
}

// StorageBackend defines the interface for the IoT Data Plane backend.
type StorageBackend interface {
	Publish(topic string, payload []byte) error
	SetBroker(broker MQTTPublisher)
	GetThingShadow(thingName, shadowName string) ([]byte, error)
	UpdateThingShadow(thingName, shadowName string, document []byte) ([]byte, error)
	DeleteThingShadow(thingName, shadowName string) error
	ListNamedShadowsForThing(thingName string) ([]string, error)
	DeleteConnection(clientID string) error
	StoreRetainedMessage(topic string, payload []byte, qos int32) error
	GetRetainedMessage(topic string) (*RetainedMessage, error)
	ListRetainedMessages() ([]*RetainedMessage, error)
}
