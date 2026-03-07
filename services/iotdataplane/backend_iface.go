package iotdataplane

// MQTTPublisher publishes a message to an MQTT topic.
type MQTTPublisher interface {
	Publish(topic string, payload []byte, retain bool, qos byte) error
}

// StorageBackend defines the interface for the IoT Data Plane backend.
type StorageBackend interface {
	Publish(topic string, payload []byte) error
	SetBroker(broker MQTTPublisher)
}
