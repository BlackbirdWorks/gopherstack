package iotdataplane

import (
	"errors"
	"sync"
)

// ErrNoBroker is returned when no MQTT broker has been wired.
var ErrNoBroker = errors.New("no mqtt broker configured")

// InMemoryBackend implements the IoT Data Plane backend.
type InMemoryBackend struct {
	broker MQTTPublisher
	mu     sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{}
}

// SetBroker wires the MQTT broker for publishing (called during CLI startup).
func (b *InMemoryBackend) SetBroker(broker MQTTPublisher) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.broker = broker
}

// Publish delivers a message to the given MQTT topic.
func (b *InMemoryBackend) Publish(topic string, payload []byte) error {
	b.mu.RLock()
	broker := b.broker
	b.mu.RUnlock()

	if broker == nil {
		return ErrNoBroker
	}

	return broker.Publish(topic, payload, false, 0)
}
