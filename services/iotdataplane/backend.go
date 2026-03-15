package iotdataplane

import (
	"errors"
	"fmt"
	"sync"
)

// ErrNoBroker is returned when no MQTT broker has been wired.
var ErrNoBroker = errors.New("no mqtt broker configured")

// ErrShadowNotFound is returned when a thing shadow is not found.
var ErrShadowNotFound = errors.New("ResourceNotFoundException")

// InMemoryBackend implements the IoT Data Plane backend.
type InMemoryBackend struct {
	broker  MQTTPublisher
	shadows map[string]map[string][]byte // thingName -> shadowName -> document
	mu      sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		shadows: make(map[string]map[string][]byte),
	}
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

// GetThingShadow returns the document for the named shadow of a thing.
// An empty shadowName refers to the classic (unnamed) shadow.
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	doc, ok := thingShadows[shadowName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	cp := make([]byte, len(doc))
	copy(cp, doc)

	return cp, nil
}

// UpdateThingShadow stores or replaces the document for the named shadow of a thing.
func (b *InMemoryBackend) UpdateThingShadow(thingName, shadowName string, document []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.shadows[thingName]; !ok {
		b.shadows[thingName] = make(map[string][]byte)
	}

	cp := make([]byte, len(document))
	copy(cp, document)
	b.shadows[thingName][shadowName] = cp

	return nil
}

// DeleteThingShadow removes the document for the named shadow of a thing.
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	if _, hasShadow := thingShadows[shadowName]; !hasShadow {
		return fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	delete(thingShadows, shadowName)

	if len(thingShadows) == 0 {
		delete(b.shadows, thingName)
	}

	return nil
}

// ListNamedShadowsForThing returns the list of named shadow names for the given thing.
// The classic (unnamed) shadow is excluded from this list.
func (b *InMemoryBackend) ListNamedShadowsForThing(thingName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return []string{}, nil
	}

	names := make([]string, 0, len(thingShadows))
	for name := range thingShadows {
		if name != "" {
			names = append(names, name)
		}
	}

	return names, nil
}
