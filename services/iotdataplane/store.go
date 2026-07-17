package iotdataplane

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements the IoT Data Plane backend.
//
// shadows and connections are "dirty" store.Table-backed collections (shadowEntry /
// connectionEntry carry no exported fields, so they can't round-trip a direct JSON
// marshal) and are NOT registered on registry -- persistence.go drives them through an
// ephemeral DTO registry instead. retainedMessages is a "clean" table (RetainedMessage
// already carries its own identity as a real field, Topic) and IS registered on registry,
// so persistence.go drives it through registry.SnapshotAll()/RestoreAll() directly. See
// store_setup.go for the full registration.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	broker           MQTTPublisher
	registry         *store.Registry
	shadows          *store.Table[shadowEntry]     // key: thingName#shadowName
	shadowsByThing   *store.Index[shadowEntry]     // secondary index keyed by thingName
	connections      *store.Table[connectionEntry] // key: clientID
	retainedMessages *store.Table[RetainedMessage] // key: topic; registered on registry
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("iotdataplane"),
		registry: store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state, including shadows, connections, and retained messages.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.shadows.Reset()
	b.connections.Reset()
}

// SetBroker wires the MQTT broker for publishing (called during CLI startup).
func (b *InMemoryBackend) SetBroker(broker MQTTPublisher) {
	b.mu.Lock("SetBroker")
	defer b.mu.Unlock()

	b.broker = broker
}
