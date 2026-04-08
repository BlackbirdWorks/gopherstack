package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrNoBroker is returned when no MQTT broker has been wired.
var ErrNoBroker = errors.New("no mqtt broker configured")

// ErrShadowNotFound is returned when a thing shadow is not found.
var ErrShadowNotFound = errors.New("ResourceNotFoundException")

// ErrVersionConflict is returned when a shadow update specifies a version
// that does not match the current shadow version (optimistic locking violation).
var ErrVersionConflict = errors.New("VersionConflictException")

// shadowEntry holds a shadow document together with its version and timestamp.
type shadowEntry struct {
	updatedAt time.Time
	document  []byte
	version   int
}

// InMemoryBackend implements the IoT Data Plane backend.
type InMemoryBackend struct {
	broker           MQTTPublisher
	shadows          map[string]map[string]*shadowEntry // thingName -> shadowName -> entry
	connections      map[string]struct{}                // clientID -> struct{}
	retainedMessages map[string]*RetainedMessage        // topic -> message
	mu               sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		shadows:          make(map[string]map[string]*shadowEntry),
		connections:      make(map[string]struct{}),
		retainedMessages: make(map[string]*RetainedMessage),
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

// buildShadowResponse merges version and timestamp into the stored shadow document.
// If the document is a valid JSON object, the fields are injected directly;
// otherwise the raw bytes are encoded as a JSON string under a "payload" key,
// which always produces valid JSON regardless of the content of doc.
func buildShadowResponse(doc []byte, version int, updatedAt time.Time) ([]byte, error) {
	var m map[string]json.RawMessage

	if err := json.Unmarshal(doc, &m); err != nil {
		// doc is not a JSON object; encode it as a plain string so json.Marshal
		// never fails on raw bytes that are not valid JSON.
		return json.Marshal(map[string]any{
			"payload":   string(doc),
			"version":   version,
			"timestamp": updatedAt.Unix(),
		})
	}

	// json.Marshal on plain int/int64 values is infallible, so errors are ignored.
	verBytes, _ := json.Marshal(version)
	tsBytes, _ := json.Marshal(updatedAt.Unix())
	m["version"] = verBytes
	m["timestamp"] = tsBytes

	return json.Marshal(m)
}

// GetThingShadow returns the shadow document for the named shadow of a thing.
// The response includes "version" and "timestamp" metadata fields.
// An empty shadowName refers to the classic (unnamed) shadow.
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	entry, ok := thingShadows[shadowName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	return buildShadowResponse(entry.document, entry.version, entry.updatedAt)
}

// UpdateThingShadow stores or replaces the document for the named shadow of a thing.
// If the document contains a "version" field it must equal the current shadow
// version; a mismatch returns ErrVersionConflict (optimistic locking).
// The version is incremented on every successful update.
// The returned bytes represent the updated shadow response including the new version.
// The response is constructed before mutating state so a marshal error cannot
// leave a partial update behind.
func (b *InMemoryBackend) UpdateThingShadow(thingName, shadowName string, document []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.shadows[thingName]; !ok {
		b.shadows[thingName] = make(map[string]*shadowEntry)
	}

	current := b.shadows[thingName][shadowName]

	// Check optimistic locking version if the caller supplied one.
	// An unmarshal error here is intentional: if the document is not valid JSON
	// or contains no "version" field, vc.Version stays nil and the lock is skipped.
	var vc struct {
		Version *int `json:"version,omitempty"`
	}

	_ = json.Unmarshal(document, &vc)

	if vc.Version != nil {
		currentVersion := 0
		if current != nil {
			currentVersion = current.version
		}

		if *vc.Version != currentVersion {
			return nil, fmt.Errorf("%w: expected %d, got %d",
				ErrVersionConflict, currentVersion, *vc.Version)
		}
	}

	newVersion := 1
	if current != nil {
		newVersion = current.version + 1
	}

	now := time.Now()

	cp := make([]byte, len(document))
	copy(cp, document)

	// Build the response before writing state so a marshal error cannot leave
	// a partial update behind.
	resp, err := buildShadowResponse(cp, newVersion, now)
	if err != nil {
		return nil, err
	}

	b.shadows[thingName][shadowName] = &shadowEntry{
		document:  cp,
		version:   newVersion,
		updatedAt: now,
	}

	return resp, nil
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

// DeleteConnection removes an MQTT client connection from the backend.
// If the clientID does not exist the operation is a no-op (idempotent).
func (b *InMemoryBackend) DeleteConnection(clientID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.connections, clientID)

	return nil
}

// AddConnectionInternal seeds a connected client ID for testing purposes.
func (b *InMemoryBackend) AddConnectionInternal(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.connections[clientID] = struct{}{}
}

// StoreRetainedMessage saves a retained MQTT message for the given topic.
// Calling this with an empty payload removes the retained message for that topic.
func (b *InMemoryBackend) StoreRetainedMessage(topic string, payload []byte, qos int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(payload) == 0 {
		delete(b.retainedMessages, topic)

		return nil
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)

	b.retainedMessages[topic] = &RetainedMessage{
		Topic:            topic,
		Payload:          cp,
		Qos:              qos,
		LastModifiedTime: time.Now().UnixMilli(),
	}

	return nil
}

// GetRetainedMessage returns the retained message stored for the given topic.
// ErrShadowNotFound is returned when no retained message exists for the topic.
func (b *InMemoryBackend) GetRetainedMessage(topic string) (*RetainedMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	msg, ok := b.retainedMessages[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrShadowNotFound, topic)
	}

	cp := *msg
	if len(msg.Payload) > 0 {
		cp.Payload = make([]byte, len(msg.Payload))
		copy(cp.Payload, msg.Payload)
	}

	return &cp, nil
}

// ListRetainedMessages returns summaries of all retained messages, sorted by topic.
func (b *InMemoryBackend) ListRetainedMessages() ([]*RetainedMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.retainedMessages))
	for topic := range b.retainedMessages {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	result := make([]*RetainedMessage, 0, len(topics))

	for _, topic := range topics {
		msg := b.retainedMessages[topic]
		cp := *msg
		result = append(result, &cp)
	}

	return result, nil
}
