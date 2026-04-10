package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNoBroker is returned when no MQTT broker has been wired.
var ErrNoBroker = errors.New("no mqtt broker configured")

// ErrShadowNotFound is returned when a thing shadow is not found.
var ErrShadowNotFound = errors.New("shadow not found")

// ErrRetainedMessageNotFound is returned when no retained message exists for a topic.
var ErrRetainedMessageNotFound = errors.New("retained message not found")

// ErrVersionConflict is returned when a shadow update specifies a version
// that does not match the current shadow version (optimistic locking violation).
var ErrVersionConflict = errors.New("VersionConflictException")

// ErrValidation is returned for invalid input parameters.
var ErrValidation = errors.New("InvalidRequestException")

// maxRetainedMessages is the maximum number of retained messages stored in memory.
// This prevents unbounded memory growth when many topics are used.
const maxRetainedMessages = 1000

// compile-time interface check.
var _ StorageBackend = (*InMemoryBackend)(nil)

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

// Reset clears all backend state, including shadows, connections, and retained messages.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.shadows = make(map[string]map[string]*shadowEntry)
	b.connections = make(map[string]struct{})
	b.retainedMessages = make(map[string]*RetainedMessage)
}

// SetBroker wires the MQTT broker for publishing (called during CLI startup).
func (b *InMemoryBackend) SetBroker(broker MQTTPublisher) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.broker = broker
}

// Publish delivers a message to the given MQTT topic.
// If no broker is configured the call is a no-op (accepted but not forwarded).
func (b *InMemoryBackend) Publish(topic string, payload []byte, qos int32) error {
	b.mu.RLock()
	broker := b.broker
	b.mu.RUnlock()

	if broker == nil {
		return ErrNoBroker
	}

	// Clamp qos to valid MQTT range [0,1] before narrowing to byte.
	var qosByte byte
	if qos > 0 {
		qosByte = 1
	}

	return broker.Publish(topic, payload, false, qosByte)
}

// sortedKeys returns a sorted copy of the keys of a map[string]V.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
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

// DeleteThingShadow removes the document for the named shadow of a thing and
// returns the last known shadow state (compatible with the AWS DeleteThingShadow
// response which always returns a Payload).
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	entry, hasShadow := thingShadows[shadowName]
	if !hasShadow {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	// Build the final state response before deletion so we can return it.
	payload, err := buildShadowResponse(entry.document, entry.version, entry.updatedAt)
	if err != nil {
		payload = entry.document
	}

	delete(thingShadows, shadowName)

	if len(thingShadows) == 0 {
		delete(b.shadows, thingName)
	}

	return payload, nil
}

// ListNamedShadowsForThing returns the sorted list of named shadow names for the given thing.
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

	sort.Strings(names)

	return names, nil
}

// DeleteConnection removes an MQTT client connection from the backend.
// If the clientID does not exist the operation is a no-op (idempotent).
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) DeleteConnection(clientID string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

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

// AddShadowInternal seeds a shadow entry for testing purposes.
func (b *InMemoryBackend) AddShadowInternal(thingName, shadowName string, document []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.shadows[thingName]; !ok {
		b.shadows[thingName] = make(map[string]*shadowEntry)
	}

	cp := make([]byte, len(document))
	copy(cp, document)

	b.shadows[thingName][shadowName] = &shadowEntry{
		document:  cp,
		version:   1,
		updatedAt: time.Now(),
	}
}

// StoreRetainedMessage saves a retained MQTT message for the given topic.
// Calling this with an empty payload removes the retained message for that topic.
// Returns ErrValidation if the cap would be exceeded.
func (b *InMemoryBackend) StoreRetainedMessage(topic string, payload []byte, qos int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(payload) == 0 {
		delete(b.retainedMessages, topic)

		return nil
	}

	if _, exists := b.retainedMessages[topic]; !exists && len(b.retainedMessages) >= maxRetainedMessages {
		return fmt.Errorf("%w: retained message limit (%d) exceeded", ErrValidation, maxRetainedMessages)
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
// ErrRetainedMessageNotFound is returned when no retained message exists for the topic.
func (b *InMemoryBackend) GetRetainedMessage(topic string) (*RetainedMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	msg, ok := b.retainedMessages[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRetainedMessageNotFound, topic)
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

	topics := sortedKeys(b.retainedMessages)
	result := make([]*RetainedMessage, 0, len(topics))

	for _, topic := range topics {
		msg := b.retainedMessages[topic]
		cp := *msg
		if len(msg.Payload) > 0 {
			cp.Payload = make([]byte, len(msg.Payload))
			copy(cp.Payload, msg.Payload)
		}

		result = append(result, &cp)
	}

	return result, nil
}
