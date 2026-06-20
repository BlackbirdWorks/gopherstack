package iotdataplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// ErrConnectionExists is returned when trying to register a clientID that is already connected.
var ErrConnectionExists = errors.New("connection already exists")

// maxRetainedMessages is the maximum number of retained messages stored in memory.
const maxRetainedMessages = 1000

// maxShadowsPerThing is the maximum number of shadows (classic + named) per thing.
const maxShadowsPerThing = 100

// maxShadowDocumentBytes is the maximum allowed shadow document size in bytes.
const maxShadowDocumentBytes = 8 * 1024

// maxTopicLength is the maximum allowed MQTT topic length per AWS IoT rules.
const maxTopicLength = 256

// maxShadowNameLength is the maximum allowed shadow name length per AWS IoT rules.
const maxShadowNameLength = 64

// maxShadowVersion is the maximum shadow version before it resets to 1.
const maxShadowVersion = 1<<31 - 1

// maxThingNameLength is the maximum allowed IoT thing name length per AWS rules.
const maxThingNameLength = 128

// maxClientTokenLength is the maximum allowed clientToken length per AWS rules.
const maxClientTokenLength = 64

// keyTimestamp is the JSON key for shadow response timestamp fields.
const keyTimestamp = "timestamp"

// shadowNameRe validates shadow names per AWS IoT rules: alphanumeric, colon, underscore, hyphen.
var shadowNameRe = regexp.MustCompile(`^[a-zA-Z0-9:_-]+$`)

// thingNameRe validates IoT thing names: alphanumeric, colon, underscore, hyphen, dot.
// Hyphen at end of character class avoids range interpretation.
var thingNameRe = regexp.MustCompile(`^[a-zA-Z0-9:_.-]+$`)

// validateThingName checks that a thing name meets AWS IoT naming rules.
func validateThingName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: thing name must not be empty", ErrValidation)
	}

	if len(name) > maxThingNameLength {
		return fmt.Errorf("%w: thing name exceeds %d characters", ErrValidation, maxThingNameLength)
	}

	if !thingNameRe.MatchString(name) {
		return fmt.Errorf("%w: thing name must match [a-zA-Z0-9:_.-]+", ErrValidation)
	}

	return nil
}

// validateClientToken checks that a clientToken meets AWS IoT rules.
// An empty token is always valid (token is optional).
// Maximum length is 64 characters per AWS documentation.
func validateClientToken(token string) error {
	if len(token) > maxClientTokenLength {
		return fmt.Errorf("%w: clientToken exceeds %d characters", ErrValidation, maxClientTokenLength)
	}

	return nil
}

// isShadowReservedName reports whether name is a reserved shadow operation keyword.
// These are forbidden by AWS IoT rules to prevent routing ambiguity.
func isShadowReservedName(name string) bool {
	switch name {
	case "update", "get", "delete", "accepted", "rejected", "delta", "documents":
		return true
	default:
		return false
	}
}

// compile-time interface check.
var _ StorageBackend = (*InMemoryBackend)(nil)

// shadowEntry holds shadow state, per-field metadata timestamps, version and update time.
type shadowEntry struct {
	desired      map[string]json.RawMessage // nil = not set
	reported     map[string]json.RawMessage // nil = not set
	metaDesired  map[string]int64           // field → epoch seconds of last update
	metaReported map[string]int64           // field → epoch seconds of last update
	updatedAt    time.Time
	version      int
}

// connectionEntry holds the state for a registered MQTT client connection.
type connectionEntry struct {
	connectedAt time.Time
	sourceIP    string
}

// InMemoryBackend implements the IoT Data Plane backend.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	broker           MQTTPublisher
	shadows          map[string]map[string]*shadowEntry // thingName -> shadowName -> entry
	connections      map[string]*connectionEntry        // clientID -> entry
	retainedMessages map[string]*RetainedMessage        // topic -> message
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("iotdataplane"),
		shadows:          make(map[string]map[string]*shadowEntry),
		connections:      make(map[string]*connectionEntry),
		retainedMessages: make(map[string]*RetainedMessage),
	}
}

// Reset clears all backend state, including shadows, connections, and retained messages.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.shadows = make(map[string]map[string]*shadowEntry)
	b.connections = make(map[string]*connectionEntry)
	b.retainedMessages = make(map[string]*RetainedMessage)
}

// SetBroker wires the MQTT broker for publishing (called during CLI startup).
func (b *InMemoryBackend) SetBroker(broker MQTTPublisher) {
	b.mu.Lock("SetBroker")
	defer b.mu.Unlock()

	b.broker = broker
}

// validateTopic checks that a topic string conforms to MQTT publishing rules.
// Wildcards (# or +) are forbidden, empty levels are rejected, and each segment
// is validated for control characters. The reserved $aws/things/{name}/shadow/*
// prefix is blocked for external publishers to prevent spoofing shadow events.
func validateTopic(topic string) error {
	if len(topic) > maxTopicLength {
		return fmt.Errorf("%w: topic exceeds %d characters", ErrValidation, maxTopicLength)
	}

	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("%w: topic must not contain wildcards (# or +)", ErrValidation)
	}

	segments := strings.Split(topic, "/")

	// Reject empty topic levels (consecutive slashes or leading/trailing slash).
	if slices.Contains(segments, "") {
		return fmt.Errorf("%w: topic must not contain empty levels", ErrValidation)
	}

	// Reject control characters (0x00–0x1F, 0x7F) in any segment.
	for _, seg := range segments {
		for _, ch := range seg {
			if ch < 0x20 || ch == 0x7F {
				return fmt.Errorf("%w: topic segment contains control character", ErrValidation)
			}
		}
	}

	// Gate $aws/things/{name}/shadow/* to internal callers; external publish would
	// spoof the shadow event topics reserved for the backend.
	if strings.HasPrefix(topic, "$aws/things/") && len(segments) >= 4 && segments[3] == "shadow" {
		return fmt.Errorf("%w: publishing to $aws/things/{name}/shadow/* is reserved for internal use", ErrValidation)
	}

	return nil
}

// validateShadowName checks that a shadow name meets AWS IoT naming rules.
// An empty name refers to the classic (unnamed) shadow and is always valid.
func validateShadowName(name string) error {
	if name == "" {
		return nil // classic shadow
	}

	if len(name) > maxShadowNameLength {
		return fmt.Errorf("%w: shadow name exceeds %d characters", ErrValidation, maxShadowNameLength)
	}

	if !shadowNameRe.MatchString(name) {
		return fmt.Errorf("%w: shadow name must match [a-zA-Z0-9:_-]+", ErrValidation)
	}

	// Reject reserved operation keywords per AWS IoT rules.
	if isShadowReservedName(name) {
		return fmt.Errorf("%w: shadow name %q is reserved and may not be used", ErrValidation, name)
	}

	return nil
}

// validateShadowDocument checks that doc is a non-empty JSON object.
func validateShadowDocument(doc []byte) error {
	if len(doc) == 0 {
		return fmt.Errorf("%w: shadow document must not be empty", ErrValidation)
	}

	if len(doc) > maxShadowDocumentBytes {
		return fmt.Errorf("%w: shadow document exceeds %d bytes", ErrValidation, maxShadowDocumentBytes)
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(doc, &obj); err != nil || obj == nil {
		return fmt.Errorf("%w: shadow document must be a valid JSON object", ErrValidation)
	}

	return nil
}

// isJSONNull reports whether v is the JSON null literal.
func isJSONNull(v json.RawMessage) bool {
	var x any

	return json.Unmarshal(v, &x) == nil && x == nil
}

// mergeStateFields merges patch into base: null-valued keys are deleted.
// The result is a new map; base and patch are not modified.
func mergeStateFields(base, patch map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(base))
	maps.Copy(result, base)

	for k, v := range patch {
		if isJSONNull(v) {
			delete(result, k)
		} else {
			result[k] = v
		}
	}

	return result
}

// updateMetaFields returns a copy of meta with timestamps updated for keys present in patch.
// Null-deleted keys are removed from the returned meta map.
func updateMetaFields(meta map[string]int64, patch map[string]json.RawMessage, ts int64) map[string]int64 {
	result := make(map[string]int64, len(meta)+len(patch))
	maps.Copy(result, meta)

	for k, v := range patch {
		if isJSONNull(v) {
			delete(result, k)
		} else {
			result[k] = ts
		}
	}

	return result
}

// computeDelta returns a map of fields where desired differs from reported (or is absent in reported).
// Returns nil when there is no delta.
func computeDelta(desired, reported map[string]json.RawMessage) map[string]json.RawMessage {
	if len(desired) == 0 {
		return nil
	}

	delta := make(map[string]json.RawMessage)

	for k, dv := range desired {
		rv, ok := reported[k]
		if !ok || string(rv) != string(dv) {
			delta[k] = dv
		}
	}

	if len(delta) == 0 {
		return nil
	}

	return delta
}

// buildMetaTimestamps converts a flat field→epoch map to the AWS metadata format:
// {"fieldName": {"timestamp": epochSeconds}}.
func buildMetaTimestamps(meta map[string]int64) map[string]map[string]int64 {
	if len(meta) == 0 {
		return nil
	}

	result := make(map[string]map[string]int64, len(meta))
	for k, ts := range meta {
		result[k] = map[string]int64{keyTimestamp: ts}
	}

	return result
}

// buildShadowResponse assembles the full AWS shadow response JSON from an entry.
// clientToken is echoed when non-empty (comes from the UpdateThingShadow request).
// AWS omits empty desired/reported sections from the state object.
func buildShadowResponse(entry *shadowEntry, clientToken string) ([]byte, error) {
	state := map[string]any{}

	if len(entry.desired) > 0 {
		state["desired"] = entry.desired
	}

	if len(entry.reported) > 0 {
		state["reported"] = entry.reported
	}

	delta := computeDelta(entry.desired, entry.reported)
	if delta != nil {
		state["delta"] = delta
	}

	resp := map[string]any{
		"state":      state,
		"version":    entry.version,
		keyTimestamp: entry.updatedAt.Unix(),
	}

	// Add per-field metadata when available.
	metaDesired := buildMetaTimestamps(entry.metaDesired)
	metaReported := buildMetaTimestamps(entry.metaReported)

	if metaDesired != nil || metaReported != nil {
		meta := map[string]any{}
		if metaDesired != nil {
			meta["desired"] = metaDesired
		}

		if metaReported != nil {
			meta["reported"] = metaReported
		}

		resp["metadata"] = meta
	}

	if clientToken != "" {
		resp["clientToken"] = clientToken
	}

	return json.Marshal(resp)
}

// Publish delivers a message to the given MQTT topic.
// If no broker is configured the call returns ErrNoBroker.
// The retain flag is forwarded to the broker so live subscribers receive RETAIN=1
// and the broker maintains retention canonically.
func (b *InMemoryBackend) Publish(topic string, payload []byte, qos int32, retain bool) error {
	b.mu.RLock("Publish")
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

	return broker.Publish(topic, payload, retain, qosByte)
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

// GetThingShadow returns the shadow document for the named shadow of a thing.
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) ([]byte, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.RLock("GetThingShadow")
	defer b.mu.RUnlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	entry, ok := thingShadows[shadowName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	return buildShadowResponse(entry, "")
}

// shadowUpdateInput holds the parsed fields from an UpdateThingShadow request body.
type shadowUpdateInput struct {
	Version       *int
	ClientToken   string
	StateDesired  json.RawMessage
	StateReported json.RawMessage
}

// parseShadowUpdateDoc validates and parses an UpdateThingShadow request body.
// It enforces the "state" key requirement, null/type checks, and clientToken length.
func parseShadowUpdateDoc(document []byte) (*shadowUpdateInput, error) {
	// Outer document uses RawMessage for State so we can detect absent vs null.
	var outer struct {
		Version     *int            `json:"version,omitempty"`
		ClientToken string          `json:"clientToken,omitempty"`
		State       json.RawMessage `json:"state"`
	}

	if err := json.Unmarshal(document, &outer); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON document", ErrValidation)
	}

	if len(outer.State) == 0 {
		return nil, fmt.Errorf("%w: missing required field: state", ErrValidation)
	}

	if isJSONNull(outer.State) {
		return nil, fmt.Errorf("%w: state must be a JSON object, not null", ErrValidation)
	}

	if err := validateClientToken(outer.ClientToken); err != nil {
		return nil, err
	}

	var stateDoc struct {
		Desired  json.RawMessage `json:"desired"`
		Reported json.RawMessage `json:"reported"`
	}

	if err := json.Unmarshal(outer.State, &stateDoc); err != nil {
		return nil, fmt.Errorf("%w: state must be a valid JSON object", ErrValidation)
	}

	return &shadowUpdateInput{
		StateDesired:  stateDoc.Desired,
		StateReported: stateDoc.Reported,
		ClientToken:   outer.ClientToken,
		Version:       outer.Version,
	}, nil
}

// applyShadowStateSection merges a raw state section into existing state.
// raw absent (nil) → keep existing; raw null → clear; raw object → merge patch.
func applyShadowStateSection(
	existing map[string]json.RawMessage,
	existingMeta map[string]int64,
	raw json.RawMessage,
	sectionName string,
	ts int64,
) (map[string]json.RawMessage, map[string]int64, error) {
	if len(raw) == 0 {
		return existing, existingMeta, nil
	}

	if isJSONNull(raw) {
		return nil, nil, nil
	}

	var patch map[string]json.RawMessage
	if err := json.Unmarshal(raw, &patch); err != nil {
		return nil, nil, fmt.Errorf("%w: state.%s must be a JSON object", ErrValidation, sectionName)
	}

	return mergeStateFields(existing, patch), updateMetaFields(existingMeta, patch, ts), nil
}

// UpdateThingShadow merges the desired/reported state from document into the stored shadow.
// AWS merge semantics: null values on individual keys delete them; a null section wipes the
// entire section; missing sections are left unchanged. The state key is required.
// The version is incremented on every successful update.
// Returns the updated shadow response including delta, metadata, and echoed clientToken.
func (b *InMemoryBackend) UpdateThingShadow(thingName, shadowName string, document []byte) ([]byte, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	if err := validateShadowDocument(document); err != nil {
		return nil, err
	}

	input, err := parseShadowUpdateDoc(document)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateThingShadow")
	defer b.mu.Unlock()

	if _, ok := b.shadows[thingName]; !ok {
		b.shadows[thingName] = make(map[string]*shadowEntry)
	}

	current := b.shadows[thingName][shadowName]

	if current == nil && len(b.shadows[thingName]) >= maxShadowsPerThing {
		return nil, fmt.Errorf("%w: shadow limit (%d) per thing exceeded for %s",
			ErrValidation, maxShadowsPerThing, thingName)
	}

	if conflictErr := checkVersionConflict(input.Version, current); conflictErr != nil {
		return nil, conflictErr
	}

	newVersion := nextShadowVersion(current)
	now := time.Now()
	ts := now.Unix()

	var existingDesired, existingReported map[string]json.RawMessage
	var existingMetaDesired, existingMetaReported map[string]int64

	if current != nil {
		existingDesired = current.desired
		existingReported = current.reported
		existingMetaDesired = current.metaDesired
		existingMetaReported = current.metaReported
	}

	newDesired, newMetaDesired, err := applyShadowStateSection(
		existingDesired, existingMetaDesired, input.StateDesired, "desired", ts)
	if err != nil {
		return nil, err
	}

	newReported, newMetaReported, err := applyShadowStateSection(
		existingReported, existingMetaReported, input.StateReported, "reported", ts)
	if err != nil {
		return nil, err
	}

	newEntry := &shadowEntry{
		version:      newVersion,
		updatedAt:    now,
		desired:      newDesired,
		reported:     newReported,
		metaDesired:  newMetaDesired,
		metaReported: newMetaReported,
	}

	resp, err := buildShadowResponse(newEntry, input.ClientToken)
	if err != nil {
		return nil, err
	}

	b.shadows[thingName][shadowName] = newEntry

	return resp, nil
}

// checkVersionConflict returns ErrVersionConflict if the request version doesn't match current.
func checkVersionConflict(requestVersion *int, current *shadowEntry) error {
	if requestVersion == nil {
		return nil
	}

	currentVersion := 0
	if current != nil {
		currentVersion = current.version
	}

	if *requestVersion != currentVersion {
		return fmt.Errorf("%w: expected %d, got %d", ErrVersionConflict, currentVersion, *requestVersion)
	}

	return nil
}

// nextShadowVersion returns version+1 for the current entry, or 1 if nil or at rollover cap.
func nextShadowVersion(current *shadowEntry) int {
	if current == nil || current.version >= maxShadowVersion {
		return 1
	}

	return current.version + 1
}

// DeleteThingShadow removes the document for the named shadow of a thing and
// returns the last known shadow state (AWS DeleteThingShadow response contract).
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) ([]byte, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteThingShadow")
	defer b.mu.Unlock()

	thingShadows, ok := b.shadows[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	entry, hasShadow := thingShadows[shadowName]
	if !hasShadow {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	payload, err := buildShadowResponse(entry, "")
	if err != nil {
		// Fallback: return a minimal valid response.
		payload, _ = json.Marshal(map[string]any{
			"version":    entry.version,
			keyTimestamp: entry.updatedAt.Unix(),
		})
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
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.RLock("ListNamedShadowsForThing")
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

// ListThingsWithShadows returns the sorted list of thing names that have at least one shadow.
func (b *InMemoryBackend) ListThingsWithShadows() []string {
	b.mu.RLock("ListThingsWithShadows")
	defer b.mu.RUnlock()

	return sortedKeys(b.shadows)
}

// RegisterConnection adds a client connection to the backend.
// Returns ErrConnectionExists if the clientID is already registered.
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) RegisterConnection(clientID, sourceIP string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	if clientID == "" {
		return fmt.Errorf("%w: clientId is required", ErrValidation)
	}

	b.mu.Lock("RegisterConnection")
	defer b.mu.Unlock()

	if _, exists := b.connections[clientID]; exists {
		return fmt.Errorf("%w: %s", ErrConnectionExists, clientID)
	}

	b.connections[clientID] = &connectionEntry{
		connectedAt: time.Now(),
		sourceIP:    sourceIP,
	}

	return nil
}

// DeleteConnection removes an MQTT client connection from the backend.
// If the clientID does not exist the operation is a no-op (idempotent).
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) DeleteConnection(clientID string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	delete(b.connections, clientID)

	return nil
}

// ListConnections returns all registered connections sorted by ConnectedAt ascending.
func (b *InMemoryBackend) ListConnections() []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	out := make([]*Connection, 0, len(b.connections))
	for clientID, entry := range b.connections {
		out = append(out, &Connection{
			ClientID:    clientID,
			SourceIP:    entry.sourceIP,
			ConnectedAt: entry.connectedAt,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ConnectedAt.Before(out[j].ConnectedAt)
	})

	return out
}

// AddConnectionInternal seeds a connected client ID for testing purposes.
func (b *InMemoryBackend) AddConnectionInternal(clientID string) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections[clientID] = &connectionEntry{connectedAt: time.Now()}
}

// AddShadowInternal seeds a shadow entry for testing purposes.
// The document is parsed to extract desired/reported state; if parsing fails or
// no state key is present, the whole document is treated as the desired state.
func (b *InMemoryBackend) AddShadowInternal(thingName, shadowName string, document []byte) {
	b.mu.Lock("AddShadowInternal")
	defer b.mu.Unlock()

	if _, ok := b.shadows[thingName]; !ok {
		b.shadows[thingName] = make(map[string]*shadowEntry)
	}

	entry := &shadowEntry{
		version:   1,
		updatedAt: time.Now(),
	}

	// Best-effort parse of state.desired / state.reported from the seeded document.
	var doc struct {
		State struct {
			Desired  map[string]json.RawMessage `json:"desired"`
			Reported map[string]json.RawMessage `json:"reported"`
		} `json:"state"`
	}

	if json.Unmarshal(document, &doc) == nil {
		if doc.State.Desired != nil {
			entry.desired = doc.State.Desired
		}
		if doc.State.Reported != nil {
			entry.reported = doc.State.Reported
		}
	}

	b.shadows[thingName][shadowName] = entry
}

// StoreRetainedMessage saves a retained MQTT message for the given topic.
// Calling this with an empty payload removes the retained message for that topic.
// When the cap is reached, the oldest entry (by LastModifiedTime) is evicted to
// make room, matching AWS LRU behaviour and preventing silent publish failures.
func (b *InMemoryBackend) StoreRetainedMessage(topic string, payload []byte, qos int32) error {
	if len(payload) > 0 && len(payload) > maxPublishBodyBytes {
		return fmt.Errorf("%w: retained payload exceeds %d bytes", ErrValidation, maxPublishBodyBytes)
	}

	b.mu.Lock("StoreRetainedMessage")
	defer b.mu.Unlock()

	if len(payload) == 0 {
		delete(b.retainedMessages, topic)

		return nil
	}

	// LRU eviction: when the cap is reached and the topic is new, evict the oldest entry.
	if _, exists := b.retainedMessages[topic]; !exists && len(b.retainedMessages) >= maxRetainedMessages {
		b.evictOldestRetained()
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

// evictOldestRetained removes the retained message with the oldest LastModifiedTime.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) evictOldestRetained() {
	var oldestTopic string

	var oldestTime int64 = -1

	for topic, msg := range b.retainedMessages {
		if oldestTime < 0 || msg.LastModifiedTime < oldestTime {
			oldestTime = msg.LastModifiedTime
			oldestTopic = topic
		}
	}

	if oldestTopic != "" {
		delete(b.retainedMessages, oldestTopic)
	}
}

// GetRetainedMessage returns the retained message stored for the given topic.
// ErrRetainedMessageNotFound is returned when no retained message exists for the topic.
func (b *InMemoryBackend) GetRetainedMessage(topic string) (*RetainedMessage, error) {
	b.mu.RLock("GetRetainedMessage")
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
	b.mu.RLock("ListRetainedMessages")
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
