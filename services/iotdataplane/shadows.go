package iotdataplane

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

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
		return fmt.Errorf("%w: shadow document exceeds %d bytes", ErrRequestTooLarge, maxShadowDocumentBytes)
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

// sortedKeys returns a sorted copy of the keys of a map[string]V.
func sortedKeys[V any](m map[string]V) []string {
	keys := collections.SortedKeys(m)

	return keys
}

// GetThingShadow returns the shadow document for the named shadow of a thing.
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) ([]byte, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.RLock("GetThingShadow")
	defer b.mu.RUnlock()

	entry, ok := b.shadows.Get(shadowKey(thingName, shadowName))
	if !ok || entry.deleted {
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

	current, _ := b.shadows.Get(shadowKey(thingName, shadowName))

	// A tombstoned (deleted) entry counts the same as "no shadow yet" for the
	// purposes of the per-thing shadow limit -- it doesn't represent a live
	// shadow, so recreating it must not be blocked by stale tombstone rows.
	if (current == nil || current.deleted) && liveShadowCount(b.shadowsByThing.Get(thingName)) >= maxShadowsPerThing {
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
		thingName:    thingName,
		shadowName:   shadowName,
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

	b.shadows.Put(newEntry)

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

// DeleteThingShadow removes the document for the named shadow of a thing.
// Per AWS docs, the response is an "empty response state document" -- only
// version and timestamp, no state/metadata/clientToken (see
// device-shadow-document.html #device-shadow-example-response-json).
//
// The shadow row is tombstoned (kept with state cleared) rather than
// physically removed: AWS explicitly documents that deleting a shadow does
// not reset its version number, so a later UpdateThingShadow that recreates
// this shadow must continue incrementing from the pre-delete version.
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) ([]byte, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteThingShadow")
	defer b.mu.Unlock()

	key := shadowKey(thingName, shadowName)

	entry, ok := b.shadows.Get(key)
	if !ok || entry.deleted {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	payload, err := json.Marshal(map[string]any{
		"version":    entry.version,
		keyTimestamp: entry.updatedAt.Unix(),
	})
	if err != nil {
		return nil, fmt.Errorf("iotdataplane: marshal delete response: %w", err)
	}

	b.shadows.Put(&shadowEntry{
		thingName:  thingName,
		shadowName: shadowName,
		version:    entry.version,
		updatedAt:  time.Now(),
		deleted:    true,
	})

	return payload, nil
}

// liveShadowCount returns the number of non-tombstoned entries in group.
func liveShadowCount(group []*shadowEntry) int {
	n := 0

	for _, e := range group {
		if !e.deleted {
			n++
		}
	}

	return n
}

// ListNamedShadowsForThing returns the sorted list of named shadow names for the given thing.
// The classic (unnamed) shadow is excluded from this list.
func (b *InMemoryBackend) ListNamedShadowsForThing(thingName string) ([]string, error) {
	if err := validateThingName(thingName); err != nil {
		return nil, err
	}

	b.mu.RLock("ListNamedShadowsForThing")
	defer b.mu.RUnlock()

	group := b.shadowsByThing.Get(thingName)

	names := make([]string, 0, len(group))
	for _, entry := range group {
		if entry.shadowName != "" && !entry.deleted {
			names = append(names, entry.shadowName)
		}
	}

	sort.Strings(names)

	return names, nil
}

// ListThingsWithShadows returns the sorted list of thing names that have at least one shadow.
func (b *InMemoryBackend) ListThingsWithShadows() []string {
	b.mu.RLock("ListThingsWithShadows")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, entry := range b.shadows.All() {
		if entry.deleted {
			continue
		}

		seen[entry.thingName] = struct{}{}
	}

	return sortedKeys(seen)
}

// AddShadowInternal seeds a shadow entry for testing purposes.
// The document is parsed to extract desired/reported state; if parsing fails or
// no state key is present, the whole document is treated as the desired state.
func (b *InMemoryBackend) AddShadowInternal(thingName, shadowName string, document []byte) {
	b.mu.Lock("AddShadowInternal")
	defer b.mu.Unlock()

	entry := &shadowEntry{
		thingName:  thingName,
		shadowName: shadowName,
		version:    1,
		updatedAt:  time.Now(),
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

	b.shadows.Put(entry)
}
