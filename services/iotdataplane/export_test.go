package iotdataplane

import "encoding/json"

// MergeStateFields exposes mergeStateFields for white-box testing.
func MergeStateFields(base, patch map[string]json.RawMessage) map[string]json.RawMessage {
	return mergeStateFields(base, patch)
}

// ValidateTopic exposes validateTopic for white-box testing.
func ValidateTopic(topic string) error { return validateTopic(topic) }

// ValidateShadowName exposes validateShadowName for white-box testing.
func ValidateShadowName(name string) error { return validateShadowName(name) }

// ShadowCount returns the total number of shadow entries across all things (for white-box testing).
func ShadowCount(b *InMemoryBackend) int {
	b.mu.RLock("ShadowCount")
	defer b.mu.RUnlock()

	total := 0
	for _, thingShadows := range b.shadows {
		total += len(thingShadows)
	}

	return total
}

// ThingCount returns the number of distinct things with at least one shadow (for white-box testing).
func ThingCount(b *InMemoryBackend) int {
	b.mu.RLock("ThingCount")
	defer b.mu.RUnlock()

	return len(b.shadows)
}

// RetainedMessageCount returns the number of retained messages (for white-box testing).
func RetainedMessageCount(b *InMemoryBackend) int {
	b.mu.RLock("RetainedMessageCount")
	defer b.mu.RUnlock()

	return len(b.retainedMessages)
}

// ConnectionCount returns the number of tracked connections (for white-box testing).
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connections)
}

// MaxShadowsPerThing exposes the cap constant for white-box testing.
const MaxShadowsPerThing = maxShadowsPerThing

// MaxShadowDocumentBytes exposes the shadow document size cap for white-box testing.
const MaxShadowDocumentBytes = maxShadowDocumentBytes

// MaxShadowVersion exposes the version rollover cap for white-box testing.
const MaxShadowVersion = maxShadowVersion

// ForceSetShadowVersion directly sets the version on a shadow entry for testing version overflow.
func ForceSetShadowVersion(b *InMemoryBackend, thingName, shadowName string, version int) {
	b.mu.Lock("ForceSetShadowVersion")
	defer b.mu.Unlock()

	thingShadows := b.shadows[thingName]
	if thingShadows == nil {
		return
	}

	entry, ok := thingShadows[shadowName]
	if !ok {
		return
	}

	entry.version = version
}

// GetShadowDesired returns the desired state map for white-box testing.
func GetShadowDesired(b *InMemoryBackend, thingName, shadowName string) map[string]json.RawMessage {
	b.mu.RLock("GetShadowDesired")
	defer b.mu.RUnlock()

	thingShadows := b.shadows[thingName]
	if thingShadows == nil {
		return nil
	}

	entry, ok := thingShadows[shadowName]
	if !ok {
		return nil
	}

	return entry.desired
}

// GetShadowReported returns the reported state map for white-box testing.
func GetShadowReported(b *InMemoryBackend, thingName, shadowName string) map[string]json.RawMessage {
	b.mu.RLock("GetShadowReported")
	defer b.mu.RUnlock()

	thingShadows := b.shadows[thingName]
	if thingShadows == nil {
		return nil
	}

	entry, ok := thingShadows[shadowName]
	if !ok {
		return nil
	}

	return entry.reported
}
