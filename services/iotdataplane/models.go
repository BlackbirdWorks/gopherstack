package iotdataplane

import (
	"encoding/json"
	"regexp"
	"time"
)

// maxRetainedMessages is the maximum number of retained messages stored in memory.
const maxRetainedMessages = 1000

// maxShadowDocumentBytes is the maximum allowed shadow document size in bytes.
const maxShadowDocumentBytes = 8 * 1024

// maxShadowStateDepth is AWS IoT's documented maximum JSON nesting depth for
// shadow state documents: "Maximum depth of JSON device state documents: 8
// levels" (in both the desired and reported sections), per the AWS General
// Reference "AWS IoT Core endpoints and quotas" page. The section's top-level
// object itself counts as depth 1.
const maxShadowStateDepth = 8

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

// compile-time interface check.
var _ StorageBackend = (*InMemoryBackend)(nil)

// shadowEntry holds shadow state, per-field metadata timestamps, version and update time.
// thingName and shadowName are unexported identity fields set once at creation (never
// mutated afterward) purely so store.Table's keyFn / secondary index can derive keys from
// the value -- see store_setup.go. shadowEntry carries no exported fields at all, so it is
// never JSON-marshaled directly; shadowEntrySnap (persistence.go) is its serialisable form.
type shadowEntry struct {
	updatedAt    time.Time
	desired      map[string]json.RawMessage // nil = not set
	reported     map[string]json.RawMessage // nil = not set
	metaDesired  map[string]int64           // field → epoch seconds of last update
	metaReported map[string]int64           // field → epoch seconds of last update
	thingName    string
	shadowName   string
	version      int
	// deleted marks a tombstoned shadow: state has been cleared by
	// DeleteThingShadow but the row is kept (instead of removed from the
	// table) so a later UpdateThingShadow that recreates this shadow
	// continues the version counter rather than resetting to 1. Real AWS
	// explicitly documents that deleting a shadow does not reset its version
	// number. Deleted entries are excluded from Get/List-style reads.
	deleted bool
}

// connectionEntry holds the state for a registered MQTT client connection.
// clientID is an unexported identity field set once at creation purely so store.Table's
// keyFn can derive a key from the value -- see store_setup.go. connectionEntry carries no
// exported fields, so it is never JSON-marshaled directly; connectionEntrySnap
// (persistence.go) is its serialisable form.
type connectionEntry struct {
	connectedAt time.Time
	clientID    string
	sourceIP    string
}
