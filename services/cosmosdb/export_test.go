package cosmosdb

import (
	"net/http"
	"time"
)

// Exported wrappers/seams for internal state used in blackbox tests.

// ParseResourcePath exposes parseResourcePath for external tests.
func ParseResourcePath(path string) (int, string, string, string) {
	kind, dbID, collID, docID := parseResourcePath(path)

	return int(kind), dbID, collID, docID
}

// ResourceTypeAndIDFor exposes resourceTypeAndIDFor for external tests.
func ResourceTypeAndIDFor(method, path string) (string, string) {
	return resourceTypeAndIDFor(method, path)
}

// IsQueryRequest exposes isQueryRequest for external tests.
func IsQueryRequest(r *http.Request) bool {
	return isQueryRequest(r)
}

// CanonicalPartitionKeyJSON exposes canonicalPartitionKeyJSON for external tests.
func CanonicalPartitionKeyJSON(v any) (string, error) {
	return canonicalPartitionKeyJSON(v)
}

// ExtractPartitionKeyValue exposes extractPartitionKeyValue for external tests.
func ExtractPartitionKeyValue(body map[string]any, path string) (any, bool) {
	return extractPartitionKeyValue(body, path)
}

// PartitionKeyFromHeader exposes partitionKeyFromHeader for external tests.
func PartitionKeyFromHeader(r *http.Request) (string, error) {
	return partitionKeyFromHeader(r)
}

// SetNowFunc replaces the backend's time provider with fn for deterministic
// testing of Timestamp/ETag logic without real sleeps.
func SetNowFunc(b *InMemoryBackend, fn func() time.Time) {
	b.nowFunc = fn
}

// SetETagFunc replaces the backend's ETag derivation function with fn for
// deterministic ETag assertions.
func SetETagFunc(b *InMemoryBackend, fn func(time.Time) string) {
	b.etagFunc = fn
}

// EtagFor exposes etagFor for external tests.
func EtagFor(t time.Time) string {
	return etagFor(t)
}

// FakeRID exposes fakeRID for external tests.
func FakeRID(name string) string {
	return fakeRID(name)
}

// DocumentAsMap exposes documentAsMap for external tests.
func DocumentAsMap(info DocumentInfo) map[string]any {
	return documentAsMap(info)
}

// Tri-state constants and combinators, exposed for external table tests of
// SQL 3VL (see sql_exec.go's top doc comment).
const (
	TriUndefined = sqlUndefined
	TriFalse     = sqlFalse
	TriTrue      = sqlTrue
)

// TriState is the exported name for sqlTriState, for external test table
// field types.
type TriState = sqlTriState

// TriAnd, TriOr, and TriNot expose triAnd/triOr/triNot for external tests.
func TriAnd(a, b TriState) TriState { return triAnd(a, b) }
func TriOr(a, b TriState) TriState  { return triOr(a, b) }
func TriNot(a TriState) TriState    { return triNot(a) }
