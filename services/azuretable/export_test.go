package azuretable

import "time"

// Exported wrappers/seams for internal state used in blackbox tests.

// SplitPath exposes splitPath for external tests.
func SplitPath(p string) (string, string) {
	return splitPath(p)
}

// ParseResource exposes parseResource for external tests.
func ParseResource(resource string) (int, string, string) {
	kind, name, inner := parseResource(resource)

	return int(kind), name, inner
}

// ParseEntityKeyPredicate exposes parseEntityKeyPredicate for external tests.
func ParseEntityKeyPredicate(predicate string) (string, string, bool) {
	return parseEntityKeyPredicate(predicate)
}

// UnquoteODataString exposes unquoteODataString for external tests.
func UnquoteODataString(s string) (string, bool) {
	return unquoteODataString(s)
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
