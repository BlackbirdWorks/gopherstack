// Package tags provides a concurrency-safe string tag map for AWS resource annotations.
//
// Tags is built on [safemap.Map] and therefore inherits full Prometheus
// instrumentation via [lockmetrics.RWMutex].
package tags

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

// Tags is a concurrency-safe collection of string key-value tag pairs.
//
// The zero value is not usable; always create via [New] or [FromMap].
type Tags struct {
	m *safemap.Map[string, string]
}

// New creates a new Tags collection. The name appears as the "lock" label
// in Prometheus metrics (e.g. "sqs.queue.my-queue.tags").
func New(name string) *Tags {
	return &Tags{m: safemap.New[string, string](name)}
}

// FromMap creates a Tags collection pre-populated from src.
func FromMap(name string, src map[string]string) *Tags {
	t := New(name)

	for k, v := range src {
		t.m.Set(k, v)
	}

	return t
}

// Get returns the value for key and whether it was found.
func (t *Tags) Get(key string) (string, bool) {
	return t.m.Get(key)
}

// Set adds or updates a tag.
func (t *Tags) Set(key, value string) {
	t.m.Set(key, value)
}

// Delete removes the tag with the given key.
func (t *Tags) Delete(key string) {
	t.m.Delete(key)
}

// HasTag reports whether key exists.
func (t *Tags) HasTag(key string) bool {
	_, ok := t.m.Get(key)

	return ok
}

// Len returns the number of tags.
func (t *Tags) Len() int {
	return t.m.Len()
}

// Clone returns a plain map[string]string copy of the tags.
func (t *Tags) Clone() map[string]string {
	return t.m.Clone()
}

// Close releases resources associated with this Tags collection, removing the
// underlying lockmetrics.RWMutex from the global Prometheus registry.
// It should be called when the Tags instance is no longer needed to prevent
// unbounded growth of the global collector.
// After Close is called, the Tags must not be used.
func (t *Tags) Close() {
	if t == nil || t.m == nil {
		return
	}

	t.m.Close()
}

// Merge adds all entries from src into t, overwriting existing keys.
func (t *Tags) Merge(src map[string]string) {
	for k, v := range src {
		t.m.Set(k, v)
	}
}

// DeleteKeys removes all tags whose key appears in keys.
func (t *Tags) DeleteKeys(keys []string) {
	for _, k := range keys {
		t.m.Delete(k)
	}
}

// MatchesFilter reports whether t contains all key-value pairs in filter.
// An empty filter always returns true.
func (t *Tags) MatchesFilter(filter map[string]string) bool {
	for k, v := range filter {
		got, ok := t.m.Get(k)
		if !ok || got != v {
			return false
		}
	}

	return true
}

// Range calls f sequentially for each tag in unspecified order.
// The underlying map is read-locked for the entire iteration; f must not
// call methods on the same Tags to avoid deadlocks.
// Iteration stops early if f returns false.
func (t *Tags) Range(f func(key, value string) bool) {
	t.m.Range(func(k, v string) bool {
		return f(k, v)
	})
}

// MarshalJSON implements [json.Marshaler].
// Tags serialises as a plain JSON object (e.g. {"env":"prod","team":"platform"}).
func (t *Tags) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Clone()) //nolint:wrapcheck // thin delegation
}

// UnmarshalJSON implements [json.Unmarshaler].
// Tags deserialises from a plain JSON object (e.g. {"env":"prod","team":"platform"}).
// An empty or null JSON value produces an empty Tags with the zero-value name; callers
// that care about the Prometheus lock name should prefer [FromMap] for static data.
// Any keys previously present in t that are absent from the JSON payload are removed,
// so the result matches the JSON payload exactly.
func (t *Tags) UnmarshalJSON(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err //nolint:wrapcheck // thin delegation
	}

	if t.m == nil {
		t.m = safemap.New[string, string]("json.tags")
	}

	// Clear any stale keys so the result matches the JSON payload exactly.
	t.m.Clear()

	for k, v := range m {
		t.m.Set(k, v)
	}

	return nil
}
