// Package safemap provides a generic, concurrency-safe map wrapper.
//
// All operations are protected by a [lockmetrics.RWMutex] for concurrent
// access with full Prometheus instrumentation.
package safemap

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Map is a generic concurrency-safe map.
//
// The zero value is not usable; always create via [New].
type Map[K comparable, V any] struct {
	mu   *lockmetrics.RWMutex
	data map[K]V
}

// New creates a new Map. The name appears as the "lock" label in Prometheus
// metrics and should be a stable, human-readable identifier
// (e.g. "sqs.queue.my-queue.tags").
func New[K comparable, V any](name string) *Map[K, V] {
	return &Map[K, V]{
		mu:   lockmetrics.New(name),
		data: make(map[K]V),
	}
}

// Get returns the value associated with key and whether the key was found.
func (m *Map[K, V]) Get(key K) (V, bool) {
	m.mu.RLock("Get")
	defer m.mu.RUnlock()

	v, ok := m.data[key]

	return v, ok
}

// Set adds or updates key to value.
func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock("Set")
	defer m.mu.Unlock()

	m.data[key] = value
}

// Delete removes key from the map.
func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock("Delete")
	defer m.mu.Unlock()

	delete(m.data, key)
}

// Range calls f sequentially for each key-value pair in unspecified order.
// The map is read-locked for the entire iteration; f must not call methods
// on the same Map to avoid deadlocks.
// Iteration stops early if f returns false.
func (m *Map[K, V]) Range(f func(K, V) bool) {
	m.mu.RLock("Range")
	defer m.mu.RUnlock()

	for k, v := range m.data {
		if !f(k, v) {
			break
		}
	}
}

// Len returns the number of entries.
func (m *Map[K, V]) Len() int {
	m.mu.RLock("Len")
	defer m.mu.RUnlock()

	return len(m.data)
}

// Keys returns all keys as a slice in unspecified order.
func (m *Map[K, V]) Keys() []K {
	m.mu.RLock("Keys")
	defer m.mu.RUnlock()

	keys := make([]K, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}

// Values returns all values as a slice in unspecified order.
func (m *Map[K, V]) Values() []V {
	m.mu.RLock("Values")
	defer m.mu.RUnlock()

	vals := make([]V, 0, len(m.data))
	for _, v := range m.data {
		vals = append(vals, v)
	}

	return vals
}

// Clone returns a shallow copy of the underlying map.
func (m *Map[K, V]) Clone() map[K]V {
	m.mu.RLock("Clone")
	defer m.mu.RUnlock()

	return maps.Clone(m.data)
}

// Clear removes all entries from the map.
func (m *Map[K, V]) Clear() {
	m.mu.Lock("Clear")
	defer m.mu.Unlock()

	clear(m.data)
}

// Close removes the Map's underlying lockmetrics.RWMutex from the global
// metrics registry. It should be called when the Map is no longer needed to
// prevent unbounded growth of the Prometheus collector.
// After Close is called, the Map must not be used.
func (m *Map[K, V]) Close() {
	if m == nil || m.mu == nil {
		return
	}

	m.mu.Close()
}
