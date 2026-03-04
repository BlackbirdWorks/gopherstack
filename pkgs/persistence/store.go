// Package persistence provides a pluggable persistence layer for Gopherstack services.
// Services implement the Persistable interface to participate in snapshot-based state
// serialisation. The Store interface abstracts the underlying storage medium.
package persistence

import "errors"

// ErrKeyNotFound is returned by Load when the requested key does not exist.
var ErrKeyNotFound = errors.New("key not found")

// Store is the persistence back-end abstraction.
// Save/Load/Delete operate on named blobs identified by (service, key).
// ListKeys returns all keys stored for a service.
type Store interface {
	// Save persists data for the given service and key.
	Save(service, key string, data []byte) error

	// Load retrieves data for the given service and key.
	// Returns ErrKeyNotFound when the key does not exist.
	Load(service, key string) ([]byte, error)

	// Delete removes the data for the given service and key.
	// It is not an error to delete a non-existent key.
	Delete(service, key string) error

	// ListKeys returns all keys stored for the given service.
	ListKeys(service string) ([]string, error)
}

// Persistable is an optional interface that service backends may implement to
// participate in Gopherstack's snapshot-based persistence.
//
// Snapshot returns an opaque JSON blob representing current backend state.
// Restore initialises backend state from a previously-captured blob.
type Persistable interface {
	Snapshot() []byte
	Restore([]byte) error
}
