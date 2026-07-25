package iotdataplane

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

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

	if b.connections.Has(clientID) {
		return fmt.Errorf("%w: %s", ErrConnectionExists, clientID)
	}

	b.connections.Put(&connectionEntry{
		clientID:    clientID,
		connectedAt: time.Now(),
		sourceIP:    sourceIP,
	})

	return nil
}

// DeleteConnection disconnects a tracked MQTT client connection.
// Returns ErrConnectionNotFound if clientID has no tracked connection (real AWS
// models ResourceNotFoundException for this op -- see ErrConnectionNotFound).
// ClientIDs beginning with '$' are rejected per AWS rules.
func (b *InMemoryBackend) DeleteConnection(clientID string) error {
	if strings.HasPrefix(clientID, "$") {
		return fmt.Errorf("%w: clientId may not start with '$'", ErrValidation)
	}

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(clientID) {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, clientID)
	}

	b.connections.Delete(clientID)

	return nil
}

// ListConnections returns all registered connections sorted by ConnectedAt ascending.
func (b *InMemoryBackend) ListConnections() []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	all := b.connections.All()
	out := make([]*Connection, 0, len(all))

	for _, entry := range all {
		out = append(out, &Connection{
			ClientID:    entry.clientID,
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

	b.connections.Put(&connectionEntry{clientID: clientID, connectedAt: time.Now()})
}
