package apigatewaymanagementapi

import "encoding/json"

type backendSnapshot struct {
	Connections map[string]*Connection     `json:"connections"`
	Messages    map[string][]PostedMessage `json:"messages"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	msgs := make(map[string][]PostedMessage, len(b.messages))
	for id, buf := range b.messages {
		msgs[id] = buf.snapshot()
	}

	snap := backendSnapshot{
		Connections: b.connections,
		Messages:    msgs,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Connections == nil {
		snap.Connections = make(map[string]*Connection)
	}

	b.connections = snap.Connections
	b.messages = make(map[string]*messageBuffer, len(snap.Messages))

	for id, msgs := range snap.Messages {
		buf := newMessageBuffer(maxMessagesPerConnection)
		buf.loadOrdered(msgs)
		b.messages[id] = buf
	}

	return nil
}

// Reset clears all in-memory connection state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.connections = make(map[string]*Connection)
	b.messages = make(map[string]*messageBuffer)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
