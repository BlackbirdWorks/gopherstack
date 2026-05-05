package iotdataplane

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// shadowEntrySnap is the serialisable form of a shadowEntry.
type shadowEntrySnap struct {
	UpdatedAt time.Time `json:"updatedAt"`
	Document  []byte    `json:"document"`
	Version   int       `json:"version"`
}

// connectionEntrySnap is the serialisable form of a connectionEntry.
type connectionEntrySnap struct {
	ConnectedAt time.Time `json:"connectedAt"`
	SourceIP    string    `json:"sourceIp,omitempty"`
}

// retainedMessageSnap is the serialisable form of a RetainedMessage.
type retainedMessageSnap struct {
	Topic            string `json:"topic"`
	Payload          []byte `json:"payload"`
	Qos              int32  `json:"qos"`
	LastModifiedTime int64  `json:"lastModifiedTime"`
}

// backendSnapshot holds a serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Shadows          map[string]map[string]*shadowEntrySnap `json:"shadows"`
	Connections      map[string]*connectionEntrySnap        `json:"connections"`
	RetainedMessages map[string]*retainedMessageSnap        `json:"retainedMessages"`
}

// Snapshot serialises backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Build serialisable shadows map.
	shadows := make(map[string]map[string]*shadowEntrySnap, len(b.shadows))
	for thingName, thingShadows := range b.shadows {
		snapShadows := make(map[string]*shadowEntrySnap, len(thingShadows))
		for shadowName, entry := range thingShadows {
			cp := make([]byte, len(entry.document))
			copy(cp, entry.document)
			snapShadows[shadowName] = &shadowEntrySnap{
				Document:  cp,
				Version:   entry.version,
				UpdatedAt: entry.updatedAt,
			}
		}

		shadows[thingName] = snapShadows
	}

	// Build serialisable connections map.
	connections := make(map[string]*connectionEntrySnap, len(b.connections))
	for clientID, entry := range b.connections {
		connections[clientID] = &connectionEntrySnap{
			ConnectedAt: entry.connectedAt,
			SourceIP:    entry.sourceIP,
		}
	}

	// Build serialisable retained messages map.
	retained := make(map[string]*retainedMessageSnap, len(b.retainedMessages))
	for topic, msg := range b.retainedMessages {
		cp := make([]byte, len(msg.Payload))
		copy(cp, msg.Payload)
		retained[topic] = &retainedMessageSnap{
			Topic:            msg.Topic,
			Payload:          cp,
			Qos:              msg.Qos,
			LastModifiedTime: msg.LastModifiedTime,
		}
	}

	snap := backendSnapshot{
		Shadows:          shadows,
		Connections:      connections,
		RetainedMessages: retained,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore deserialises backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Shadows == nil {
		snap.Shadows = make(map[string]map[string]*shadowEntrySnap)
	}

	if snap.Connections == nil {
		snap.Connections = make(map[string]*connectionEntrySnap)
	}

	if snap.RetainedMessages == nil {
		snap.RetainedMessages = make(map[string]*retainedMessageSnap)
	}

	// Restore shadows.
	b.shadows = make(map[string]map[string]*shadowEntry, len(snap.Shadows))
	for thingName, thingShadows := range snap.Shadows {
		restored := make(map[string]*shadowEntry, len(thingShadows))
		for shadowName, es := range thingShadows {
			cp := make([]byte, len(es.Document))
			copy(cp, es.Document)
			restored[shadowName] = &shadowEntry{
				document:  cp,
				version:   es.Version,
				updatedAt: es.UpdatedAt,
			}
		}

		b.shadows[thingName] = restored
	}

	// Restore connections.
	b.connections = make(map[string]*connectionEntry, len(snap.Connections))
	for clientID, entry := range snap.Connections {
		b.connections[clientID] = &connectionEntry{
			connectedAt: entry.ConnectedAt,
			sourceIP:    entry.SourceIP,
		}
	}

	// Restore retained messages.
	b.retainedMessages = make(map[string]*RetainedMessage, len(snap.RetainedMessages))
	for topic, rm := range snap.RetainedMessages {
		cp := make([]byte, len(rm.Payload))
		copy(cp, rm.Payload)
		b.retainedMessages[topic] = &RetainedMessage{
			Topic:            rm.Topic,
			Payload:          cp,
			Qos:              rm.Qos,
			LastModifiedTime: rm.LastModifiedTime,
		}
	}

	return nil
}

// Snapshot implements persistence by delegating to the backend if it supports it.
func (h *Handler) Snapshot() []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return s.Snapshot()
}

// Restore implements persistence by delegating to the backend if it supports it.
func (h *Handler) Restore(data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return ErrNoSnapshot
	}

	return s.Restore(data)
}
