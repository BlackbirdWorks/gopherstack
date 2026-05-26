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
	UpdatedAt    time.Time                        `json:"updatedAt"`
	Version      int                              `json:"version"`
	Desired      map[string]json.RawMessage       `json:"desired,omitempty"`
	Reported     map[string]json.RawMessage       `json:"reported,omitempty"`
	MetaDesired  map[string]int64                 `json:"metaDesired,omitempty"`
	MetaReported map[string]int64                 `json:"metaReported,omitempty"`
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
			snap := &shadowEntrySnap{
				Version:   entry.version,
				UpdatedAt: entry.updatedAt,
			}

			if entry.desired != nil {
				snap.Desired = make(map[string]json.RawMessage, len(entry.desired))
				for k, v := range entry.desired {
					cp := make(json.RawMessage, len(v))
					copy(cp, v)
					snap.Desired[k] = cp
				}
			}

			if entry.reported != nil {
				snap.Reported = make(map[string]json.RawMessage, len(entry.reported))
				for k, v := range entry.reported {
					cp := make(json.RawMessage, len(v))
					copy(cp, v)
					snap.Reported[k] = cp
				}
			}

			if entry.metaDesired != nil {
				snap.MetaDesired = make(map[string]int64, len(entry.metaDesired))
				for k, v := range entry.metaDesired {
					snap.MetaDesired[k] = v
				}
			}

			if entry.metaReported != nil {
				snap.MetaReported = make(map[string]int64, len(entry.metaReported))
				for k, v := range entry.metaReported {
					snap.MetaReported[k] = v
				}
			}

			snapShadows[shadowName] = snap
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
			entry := &shadowEntry{
				version:   es.Version,
				updatedAt: es.UpdatedAt,
			}

			if es.Desired != nil {
				entry.desired = make(map[string]json.RawMessage, len(es.Desired))
				for k, v := range es.Desired {
					cp := make(json.RawMessage, len(v))
					copy(cp, v)
					entry.desired[k] = cp
				}
			}

			if es.Reported != nil {
				entry.reported = make(map[string]json.RawMessage, len(es.Reported))
				for k, v := range es.Reported {
					cp := make(json.RawMessage, len(v))
					copy(cp, v)
					entry.reported[k] = cp
				}
			}

			if es.MetaDesired != nil {
				entry.metaDesired = make(map[string]int64, len(es.MetaDesired))
				for k, v := range es.MetaDesired {
					entry.metaDesired[k] = v
				}
			}

			if es.MetaReported != nil {
				entry.metaReported = make(map[string]int64, len(es.MetaReported))
				for k, v := range es.MetaReported {
					entry.metaReported[k] = v
				}
			}

			restored[shadowName] = entry
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
