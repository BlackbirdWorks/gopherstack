package iotdataplane

import (
	"encoding/json"
	"errors"
	"maps"
	"time"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// shadowEntrySnap is the serialisable form of a shadowEntry.
type shadowEntrySnap struct {
	Desired      map[string]json.RawMessage `json:"desired,omitempty"`
	Reported     map[string]json.RawMessage `json:"reported,omitempty"`
	MetaDesired  map[string]int64           `json:"metaDesired,omitempty"`
	MetaReported map[string]int64           `json:"metaReported,omitempty"`
	UpdatedAt    time.Time                  `json:"updatedAt"`
	Version      int                        `json:"version"`
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

// copyRawMessages returns a deep copy of a map[string]json.RawMessage.
func copyRawMessages(src map[string]json.RawMessage) map[string]json.RawMessage {
	if src == nil {
		return nil
	}

	dst := make(map[string]json.RawMessage, len(src))
	for k, v := range src {
		cp := make(json.RawMessage, len(v))
		copy(cp, v)
		dst[k] = cp
	}

	return dst
}

// copyInt64Map returns a shallow copy of a map[string]int64.
func copyInt64Map(src map[string]int64) map[string]int64 {
	if src == nil {
		return nil
	}

	dst := make(map[string]int64, len(src))
	maps.Copy(dst, src)

	return dst
}

// entryToSnap converts a shadowEntry to its serialisable form.
func entryToSnap(entry *shadowEntry) *shadowEntrySnap {
	return &shadowEntrySnap{
		Version:      entry.version,
		UpdatedAt:    entry.updatedAt,
		Desired:      copyRawMessages(entry.desired),
		Reported:     copyRawMessages(entry.reported),
		MetaDesired:  copyInt64Map(entry.metaDesired),
		MetaReported: copyInt64Map(entry.metaReported),
	}
}

// snapToEntry converts a shadowEntrySnap back to its runtime form.
func snapToEntry(es *shadowEntrySnap) *shadowEntry {
	return &shadowEntry{
		version:      es.Version,
		updatedAt:    es.UpdatedAt,
		desired:      copyRawMessages(es.Desired),
		reported:     copyRawMessages(es.Reported),
		metaDesired:  copyInt64Map(es.MetaDesired),
		metaReported: copyInt64Map(es.MetaReported),
	}
}

// Snapshot serialises backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	shadows := make(map[string]map[string]*shadowEntrySnap, len(b.shadows))
	for thingName, thingShadows := range b.shadows {
		snapShadows := make(map[string]*shadowEntrySnap, len(thingShadows))
		for shadowName, entry := range thingShadows {
			snapShadows[shadowName] = entryToSnap(entry)
		}

		shadows[thingName] = snapShadows
	}

	connections := make(map[string]*connectionEntrySnap, len(b.connections))
	for clientID, entry := range b.connections {
		connections[clientID] = &connectionEntrySnap{
			ConnectedAt: entry.connectedAt,
			SourceIP:    entry.sourceIP,
		}
	}

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

	b.shadows = restoreShadows(snap.Shadows)
	b.connections = restoreConnections(snap.Connections)
	b.retainedMessages = restoreRetainedMessages(snap.RetainedMessages)

	return nil
}

// restoreShadows rebuilds the runtime shadows map from a snapshot.
func restoreShadows(snapShadows map[string]map[string]*shadowEntrySnap) map[string]map[string]*shadowEntry {
	if snapShadows == nil {
		return make(map[string]map[string]*shadowEntry)
	}

	result := make(map[string]map[string]*shadowEntry, len(snapShadows))
	for thingName, thingShadows := range snapShadows {
		restored := make(map[string]*shadowEntry, len(thingShadows))
		for shadowName, es := range thingShadows {
			restored[shadowName] = snapToEntry(es)
		}

		result[thingName] = restored
	}

	return result
}

// restoreConnections rebuilds the runtime connections map from a snapshot.
func restoreConnections(snapConns map[string]*connectionEntrySnap) map[string]*connectionEntry {
	if snapConns == nil {
		return make(map[string]*connectionEntry)
	}

	result := make(map[string]*connectionEntry, len(snapConns))
	for clientID, entry := range snapConns {
		result[clientID] = &connectionEntry{
			connectedAt: entry.ConnectedAt,
			sourceIP:    entry.SourceIP,
		}
	}

	return result
}

// restoreRetainedMessages rebuilds the runtime retained messages map from a snapshot.
func restoreRetainedMessages(snapMsgs map[string]*retainedMessageSnap) map[string]*RetainedMessage {
	if snapMsgs == nil {
		return make(map[string]*RetainedMessage)
	}

	result := make(map[string]*RetainedMessage, len(snapMsgs))
	for topic, rm := range snapMsgs {
		cp := make([]byte, len(rm.Payload))
		copy(cp, rm.Payload)

		result[topic] = &RetainedMessage{
			Topic:            rm.Topic,
			Payload:          cp,
			Qos:              rm.Qos,
			LastModifiedTime: rm.LastModifiedTime,
		}
	}

	return result
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
