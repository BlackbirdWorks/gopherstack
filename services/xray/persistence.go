package xray

import "encoding/json"

type backendSnapshot struct {
	Groups        map[string]*Group        `json:"groups"`
	SamplingRules map[string]*SamplingRule `json:"samplingRules"`
	Traces        map[string]*Trace        `json:"traces"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:        b.groups,
		SamplingRules: b.samplingRules,
		Traces:        b.traces,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Groups == nil {
		snap.Groups = make(map[string]*Group)
	}

	if snap.SamplingRules == nil {
		snap.SamplingRules = make(map[string]*SamplingRule)
	}

	if snap.Traces == nil {
		snap.Traces = make(map[string]*Trace)
	}

	b.groups = snap.Groups
	b.samplingRules = snap.SamplingRules
	b.traces = snap.Traces

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
