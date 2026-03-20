package elastictranscoder

import (
	"encoding/json"
)

type backendSnapshot struct {
	Pipelines map[string]*Pipeline `json:"pipelines"`
	Presets   map[string]*Preset   `json:"presets"`
	Jobs      map[string]*Job      `json:"jobs"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	pipelinesCopy := make(map[string]*Pipeline, len(b.pipelines))
	for k, v := range b.pipelines {
		cp := *v
		pipelinesCopy[k] = &cp
	}

	presetsCopy := make(map[string]*Preset, len(b.presets))
	for k, v := range b.presets {
		cp := *v
		presetsCopy[k] = &cp
	}

	jobsCopy := make(map[string]*Job, len(b.jobs))
	for k, v := range b.jobs {
		cp := *v
		jobsCopy[k] = &cp
	}

	snap := backendSnapshot{
		Pipelines: pipelinesCopy,
		Presets:   presetsCopy,
		Jobs:      jobsCopy,
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

	if snap.Pipelines == nil {
		snap.Pipelines = make(map[string]*Pipeline)
	}

	if snap.Presets == nil {
		snap.Presets = make(map[string]*Preset)
	}

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	b.pipelines = snap.Pipelines
	b.presets = snap.Presets
	b.jobs = snap.Jobs

	// Rebuild name index from restored pipelines.
	b.pipelinesByName = make(map[string]string, len(b.pipelines))
	for id, p := range b.pipelines {
		b.pipelinesByName[p.Name] = id
	}

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
