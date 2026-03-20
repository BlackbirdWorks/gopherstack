package glacier

import "encoding/json"

type vaultSnapshot struct {
	Vault *Vault   `json:"vault"`
	Key   vaultKey `json:"key"`
}

type archiveSnapshot struct {
	Archives map[string]*Archive `json:"archives"`
	Key      vaultKey            `json:"key"`
}

type jobSnapshot struct {
	Jobs map[string]*Job `json:"jobs"`
	Key  vaultKey        `json:"key"`
}

type backendSnapshot struct {
	Vaults   []vaultSnapshot   `json:"vaults"`
	Archives []archiveSnapshot `json:"archives"`
	Jobs     []jobSnapshot     `json:"jobs"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Vaults:   make([]vaultSnapshot, 0, len(b.vaults)),
		Archives: make([]archiveSnapshot, 0, len(b.archives)),
		Jobs:     make([]jobSnapshot, 0, len(b.jobs)),
	}

	for k, v := range b.vaults {
		snap.Vaults = append(snap.Vaults, vaultSnapshot{Key: k, Vault: v})
	}

	for k, archives := range b.archives {
		snap.Archives = append(snap.Archives, archiveSnapshot{Key: k, Archives: archives})
	}

	for k, jobs := range b.jobs {
		snap.Jobs = append(snap.Jobs, jobSnapshot{Key: k, Jobs: jobs})
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

	b.mu.Lock()
	defer b.mu.Unlock()

	b.vaults = make(map[vaultKey]*Vault)
	b.archives = make(map[vaultKey]map[string]*Archive)
	b.jobs = make(map[vaultKey]map[string]*Job)

	for _, vs := range snap.Vaults {
		b.vaults[vs.Key] = vs.Vault
	}

	for _, as := range snap.Archives {
		if as.Archives == nil {
			as.Archives = make(map[string]*Archive)
		}

		b.archives[as.Key] = as.Archives
	}

	for _, js := range snap.Jobs {
		if js.Jobs == nil {
			js.Jobs = make(map[string]*Job)
		}

		b.jobs[js.Key] = js.Jobs
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(data)
	}

	return nil
}
