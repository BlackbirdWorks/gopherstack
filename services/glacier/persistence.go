package glacier

import (
	"encoding/json"
	"log/slog"
)

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

type multipartUploadSnapshot struct {
	Uploads map[string]*MultipartUpload `json:"uploads"`
	Key     vaultKey                    `json:"key"`
}

type multipartPartSnapshot struct {
	Key   uploadKey       `json:"key"`
	Parts []MultipartPart `json:"parts"`
}

type provisionedCapacitySnapshot struct {
	AccountID string                 `json:"accountID"`
	Caps      []*ProvisionedCapacity `json:"caps"`
}

type vaultLockSnapshot struct {
	Lock *VaultLock `json:"lock"`
	Key  vaultKey   `json:"key"`
}

type backendSnapshot struct {
	Vaults              []vaultSnapshot               `json:"vaults"`
	Archives            []archiveSnapshot             `json:"archives"`
	Jobs                []jobSnapshot                 `json:"jobs"`
	MultipartUploads    []multipartUploadSnapshot     `json:"multipartUploads"`
	MultipartParts      []multipartPartSnapshot       `json:"multipartParts"`
	VaultLocks          []vaultLockSnapshot           `json:"vaultLocks,omitempty"`
	ProvisionedCapacity []provisionedCapacitySnapshot `json:"provisionedCapacity"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Vaults:           make([]vaultSnapshot, 0, len(b.vaults)),
		Archives:         make([]archiveSnapshot, 0, len(b.archives)),
		Jobs:             make([]jobSnapshot, 0, len(b.jobs)),
		MultipartUploads: make([]multipartUploadSnapshot, 0, len(b.multipartUploads)),
		MultipartParts:   make([]multipartPartSnapshot, 0, len(b.multipartParts)),
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

	for k, uploads := range b.multipartUploads {
		snap.MultipartUploads = append(snap.MultipartUploads, multipartUploadSnapshot{Key: k, Uploads: uploads})
	}

	for k, parts := range b.multipartParts {
		snap.MultipartParts = append(snap.MultipartParts, multipartPartSnapshot{Key: k, Parts: parts})
	}

	for accountID, caps := range b.provisionedCapacity {
		snap.ProvisionedCapacity = append(snap.ProvisionedCapacity, provisionedCapacitySnapshot{
			AccountID: accountID,
			Caps:      caps,
		})
	}

	for k, lock := range b.vaultLocks {
		snap.VaultLocks = append(snap.VaultLocks, vaultLockSnapshot{Key: k, Lock: lock})
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("glacier: failed to marshal snapshot", "error", err)

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
	b.multipartUploads = make(map[vaultKey]map[string]*MultipartUpload)
	b.multipartParts = make(map[uploadKey][]MultipartPart)
	b.vaultLocks = make(map[vaultKey]*VaultLock)
	b.provisionedCapacity = make(map[string][]*ProvisionedCapacity)

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

	for _, us := range snap.MultipartUploads {
		if us.Uploads == nil {
			us.Uploads = make(map[string]*MultipartUpload)
		}

		b.multipartUploads[us.Key] = us.Uploads
	}

	for _, ps := range snap.MultipartParts {
		b.multipartParts[ps.Key] = ps.Parts
	}

	for _, cs := range snap.ProvisionedCapacity {
		b.provisionedCapacity[cs.AccountID] = cs.Caps
	}

	for _, ls := range snap.VaultLocks {
		b.vaultLocks[ls.Key] = ls.Lock
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
