package textract

import (
	"encoding/json"
	"log/slog"
	"maps"
)

type backendSnapshot struct {
	Jobs                   map[string]*DocumentJob    `json:"jobs"`
	ExpenseJobs            map[string]*ExpenseJob     `json:"expenseJobs"`
	LendingJobs            map[string]*LendingJob     `json:"lendingJobs"`
	Adapters               map[string]*Adapter        `json:"adapters"`
	AdapterVersions        map[string]*AdapterVersion `json:"adapterVersions"`
	ClientTokenToJobID     map[string]string          `json:"clientTokenToJobId,omitempty"`
	AdapterClientTokenToID map[string]string          `json:"adapterClientTokenToId,omitempty"`
}

// ensureNonNilMaps guarantees that all map fields in the snapshot are non-nil.
func (s *backendSnapshot) ensureNonNilMaps() {
	if s.Jobs == nil {
		s.Jobs = make(map[string]*DocumentJob)
	}

	if s.ExpenseJobs == nil {
		s.ExpenseJobs = make(map[string]*ExpenseJob)
	}

	if s.LendingJobs == nil {
		s.LendingJobs = make(map[string]*LendingJob)
	}

	if s.Adapters == nil {
		s.Adapters = make(map[string]*Adapter)
	}

	if s.AdapterVersions == nil {
		s.AdapterVersions = make(map[string]*AdapterVersion)
	}

	if s.ClientTokenToJobID == nil {
		s.ClientTokenToJobID = make(map[string]string)
	}

	if s.AdapterClientTokenToID == nil {
		s.AdapterClientTokenToID = make(map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	jobsCopy := make(map[string]*DocumentJob, len(b.jobs))
	for k, v := range b.jobs {
		jobsCopy[k] = cloneJob(v)
	}

	expenseJobsCopy := make(map[string]*ExpenseJob, len(b.expenseJobs))
	for k, v := range b.expenseJobs {
		expenseJobsCopy[k] = cloneExpenseJob(v)
	}

	lendingJobsCopy := make(map[string]*LendingJob, len(b.lendingJobs))
	for k, v := range b.lendingJobs {
		lendingJobsCopy[k] = cloneLendingJob(v)
	}

	adaptersCopy := make(map[string]*Adapter, len(b.adapters))
	for k, v := range b.adapters {
		adaptersCopy[k] = cloneAdapter(v)
	}

	adapterVersionsCopy := make(map[string]*AdapterVersion, len(b.adapterVersions))
	for k, v := range b.adapterVersions {
		adapterVersionsCopy[k] = cloneAdapterVersion(v)
	}

	tokenMapCopy := make(map[string]string, len(b.clientTokenToJobID))
	maps.Copy(tokenMapCopy, b.clientTokenToJobID)

	adapterTokenMapCopy := make(map[string]string, len(b.adapterClientTokenToID))
	maps.Copy(adapterTokenMapCopy, b.adapterClientTokenToID)

	snap := backendSnapshot{
		Jobs:                   jobsCopy,
		ExpenseJobs:            expenseJobsCopy,
		LendingJobs:            lendingJobsCopy,
		Adapters:               adaptersCopy,
		AdapterVersions:        adapterVersionsCopy,
		ClientTokenToJobID:     tokenMapCopy,
		AdapterClientTokenToID: adapterTokenMapCopy,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("textract: failed to snapshot backend", "error", err)

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

	snap.ensureNonNilMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.jobs = snap.Jobs
	b.expenseJobs = snap.ExpenseJobs
	b.lendingJobs = snap.LendingJobs
	b.adapters = snap.Adapters
	b.adapterVersions = snap.AdapterVersions
	b.clientTokenToJobID = snap.ClientTokenToJobID
	b.adapterClientTokenToID = snap.AdapterClientTokenToID

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
