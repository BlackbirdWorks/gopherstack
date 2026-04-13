package textract

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Jobs            map[string]*DocumentJob    `json:"jobs"`
	ExpenseJobs     map[string]*ExpenseJob     `json:"expenseJobs"`
	LendingJobs     map[string]*LendingJob     `json:"lendingJobs"`
	Adapters        map[string]*Adapter        `json:"adapters"`
	AdapterVersions map[string]*AdapterVersion `json:"adapterVersions"`
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
		cp := *v
		cp.ExpenseDocuments = make([]ExpenseDocument, len(v.ExpenseDocuments))
		copy(cp.ExpenseDocuments, v.ExpenseDocuments)
		expenseJobsCopy[k] = &cp
	}

	lendingJobsCopy := make(map[string]*LendingJob, len(b.lendingJobs))
	for k, v := range b.lendingJobs {
		cp := *v
		cp.Results = make([]LendingResult, len(v.Results))
		copy(cp.Results, v.Results)
		lendingJobsCopy[k] = &cp
	}

	adaptersCopy := make(map[string]*Adapter, len(b.adapters))
	for k, v := range b.adapters {
		adaptersCopy[k] = cloneAdapter(v)
	}

	adapterVersionsCopy := make(map[string]*AdapterVersion, len(b.adapterVersions))
	for k, v := range b.adapterVersions {
		adapterVersionsCopy[k] = cloneAdapterVersion(v)
	}

	snap := backendSnapshot{
		Jobs:            jobsCopy,
		ExpenseJobs:     expenseJobsCopy,
		LendingJobs:     lendingJobsCopy,
		Adapters:        adaptersCopy,
		AdapterVersions: adapterVersionsCopy,
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*DocumentJob)
	}

	if snap.ExpenseJobs == nil {
		snap.ExpenseJobs = make(map[string]*ExpenseJob)
	}

	if snap.LendingJobs == nil {
		snap.LendingJobs = make(map[string]*LendingJob)
	}

	if snap.Adapters == nil {
		snap.Adapters = make(map[string]*Adapter)
	}

	if snap.AdapterVersions == nil {
		snap.AdapterVersions = make(map[string]*AdapterVersion)
	}

	b.jobs = snap.Jobs
	b.expenseJobs = snap.ExpenseJobs
	b.lendingJobs = snap.LendingJobs
	b.adapters = snap.Adapters
	b.adapterVersions = snap.AdapterVersions

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
