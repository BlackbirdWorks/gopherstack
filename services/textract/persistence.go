package textract

import (
	"encoding/json"
	"log/slog"
	"maps"
)

// backendSnapshot persists the backend state. All resource maps are nested by
// region (outer key = region) for isolation.
type backendSnapshot struct {
	Jobs                   map[string]map[string]*DocumentJob    `json:"jobs"`
	ExpenseJobs            map[string]map[string]*ExpenseJob     `json:"expenseJobs"`
	LendingJobs            map[string]map[string]*LendingJob     `json:"lendingJobs"`
	Adapters               map[string]map[string]*Adapter        `json:"adapters"`
	AdapterVersions        map[string]map[string]*AdapterVersion `json:"adapterVersions"`
	ClientTokenToJobID     map[string]map[string]string          `json:"clientTokenToJobId,omitempty"`
	AdapterClientTokenToID map[string]map[string]string          `json:"adapterClientTokenToId,omitempty"`
}

// ensureNonNilMaps guarantees that all map fields in the snapshot are non-nil.
func (s *backendSnapshot) ensureNonNilMaps() {
	if s.Jobs == nil {
		s.Jobs = make(map[string]map[string]*DocumentJob)
	}

	if s.ExpenseJobs == nil {
		s.ExpenseJobs = make(map[string]map[string]*ExpenseJob)
	}

	if s.LendingJobs == nil {
		s.LendingJobs = make(map[string]map[string]*LendingJob)
	}

	if s.Adapters == nil {
		s.Adapters = make(map[string]map[string]*Adapter)
	}

	if s.AdapterVersions == nil {
		s.AdapterVersions = make(map[string]map[string]*AdapterVersion)
	}

	if s.ClientTokenToJobID == nil {
		s.ClientTokenToJobID = make(map[string]map[string]string)
	}

	if s.AdapterClientTokenToID == nil {
		s.AdapterClientTokenToID = make(map[string]map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	jobsCopy := make(map[string]map[string]*DocumentJob, len(b.jobs))

	for region, regionJobs := range b.jobs {
		regionCopy := make(map[string]*DocumentJob, len(regionJobs))
		for k, v := range regionJobs {
			regionCopy[k] = cloneJob(v)
		}

		jobsCopy[region] = regionCopy
	}

	expenseJobsCopy := make(map[string]map[string]*ExpenseJob, len(b.expenseJobs))

	for region, regionJobs := range b.expenseJobs {
		regionCopy := make(map[string]*ExpenseJob, len(regionJobs))
		for k, v := range regionJobs {
			regionCopy[k] = cloneExpenseJob(v)
		}

		expenseJobsCopy[region] = regionCopy
	}

	lendingJobsCopy := make(map[string]map[string]*LendingJob, len(b.lendingJobs))

	for region, regionJobs := range b.lendingJobs {
		regionCopy := make(map[string]*LendingJob, len(regionJobs))
		for k, v := range regionJobs {
			regionCopy[k] = cloneLendingJob(v)
		}

		lendingJobsCopy[region] = regionCopy
	}

	adaptersCopy := make(map[string]map[string]*Adapter, len(b.adapters))

	for region, regionAdapters := range b.adapters {
		regionCopy := make(map[string]*Adapter, len(regionAdapters))
		for k, v := range regionAdapters {
			regionCopy[k] = cloneAdapter(v)
		}

		adaptersCopy[region] = regionCopy
	}

	adapterVersionsCopy := make(map[string]map[string]*AdapterVersion, len(b.adapterVersions))

	for region, regionVersions := range b.adapterVersions {
		regionCopy := make(map[string]*AdapterVersion, len(regionVersions))
		for k, v := range regionVersions {
			regionCopy[k] = cloneAdapterVersion(v)
		}

		adapterVersionsCopy[region] = regionCopy
	}

	tokenMapCopy := make(map[string]map[string]string, len(b.clientTokenToJobID))

	for region, regionTokens := range b.clientTokenToJobID {
		regionCopy := make(map[string]string, len(regionTokens))
		maps.Copy(regionCopy, regionTokens)
		tokenMapCopy[region] = regionCopy
	}

	adapterTokenMapCopy := make(map[string]map[string]string, len(b.adapterClientTokenToID))

	for region, regionTokens := range b.adapterClientTokenToID {
		regionCopy := make(map[string]string, len(regionTokens))
		maps.Copy(regionCopy, regionTokens)
		adapterTokenMapCopy[region] = regionCopy
	}

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
