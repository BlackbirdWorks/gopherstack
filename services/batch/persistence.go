package batch

import "encoding/json"

type backendSnapshot struct {
	ComputeEnvironments map[string]*ComputeEnvironment `json:"computeEnvironments"`
	JobQueues           map[string]*JobQueue           `json:"jobQueues"`
	JobDefinitions      map[string]*JobDefinition      `json:"jobDefinitions"`
	Jobs                map[string]*Job                `json:"jobs"`
	JobsByQueue         map[string][]string            `json:"jobsByQueue"`
	JobDefRevisions     map[string]int32               `json:"jobDefRevisions"`
	AccountID           string                         `json:"accountID"`
	Region              string                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ComputeEnvironments: b.computeEnvironments,
		JobQueues:           b.jobQueues,
		JobDefinitions:      b.jobDefinitions,
		Jobs:                b.jobs,
		JobsByQueue:         b.jobsByQueue,
		JobDefRevisions:     b.jobDefRevisions,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	data, _ := json.Marshal(snap)

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

	if snap.ComputeEnvironments == nil {
		snap.ComputeEnvironments = make(map[string]*ComputeEnvironment)
	}

	if snap.JobQueues == nil {
		snap.JobQueues = make(map[string]*JobQueue)
	}

	if snap.JobDefinitions == nil {
		snap.JobDefinitions = make(map[string]*JobDefinition)
	}

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	if snap.JobsByQueue == nil {
		snap.JobsByQueue = make(map[string][]string)
	}

	if snap.JobDefRevisions == nil {
		snap.JobDefRevisions = make(map[string]int32)
	}

	b.computeEnvironments = snap.ComputeEnvironments
	b.jobQueues = snap.JobQueues
	b.jobDefinitions = snap.JobDefinitions
	b.jobs = snap.Jobs
	b.jobsByQueue = snap.JobsByQueue
	b.jobDefRevisions = snap.JobDefRevisions
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
