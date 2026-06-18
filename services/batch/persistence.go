package batch

import "encoding/json"

// backendSnapshot is the serialisation form of the backend. All resource maps are
// nested by region (outer key = region) so that region isolation survives a
// snapshot/restore round-trip.
type backendSnapshot struct {
	ComputeEnvironments map[string]map[string]*ComputeEnvironment `json:"computeEnvironments"`
	JobQueues           map[string]map[string]*JobQueue           `json:"jobQueues"`
	JobDefinitions      map[string]map[string]*JobDefinition      `json:"jobDefinitions"`
	Jobs                map[string]map[string]*Job                `json:"jobs"`
	JobsByQueue         map[string]map[string][]string            `json:"jobsByQueue"`
	JobDefRevisions     map[string]map[string]int32               `json:"jobDefRevisions"`
	ConsumableResources map[string]map[string]*ConsumableResource `json:"consumableResources"`
	SchedulingPolicies  map[string]map[string]*SchedulingPolicy   `json:"schedulingPolicies"`
	ServiceEnvironments map[string]map[string]*ServiceEnvironment `json:"serviceEnvironments"`
	AccountID           string                                    `json:"accountID"`
	Region              string                                    `json:"region"`
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
		ConsumableResources: b.consumableResources,
		SchedulingPolicies:  b.schedulingPolicies,
		ServiceEnvironments: b.serviceEnvironments,
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

	b.initMaps()

	if snap.ComputeEnvironments != nil {
		b.computeEnvironments = snap.ComputeEnvironments
	}

	if snap.JobQueues != nil {
		b.jobQueues = snap.JobQueues
	}

	if snap.JobDefinitions != nil {
		b.jobDefinitions = snap.JobDefinitions
	}

	if snap.Jobs != nil {
		b.jobs = snap.Jobs
	}

	if snap.JobsByQueue != nil {
		b.jobsByQueue = snap.JobsByQueue
	}

	if snap.JobDefRevisions != nil {
		b.jobDefRevisions = snap.JobDefRevisions
	}

	if snap.ConsumableResources != nil {
		b.consumableResources = snap.ConsumableResources
	}

	if snap.SchedulingPolicies != nil {
		b.schedulingPolicies = snap.SchedulingPolicies
	}

	if snap.ServiceEnvironments != nil {
		b.serviceEnvironments = snap.ServiceEnvironments
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the per-region name → ARN index and the job ARN → ID index from the
	// restored state (these cross-index maps are not persisted directly).
	b.rebuildIndexes()

	return nil
}

// rebuildIndexes reconstructs the schedulingPolicyByName and jobsByARN cross-index
// maps from the restored resource maps. Callers must hold the write lock.
func (b *InMemoryBackend) rebuildIndexes() {
	b.schedulingPolicyByName = make(map[string]map[string]string, len(b.schedulingPolicies))
	for region, policies := range b.schedulingPolicies {
		byName := b.schedulingPolicyByNameStore(region)
		for policyARN, sp := range policies {
			byName[sp.Name] = policyARN
		}
	}

	b.jobsByARN = make(map[string]map[string]string, len(b.jobs))
	for region, jobs := range b.jobs {
		byARN := b.jobsByARNStore(region)
		for jobID, j := range jobs {
			byARN[j.JobARN] = jobID
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
