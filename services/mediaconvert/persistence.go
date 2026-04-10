package mediaconvert

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Queues       map[string]*Queue            `json:"queues"`
	JobTemplates map[string]*JobTemplate      `json:"jobTemplates"`
	Jobs         map[string]*Job              `json:"jobs"`
	Presets      map[string]*Preset           `json:"presets"`
	Tags         map[string]map[string]string `json:"tags"`
	Certificates map[string]struct{}          `json:"certificates"`
	Policy       *Policy                      `json:"policy,omitempty"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Queues:       b.queues,
		JobTemplates: b.jobTemplates,
		Jobs:         b.jobs,
		Presets:      b.presets,
		Tags:         b.tags,
		Certificates: b.certificates,
		Policy:       b.policy,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("mediaconvert: failed to marshal snapshot", "error", err)

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

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.queues = snap.Queues
	b.jobTemplates = snap.JobTemplates
	b.jobs = snap.Jobs
	b.presets = snap.Presets
	b.tags = snap.Tags
	b.certificates = snap.Certificates
	b.policy = snap.Policy
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises any nil maps in the snapshot to avoid nil-map panics.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Queues == nil {
		snap.Queues = make(map[string]*Queue)
	}

	if snap.JobTemplates == nil {
		snap.JobTemplates = make(map[string]*JobTemplate)
	}

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	if snap.Presets == nil {
		snap.Presets = make(map[string]*Preset)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.Certificates == nil {
		snap.Certificates = make(map[string]struct{})
	}
}
