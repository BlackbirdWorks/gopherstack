package emrserverless

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Applications    map[string]*Application       `json:"applications"`
	ApplicationARNs map[string]string             `json:"applicationARNs"`
	JobRunARNs      map[string][2]string          `json:"jobRunARNs"`
	JobRuns         map[string]map[string]*JobRun `json:"jobRuns"`
	AccountID       string                        `json:"accountID"`
	Region          string                        `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Applications == nil {
		s.Applications = make(map[string]*Application)
	}

	if s.ApplicationARNs == nil {
		s.ApplicationARNs = make(map[string]string)
	}

	if s.JobRunARNs == nil {
		s.JobRunARNs = make(map[string][2]string)
	}

	if s.JobRuns == nil {
		s.JobRuns = make(map[string]map[string]*JobRun)
	}

	for appID, runs := range s.JobRuns {
		if runs == nil {
			s.JobRuns[appID] = make(map[string]*JobRun)
		}
	}

	for _, app := range s.Applications {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}
	}

	for _, runs := range s.JobRuns {
		for _, jr := range runs {
			if jr.Tags == nil {
				jr.Tags = make(map[string]string)
			}
		}
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications:    b.applications,
		ApplicationARNs: b.applicationARNs,
		JobRunARNs:      b.jobRunARNs,
		JobRuns:         b.jobRuns,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("emrserverless: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.applications = snap.Applications
	b.applicationARNs = snap.ApplicationARNs
	b.jobRunARNs = snap.JobRunARNs
	b.jobRuns = snap.JobRuns
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
