package emrserverless

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Applications    map[string]*Application        `json:"applications"`
	ApplicationARNs map[string]string              `json:"applicationARNs"`
	JobRunARNs      map[string][2]string           `json:"jobRunARNs"`
	JobRuns         map[string]map[string]*JobRun  `json:"jobRuns"`
	SessionARNs     map[string][2]string           `json:"sessionARNs"`
	Sessions        map[string]map[string]*Session `json:"sessions"`
	SessionTokens   map[string]map[string]string   `json:"sessionTokens"`
	AccountID       string                         `json:"accountID"`
	Region          string                         `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	s.ensureMaps()
	s.ensureResourceTags()
}

func (s *backendSnapshot) ensureMaps() {
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

	if s.SessionARNs == nil {
		s.SessionARNs = make(map[string][2]string)
	}

	if s.Sessions == nil {
		s.Sessions = make(map[string]map[string]*Session)
	}

	if s.SessionTokens == nil {
		s.SessionTokens = make(map[string]map[string]string)
	}

	for appID, runs := range s.JobRuns {
		if runs == nil {
			s.JobRuns[appID] = make(map[string]*JobRun)
		}
	}
}

func (s *backendSnapshot) ensureResourceTags() {
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

	for _, sessions := range s.Sessions {
		for _, session := range sessions {
			if session.Tags == nil {
				session.Tags = make(map[string]string)
			}
		}
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications:    b.applications,
		ApplicationARNs: b.applicationARNs,
		JobRunARNs:      b.jobRunARNs,
		JobRuns:         b.jobRuns,
		SessionARNs:     b.sessionARNs,
		Sessions:        b.sessions,
		SessionTokens:   b.sessionTokens,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "emrserverless: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "emrserverless", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.applications = snap.Applications
	b.applicationARNs = snap.ApplicationARNs
	b.jobRunARNs = snap.JobRunARNs
	b.jobRuns = snap.JobRuns
	b.sessionARNs = snap.SessionARNs
	b.sessions = snap.Sessions
	b.sessionTokens = snap.SessionTokens
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
