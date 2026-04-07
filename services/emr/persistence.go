package emr

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters              map[string]*Cluster               `json:"clusters"`
	ArnIndex              map[string]string                 `json:"arnIndex"`
	SecurityConfigs       map[string]*SecurityConfiguration `json:"securityConfigs"`
	Studios               map[string]*Studio                `json:"studios"`
	StudioSessionMappings map[string]*StudioSessionMapping  `json:"studioSessionMappings"`
	PersistentAppUIs      map[string]*PersistentAppUI       `json:"persistentAppUIs"`
	AccountID             string                            `json:"accountID"`
	Region                string                            `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}

	if s.ArnIndex == nil {
		s.ArnIndex = make(map[string]string)
	}

	if s.SecurityConfigs == nil {
		s.SecurityConfigs = make(map[string]*SecurityConfiguration)
	}

	if s.Studios == nil {
		s.Studios = make(map[string]*Studio)
	}

	if s.StudioSessionMappings == nil {
		s.StudioSessionMappings = make(map[string]*StudioSessionMapping)
	}

	if s.PersistentAppUIs == nil {
		s.PersistentAppUIs = make(map[string]*PersistentAppUI)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:              b.clusters,
		ArnIndex:              b.arnIndex,
		SecurityConfigs:       b.securityConfigs,
		Studios:               b.studios,
		StudioSessionMappings: b.studioSessionMappings,
		PersistentAppUIs:      b.persistentAppUIs,
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("emr: Snapshot marshal failure", "error", err)

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

	b.clusters = snap.Clusters
	b.arnIndex = snap.ArnIndex
	b.securityConfigs = snap.SecurityConfigs
	b.studios = snap.Studios
	b.studioSessionMappings = snap.StudioSessionMappings
	b.persistentAppUIs = snap.PersistentAppUIs
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
