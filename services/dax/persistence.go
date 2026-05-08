package dax

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters     map[string]*Cluster          `json:"clusters"`
	ParamGroups  map[string]*ParameterGroup   `json:"paramGroups"`
	SubnetGroups map[string]*SubnetGroup      `json:"subnetGroups"`
	Tags         map[string]map[string]string `json:"tags"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}

	if s.ParamGroups == nil {
		s.ParamGroups = make(map[string]*ParameterGroup)
	}

	if s.SubnetGroups == nil {
		s.SubnetGroups = make(map[string]*SubnetGroup)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:     b.clusters,
		ParamGroups:  b.paramGroups,
		SubnetGroups: b.subnetGroups,
		Tags:         b.tags,
		AccountID:    b.AccountID,
		Region:       b.Region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Error("dax: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.paramGroups = snap.ParamGroups
	b.subnetGroups = snap.SubnetGroups
	b.tags = snap.Tags
	b.AccountID = snap.AccountID
	b.Region = snap.Region

	return nil
}
