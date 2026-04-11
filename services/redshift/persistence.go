package redshift

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters       map[string]*Cluster               `json:"clusters"`
	ReservedNodes  map[string]*ReservedNode          `json:"reservedNodes"`
	Partners       map[string]*Partner               `json:"partners"`
	DataShares     map[string]*DataShare             `json:"dataShares"`
	SecurityGroups map[string]*ClusterSecurityGroup  `json:"securityGroups"`
	Snapshots      map[string]*Snapshot              `json:"snapshots"`
	EndpointAuths  map[string]*EndpointAuthorization `json:"endpointAuths"`
	ActiveResizes  map[string]*ResizeProgress        `json:"activeResizes"`
	AccountID      string                            `json:"accountID"`
	Region         string                            `json:"region"`
}

func (s *backendSnapshot) ensureNonNilMaps() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}
	if s.ReservedNodes == nil {
		s.ReservedNodes = make(map[string]*ReservedNode)
	}
	if s.Partners == nil {
		s.Partners = make(map[string]*Partner)
	}
	if s.DataShares == nil {
		s.DataShares = make(map[string]*DataShare)
	}
	if s.SecurityGroups == nil {
		s.SecurityGroups = make(map[string]*ClusterSecurityGroup)
	}
	if s.Snapshots == nil {
		s.Snapshots = make(map[string]*Snapshot)
	}
	if s.EndpointAuths == nil {
		s.EndpointAuths = make(map[string]*EndpointAuthorization)
	}
	if s.ActiveResizes == nil {
		s.ActiveResizes = make(map[string]*ResizeProgress)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:       b.clusters,
		ReservedNodes:  b.reservedNodes,
		Partners:       b.partners,
		DataShares:     b.dataShares,
		SecurityGroups: b.securityGroups,
		Snapshots:      b.snapshots,
		EndpointAuths:  b.endpointAuths,
		ActiveResizes:  b.activeResizes,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("redshift: failed to marshal snapshot", "error", err)

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

	b.clusters = snap.Clusters
	b.reservedNodes = snap.ReservedNodes
	b.partners = snap.Partners
	b.dataShares = snap.DataShares
	b.securityGroups = snap.SecurityGroups
	b.snapshots = snap.Snapshots
	b.endpointAuths = snap.EndpointAuths
	b.activeResizes = snap.ActiveResizes
	b.accountID = snap.AccountID
	b.region = snap.Region

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
