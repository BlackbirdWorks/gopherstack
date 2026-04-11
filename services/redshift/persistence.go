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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}
	if snap.ReservedNodes == nil {
		snap.ReservedNodes = make(map[string]*ReservedNode)
	}
	if snap.Partners == nil {
		snap.Partners = make(map[string]*Partner)
	}
	if snap.DataShares == nil {
		snap.DataShares = make(map[string]*DataShare)
	}
	if snap.SecurityGroups == nil {
		snap.SecurityGroups = make(map[string]*ClusterSecurityGroup)
	}
	if snap.Snapshots == nil {
		snap.Snapshots = make(map[string]*Snapshot)
	}
	if snap.EndpointAuths == nil {
		snap.EndpointAuths = make(map[string]*EndpointAuthorization)
	}
	if snap.ActiveResizes == nil {
		snap.ActiveResizes = make(map[string]*ResizeProgress)
	}

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
