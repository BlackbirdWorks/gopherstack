package memorydb

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters                   map[string]*Cluster                   `json:"clusters"`
	ACLs                       map[string]*ACL                       `json:"acls"`
	SubnetGroups               map[string]*SubnetGroup               `json:"subnetGroups"`
	Users                      map[string]*User                      `json:"users"`
	ParameterGroups            map[string]*ParameterGroup            `json:"parameterGroups"`
	Snapshots                  map[string]*Snapshot                  `json:"snapshots"`
	MultiRegionClusters        map[string]*MultiRegionCluster        `json:"multiRegionClusters"`
	MultiRegionParameterGroups map[string]*MultiRegionParameterGroup `json:"multiRegionParameterGroups"`
	ARNToResource              map[string]resourceRef                `json:"arnToResource"`
	AccountID                  string                                `json:"accountID"`
	Region                     string                                `json:"region"`
	Events                     []*Event                              `json:"events"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:                   b.clusters,
		ACLs:                       b.acls,
		SubnetGroups:               b.subnetGroups,
		Users:                      b.users,
		ParameterGroups:            b.parameterGroups,
		Snapshots:                  b.snapshots,
		MultiRegionClusters:        b.multiRegionClusters,
		MultiRegionParameterGroups: b.multiRegionParameterGroups,
		Events:                     b.events,
		ARNToResource:              b.arnToResource,
		AccountID:                  b.accountID,
		Region:                     b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("memorydb: failed to marshal snapshot", "error", err)

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
	fixNilTagsInSnapshot(&snap)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.acls = snap.ACLs
	b.subnetGroups = snap.SubnetGroups
	b.users = snap.Users
	b.parameterGroups = snap.ParameterGroups
	b.snapshots = snap.Snapshots
	b.multiRegionClusters = snap.MultiRegionClusters
	b.multiRegionParameterGroups = snap.MultiRegionParameterGroups
	b.events = snap.Events
	b.arnToResource = snap.ARNToResource
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	if snap.ACLs == nil {
		snap.ACLs = make(map[string]*ACL)
	}

	if snap.SubnetGroups == nil {
		snap.SubnetGroups = make(map[string]*SubnetGroup)
	}

	if snap.Users == nil {
		snap.Users = make(map[string]*User)
	}

	if snap.ParameterGroups == nil {
		snap.ParameterGroups = make(map[string]*ParameterGroup)
	}

	if snap.Snapshots == nil {
		snap.Snapshots = make(map[string]*Snapshot)
	}

	if snap.MultiRegionClusters == nil {
		snap.MultiRegionClusters = make(map[string]*MultiRegionCluster)
	}

	if snap.MultiRegionParameterGroups == nil {
		snap.MultiRegionParameterGroups = make(map[string]*MultiRegionParameterGroup)
	}

	if snap.ARNToResource == nil {
		snap.ARNToResource = make(map[string]resourceRef)
	}

	if snap.Events == nil {
		snap.Events = []*Event{}
	}
}

// fixNilTagsInSnapshot ensures all restored resources have non-nil tag maps.
func fixNilTagsInSnapshot(snap *backendSnapshot) {
	for _, c := range snap.Clusters {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
	}

	for _, a := range snap.ACLs {
		if a.Tags == nil {
			a.Tags = make(map[string]string)
		}
	}

	for _, sg := range snap.SubnetGroups {
		if sg.Tags == nil {
			sg.Tags = make(map[string]string)
		}
	}

	for _, u := range snap.Users {
		if u.Tags == nil {
			u.Tags = make(map[string]string)
		}
	}

	for _, pg := range snap.ParameterGroups {
		if pg.Tags == nil {
			pg.Tags = make(map[string]string)
		}

		if pg.Parameters == nil {
			pg.Parameters = make(map[string]string)
		}
	}

	for _, s := range snap.Snapshots {
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}
	}

	for _, mrc := range snap.MultiRegionClusters {
		if mrc.Tags == nil {
			mrc.Tags = make(map[string]string)
		}
	}

	for _, mrpg := range snap.MultiRegionParameterGroups {
		if mrpg.Tags == nil {
			mrpg.Tags = make(map[string]string)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
