package memorydb

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters                   map[string]map[string]*Cluster                `json:"clusters"`
	ACLs                       map[string]map[string]*ACL                   `json:"acls"`
	SubnetGroups               map[string]map[string]*SubnetGroup           `json:"subnetGroups"`
	Users                      map[string]map[string]*User                  `json:"users"`
	ParameterGroups            map[string]map[string]*ParameterGroup        `json:"parameterGroups"`
	Snapshots                  map[string]map[string]*Snapshot              `json:"snapshots"`
	MultiRegionClusters        map[string]*MultiRegionCluster               `json:"multiRegionClusters"`
	MultiRegionParameterGroups map[string]*MultiRegionParameterGroup        `json:"multiRegionParameterGroups"`
	ReservedNodes              map[string]map[string]*ReservedNode          `json:"reservedNodes"`
	ARNToResource              map[string]map[string]resourceRef            `json:"arnToResource"`
	ServiceUpdates             map[string]*ServiceUpdate                    `json:"serviceUpdates"`
	Events                     map[string][]*Event                          `json:"events"`
	AccountID                  string                                       `json:"accountID"`
	DefaultRegion              string                                       `json:"defaultRegion"`
}

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
		ReservedNodes:              b.reservedNodes,
		Events:                     b.events,
		ARNToResource:              b.arnToResource,
		ServiceUpdates:             b.serviceUpdates,
		AccountID:                  b.accountID,
		DefaultRegion:              b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("memorydb: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

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
	b.reservedNodes = snap.ReservedNodes
	b.events = snap.Events
	b.arnToResource = snap.ARNToResource
	b.serviceUpdates = snap.ServiceUpdates
	b.accountID = snap.AccountID
	b.defaultRegion = snap.DefaultRegion

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]map[string]*Cluster)
	}
	if snap.ACLs == nil {
		snap.ACLs = make(map[string]map[string]*ACL)
	}
	if snap.SubnetGroups == nil {
		snap.SubnetGroups = make(map[string]map[string]*SubnetGroup)
	}
	if snap.Users == nil {
		snap.Users = make(map[string]map[string]*User)
	}
	if snap.ParameterGroups == nil {
		snap.ParameterGroups = make(map[string]map[string]*ParameterGroup)
	}
	if snap.Snapshots == nil {
		snap.Snapshots = make(map[string]map[string]*Snapshot)
	}
	if snap.MultiRegionClusters == nil {
		snap.MultiRegionClusters = make(map[string]*MultiRegionCluster)
	}
	if snap.MultiRegionParameterGroups == nil {
		snap.MultiRegionParameterGroups = make(map[string]*MultiRegionParameterGroup)
	}
	if snap.ReservedNodes == nil {
		snap.ReservedNodes = make(map[string]map[string]*ReservedNode)
	}
	if snap.ARNToResource == nil {
		snap.ARNToResource = make(map[string]map[string]resourceRef)
	}
	if snap.ServiceUpdates == nil {
		snap.ServiceUpdates = make(map[string]*ServiceUpdate)
	}
	if snap.Events == nil {
		snap.Events = make(map[string][]*Event)
	}
}

func fixNilTagsInSnapshot(snap *backendSnapshot) {
	fixCoreResourceTags(snap)
	fixExtendedResourceTags(snap)
}

func fixCoreResourceTags(snap *backendSnapshot) {
	for _, regionClusters := range snap.Clusters {
		for _, c := range regionClusters {
			if c.Tags == nil {
				c.Tags = make(map[string]string)
			}
		}
	}
	for _, regionACLs := range snap.ACLs {
		for _, a := range regionACLs {
			if a.Tags == nil {
				a.Tags = make(map[string]string)
			}
		}
	}
	for _, regionSGs := range snap.SubnetGroups {
		for _, sg := range regionSGs {
			if sg.Tags == nil {
				sg.Tags = make(map[string]string)
			}
		}
	}
	for _, regionUsers := range snap.Users {
		for _, u := range regionUsers {
			if u.Tags == nil {
				u.Tags = make(map[string]string)
			}
		}
	}
}

func fixExtendedResourceTags(snap *backendSnapshot) {
	for _, regionPGs := range snap.ParameterGroups {
		for _, pg := range regionPGs {
			if pg.Tags == nil {
				pg.Tags = make(map[string]string)
			}
			if pg.Parameters == nil {
				pg.Parameters = make(map[string]string)
			}
		}
	}
	for _, regionSnaps := range snap.Snapshots {
		for _, s := range regionSnaps {
			if s.Tags == nil {
				s.Tags = make(map[string]string)
			}
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
		if mrpg.Parameters == nil {
			mrpg.Parameters = make(map[string]string)
		}
	}
}

func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
