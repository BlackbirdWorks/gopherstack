package elasticache

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// clusterSnapshot captures the serialisable fields of a Cluster (omits the miniredis instance).
type clusterSnapshot struct {
	CreatedAt                  time.Time  `json:"createdAt"`
	Tags                       *tags.Tags `json:"tags,omitempty"`
	ClusterID                  string     `json:"clusterID"`
	Engine                     string     `json:"engine"`
	EngineVersion              string     `json:"engineVersion"`
	Status                     string     `json:"status"`
	Endpoint                   string     `json:"endpoint"`
	NodeType                   string     `json:"nodeType"`
	ARN                        string     `json:"arn"`
	CacheParameterGroupName    string     `json:"cacheParameterGroupName,omitempty"`
	PreferredMaintenanceWindow string     `json:"preferredMaintenanceWindow,omitempty"`
	SnapshotWindow             string     `json:"snapshotWindow,omitempty"`
	Port                       int        `json:"port"`
	NumCacheNodes              int        `json:"numCacheNodes"`
}

type backendSnapshot struct {
	Clusters                  map[string]*clusterSnapshot             `json:"clusters"`
	ReplicationGroups         map[string]*ReplicationGroup            `json:"replicationGroups"`
	ParameterGroups           map[string]*CacheParameterGroup         `json:"parameterGroups"`
	SubnetGroups              map[string]*CacheSubnetGroup            `json:"subnetGroups"`
	Snapshots                 map[string]*CacheSnapshot               `json:"snapshots"`
	CacheSecurityGroups       map[string]*CacheSecurityGroup          `json:"cacheSecurityGroups,omitempty"`
	CacheSecurityGroupIngress map[string][]EC2SecurityGroupMembership `json:"cacheSecurityGroupIngress,omitempty"`
	GlobalReplicationGroups   map[string]*GlobalReplicationGroup      `json:"globalReplicationGroups,omitempty"`
	ServerlessCaches          map[string]*ServerlessCache             `json:"serverlessCaches,omitempty"`
	ServerlessCacheSnapshots  map[string]*ServerlessCacheSnapshot     `json:"serverlessCacheSnapshots,omitempty"`
	Users                     map[string]*User                        `json:"users,omitempty"`
	UserGroups                map[string]*UserGroup                   `json:"userGroups,omitempty"`
	ReservedCacheNodes        map[string]*ReservedCacheNode           `json:"reservedCacheNodes,omitempty"`
	EngineMode                string                                  `json:"engineMode"`
	AccountID                 string                                  `json:"accountID"`
	Region                    string                                  `json:"region"`
	Events                    []CacheEvent                            `json:"events,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	clusters := make(map[string]*clusterSnapshot, len(b.clusters))
	for k, c := range b.clusters {
		clusters[k] = &clusterSnapshot{
			CreatedAt:                  c.CreatedAt,
			Tags:                       c.Tags,
			ClusterID:                  c.ClusterID,
			Engine:                     c.Engine,
			EngineVersion:              c.EngineVersion,
			Status:                     c.Status,
			Endpoint:                   c.Endpoint,
			NodeType:                   c.NodeType,
			ARN:                        c.ARN,
			CacheParameterGroupName:    c.CacheParameterGroupName,
			PreferredMaintenanceWindow: c.PreferredMaintenanceWindow,
			SnapshotWindow:             c.SnapshotWindow,
			Port:                       c.Port,
			NumCacheNodes:              c.NumCacheNodes,
		}
	}

	snap := backendSnapshot{
		Clusters:                  clusters,
		ReplicationGroups:         b.replicationGroups,
		ParameterGroups:           b.parameterGroups,
		SubnetGroups:              b.subnetGroups,
		Snapshots:                 b.snapshots,
		CacheSecurityGroups:       b.cacheSecurityGroups,
		CacheSecurityGroupIngress: b.cacheSecurityGroupIngress,
		GlobalReplicationGroups:   b.globalReplicationGroups,
		ServerlessCaches:          b.serverlessCaches,
		ServerlessCacheSnapshots:  b.serverlessCacheSnapshots,
		Users:                     b.users,
		UserGroups:                b.userGroups,
		ReservedCacheNodes:        b.reservedCacheNodes,
		Events:                    b.events.marshalJSON(),
		EngineMode:                b.engineMode,
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// restoreClusters converts the snapshot's clusterSnapshot map into Cluster objects.
func restoreClusters(snap map[string]*clusterSnapshot) map[string]*Cluster {
	clusters := make(map[string]*Cluster, len(snap))
	for k, cs := range snap {
		clusters[k] = &Cluster{
			CreatedAt:                  cs.CreatedAt,
			Tags:                       cs.Tags,
			ClusterID:                  cs.ClusterID,
			Engine:                     cs.Engine,
			EngineVersion:              cs.EngineVersion,
			Status:                     cs.Status,
			Endpoint:                   cs.Endpoint,
			NodeType:                   cs.NodeType,
			ARN:                        cs.ARN,
			CacheParameterGroupName:    cs.CacheParameterGroupName,
			PreferredMaintenanceWindow: cs.PreferredMaintenanceWindow,
			SnapshotWindow:             cs.SnapshotWindow,
			Port:                       cs.Port,
			NumCacheNodes:              cs.NumCacheNodes,
		}
	}

	return clusters
}

// restoreNewOpMaps assigns the new-ops maps from the snapshot into the backend.
func (b *InMemoryBackend) restoreNewOpMaps(snap *backendSnapshot) {
	if snap.CacheSecurityGroups != nil {
		b.cacheSecurityGroups = snap.CacheSecurityGroups
	} else {
		b.cacheSecurityGroups = make(map[string]*CacheSecurityGroup)
	}

	if snap.CacheSecurityGroupIngress != nil {
		b.cacheSecurityGroupIngress = snap.CacheSecurityGroupIngress
	} else {
		b.cacheSecurityGroupIngress = make(map[string][]EC2SecurityGroupMembership)
	}

	if snap.GlobalReplicationGroups != nil {
		b.globalReplicationGroups = snap.GlobalReplicationGroups
	} else {
		b.globalReplicationGroups = make(map[string]*GlobalReplicationGroup)
	}

	if snap.ServerlessCaches != nil {
		b.serverlessCaches = snap.ServerlessCaches
	} else {
		b.serverlessCaches = make(map[string]*ServerlessCache)
	}

	if snap.ServerlessCacheSnapshots != nil {
		b.serverlessCacheSnapshots = snap.ServerlessCacheSnapshots
	} else {
		b.serverlessCacheSnapshots = make(map[string]*ServerlessCacheSnapshot)
	}

	if snap.Users != nil {
		b.users = snap.Users
	} else {
		b.users = make(map[string]*User)
	}

	if snap.UserGroups != nil {
		b.userGroups = snap.UserGroups
	} else {
		b.userGroups = make(map[string]*UserGroup)
	}

	if snap.ReservedCacheNodes != nil {
		b.reservedCacheNodes = snap.ReservedCacheNodes
	} else {
		b.reservedCacheNodes = make(map[string]*ReservedCacheNode)
	}
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
		snap.Clusters = make(map[string]*clusterSnapshot)
	}

	if snap.ReplicationGroups == nil {
		snap.ReplicationGroups = make(map[string]*ReplicationGroup)
	}

	if snap.ParameterGroups == nil {
		snap.ParameterGroups = make(map[string]*CacheParameterGroup)
	}

	if snap.SubnetGroups == nil {
		snap.SubnetGroups = make(map[string]*CacheSubnetGroup)
	}

	if snap.Snapshots == nil {
		snap.Snapshots = make(map[string]*CacheSnapshot)
	}

	b.clusters = restoreClusters(snap.Clusters)
	b.replicationGroups = snap.ReplicationGroups
	b.parameterGroups = snap.ParameterGroups
	b.subnetGroups = snap.SubnetGroups
	b.snapshots = snap.Snapshots

	b.restoreNewOpMaps(&snap)

	b.events.restoreFromSlice(snap.Events)

	b.engineMode = snap.EngineMode
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Re-init default parameter groups if they are missing (e.g., old snapshots).
	for _, dpg := range builtinParameterGroupFamilies() {
		if _, ok := b.parameterGroups[dpg.name]; !ok {
			pg := &CacheParameterGroup{
				Name:        dpg.name,
				Family:      dpg.family,
				Description: "Default parameter group for " + dpg.family,
				ARN:         b.parameterGroupARN(dpg.name),
				IsGlobal:    true,
				Parameters:  make(map[string]string),
				Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
			}
			b.parameterGroups[dpg.name] = pg
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
