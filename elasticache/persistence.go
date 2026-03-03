package elasticache

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// clusterSnapshot captures the serialisable fields of a Cluster (omits the miniredis instance).
type clusterSnapshot struct {
	CreatedAt     time.Time  `json:"createdAt"`
	Tags          *tags.Tags `json:"tags,omitempty"`
	ClusterID     string     `json:"clusterID"`
	Engine        string     `json:"engine"`
	EngineVersion string     `json:"engineVersion"`
	Status        string     `json:"status"`
	Endpoint      string     `json:"endpoint"`
	NodeType      string     `json:"nodeType"`
	ARN           string     `json:"arn"`
	Port          int        `json:"port"`
	NumCacheNodes int        `json:"numCacheNodes"`
}

type backendSnapshot struct {
	Clusters          map[string]*clusterSnapshot  `json:"clusters"`
	ReplicationGroups map[string]*ReplicationGroup `json:"replicationGroups"`
	EngineMode        string                       `json:"engineMode"`
	AccountID         string                       `json:"accountID"`
	Region            string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	clusters := make(map[string]*clusterSnapshot, len(b.clusters))
	for k, c := range b.clusters {
		clusters[k] = &clusterSnapshot{
			CreatedAt:     c.CreatedAt,
			Tags:          c.Tags,
			ClusterID:     c.ClusterID,
			Engine:        c.Engine,
			EngineVersion: c.EngineVersion,
			Status:        c.Status,
			Endpoint:      c.Endpoint,
			NodeType:      c.NodeType,
			ARN:           c.ARN,
			Port:          c.Port,
			NumCacheNodes: c.NumCacheNodes,
		}
	}

	snap := backendSnapshot{
		Clusters:          clusters,
		ReplicationGroups: b.replicationGroups,
		EngineMode:        b.engineMode,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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
		snap.Clusters = make(map[string]*clusterSnapshot)
	}

	if snap.ReplicationGroups == nil {
		snap.ReplicationGroups = make(map[string]*ReplicationGroup)
	}

	clusters := make(map[string]*Cluster, len(snap.Clusters))
	for k, cs := range snap.Clusters {
		clusters[k] = &Cluster{
			CreatedAt:     cs.CreatedAt,
			Tags:          cs.Tags,
			ClusterID:     cs.ClusterID,
			Engine:        cs.Engine,
			EngineVersion: cs.EngineVersion,
			Status:        cs.Status,
			Endpoint:      cs.Endpoint,
			NodeType:      cs.NodeType,
			ARN:           cs.ARN,
			Port:          cs.Port,
			NumCacheNodes: cs.NumCacheNodes,
		}
	}

	b.clusters = clusters
	b.replicationGroups = snap.ReplicationGroups
	b.engineMode = snap.EngineMode
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
