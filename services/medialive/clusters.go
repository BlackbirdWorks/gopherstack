package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Cluster operations ---

// CreateCluster creates a new Cluster.
func (b *InMemoryBackend) CreateCluster(
	name, clusterType, instanceRoleArn string,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if clusterType == "" {
		clusterType = "ON_PREMISES"
	}

	id := newID()
	c := &storedCluster{
		ARN:             b.clusterARN(id),
		ID:              id,
		Name:            name,
		ClusterType:     clusterType,
		InstanceRoleArn: instanceRoleArn,
		State:           clusterStateActive,
		Tags:            copyTags(tags),
		Nodes:           make(map[string]*storedNode),
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	b.clusters.Put(c)

	return c.toCluster(), nil
}

// DescribeCluster returns a Cluster by ID.
func (b *InMemoryBackend) DescribeCluster(clusterID string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return c.toCluster(), nil
}

// UpdateCluster updates a Cluster's mutable fields.
func (b *InMemoryBackend) UpdateCluster(clusterID, name string) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if name != "" {
		c.Name = name
	}

	return c.toCluster(), nil
}

// DeleteCluster deletes a Cluster.
func (b *InMemoryBackend) DeleteCluster(clusterID string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	c.State = clusterStateDeleted
	b.clusters.Delete(clusterID)

	return c.toCluster(), nil
}

// ListClusters returns a paginated list of Clusters.
func (b *InMemoryBackend) ListClusters(
	maxResults int,
	nextToken string,
) ([]*ClusterSummary, string, error) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	all := b.clusters.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ClusterSummary, 0, len(pg.Data))
	for _, c := range pg.Data {
		summaries = append(summaries, c.toSummary())
	}

	return summaries, pg.Next, nil
}

// ListClusterAlerts returns alerts for a Cluster.
func (b *InMemoryBackend) ListClusterAlerts(
	clusterID string,
	_ int,
	_ string,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListClusterAlerts")
	defer b.mu.RUnlock()

	cl, ok := b.clusters.Get(clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	// Field names/casing here mirror the real ClusterAlert shape
	// (id/alertType/message/state/setTimestamp -- verified against
	// aws-sdk-go-v2/service/medialive's ClusterAlert deserializer); there
	// is no "AlertCode"/"AlertMessage"/"SetTime"/"ClearedTime" on the real
	// wire.
	var alerts []map[string]any
	if cl.State != clusterStateActive {
		alerts = []map[string]any{
			{
				keyID:           "cluster-not-ready",
				"alertType":     "CLUSTER_NOT_READY",
				keyLowerMessage: "Cluster is not in ACTIVE state",
				keyState:        "SET",
				"setTimestamp":  formatISO8601(time.Unix(0, 0).UTC()),
			},
		}
	} else {
		alerts = []map[string]any{}
	}

	return alerts, "", nil
}
