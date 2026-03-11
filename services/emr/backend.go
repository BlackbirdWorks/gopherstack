package emr

import (
	"fmt"
	"maps"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
)

// ClusterStatus holds the status fields for a Cluster.
type ClusterStatus struct {
	StateChangeReason map[string]any `json:"StateChangeReason,omitempty"`
	Timeline          map[string]any `json:"Timeline,omitempty"`
	State             string         `json:"State"`
}

// Tag is an EMR resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Cluster represents an EMR cluster.
type Cluster struct {
	Status       ClusterStatus `json:"Status"`
	ID           string        `json:"Id"`
	Name         string        `json:"Name"`
	ARN          string        `json:"ClusterArn"`
	ReleaseLabel string        `json:"ReleaseLabel"`
	Tags         []Tag         `json:"Tags,omitempty"`
}

// ClusterSummary is a trimmed-down view used for ListClusters.
type ClusterSummary struct {
	ID           string        `json:"Id"`
	Name         string        `json:"Name"`
	Status       ClusterStatus `json:"Status"`
	ClusterArn   string        `json:"ClusterArn"`
	ReleaseLabel string        `json:"ReleaseLabel"`
}

// InMemoryBackend stores EMR state in memory.
type InMemoryBackend struct {
	clusters  map[string]*Cluster
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
	counter   atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:  make(map[string]*Cluster),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("emr"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) nextID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("j-%013d", n)
}

// RunJobFlow creates a new EMR cluster.
func (b *InMemoryBackend) RunJobFlow(name, releaseLabel string, tags []Tag) (*Cluster, error) {
	b.mu.Lock("RunJobFlow")
	defer b.mu.Unlock()

	id := b.nextID()
	clusterARN := arn.Build("elasticmapreduce", b.region, b.accountID, "cluster/"+id)

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	cluster := &Cluster{
		ID:           id,
		Name:         name,
		ReleaseLabel: releaseLabel,
		ARN:          clusterARN,
		Status: ClusterStatus{
			State:             "WAITING",
			StateChangeReason: map[string]any{"Code": "USER_REQUEST", "Message": ""},
			Timeline:          map[string]any{"CreationDateTime": 0},
		},
		Tags: tagsCopy,
	}
	b.clusters[id] = cluster
	cp := *cluster

	return &cp, nil
}

// DescribeCluster returns a cluster by its ID.
func (b *InMemoryBackend) DescribeCluster(id string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[id]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
	}

	cp := *cluster

	return &cp, nil
}

// ListClusters returns all clusters as summaries.
func (b *InMemoryBackend) ListClusters() []ClusterSummary {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	list := make([]ClusterSummary, 0, len(b.clusters))

	for _, c := range b.clusters {
		list = append(list, ClusterSummary{
			ID:           c.ID,
			Name:         c.Name,
			Status:       c.Status,
			ClusterArn:   c.ARN,
			ReleaseLabel: c.ReleaseLabel,
		})
	}

	return list
}

// TerminateJobFlows marks the specified clusters as TERMINATING.
func (b *InMemoryBackend) TerminateJobFlows(ids []string) error {
	b.mu.Lock("TerminateJobFlows")
	defer b.mu.Unlock()

	for _, id := range ids {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		cluster.Status.State = "TERMINATING"
	}

	return nil
}

// AddTags adds or updates tags on a cluster identified by ARN or ID.
func (b *InMemoryBackend) AddTags(resourceID string, tags []Tag) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// RemoveTags removes tags from a cluster identified by ARN or ID.
func (b *InMemoryBackend) RemoveTags(resourceID string, tagKeys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// ListTagsForResource returns tags for a cluster identified by ARN or ID.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	out := tagsToMap(cluster.Tags)
	result := make(map[string]string, len(out))
	maps.Copy(result, out)

	return result, nil
}

// findClusterByIDOrARN looks up a cluster by either its ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findClusterByIDOrARN(idOrARN string) *Cluster {
	if c, ok := b.clusters[idOrARN]; ok {
		return c
	}

	for _, c := range b.clusters {
		if c.ARN == idOrARN {
			return c
		}
	}

	return nil
}

func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func mapToTags(m map[string]string) []Tag {
	tags := make([]Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags
}
