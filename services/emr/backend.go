package emr

import (
	"fmt"
	"maps"
	"sync/atomic"
	"time"

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

const (
	// StateWaiting is the initial cluster state after creation.
	StateWaiting = "WAITING"
	// StateTerminated is the final cluster state after termination.
	StateTerminated = "TERMINATED"
	// StateTerminatedWithErrors is the final cluster state for a failed termination.
	StateTerminatedWithErrors = "TERMINATED_WITH_ERRORS"

	// Timeline keys used in ClusterStatus.Timeline.
	timelineKeyCreation = "CreationDateTime"
	timelineKeyEnd      = "EndDateTime"
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

// InstanceGroupStatus is the status of an EMR instance group.
type InstanceGroupStatus struct {
	State string `json:"State"`
}

// InstanceGroupSpec is the input specification for an instance group from RunJobFlow.
type InstanceGroupSpec struct {
	Name          string `json:"Name"`
	Market        string `json:"Market"`
	InstanceRole  string `json:"InstanceRole"`
	InstanceType  string `json:"InstanceType"`
	InstanceCount int    `json:"InstanceCount"`
}

// InstanceGroup represents an EMR instance group returned by ListInstanceGroups.
type InstanceGroup struct {
	Status                 InstanceGroupStatus `json:"Status"`
	ID                     string              `json:"Id"`
	Name                   string              `json:"Name"`
	Market                 string              `json:"Market"`
	InstanceGroupType      string              `json:"InstanceGroupType"`
	InstanceType           string              `json:"InstanceType"`
	RequestedInstanceCount int                 `json:"RequestedInstanceCount"`
	RunningInstanceCount   int                 `json:"RunningInstanceCount"`
}

// EC2InstanceAttributes represents EC2 instance attributes for an EMR cluster.
// Fields are omitted because the in-memory backend does not simulate EC2 networking.
// The struct must be non-nil in DescribeCluster responses to prevent a nil-pointer
// panic in the Terraform provider's flattenEC2InstanceAttributes function.
type EC2InstanceAttributes struct{}

// Cluster represents an EMR cluster.
type Cluster struct {
	TerminatedAt          time.Time              `json:"TerminatedAt,omitzero"`
	Status                ClusterStatus          `json:"Status"`
	ID                    string                 `json:"Id"`
	Name                  string                 `json:"Name"`
	ARN                   string                 `json:"ClusterArn"`
	ReleaseLabel          string                 `json:"ReleaseLabel"`
	Ec2InstanceAttributes *EC2InstanceAttributes `json:"Ec2InstanceAttributes"`
	Tags                  []Tag                  `json:"Tags,omitempty"`
	instanceGroups        []InstanceGroup        // not serialized — returned only by ListInstanceGroups
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
func (b *InMemoryBackend) RunJobFlow(
	name, releaseLabel string,
	tags []Tag,
	specs []InstanceGroupSpec,
) (*Cluster, error) {
	b.mu.Lock("RunJobFlow")
	defer b.mu.Unlock()

	id := b.nextID()
	clusterNum := b.counter.Load()
	clusterARN := arn.Build("elasticmapreduce", b.region, b.accountID, "cluster/"+id)

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	groups := make([]InstanceGroup, 0, len(specs))
	for i, spec := range specs {
		market := spec.Market
		if market == "" {
			market = "ON_DEMAND"
		}

		groups = append(groups, InstanceGroup{
			ID:                     fmt.Sprintf("ig-%013d%d", clusterNum, i),
			Name:                   spec.Name,
			Market:                 market,
			InstanceGroupType:      spec.InstanceRole,
			InstanceType:           spec.InstanceType,
			RequestedInstanceCount: spec.InstanceCount,
			RunningInstanceCount:   spec.InstanceCount,
			Status:                 InstanceGroupStatus{State: "RUNNING"},
		})
	}

	cluster := &Cluster{
		ID:                    id,
		Name:                  name,
		ReleaseLabel:          releaseLabel,
		ARN:                   clusterARN,
		Ec2InstanceAttributes: &EC2InstanceAttributes{},
		Status: ClusterStatus{
			State:             StateWaiting,
			StateChangeReason: map[string]any{"Code": "USER_REQUEST", "Message": ""},
			Timeline:          map[string]any{timelineKeyCreation: 0},
		},
		Tags:           tagsCopy,
		instanceGroups: groups,
	}
	b.clusters[id] = cluster
	cp := cluster.clone()

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

	cp := cluster.clone()

	return &cp, nil
}

// clone returns a deep copy of the Cluster.
func (c Cluster) clone() Cluster {
	cp := c

	if c.Tags != nil {
		cp.Tags = make([]Tag, len(c.Tags))
		copy(cp.Tags, c.Tags)
	}

	if c.instanceGroups != nil {
		cp.instanceGroups = make([]InstanceGroup, len(c.instanceGroups))
		copy(cp.instanceGroups, c.instanceGroups)
	}

	cp.Status.StateChangeReason = maps.Clone(c.Status.StateChangeReason)
	cp.Status.Timeline = maps.Clone(c.Status.Timeline)

	return cp
}

// ListClusters returns all active (non-terminated) clusters as summaries.
func (b *InMemoryBackend) ListClusters() []ClusterSummary {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	list := make([]ClusterSummary, 0, len(b.clusters))

	for _, c := range b.clusters {
		if c.Status.State == StateTerminated || c.Status.State == StateTerminatedWithErrors {
			continue
		}

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

// TerminateJobFlows marks the specified clusters as TERMINATED and removes them from active listing.
// Clusters that are already in a terminal state are silently skipped, matching AWS behavior.
func (b *InMemoryBackend) TerminateJobFlows(ids []string) error {
	b.mu.Lock("TerminateJobFlows")
	defer b.mu.Unlock()

	for _, id := range ids {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		// AWS is idempotent: terminating an already-terminal cluster is a no-op.
		if cluster.Status.State == StateTerminated || cluster.Status.State == StateTerminatedWithErrors {
			continue
		}

		now := time.Now()
		cluster.Status.State = StateTerminated
		cluster.Status.Timeline[timelineKeyEnd] = now.UnixMilli()
		cluster.TerminatedAt = now
	}

	return nil
}

// ListInstanceGroups returns the instance groups for a cluster by its ID.
func (b *InMemoryBackend) ListInstanceGroups(clusterID string) ([]InstanceGroup, error) {
	b.mu.RLock("ListInstanceGroups")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	groups := make([]InstanceGroup, len(cluster.instanceGroups))
	copy(groups, cluster.instanceGroups)

	return groups, nil
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.counter.Store(0)
}
