package ecs

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// resolveCluster returns the cluster ARN/name to use, defaulting to "default".
func (b *InMemoryBackend) resolveCluster(cluster string) string {
	if cluster == "" {
		return defaultCluster
	}

	return cluster
}

// clusterKey extracts the cluster name from either a full ARN or a bare name.
func clusterKey(clusterRef string) string {
	if !strings.HasPrefix(clusterRef, "arn:") {
		return clusterRef
	}

	for i := len(clusterRef) - 1; i >= 0; i-- {
		if clusterRef[i] == '/' {
			return clusterRef[i+1:]
		}
	}

	return clusterRef
}

// CreateCluster creates a new ECS cluster.
func (b *InMemoryBackend) CreateCluster(input CreateClusterInput) (*Cluster, error) {
	name := input.ClusterName
	if name == "" {
		name = defaultCluster
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if b.clusters.Has(name) {
		return nil, fmt.Errorf("%w: %s", ErrClusterAlreadyExists, name)
	}

	if err := b.validateCapacityProviderStrategyLocked(input.DefaultCapacityProviderStrategy); err != nil {
		return nil, err
	}

	cluster := &Cluster{
		CreatedAt:                       time.Now(),
		ClusterArn:                      arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", name)),
		ClusterName:                     name,
		Status:                          statusActive,
		Settings:                        input.Settings,
		CapacityProviders:               input.CapacityProviders,
		DefaultCapacityProviderStrategy: input.DefaultCapacityProviderStrategy,
	}
	b.clusters.Put(cluster)

	if len(input.Tags) > 0 {
		b.setResourceTagsLocked(cluster.ClusterArn, input.Tags)
	}

	cp := *cluster

	return &cp, nil
}

// ListClusters returns all clusters.
func (b *InMemoryBackend) ListClusters() ([]Cluster, error) {
	clusters, _, err := b.DescribeClusters(nil)

	return clusters, err
}

// DescribeClusters returns cluster metadata.
// Unknown cluster names are returned as failures, not errors, matching AWS behaviour.
func (b *InMemoryBackend) DescribeClusters(clusterNames []string) ([]Cluster, []Failure, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if len(clusterNames) == 0 {
		all := b.clusters.All()
		out := make([]Cluster, 0, len(all))
		for _, c := range all {
			out = append(out, b.enrichCluster(c))
		}

		return out, nil, nil
	}

	out := make([]Cluster, 0, len(clusterNames))
	failures := make([]Failure, 0, len(clusterNames))

	for _, name := range clusterNames {
		key := clusterKey(name)

		c, ok := b.clusters.Get(key)
		if !ok {
			failures = append(failures, Failure{
				Arn:    name,
				Reason: statusMissing,
				Detail: fmt.Sprintf("cluster %s not found", name),
			})

			continue
		}

		out = append(out, b.enrichCluster(c))
	}

	return out, failures, nil
}

// enrichCluster fills in runtime-computed counts for a cluster.
// Must be called with at least an RLock held.
// Running and pending task counts are cached on the cluster struct and updated
// incrementally at each state transition to avoid an O(n) task scan here.
func (b *InMemoryBackend) enrichCluster(c *Cluster) Cluster {
	cp := *c

	cp.ActiveServicesCount = len(b.servicesByCluster.Get(c.ClusterName))
	cp.RegisteredContainerInstancesCount = len(b.containerInstancesByCluster.Get(c.ClusterName))

	// RunningTasksCount and PendingTasksCount are maintained as cached counters
	// on the Cluster struct. No task iteration needed here.

	return cp
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(clusterName string) (*Cluster, error) {
	key := clusterKey(clusterName)

	var (
		found       bool
		cp          Cluster
		tasksToStop []*Task
	)

	// Snapshot task pointers while still holding the lock so we can stop their
	// Docker containers after releasing it.  Performing Docker API calls under
	// the backend lock would unnecessarily serialize all other operations.
	func() {
		b.mu.Lock("DeleteCluster")
		defer b.mu.Unlock()

		c, ok := b.clusters.Get(key)
		if !ok {
			return
		}

		found = true

		clusterTasks := b.tasksInClusterLocked(key)
		tasksToStop = make([]*Task, 0, len(clusterTasks))

		if b.runner != nil {
			tasksToStop = append(tasksToStop, clusterTasks...)
		}

		// Delete task sets and service deployments for all services in this cluster
		// before removing the services map, preventing stale entries on cluster recreation.
		for _, svc := range b.servicesInClusterLocked(key) {
			b.deleteTaskSetsForServiceLocked(svc.ServiceArn)
			b.deleteServiceDeploymentsForServiceLocked(svc.ServiceArn)
			delete(b.serviceIndex, svcRef{cluster: key, name: svc.ServiceName})
		}

		// Clean up per-task state for all tasks in this cluster to avoid memory leaks.
		for _, task := range clusterTasks {
			b.taskProtections.Delete(task.TaskArn)
			delete(b.lifecycle, task.TaskArn)
		}

		b.clusters.Delete(key)
		b.deleteResourceTagsLocked(c.ClusterArn)
		b.deleteServicesForClusterLocked(key)
		b.deleteTasksForClusterLocked(key)
		b.deleteContainerInstancesForClusterLocked(key)
		delete(b.attributes, key)
		delete(b.tasksByInstance, key)

		cp = *c
	}()

	if !found {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	// Docker API calls happen outside the lock so other backend operations are
	// not serialized behind potentially slow container stops.
	for _, task := range tasksToStop {
		_ = b.runner.StopTask(task)
	}

	// Notify hooks (e.g. reconciler semaphore eviction) after releasing the lock.
	b.fireClusterDeleteHooks(key)

	return &cp, nil
}

// ensureClusterLocked returns the cluster maps, auto-creating the default cluster if needed.
// Must be called with write lock held.
func (b *InMemoryBackend) ensureClusterLocked(clusterName string) {
	if !b.clusters.Has(clusterName) && clusterName == defaultCluster {
		b.clusters.Put(&Cluster{
			CreatedAt: time.Now(),
			ClusterArn: arn.Build(
				"ecs",
				b.region,
				b.accountID,
				fmt.Sprintf("cluster/%s", clusterName),
			),
			ClusterName: clusterName,
			Status:      statusActive,
		})
	}
}

// PutClusterCapacityProviders associates capacity providers with a cluster.
func (b *InMemoryBackend) PutClusterCapacityProviders(
	cluster string,
	capacityProviders []string,
	defaultCapacityProviderStrategy []CapacityProviderStrategyItem,
) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("PutClusterCapacityProviders")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if err := b.validateCapacityProviderStrategyLocked(defaultCapacityProviderStrategy); err != nil {
		return nil, err
	}

	c.CapacityProviders = capacityProviders
	c.DefaultCapacityProviderStrategy = defaultCapacityProviderStrategy

	cp := b.enrichCluster(c)

	return &cp, nil
}

// UpdateClusterSettings updates the settings for an ECS cluster.
func (b *InMemoryBackend) UpdateClusterSettings(
	cluster string,
	settings []ClusterSetting,
) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateClusterSettings")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	c.Settings = settings

	cp := b.enrichCluster(c)

	return &cp, nil
}

// UpdateCluster updates an ECS cluster's capacity providers, default strategy, or settings.
func (b *InMemoryBackend) UpdateCluster(input UpdateClusterInput) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	if input.DefaultCapacityProviderStrategy != nil {
		if err := b.validateCapacityProviderStrategyLocked(input.DefaultCapacityProviderStrategy); err != nil {
			return nil, err
		}
	}

	if input.CapacityProviders != nil {
		c.CapacityProviders = input.CapacityProviders
	}

	if input.DefaultCapacityProviderStrategy != nil {
		c.DefaultCapacityProviderStrategy = input.DefaultCapacityProviderStrategy
	}

	if input.Settings != nil {
		c.Settings = input.Settings
	}

	cp := b.enrichCluster(c)

	return &cp, nil
}
