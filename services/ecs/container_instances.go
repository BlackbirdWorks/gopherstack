package ecs

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrContainerInstanceNotFound is returned when a container instance does not exist.
var ErrContainerInstanceNotFound = awserr.New(
	"ContainerInstanceNotFoundException",
	awserr.ErrNotFound,
)

// RegisterContainerInstance registers a container instance to a cluster.
func (b *InMemoryBackend) RegisterContainerInstance(
	cluster, ec2InstanceID string,
) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("RegisterContainerInstance")
	defer b.mu.Unlock()

	b.ensureClusterLocked(clusterName)

	clusterObj, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	instanceArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:container-instance/%s/%s",
		b.region, b.accountID, clusterName, uuid.NewString(),
	)

	ci := &ContainerInstance{
		RegisteredAt:         time.Now(),
		ContainerInstanceArn: instanceArn,
		EC2InstanceID:        ec2InstanceID,
		ClusterArn:           clusterObj.ClusterArn,
		Status:               statusActive,
		AgentConnected:       true,
		Version:              1,
	}

	b.containerInstances.Put(ci)

	cp := *ci

	return &cp, nil
}

// DeregisterContainerInstance removes a container instance from a cluster.
func (b *InMemoryBackend) DeregisterContainerInstance(
	cluster, containerInstance string,
	force bool,
) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeregisterContainerInstance")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	ci, ok := b.containerInstances.Get(scopedKey(clusterName, containerInstance))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, containerInstance)
	}

	if !force {
		for _, t := range b.tasksByCluster.Get(clusterName) {
			if t.ContainerInstanceArn == containerInstance && t.LastStatus == statusRunning {
				return nil, fmt.Errorf(
					"%w: container instance has running tasks; use force=true to override",
					ErrInvalidParameter,
				)
			}
		}
	}

	b.containerInstances.Delete(scopedKey(clusterName, containerInstance))

	cp := *ci
	cp.Status = statusInactive

	return &cp, nil
}

// DescribeContainerInstances returns container instances for a cluster.
func (b *InMemoryBackend) DescribeContainerInstances(
	cluster string,
	containerInstances []string,
) ([]ContainerInstance, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeContainerInstances")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(containerInstances) == 0 {
		instances := b.containerInstancesByCluster.Get(clusterName)
		out := make([]ContainerInstance, 0, len(instances))
		for _, ci := range instances {
			out = append(out, b.enrichContainerInstance(ci, clusterName))
		}

		return out, nil, nil
	}

	out := make([]ContainerInstance, 0, len(containerInstances))
	failures := make([]Failure, 0, len(containerInstances))

	for _, ref := range containerInstances {
		ci, found := b.containerInstances.Get(scopedKey(clusterName, ref))
		if !found {
			failures = append(failures, Failure{
				Arn:    ref,
				Reason: statusMissing,
				Detail: fmt.Sprintf("container instance %s not found", ref),
			})

			continue
		}

		out = append(out, b.enrichContainerInstance(ci, clusterName))
	}

	return out, failures, nil
}

// enrichContainerInstance fills in running/pending task counts.
// Uses the tasksByInstance reverse index for O(k) lookup instead of O(n).
// Must be called with at least an RLock held.
func (b *InMemoryBackend) enrichContainerInstance(
	ci *ContainerInstance,
	clusterName string,
) ContainerInstance {
	cp := *ci

	running := 0
	pending := 0

	if instanceIndex, ok := b.tasksByInstance[clusterName]; ok {
		for taskArn := range instanceIndex[ci.ContainerInstanceArn] {
			if t, found := b.tasks.Get(taskArn); found && clusterKey(t.ClusterArn) == clusterName {
				switch t.LastStatus {
				case statusRunning:
					running++
				case statusPending, statusProvisioning:
					pending++
				}
			}
		}
	} else {
		// Fallback to linear scan when index is not populated (e.g. after restore).
		for _, t := range b.tasksByCluster.Get(clusterName) {
			if t.ContainerInstanceArn == ci.ContainerInstanceArn {
				switch t.LastStatus {
				case statusRunning:
					running++
				case statusPending, statusProvisioning:
					pending++
				}
			}
		}
	}

	cp.RunningTasksCount = running
	cp.PendingTasksCount = pending

	return cp
}

// indexTaskOnInstance adds a task to the reverse containerInstance→task index.
// Must be called with write lock held.
func (b *InMemoryBackend) indexTaskOnInstance(clusterName, instanceArn, taskArn string) {
	if instanceArn == "" {
		return
	}

	if b.tasksByInstance[clusterName] == nil {
		b.tasksByInstance[clusterName] = make(map[string]map[string]bool)
	}

	if b.tasksByInstance[clusterName][instanceArn] == nil {
		b.tasksByInstance[clusterName][instanceArn] = make(map[string]bool)
	}

	b.tasksByInstance[clusterName][instanceArn][taskArn] = true
}

// unindexTaskFromInstance removes a task from the reverse index.
// Must be called with write lock held.
func (b *InMemoryBackend) unindexTaskFromInstance(clusterName, instanceArn, taskArn string) {
	if instanceArn == "" {
		return
	}

	if index, ok := b.tasksByInstance[clusterName]; ok {
		delete(index[instanceArn], taskArn)
	}
}

// ListContainerInstances returns container instance ARNs for a cluster.
func (b *InMemoryBackend) ListContainerInstances(cluster, status string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListContainerInstances")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	instances := b.containerInstancesByCluster.Get(clusterName)
	arns := make([]string, 0, len(instances))
	for _, ci := range instances {
		if status != "" && ci.Status != status {
			continue
		}

		arns = append(arns, ci.ContainerInstanceArn)
	}

	return arns, nil
}

// UpdateContainerInstancesState updates the status of container instances.
func (b *InMemoryBackend) UpdateContainerInstancesState(
	cluster string,
	containerInstances []string,
	status string,
) ([]ContainerInstance, error) {
	switch status {
	case "ACTIVE", "DRAINING":
	default:
		return nil, fmt.Errorf(
			"%w: status must be ACTIVE or DRAINING, got %q",
			ErrInvalidParameter,
			status,
		)
	}

	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateContainerInstancesState")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	out := make([]ContainerInstance, 0, len(containerInstances))

	for _, ref := range containerInstances {
		ci, found := b.containerInstances.Get(scopedKey(clusterName, ref))
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, ref)
		}

		ci.Status = status
		ci.Version++

		cp := *ci
		out = append(out, cp)
	}

	return out, nil
}

// UpdateContainerAgent initiates an update of the container agent on the given instance.
func (b *InMemoryBackend) UpdateContainerAgent(
	cluster, containerInstance string,
) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateContainerAgent")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	ci, ok := b.containerInstances.Get(scopedKey(clusterName, containerInstance))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, containerInstance)
	}

	ci.AgentUpdateStatus = "PENDING"

	out := b.enrichContainerInstance(ci, clusterName)

	return &out, nil
}

// ListAttributes returns attributes for resources in a cluster, optionally filtered by
// attribute name, target type, and target ID.
func (b *InMemoryBackend) ListAttributes(
	cluster, targetType, attributeName, targetID string,
) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListAttributes")
	defer b.mu.RUnlock()

	clusterAttrs := b.attributes[clusterName]

	out := make([]Attribute, 0, len(clusterAttrs))

	for _, attr := range clusterAttrs {
		if attributeName != "" && attr.Name != attributeName {
			continue
		}

		if targetType != "" && attr.TargetType != targetType {
			continue
		}

		if targetID != "" && attr.TargetID != targetID {
			continue
		}

		out = append(out, *attr)
	}

	return out, nil
}

// PutAttributes creates or updates custom attributes on ECS resources in a cluster.
func (b *InMemoryBackend) PutAttributes(cluster string, attrs []Attribute) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("PutAttributes")
	defer b.mu.Unlock()

	if b.attributes[clusterName] == nil {
		b.attributes[clusterName] = make(map[string]*Attribute)
	}

	out := make([]Attribute, 0, len(attrs))

	for _, attr := range attrs {
		key := attributeKey(attr.Name, attr.TargetID)
		a := attr
		b.attributes[clusterName][key] = &a
		out = append(out, a)
	}

	return out, nil
}

// DeleteAttributes deletes custom attributes from ECS resources.
func (b *InMemoryBackend) DeleteAttributes(cluster string, attrs []Attribute) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeleteAttributes")
	defer b.mu.Unlock()

	deleted := make([]Attribute, 0, len(attrs))

	for _, attr := range attrs {
		key := attributeKey(attr.Name, attr.TargetID)

		clusterAttrs := b.attributes[clusterName]
		if clusterAttrs == nil {
			continue
		}

		if _, ok := clusterAttrs[key]; ok {
			delete(clusterAttrs, key)
			deleted = append(deleted, attr)
		}
	}

	return deleted, nil
}

// attributeKey builds the map key for an attribute.
func attributeKey(name, targetID string) string {
	return name + ":" + targetID
}

// AddAttributeInternal adds an attribute directly (seed helper for tests).
func (b *InMemoryBackend) AddAttributeInternal(cluster string, attr *Attribute) {
	b.mu.Lock("AddAttributeInternal")
	defer b.mu.Unlock()

	if b.attributes[cluster] == nil {
		b.attributes[cluster] = make(map[string]*Attribute)
	}

	key := attributeKey(attr.Name, attr.TargetID)
	b.attributes[cluster][key] = attr
}
