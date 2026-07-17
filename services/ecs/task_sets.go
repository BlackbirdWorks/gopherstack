package ecs

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrTaskSetNotFound is returned when a task set does not exist.
var ErrTaskSetNotFound = awserr.New("TaskSetNotFoundException", awserr.ErrNotFound)

const defaultTaskSetScaleValue = 100.0

// CreateTaskSet creates a task set within a service.
func (b *InMemoryBackend) CreateTaskSet(input CreateTaskSetInput) (*TaskSet, error) {
	if input.Service == "" {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidParameter)
	}

	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("CreateTaskSet")
	defer b.mu.Unlock()

	svcKey := serviceKey(input.Service)

	svc, ok := b.services.Get(scopedKey(clusterName, svcKey))
	if !ok {
		if !b.clusters.Has(clusterName) {
			return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
		}

		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, input.Service)
	}

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()[:8]
	taskSetArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:task-set/%s/%s/ecs-svc-%s",
		b.region, b.accountID, clusterName, svcKey, id,
	)

	launchType := input.LaunchType
	if launchType == "" {
		launchType = launchTypeFargate
	}

	platformVersion := input.PlatformVersion
	if platformVersion == "" {
		platformVersion = platformVersionLatest
	}

	scale := TaskSetScale{Unit: "PERCENT", Value: defaultTaskSetScaleValue}
	if input.Scale != nil {
		scale = *input.Scale
	}

	now := time.Now()
	ts := &TaskSet{
		TaskSetArn: taskSetArn,
		ID:         "ecs-svc-" + id,
		ServiceArn: svc.ServiceArn,
		ClusterArn: arn.Build(
			"ecs",
			b.region,
			b.accountID,
			fmt.Sprintf("cluster/%s", clusterName),
		),
		TaskDefinition:       td.TaskDefinitionArn,
		Status:               statusActive,
		ExternalID:           input.ExternalID,
		PlatformVersion:      platformVersion,
		LaunchType:           launchType,
		Scale:                scale,
		StabilityStatus:      "STEADY_STATE",
		StabilityStatusAt:    now,
		CreatedAt:            now,
		UpdatedAt:            now,
		LoadBalancers:        input.LoadBalancers,
		ServiceRegistries:    input.ServiceRegistries,
		NetworkConfiguration: input.NetworkConfiguration,
	}

	b.taskSets.Put(ts)

	cp := *ts

	return &cp, nil
}

// DeleteTaskSet removes a task set.
func (b *InMemoryBackend) DeleteTaskSet(cluster, service, taskSet string) (*TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeleteTaskSet")
	defer b.mu.Unlock()

	svcKey := serviceKey(service)

	svc, ok := b.services.Get(scopedKey(clusterName, svcKey))
	if !ok {
		if !b.clusters.Has(clusterName) {
			return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
		}

		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	ts, ok := b.taskSets.Get(scopedKey(svc.ServiceArn, taskSet))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	b.taskSets.Delete(scopedKey(svc.ServiceArn, taskSet))

	cp := *ts

	return &cp, nil
}

// DescribeTaskSets returns task sets for a service.
func (b *InMemoryBackend) DescribeTaskSets(
	cluster, service string,
	taskSets []string,
) ([]TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeTaskSets")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := b.services.Get(scopedKey(clusterName, svcKey))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	sets := b.taskSetsByService.Get(svc.ServiceArn)

	if len(taskSets) == 0 {
		out := make([]TaskSet, 0, len(sets))
		for _, ts := range sets {
			out = append(out, *ts)
		}

		return out, nil
	}

	out := make([]TaskSet, 0, len(taskSets))

	for _, ref := range taskSets {
		ts, found := b.taskSets.Get(scopedKey(svc.ServiceArn, ref))
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, ref)
		}

		out = append(out, *ts)
	}

	return out, nil
}

// UpdateTaskSet updates the scale of a task set.
func (b *InMemoryBackend) UpdateTaskSet(
	cluster, service, taskSet string,
	scale TaskSetScale,
) (*TaskSet, error) {
	if scale.Unit != "PERCENT" {
		return nil, fmt.Errorf(
			"%w: scale unit must be PERCENT, got %q",
			ErrInvalidParameter,
			scale.Unit,
		)
	}

	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateTaskSet")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := b.services.Get(scopedKey(clusterName, svcKey))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	ts, ok := b.taskSets.Get(scopedKey(svc.ServiceArn, taskSet))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	ts.Scale = scale
	ts.UpdatedAt = time.Now()

	cp := *ts

	return &cp, nil
}

// UpdateServicePrimaryTaskSet sets the primary task set for a service.
func (b *InMemoryBackend) UpdateServicePrimaryTaskSet(
	cluster, service, primaryTaskSet string,
) (*TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateServicePrimaryTaskSet")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := b.services.Get(scopedKey(clusterName, svcKey))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	primary, found := b.taskSets.Get(scopedKey(svc.ServiceArn, primaryTaskSet))
	if !found {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, primaryTaskSet)
	}

	now := time.Now()

	for _, ts := range b.taskSetsByService.Get(svc.ServiceArn) {
		if ts.TaskSetArn == primaryTaskSet {
			ts.Status = "PRIMARY"
		} else {
			ts.Status = statusActive
		}

		ts.UpdatedAt = now
	}

	cp := *primary

	return &cp, nil
}
