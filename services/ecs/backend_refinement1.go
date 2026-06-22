package ecs

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// UpdateClusterInput holds input for UpdateCluster.
type UpdateClusterInput struct {
	Cluster                         string
	CapacityProviders               []string
	DefaultCapacityProviderStrategy []CapacityProviderStrategyItem
	Settings                        []ClusterSetting
}

// UpdateCapacityProviderInput holds input for UpdateCapacityProvider.
type UpdateCapacityProviderInput struct {
	Name                     string
	Status                   string
	AutoScalingGroupProvider *AutoScalingGroupProvider
	Tags                     []Tag
}

// StartTaskInput holds input for StartTask (place a task on a specific container instance).
type StartTaskInput struct {
	Cluster            string
	TaskDefinition     string
	Group              string
	StartedBy          string
	ContainerInstances []string
}

// resourceTagKey returns the tags-map key for a resource ARN.
func resourceTagKey(resourceArn string) string { return resourceArn }

// ---- UpdateCluster ----

// UpdateCluster updates an ECS cluster's capacity providers, default strategy, or settings.
func (b *InMemoryBackend) UpdateCluster(input UpdateClusterInput) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
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

// ---- UpdateCapacityProvider ----

// UpdateCapacityProvider updates tags or status of a capacity provider.
func (b *InMemoryBackend) UpdateCapacityProvider(input UpdateCapacityProviderInput) (*CapacityProvider, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateCapacityProvider")
	defer b.mu.Unlock()

	_, cp := b.findCapacityProviderLocked(input.Name)
	if cp == nil {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderNotFound, input.Name)
	}

	if input.Status != "" {
		cp.Status = input.Status
	}

	if input.AutoScalingGroupProvider != nil {
		cp.AutoScalingGroupProvider = input.AutoScalingGroupProvider
	}

	if input.Tags != nil {
		cp.Tags = copyTags(input.Tags)
	}

	out := *cp
	out.Tags = copyTags(cp.Tags)

	return &out, nil
}

// ---- ListTaskDefinitionFamilies ----

// ListTaskDefinitionFamilies returns distinct task definition family names.
func (b *InMemoryBackend) ListTaskDefinitionFamilies(familyPrefix, status string) ([]string, error) {
	b.mu.RLock("ListTaskDefinitionFamilies")
	defer b.mu.RUnlock()

	families := make([]string, 0, len(b.taskDefinitions))

	for family, revs := range b.taskDefinitions {
		if familyPrefix != "" && !strings.HasPrefix(family, familyPrefix) {
			continue
		}

		// If a status filter is given, include the family only when at least one
		// revision matches that status.
		if status != "" {
			found := false

			for _, td := range revs {
				if strings.EqualFold(td.Status, status) {
					found = true

					break
				}
			}

			if !found {
				continue
			}
		}

		families = append(families, family)
	}

	return families, nil
}

// ---- StartTask ----

// StartTask places tasks on specific container instances (as opposed to RunTask which auto-places).
func (b *InMemoryBackend) StartTask(input StartTaskInput) ([]Task, error) {
	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	if len(input.ContainerInstances) == 0 {
		return nil, fmt.Errorf("%w: at least one container instance is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("StartTask")

	b.ensureClusterLocked(clusterName)

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	clusterArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", clusterName))

	tasks := make([]Task, 0, len(input.ContainerInstances))

	for _, ciArn := range input.ContainerInstances {
		taskID := uuid.New().String()
		taskArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("task/%s/%s", clusterName, taskID))
		now := time.Now()

		t := &Task{
			TaskArn:              taskArn,
			ClusterArn:           clusterArn,
			TaskDefinitionArn:    td.TaskDefinitionArn,
			LastStatus:           statusRunning,
			DesiredStatus:        statusRunning,
			Group:                input.Group,
			LaunchType:           "EC2",
			ContainerInstanceArn: ciArn,
			StartedBy:            input.StartedBy,
			StartedAt:            &now,
		}

		b.tasks[clusterName][taskArn] = t
		tasks = append(tasks, *t)
	}

	b.mu.Unlock()

	return tasks, nil
}

// ---- ListServicesByNamespace ----

// ListServicesByNamespace returns service ARNs in a cluster whose service name contains the namespace prefix.
func (b *InMemoryBackend) ListServicesByNamespace(cluster, namespace string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListServicesByNamespace")
	defer b.mu.RUnlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return []string{}, nil
	}

	out := make([]string, 0, len(svcs))

	for _, svc := range svcs {
		if namespace == "" || strings.Contains(svc.ServiceName, namespace) {
			out = append(out, svc.ServiceArn)
		}
	}

	return out, nil
}

// ---- Tagging ----

// TagResource applies key/value tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		b.resourceTags = make(map[string][]Tag)
	}

	existing := b.resourceTags[resourceTagKey(resourceArn)]
	// Merge: existing tags with same key are overwritten.
	tagMap := make(map[string]string, len(existing)+len(tags))

	for _, t := range existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range tags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	b.resourceTags[resourceTagKey(resourceArn)] = merged

	return nil
}

// UntagResource removes tags with the given keys from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		return nil
	}

	key := resourceTagKey(resourceArn)
	existing := b.resourceTags[key]

	if len(existing) == 0 {
		return nil
	}

	keySet := make(map[string]struct{}, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := existing[:0]

	for _, t := range existing {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[key] = filtered

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) ([]Tag, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.resourceTags == nil {
		return []Tag{}, nil
	}

	tags := b.resourceTags[resourceTagKey(resourceArn)]

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out, nil
}

// ---- ListServiceDeployments ----

// ListServiceDeployments returns service deployment ARNs for a service in a cluster.
func (b *InMemoryBackend) ListServiceDeployments(cluster, service string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListServiceDeployments")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.serviceDeployments))

	for _, sd := range b.serviceDeployments {
		if sd.ClusterArn != "" && !strings.HasSuffix(sd.ClusterArn, "/"+clusterName) {
			continue
		}

		if service != "" {
			svcKey := serviceKey(service)
			if !strings.HasSuffix(sd.ServiceArn, "/"+svcKey) {
				continue
			}
		}

		out = append(out, sd.ServiceDeploymentArn)
	}

	return out, nil
}

// ---- StopServiceDeployment ----

var errServiceDeploymentAlreadyStopped = awserr.New(
	"ServiceDeploymentAlreadyStoppedException", awserr.ErrInvalidParameter,
)

// StopServiceDeployment stops an in-progress service deployment.
func (b *InMemoryBackend) StopServiceDeployment(serviceDeploymentArn string) (*ServiceDeployment, error) {
	if serviceDeploymentArn == "" {
		return nil, fmt.Errorf("%w: serviceDeploymentArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("StopServiceDeployment")
	defer b.mu.Unlock()

	sd, ok := b.serviceDeployments[serviceDeploymentArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceDeploymentNotFound, serviceDeploymentArn)
	}

	if sd.Status == statusStopped {
		return nil, fmt.Errorf("%w: %s", errServiceDeploymentAlreadyStopped, serviceDeploymentArn)
	}

	sd.Status = statusStopped
	now := time.Now()
	sd.UpdatedAt = &now

	out := *sd

	return &out, nil
}
