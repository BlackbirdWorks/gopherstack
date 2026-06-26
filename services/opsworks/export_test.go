package opsworks

import "sort"

// HandlerDispatchKeys returns the sorted operation keys from the handler's dispatch table.
func HandlerDispatchKeys(h *Handler) []string {
	keys := make([]string, 0, len(h.ops))
	for k := range h.ops {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// StackCount returns the number of stored stacks.
func StackCount(b *InMemoryBackend) int {
	b.mu.RLock("StackCount")
	defer b.mu.RUnlock()

	return len(b.stacks)
}

// LayerCount returns the number of stored layers.
func LayerCount(b *InMemoryBackend) int {
	b.mu.RLock("LayerCount")
	defer b.mu.RUnlock()

	return len(b.layers)
}

// InstanceCount returns the number of stored instances.
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances)
}

// AppCount returns the number of stored apps.
func AppCount(b *InMemoryBackend) int {
	b.mu.RLock("AppCount")
	defer b.mu.RUnlock()

	return len(b.apps)
}

// DeploymentCount returns the number of stored deployments.
func DeploymentCount(b *InMemoryBackend) int {
	b.mu.RLock("DeploymentCount")
	defer b.mu.RUnlock()

	return len(b.deployments)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// UserProfileCount returns the number of stored user profiles.
func UserProfileCount(b *InMemoryBackend) int {
	b.mu.RLock("UserProfileCount")
	defer b.mu.RUnlock()

	return len(b.userProfiles)
}

// ElasticLBCount returns the number of stored elastic load balancers.
func ElasticLBCount(b *InMemoryBackend) int {
	b.mu.RLock("ElasticLBCount")
	defer b.mu.RUnlock()

	return len(b.elasticLBs)
}

// ElasticIPCount returns the number of stored elastic IPs.
func ElasticIPCount(b *InMemoryBackend) int {
	b.mu.RLock("ElasticIPCount")
	defer b.mu.RUnlock()

	return len(b.elasticIPs)
}

// VolumeCount returns the number of stored volumes.
func VolumeCount(b *InMemoryBackend) int {
	b.mu.RLock("VolumeCount")
	defer b.mu.RUnlock()

	return len(b.volumes)
}

// RdsDBInstanceCount returns the number of stored RDS DB instances.
func RdsDBInstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("RdsDBInstanceCount")
	defer b.mu.RUnlock()

	return len(b.rdsDBInstances)
}

// EcsClusterCount returns the number of stored ECS clusters.
func EcsClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("EcsClusterCount")
	defer b.mu.RUnlock()

	return len(b.ecsClusters)
}
