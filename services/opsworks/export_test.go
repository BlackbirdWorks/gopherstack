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
