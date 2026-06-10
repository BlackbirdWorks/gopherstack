package rds

import (
	"context"
	"time"
)

// FlushInstanceLifecycle immediately transitions all pending instances to available.
// This is a test helper that bypasses the reconciler delay.
func FlushInstanceLifecycle(b *InMemoryBackend) {
	b.mu.Lock("FlushInstanceLifecycle")
	defer b.mu.Unlock()

	for id, inst := range b.instances {
		if inst.DBInstanceStatus == instanceStatusCreating || inst.DBInstanceStatus == instanceStatusModifying {
			inst.DBInstanceStatus = instanceStatusAvailable
		}

		delete(b.instanceReadyAt, id)
	}
}

// RDSIDFromARNForTest exposes rdsIDFromARN for unit tests.
func RDSIDFromARNForTest(arnOrID string) string {
	return rdsIDFromARN(arnOrID)
}

// InjectExpiredFaultForTest directly inserts an expired fisFailoverFault entry
// into the backend without starting a cleanup goroutine, allowing tests to
// exercise the lazy-eviction path in IsClusterFailoverActive.
func (b *InMemoryBackend) InjectExpiredFaultForTest(clusterID string) {
	b.mu.Lock("InjectExpiredFaultForTest")
	defer b.mu.Unlock()

	b.fisFailoverFaults[clusterID] = time.Now().Add(-time.Hour) // already expired
}

// ScheduleFailoverFaultCleanupForTest exposes scheduleFailoverFaultCleanup for tests.
func (b *InMemoryBackend) ScheduleFailoverFaultCleanupForTest(ctx context.Context, ids []string, dur time.Duration) {
	b.scheduleFailoverFaultCleanup(ctx, ids, dur)
}

// InstanceCount returns the number of DB instances in the backend.
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances)
}

// ClusterCount returns the number of DB clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// SnapshotCount returns the number of DB snapshots in the backend.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return len(b.snapshots)
}

// EventSubscriptionCount returns the number of event subscriptions in the backend.
func EventSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.eventSubscriptions)
}

// EventMessagesForSource returns published event messages for a source identifier.
func EventMessagesForSource(b *InMemoryBackend, sourceID string) []string {
	b.mu.RLock("EventMessagesForSource")
	defer b.mu.RUnlock()

	messages := make([]string, 0, len(b.events))
	for _, event := range b.events {
		if sourceID != "" && event.SourceIdentifier != sourceID {
			continue
		}

		messages = append(messages, event.Message)
	}

	return messages
}

// SecurityGroupCount returns the number of DB security groups in the backend.
func SecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SecurityGroupCount")
	defer b.mu.RUnlock()

	return len(b.dbSecurityGroups)
}

// BlueGreenDeploymentCount returns the number of Blue/Green Deployments in the backend.
func BlueGreenDeploymentCount(b *InMemoryBackend) int {
	b.mu.RLock("BlueGreenDeploymentCount")
	defer b.mu.RUnlock()

	return len(b.blueGreenDeployments)
}

// ClusterRoleCount returns the number of IAM roles associated with a cluster.
func ClusterRoleCount(b *InMemoryBackend, clusterID string) int {
	b.mu.RLock("ClusterRoleCount")
	defer b.mu.RUnlock()

	return len(b.clusterRoles[clusterID])
}

// InstanceRoleCount returns the number of IAM roles associated with an instance.
func InstanceRoleCount(b *InMemoryBackend, instanceID string) int {
	b.mu.RLock("InstanceRoleCount")
	defer b.mu.RUnlock()

	return len(b.instanceRoles[instanceID])
}

// HandlerOpsLen returns the number of operations listed in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// RecommendationCount returns the number of recommendations in the backend.
func RecommendationCount(b *InMemoryBackend) int {
	b.mu.RLock("RecommendationCount")
	defer b.mu.RUnlock()

	return len(b.recommendations)
}

// IntegrationCount returns the number of integrations in the backend.
func IntegrationCount(b *InMemoryBackend) int {
	b.mu.RLock("IntegrationCount")
	defer b.mu.RUnlock()

	return len(b.integrations)
}

// ShardGroupCount returns the number of shard groups in the backend.
func ShardGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ShardGroupCount")
	defer b.mu.RUnlock()

	return len(b.shardGroups)
}
