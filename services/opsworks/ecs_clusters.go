package opsworks

import (
	"strings"
	"time"
)

// RegisterEcsCluster registers an ECS cluster with a stack. EcsClusterArn
// and StackId are both "This member is required" on the real
// RegisterEcsClusterInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_RegisterEcsCluster.go).
func (b *InMemoryBackend) RegisterEcsCluster(ecsClusterArn, stackID string) (string, error) {
	if ecsClusterArn == "" || stackID == "" {
		return "", ErrValidation
	}

	b.mu.Lock("RegisterEcsCluster")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return "", ErrStackNotFound
	}

	name := ecsClusterArn
	if idx := strings.LastIndex(ecsClusterArn, "/"); idx >= 0 {
		name = ecsClusterArn[idx+1:]
	}

	b.ecsClusters.Put(&storedEcsCluster{
		RegisteredAt:   time.Now().UTC(),
		EcsClusterArn:  ecsClusterArn,
		EcsClusterName: name,
		StackID:        stackID,
		Status:         ecsClusterStatusRegistered,
	})

	return ecsClusterArn, nil
}

// DeregisterEcsCluster removes a registered ECS cluster.
func (b *InMemoryBackend) DeregisterEcsCluster(ecsClusterArn string) error {
	b.mu.Lock("DeregisterEcsCluster")
	defer b.mu.Unlock()

	if !b.ecsClusters.Delete(ecsClusterArn) {
		return ErrEcsClusterNotFound
	}

	return nil
}

// DescribeEcsClusters returns ECS clusters filtered by stack or ARN list.
func (b *InMemoryBackend) DescribeEcsClusters(stackID string, ecsClusterArns []string) ([]*EcsCluster, error) {
	b.mu.RLock("DescribeEcsClusters")
	defer b.mu.RUnlock()

	if len(ecsClusterArns) > 0 {
		result := make([]*EcsCluster, 0, len(ecsClusterArns))
		for _, clusterArn := range ecsClusterArns {
			e, ok := b.ecsClusters.Get(clusterArn)
			if !ok {
				return nil, ErrEcsClusterNotFound
			}
			result = append(result, e.toEcsCluster())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.ecsClusters.All, b.ecsClustersByStack.Get)

	result := make([]*EcsCluster, 0, len(source))
	for _, e := range source {
		result = append(result, e.toEcsCluster())
	}

	return result, nil
}
