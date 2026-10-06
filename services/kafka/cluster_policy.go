package kafka

import (
	"context"
	"fmt"
)

// --- Cluster policy get/put operations ---

// GetClusterPolicy retrieves the policy document for a cluster.
// Returns ErrNotFound when the cluster exists but has no policy set — matching AWS behavior.
func (b *InMemoryBackend) GetClusterPolicy(_ context.Context, clusterArn string) (string, string, error) {
	b.mu.RLock("GetClusterPolicy")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return "", "", ErrNotFound
	}

	policy, ok := b.clusterPolicies[clusterArn]
	if !ok {
		return "", "", fmt.Errorf("no resource-based policy found for cluster %q: %w", clusterArn, ErrNotFound)
	}

	return policy, b.clusterPolicyVersions[clusterArn], nil
}

// PutClusterPolicy sets the policy document and returns its new version. A non-empty
// currentVersion must match the stored policy's version.
func (b *InMemoryBackend) PutClusterPolicy(
	_ context.Context, clusterArn, policy, currentVersion string,
) (string, error) {
	b.mu.Lock("PutClusterPolicy")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return "", ErrNotFound
	}

	if _, exists := b.clusterPolicies[clusterArn]; exists && currentVersion != "" &&
		currentVersion != b.clusterPolicyVersions[clusterArn] {
		return "", fmt.Errorf("the specified policy version is not current: %w", ErrValidation)
	}

	version := nextVersionToken()
	b.clusterPolicies[clusterArn] = policy
	b.clusterPolicyVersions[clusterArn] = version

	return version, nil
}

// DeleteClusterPolicy deletes the policy attached to an MSK cluster.
func (b *InMemoryBackend) DeleteClusterPolicy(_ context.Context, clusterArn string) error {
	b.mu.Lock("DeleteClusterPolicy")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	delete(b.clusterPolicies, clusterArn)
	delete(b.clusterPolicyVersions, clusterArn)

	return nil
}
