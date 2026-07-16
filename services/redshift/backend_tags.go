package redshift

import "fmt"

// DescribeTags returns all tags across all clusters.
func (b *InMemoryBackend) DescribeTags() map[string]map[string]string {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	all := b.clusters.All()
	result := make(map[string]map[string]string, len(all))

	for _, c := range all {
		result[c.ClusterIdentifier] = c.Tags.Clone()
	}

	return result
}

// CreateTags adds or updates tags on the specified cluster.
func (b *InMemoryBackend) CreateTags(clusterID string, kv map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	c, exists := b.clusters.Get(clusterID)
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.Merge(kv)

	return nil
}

// DeleteTags removes tag keys from the specified cluster.
func (b *InMemoryBackend) DeleteTags(clusterID string, keys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	c, exists := b.clusters.Get(clusterID)
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.DeleteKeys(keys)

	return nil
}
