package redshift

import (
	"fmt"
)

// ModifyCluster modifies a cluster's node type, number of nodes, or other attributes.
func (b *InMemoryBackend) ModifyCluster(
	id string,
	nodeType string,
	numberOfNodes int,
	_ string, // masterUserPassword is accepted but not stored (in-memory backend doesn't store passwords)
	encrypted bool,
	enhancedVpcRouting bool,
) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	if nodeType != "" {
		cluster.NodeType = nodeType
	}

	if numberOfNodes > 0 {
		cluster.NumberOfNodes = numberOfNodes
	}

	if encrypted {
		cluster.Encrypted = encrypted
	}

	if enhancedVpcRouting {
		cluster.EnhancedVpcRouting = enhancedVpcRouting
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// RebootCluster initiates a reboot of the specified cluster.
func (b *InMemoryBackend) RebootCluster(id string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RebootCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cluster.Status = "rebooting"
	cp := cloneCluster(cluster)
	// Immediately set back to available for in-memory simplicity.
	cluster.Status = clusterStatusAvailable

	return &cp, nil
}

// PauseCluster pauses the specified cluster.
func (b *InMemoryBackend) PauseCluster(id string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("PauseCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cluster.Status = "paused"
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ResumeCluster resumes a paused cluster.
func (b *InMemoryBackend) ResumeCluster(id string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResumeCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cluster.Status = clusterStatusAvailable
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ResizeCluster initiates a resize of the specified cluster.
func (b *InMemoryBackend) ResizeCluster(
	id, nodeType, clusterType string,
	numberOfNodes int,
	_ bool, // classic is accepted but not used in the in-memory implementation
) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResizeCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	if nodeType != "" {
		cluster.NodeType = nodeType
	}

	if clusterType != "" {
		cluster.ClusterType = clusterType
	}

	if numberOfNodes > 0 {
		cluster.NumberOfNodes = numberOfNodes
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// RotateEncryptionKey rotates the encryption key for the specified cluster.
func (b *InMemoryBackend) RotateEncryptionKey(id string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RotateEncryptionKey")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cluster.Encrypted = true
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyClusterIamRoles modifies the IAM roles associated with a cluster.
// This in-memory implementation accepts the call without persisting IAM roles.
func (b *InMemoryBackend) ModifyClusterIamRoles(id string, _, _ []string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterIamRoles")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyClusterMaintenance modifies the maintenance settings of a cluster.
// This in-memory implementation accepts the call without persisting maintenance windows.
func (b *InMemoryBackend) ModifyClusterMaintenance(
	id, _ string,
	_ bool,
) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterMaintenance")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}
