package redshift

import (
	"fmt"
	"sort"
)

// ModifyCluster modifies a cluster's attributes.
// When applyImmediately is false, changes are stored in PendingModifiedValues
// and returned without being applied to the live cluster.
//
// encrypted and enhancedVpcRouting are tri-state (nil means "not specified,
// leave unchanged"): real ModifyClusterInput.Encrypted/EnhancedVpcRouting are
// *bool, and the SDK can explicitly send "false" to disable either setting
// (e.g. to decrypt a cluster). A plain bool cannot distinguish "not sent"
// from "explicitly false", which previously made it impossible to ever turn
// either setting off via ModifyCluster.
func (b *InMemoryBackend) ModifyCluster(
	id string,
	nodeType string,
	numberOfNodes int,
	_ string, // masterUserPassword is accepted but not stored
	encrypted *bool,
	enhancedVpcRouting *bool,
	applyImmediately bool,
) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	if !applyImmediately {
		pending := &ClusterPendingModifiedValues{}
		if nodeType != "" {
			pending.NodeType = nodeType
		}

		if numberOfNodes > 0 {
			pending.NumberOfNodes = numberOfNodes
		}

		if encrypted != nil {
			pending.Encrypted = *encrypted
		}

		cluster.PendingModifiedValues = pending
		cp := cloneCluster(cluster)

		return &cp, nil
	}

	if nodeType != "" {
		cluster.NodeType = nodeType
	}

	if numberOfNodes > 0 {
		cluster.NumberOfNodes = numberOfNodes
	}

	if encrypted != nil {
		cluster.Encrypted = *encrypted
	}

	if enhancedVpcRouting != nil {
		cluster.EnhancedVpcRouting = *enhancedVpcRouting
	}

	cluster.PendingModifiedValues = nil
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

	cluster, exists := b.clusters.Get(id)
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

	cluster, exists := b.clusters.Get(id)
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

	cluster, exists := b.clusters.Get(id)
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

	cluster, exists := b.clusters.Get(id)
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

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cluster.Encrypted = true
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyClusterIamRoles adds and removes IAM roles on a cluster.
func (b *InMemoryBackend) ModifyClusterIamRoles(id string, addRoles, removeRoles []string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterIamRoles")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	// Build a set of current roles for O(1) lookup.
	roleSet := make(map[string]struct{}, len(cluster.IamRoles))
	for _, r := range cluster.IamRoles {
		roleSet[r] = struct{}{}
	}

	for _, r := range addRoles {
		roleSet[r] = struct{}{}
	}

	for _, r := range removeRoles {
		delete(roleSet, r)
	}

	roles := make([]string, 0, len(roleSet))
	for r := range roleSet {
		roles = append(roles, r)
	}

	sort.Strings(roles)
	cluster.IamRoles = roles

	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyClusterMaintenance modifies the maintenance settings of a cluster.
func (b *InMemoryBackend) ModifyClusterMaintenance(
	id, maintenanceTrack string,
	_ bool,
) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterMaintenance")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	if maintenanceTrack != "" {
		cluster.PreferredMaintenanceWindow = maintenanceTrack
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}
