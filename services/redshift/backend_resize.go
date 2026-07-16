package redshift

import "fmt"

// CancelResize cancels an active resize operation for a cluster and returns the final resize status.
func (b *InMemoryBackend) CancelResize(clusterID string) (*ResizeProgress, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelResize")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	resize, exists := b.activeResizes[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: no active resize for cluster %s", ErrResizeNotFound, clusterID)
	}

	if !resize.AllowCancelResize {
		return nil, fmt.Errorf(
			"%w: resize for cluster %s cannot be cancelled at this stage",
			ErrResizeNotCancellable,
			clusterID,
		)
	}

	cp := *resize
	cp.Status = resizeStatusCancelled
	delete(b.activeResizes, clusterID)

	return &cp, nil
}

// DescribeResize returns the active resize progress for a cluster.
func (b *InMemoryBackend) DescribeResize(clusterID string) (*ResizeProgress, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeResize")
	defer b.mu.RUnlock()

	rp, exists := b.activeResizes[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: no active resize for cluster %s", ErrResizeNotFound, clusterID)
	}

	cp := *rp
	cp.ImportTablesCompleted = make([]string, len(rp.ImportTablesCompleted))
	copy(cp.ImportTablesCompleted, rp.ImportTablesCompleted)
	cp.ImportTablesInProgress = make([]string, len(rp.ImportTablesInProgress))
	copy(cp.ImportTablesInProgress, rp.ImportTablesInProgress)
	cp.ImportTablesNotStarted = make([]string, len(rp.ImportTablesNotStarted))
	copy(cp.ImportTablesNotStarted, rp.ImportTablesNotStarted)

	return &cp, nil
}

// AddActiveResizeInternal seeds an active resize directly into the backend.
func (b *InMemoryBackend) AddActiveResizeInternal(clusterID string, resize *ResizeProgress) {
	b.mu.Lock("AddActiveResizeInternal")
	defer b.mu.Unlock()
	b.activeResizes[clusterID] = resize
}
