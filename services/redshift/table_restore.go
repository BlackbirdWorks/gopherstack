package redshift

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateTableRestoreStatus creates a table restore status entry.
func (b *InMemoryBackend) CreateTableRestoreStatus(
	clusterID, snapshotID, sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string,
) (*TableRestoreStatus, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTableRestoreStatus")
	defer b.mu.Unlock()

	restoreID := uuid.New().String()
	tr := &TableRestoreStatus{
		TableRestoreRequestID: restoreID,
		ClusterIdentifier:     clusterID,
		SnapshotIdentifier:    snapshotID,
		Status:                "IN_PROGRESS",
		SourceDatabaseName:    sourceDatabaseName,
		SourceTableName:       sourceTableName,
		TargetDatabaseName:    targetDatabaseName,
		TargetTableName:       targetTableName,
		RequestTime:           time.Now().UTC(),
	}
	b.tableRestores.Put(tr)

	cp := *tr

	return &cp, nil
}

// DescribeTableRestoreStatus returns table restore status records for a cluster.
func (b *InMemoryBackend) DescribeTableRestoreStatus(clusterID string) ([]TableRestoreStatus, error) {
	b.mu.RLock("DescribeTableRestoreStatus")
	defer b.mu.RUnlock()

	result := make([]TableRestoreStatus, 0, b.tableRestores.Len())

	for _, tr := range b.tableRestores.All() {
		if clusterID != "" && tr.ClusterIdentifier != clusterID {
			continue
		}

		cp := *tr
		result = append(result, cp)
	}

	return result, nil
}
