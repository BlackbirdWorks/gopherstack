package redshift

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// defaultSchemaName is RestoreTableFromClusterSnapshotInput.SourceSchemaName/
// TargetSchemaName's documented default ("If you do not specify a
// SourceSchemaName value, the default is public").
const defaultSchemaName = "public"

// CreateTableRestoreStatus creates a table restore status entry.
// sourceSchemaName/targetSchemaName mirror real
// RestoreTableFromClusterSnapshotInput.SourceSchemaName/TargetSchemaName
// (redshift@v1.65.4 api_op_RestoreTableFromClusterSnapshot.go);
// types.TableRestoreStatus.SourceSchemaName/TargetSchemaName echo them back
// on the wire.
func (b *InMemoryBackend) CreateTableRestoreStatus(
	clusterID, snapshotID, sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string,
	sourceSchemaName, targetSchemaName string,
) (*TableRestoreStatus, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if sourceSchemaName == "" {
		sourceSchemaName = defaultSchemaName
	}

	if targetSchemaName == "" {
		targetSchemaName = defaultSchemaName
	}

	b.mu.Lock("CreateTableRestoreStatus")
	defer b.mu.Unlock()

	restoreID := uuid.New().String()
	tr := &TableRestoreStatus{
		TableRestoreRequestID: restoreID,
		ClusterIdentifier:     clusterID,
		SnapshotIdentifier:    snapshotID,
		Status:                tableRestoreStatusInProgress,
		SourceDatabaseName:    sourceDatabaseName,
		SourceSchemaName:      sourceSchemaName,
		SourceTableName:       sourceTableName,
		TargetDatabaseName:    targetDatabaseName,
		TargetSchemaName:      targetSchemaName,
		TargetTableName:       targetTableName,
		RequestTime:           time.Now().UTC(),
	}
	b.tableRestores.Put(tr)
	b.tableRestoreReadyAt[restoreID] = time.Now().UTC().Add(tableRestoreCompletionDelay)

	cp := *tr

	return &cp, nil
}

// DescribeTableRestoreStatus returns table restore status records for a cluster.
func (b *InMemoryBackend) DescribeTableRestoreStatus(clusterID string) ([]TableRestoreStatus, error) {
	b.advanceTableRestoreStates(time.Now())

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
