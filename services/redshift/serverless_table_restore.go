package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const slTableRestoreStatusSucceeded = "SUCCEEDED"

// RestoreTableFromSnapshotParams holds the shared fields of
// RestoreTableFromSnapshotInput/RestoreTableFromRecoveryPointInput --
// SnapshotName and RecoveryPointID are mutually exclusive, set by the
// respective caller (see RestoreTableFromSnapshotSL/RestoreTableFromRecoveryPointSL).
// Field order/names must stay identical to slTableRestoreReq in
// handler_serverless_table_restore.go: toParams() converts between them with
// a direct struct conversion.
//
// ActivateCaseSensitiveIdentifier is accepted for wire compatibility but
// intentionally inert: this backend does not execute queries against
// restored tables, so there is no case-sensitive identifier matching to gate.
type RestoreTableFromSnapshotParams struct {
	ActivateCaseSensitiveIdentifier *bool
	NamespaceName                   string
	NewTableName                    string
	SnapshotName                    string
	RecoveryPointID                 string
	SourceDatabaseName              string
	SourceSchemaName                string
	SourceTableName                 string
	TargetDatabaseName              string
	TargetSchemaName                string
	WorkgroupName                   string
}

// RestoreTableFromSnapshotSL restores a single table from a serverless
// snapshot. NamespaceName/NewTableName/SnapshotName/SourceDatabaseName/
// SourceTableName/WorkgroupName are all required on the real
// RestoreTableFromSnapshotInput (confirmed against service-2.json).
func (b *InMemoryBackend) RestoreTableFromSnapshotSL(
	p RestoreTableFromSnapshotParams,
) (*ServerlessTableRestoreStatus, error) {
	b.mu.Lock("RestoreTableFromSnapshotSL")
	defer b.mu.Unlock()

	if _, ok := b.slSnapshots.Get(p.SnapshotName); !ok {
		return nil, fmt.Errorf("%w: snapshot %q not found", ErrServerlessSnapshotNotFound, p.SnapshotName)
	}

	return b.createTableRestoreStatusLocked(p), nil
}

// RestoreTableFromRecoveryPointSL restores a single table from a recovery
// point. NamespaceName/NewTableName/RecoveryPointID/SourceDatabaseName/
// SourceTableName/WorkgroupName are all required on the real
// RestoreTableFromRecoveryPointInput (confirmed against service-2.json).
func (b *InMemoryBackend) RestoreTableFromRecoveryPointSL(
	p RestoreTableFromSnapshotParams,
) (*ServerlessTableRestoreStatus, error) {
	b.mu.Lock("RestoreTableFromRecoveryPointSL")
	defer b.mu.Unlock()

	if _, ok := b.slRecoveryPoints.Get(p.RecoveryPointID); !ok {
		return nil, fmt.Errorf("%w: recovery point %q not found", ErrRecoveryPointNotFound, p.RecoveryPointID)
	}

	return b.createTableRestoreStatusLocked(p), nil
}

// createTableRestoreStatusLocked records a table restore request and marks it
// SUCCEEDED immediately -- this backend applies every resource change
// synchronously (matching the rest of this service, e.g. snapshots created
// instantaneously), so ProgressInMegaBytes/TotalDataInMegaBytes are honestly
// left at zero rather than fabricated (there is no real data to move).
// Callers must already hold b.mu for writing.
func (b *InMemoryBackend) createTableRestoreStatusLocked(
	p RestoreTableFromSnapshotParams,
) *ServerlessTableRestoreStatus {
	_ = p.ActivateCaseSensitiveIdentifier // no query execution to gate, see RestoreTableFromSnapshotParams doc comment

	tr := &ServerlessTableRestoreStatus{
		TableRestoreRequestID: uuid.New().String(),
		NamespaceName:         p.NamespaceName,
		NewTableName:          p.NewTableName,
		SnapshotName:          p.SnapshotName,
		RecoveryPointID:       p.RecoveryPointID,
		SourceDatabaseName:    p.SourceDatabaseName,
		SourceSchemaName:      p.SourceSchemaName,
		SourceTableName:       p.SourceTableName,
		TargetDatabaseName:    p.TargetDatabaseName,
		TargetSchemaName:      p.TargetSchemaName,
		WorkgroupName:         p.WorkgroupName,
		Status:                slTableRestoreStatusSucceeded,
		RequestTime:           time.Now(),
	}
	b.slTableRestoreStatuses.Put(tr)
	b.slTableRestoreStatusIdx.insert(tr.TableRestoreRequestID)

	cp := *tr

	return &cp
}

// GetTableRestoreStatusSL returns a table restore request's status by ID.
func (b *InMemoryBackend) GetTableRestoreStatusSL(tableRestoreRequestID string) (*ServerlessTableRestoreStatus, error) {
	b.mu.RLock("GetTableRestoreStatusSL")
	defer b.mu.RUnlock()

	tr, ok := b.slTableRestoreStatuses.Get(tableRestoreRequestID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: table restore request %q not found",
			ErrTableRestoreSLNotFound,
			tableRestoreRequestID,
		)
	}

	cp := *tr

	return &cp, nil
}

// ListTableRestoreStatusSL returns table restore requests, optionally
// filtered by namespaceName/workgroupName.
func (b *InMemoryBackend) ListTableRestoreStatusSL(
	namespaceName, workgroupName string, maxResults int, nextToken string,
) ([]*ServerlessTableRestoreStatus, string) {
	b.mu.RLock("ListTableRestoreStatusSL")
	defer b.mu.RUnlock()

	keys := b.slTableRestoreStatusIdx.ordered()
	list := make([]*ServerlessTableRestoreStatus, 0, len(keys))

	for _, id := range keys {
		tr, ok := b.slTableRestoreStatuses.Get(id)
		if !ok {
			continue
		}

		if namespaceName != "" && tr.NamespaceName != namespaceName {
			continue
		}

		if workgroupName != "" && tr.WorkgroupName != workgroupName {
			continue
		}

		cp := *tr
		list = append(list, &cp)
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessTableRestoreStatus{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
