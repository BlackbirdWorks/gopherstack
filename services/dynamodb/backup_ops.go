package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// backupARN builds the ARN for a DynamoDB backup.
// Format: arn:aws:dynamodb:{region}:{account}:table/{table}/backup/{timestamp}-{unique}.
// The unique suffix prevents ARN collisions when multiple backups are created in the same millisecond.
func backupARN(region, accountID, tableName string, ts time.Time) string {
	resource := fmt.Sprintf("table/%s/backup/%016d-%s", tableName, ts.UnixMilli(), uuid.New().String()[:16])

	return arn.Build("dynamodb", region, accountID, resource)
}

func (h *DynamoDBHandler) createBackup(ctx context.Context, body []byte) (any, error) {
	var req models.CreateBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if req.BackupName == "" {
		return nil, NewValidationException("BackupName is required")
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	region := h.regionFromHandlerContext(ctx)

	table, err := db.getTable(ctx, req.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock("CreateBackup")
	// Deep copy items inside the read lock so the snapshot is consistent
	// and races with concurrent writes are avoided.
	itemsCopy := deepCopyItems(table.Items)
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	tableArn := table.TableArn
	tableID := table.TableID
	table.mu.RUnlock()

	now := time.Now()
	bkpARN := backupARN(region, db.accountID, req.TableName, now)

	const avgItemSizeBytes = 400
	sizeBytes := int64(len(itemsCopy)) * avgItemSizeBytes

	backup := &Backup{
		BackupArn:            bkpARN,
		BackupName:           req.BackupName,
		BackupStatus:         models.BackupStatusAvailable,
		BackupType:           models.BackupTypeUser,
		TableName:            req.TableName,
		TableArn:             tableArn,
		TableID:              tableID,
		CreationDateTime:     now,
		Items:                itemsCopy,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		SizeBytes:            sizeBytes,
	}

	db.mu.Lock("CreateBackup")
	db.Backups[bkpARN] = backup
	db.mu.Unlock()

	return &models.CreateBackupOutput{
		BackupDetails: models.BackupDetails{
			BackupArn:              bkpARN,
			BackupName:             req.BackupName,
			BackupStatus:           models.BackupStatusAvailable,
			BackupType:             models.BackupTypeUser,
			BackupCreationDateTime: now.UTC().Format(time.RFC3339),
			BackupSizeBytes:        sizeBytes,
		},
	}, nil
}

func (h *DynamoDBHandler) describeBackup(_ context.Context, body []byte) (any, error) {
	var req models.DescribeBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.BackupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	db.mu.RLock("DescribeBackup")
	backup, exists := db.Backups[req.BackupArn]
	var backupCopy Backup
	if exists {
		backupCopy = *backup
	}
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("backup not found: %s", req.BackupArn))
	}

	return &models.DescribeBackupOutput{
		BackupDescription: buildBackupDescription(&backupCopy),
	}, nil
}

func (h *DynamoDBHandler) deleteBackup(_ context.Context, body []byte) (any, error) {
	var req models.DeleteBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.BackupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	db.mu.Lock("DeleteBackup")
	backup, exists := db.Backups[req.BackupArn]
	if !exists {
		db.mu.Unlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("backup not found: %s", req.BackupArn))
	}

	// Copy the struct and set deleted status before releasing lock.
	backupCopy := *backup
	backupCopy.BackupStatus = models.BackupStatusDeleted

	delete(db.Backups, req.BackupArn)
	db.mu.Unlock()

	return &models.DeleteBackupOutput{
		BackupDescription: buildBackupDescription(&backupCopy),
	}, nil
}

func (h *DynamoDBHandler) listBackups(_ context.Context, body []byte) (any, error) {
	var req models.ListBackupsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	db.mu.RLock("ListBackups")
	summaries := collectBackupSummaries(db, req.TableName, req.BackupType)
	db.mu.RUnlock()

	// Sort by creation time (then ARN) for deterministic ordering.
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].BackupCreationDateTime != summaries[j].BackupCreationDateTime {
			return summaries[i].BackupCreationDateTime < summaries[j].BackupCreationDateTime
		}

		return summaries[i].BackupArn < summaries[j].BackupArn
	})

	page, lastEvaluatedArn := paginateBackupSummaries(summaries, req.ExclusiveStartBackupArn, req.Limit)

	return &models.ListBackupsOutput{
		BackupSummaries:        page,
		LastEvaluatedBackupArn: lastEvaluatedArn,
	}, nil
}

// collectBackupSummaries gathers matching backup summaries from the in-memory store.
// Must be called while holding db.mu (read or write lock).
func collectBackupSummaries(db *InMemoryDB, tableName, backupType string) []models.BackupSummary {
	summaries := make([]models.BackupSummary, 0, len(db.Backups))

	for _, b := range db.Backups {
		if tableName != "" && b.TableName != tableName {
			continue
		}

		if backupType != "" && b.BackupType != backupType {
			continue
		}

		summaries = append(summaries, models.BackupSummary{
			BackupArn:              b.BackupArn,
			BackupName:             b.BackupName,
			BackupStatus:           b.BackupStatus,
			BackupType:             b.BackupType,
			BackupCreationDateTime: b.CreationDateTime.UTC().Format(time.RFC3339),
			TableName:              b.TableName,
			TableArn:               b.TableArn,
			TableID:                b.TableID,
		})
	}

	return summaries
}

// paginateBackupSummaries applies cursor-based pagination to a sorted backup summary list.
// It returns the page and the last-evaluated ARN (empty if no more pages).
func paginateBackupSummaries(
	summaries []models.BackupSummary,
	startArn string,
	limit int,
) ([]models.BackupSummary, string) {
	// Apply ExclusiveStartBackupArn as the starting cursor.
	start := 0
	if startArn != "" {
		for i, s := range summaries {
			if s.BackupArn == startArn {
				start = i + 1

				break
			}
		}
	}

	// Apply pagination limit relative to the starting cursor.
	end := len(summaries)
	lastEvaluatedArn := ""

	if limit > 0 && start+limit < len(summaries) {
		end = start + limit
		lastEvaluatedArn = summaries[end-1].BackupArn
	}

	if start >= len(summaries) {
		return []models.BackupSummary{}, lastEvaluatedArn
	}

	return summaries[start:end], lastEvaluatedArn
}

func (h *DynamoDBHandler) restoreTableFromBackup(ctx context.Context, body []byte) (any, error) {
	var req models.RestoreTableFromBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.BackupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	if req.TargetTableName == "" {
		return nil, NewValidationException("TargetTableName is required")
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	db.mu.RLock("RestoreTableFromBackup.lookup")
	backup, exists := db.Backups[req.BackupArn]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("backup not found: %s", req.BackupArn))
	}

	region := h.regionFromHandlerContext(ctx)

	db.mu.Lock("RestoreTableFromBackup")
	if _, rExists := db.Tables[region]; !rExists {
		db.Tables[region] = make(map[string]*Table)
	}

	if _, tExists := db.Tables[region][req.TargetTableName]; tExists {
		db.mu.Unlock()

		return nil, NewResourceInUseException(
			fmt.Sprintf("table already exists: %s", req.TargetTableName),
		)
	}

	// Deep copy items from the backup.
	itemsCopy := deepCopyItems(backup.Items)

	keySchema := make([]models.KeySchemaElement, len(backup.KeySchema))
	copy(keySchema, backup.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(backup.AttributeDefinitions))
	copy(attrDefs, backup.AttributeDefinitions)

	now := time.Now()
	newTableID := uuid.New().String()
	newTable := &Table{
		Name:                 req.TargetTableName,
		TableID:              newTableID,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		Items:                itemsCopy,
		Status:               models.TableStatusActive,
		CreationDateTime:     now,
		TableArn:             arn.Build("dynamodb", region, db.accountID, "table/"+req.TargetTableName),
		mu:                   lockmetrics.New("ddb.table." + req.TargetTableName),
		ProvisionedThroughput: models.ProvisionedThroughputDescription{
			ReadCapacityUnits:  models.DefaultReadCapacity,
			WriteCapacityUnits: models.DefaultWriteCapacity,
		},
	}
	newTable.initializeIndexes()
	newTable.rebuildIndexes()

	db.Tables[region][req.TargetTableName] = newTable
	db.mu.Unlock()

	itemCount := int64(len(itemsCopy))

	return &models.RestoreTableFromBackupOutput{
		TableDescription: models.TableDescription{
			TableName:            req.TargetTableName,
			TableStatus:          models.TableStatusActive,
			TableArn:             newTable.TableArn,
			TableID:              newTableID,
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			ItemCount:            int(itemCount),
		},
	}, nil
}

func (h *DynamoDBHandler) restoreTableToPointInTime(ctx context.Context, body []byte) (any, error) {
	var req models.RestoreTableToPointInTimeInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.SourceTableName == "" {
		return nil, NewValidationException("SourceTableName is required")
	}

	if req.TargetTableName == "" {
		return nil, NewValidationException("TargetTableName is required")
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil, NewInternalServerError("backup operations require in-memory backend")
	}

	// For PITR, look up the source table and verify PITR is enabled.
	sourceTable, err := db.getTable(ctx, req.SourceTableName)
	if err != nil {
		return nil, err
	}

	sourceTable.mu.RLock("RestoreTableToPointInTime")
	pitrEnabled := sourceTable.PITREnabled
	// Deep copy items inside the read lock to avoid races with concurrent writes.
	itemsCopy := deepCopyItems(sourceTable.Items)
	keySchema := make([]models.KeySchemaElement, len(sourceTable.KeySchema))
	copy(keySchema, sourceTable.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(sourceTable.AttributeDefinitions))
	copy(attrDefs, sourceTable.AttributeDefinitions)
	sourceTable.mu.RUnlock()

	if !pitrEnabled {
		return nil, NewValidationException(
			fmt.Sprintf("point in time recovery is not enabled for table: %s", req.SourceTableName),
		)
	}

	region := h.regionFromHandlerContext(ctx)

	db.mu.Lock("RestoreTableToPointInTime")
	if _, rExists := db.Tables[region]; !rExists {
		db.Tables[region] = make(map[string]*Table)
	}

	if _, tExists := db.Tables[region][req.TargetTableName]; tExists {
		db.mu.Unlock()

		return nil, NewResourceInUseException(
			fmt.Sprintf("table already exists: %s", req.TargetTableName),
		)
	}

	now := time.Now()
	newTableID := uuid.New().String()
	newTable := &Table{
		Name:                 req.TargetTableName,
		TableID:              newTableID,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		Items:                itemsCopy,
		Status:               models.TableStatusActive,
		CreationDateTime:     now,
		TableArn:             arn.Build("dynamodb", region, db.accountID, "table/"+req.TargetTableName),
		mu:                   lockmetrics.New("ddb.table." + req.TargetTableName),
		ProvisionedThroughput: models.ProvisionedThroughputDescription{
			ReadCapacityUnits:  models.DefaultReadCapacity,
			WriteCapacityUnits: models.DefaultWriteCapacity,
		},
	}
	newTable.initializeIndexes()
	newTable.rebuildIndexes()

	db.Tables[region][req.TargetTableName] = newTable
	db.mu.Unlock()

	return &models.RestoreTableToPointInTimeOutput{
		TableDescription: models.TableDescription{
			TableName:            req.TargetTableName,
			TableStatus:          models.TableStatusActive,
			TableArn:             newTable.TableArn,
			TableID:              newTableID,
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			ItemCount:            len(itemsCopy),
		},
	}, nil
}

// buildBackupDescription constructs a BackupDescription from an internal Backup record.
func buildBackupDescription(b *Backup) models.BackupDescription {
	return models.BackupDescription{
		BackupDetails: models.BackupDetails{
			BackupArn:              b.BackupArn,
			BackupName:             b.BackupName,
			BackupStatus:           b.BackupStatus,
			BackupType:             b.BackupType,
			BackupCreationDateTime: b.CreationDateTime.UTC().Format(time.RFC3339),
			BackupSizeBytes:        b.SizeBytes,
		},
		SourceTableDetails: models.SourceTableDetails{
			TableName: b.TableName,
			TableArn:  b.TableArn,
			TableID:   b.TableID,
			KeySchema: b.KeySchema,
		},
	}
}

// regionFromHandlerContext extracts the region from context using the regionContextKey.
func (h *DynamoDBHandler) regionFromHandlerContext(ctx context.Context) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return h.DefaultRegion
}

// deepCopyItems returns a deep copy of all provided DynamoDB items.
// DynamoDB attribute values may contain nested maps and lists, so a simple
// shallow copy is insufficient. Uses recursive map copy for efficiency.
func deepCopyItems(items []map[string]any) []map[string]any {
	if len(items) == 0 {
		return []map[string]any{}
	}

	copied := make([]map[string]any, len(items))
	for i, item := range items {
		copied[i] = deepCopyItem(item)
	}

	return copied
}
