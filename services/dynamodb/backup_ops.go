package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// backupARN builds the ARN for a DynamoDB backup.
// Format: arn:aws:dynamodb:{region}:{account}:table/{table}/backup/{timestamp}.
func backupARN(region, accountID, tableName string, ts time.Time) string {
	resource := fmt.Sprintf("table/%s/backup/%016d", tableName, ts.UnixMilli())

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
	rawItems := table.Items
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	tableArn := table.TableArn
	tableID := table.TableID
	table.mu.RUnlock()

	// Deep copy items so the backup snapshot is immutable.
	itemsCopy, err := deepCopyItems(rawItems)
	if err != nil {
		return nil, NewInternalServerError(fmt.Sprintf("failed to snapshot items: %s", err.Error()))
	}

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
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("backup not found: %s", req.BackupArn))
	}

	return &models.DescribeBackupOutput{
		BackupDescription: buildBackupDescription(backup),
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

	delete(db.Backups, req.BackupArn)
	db.mu.Unlock()

	backup.BackupStatus = models.BackupStatusDeleted

	return &models.DeleteBackupOutput{
		BackupDescription: buildBackupDescription(backup),
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
	summaries := make([]models.BackupSummary, 0, len(db.Backups))

	for _, b := range db.Backups {
		if req.TableName != "" && b.TableName != req.TableName {
			continue
		}

		if req.BackupType != "" && b.BackupType != req.BackupType {
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

	db.mu.RUnlock()

	// Sort by creation time for deterministic ordering.
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].BackupCreationDateTime < summaries[j].BackupCreationDateTime
	})

	// Apply pagination limit.
	if req.Limit > 0 && len(summaries) > req.Limit {
		summaries = summaries[:req.Limit]
	}

	return &models.ListBackupsOutput{
		BackupSummaries: summaries,
	}, nil
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
	itemsCopy, err := deepCopyItems(backup.Items)
	if err != nil {
		db.mu.Unlock()

		return nil, NewInternalServerError(fmt.Sprintf("failed to copy items: %s", err.Error()))
	}

	keySchema := make([]models.KeySchemaElement, len(backup.KeySchema))
	copy(keySchema, backup.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(backup.AttributeDefinitions))
	copy(attrDefs, backup.AttributeDefinitions)

	now := time.Now()
	newTable := &Table{
		Name:                 req.TargetTableName,
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
	rawItems := sourceTable.Items
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

	// Deep copy items so the restored table is isolated from the source.
	itemsCopy, err := deepCopyItems(rawItems)
	if err != nil {
		return nil, NewInternalServerError(fmt.Sprintf("failed to copy items: %s", err.Error()))
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
	newTable := &Table{
		Name:                 req.TargetTableName,
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

// deepCopyItems returns a deep copy of DynamoDB items via JSON round-trip.
// DynamoDB attribute values may contain nested maps and lists, so a simple
// shallow copy is insufficient. The JSON round-trip handles arbitrary depth.
func deepCopyItems(items []map[string]any) ([]map[string]any, error) {
	if len(items) == 0 {
		return []map[string]any{}, nil
	}

	data, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("deep copy marshal: %w", err)
	}

	var copied []map[string]any
	err = json.Unmarshal(data, &copied)
	if err != nil {
		return nil, fmt.Errorf("deep copy unmarshal: %w", err)
	}

	return copied, nil
}
