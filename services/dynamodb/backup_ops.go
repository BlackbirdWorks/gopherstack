package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

	out, err := h.Backend.CreateBackup(ctx, &sdkdynamodb.CreateBackupInput{
		TableName:  &req.TableName,
		BackupName: &req.BackupName,
	})
	if err != nil {
		return nil, err
	}

	bd := out.BackupDetails

	return &models.CreateBackupOutput{
		BackupDetails: models.BackupDetails{
			BackupArn:              aws.ToString(bd.BackupArn),
			BackupName:             aws.ToString(bd.BackupName),
			BackupStatus:           string(bd.BackupStatus),
			BackupType:             string(bd.BackupType),
			BackupCreationDateTime: aws.ToTime(bd.BackupCreationDateTime).UTC().Format(time.RFC3339),
			BackupSizeBytes:        aws.ToInt64(bd.BackupSizeBytes),
		},
	}, nil
}

func (h *DynamoDBHandler) describeBackup(ctx context.Context, body []byte) (any, error) {
	var req models.DescribeBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.BackupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	out, err := h.Backend.DescribeBackup(ctx, &sdkdynamodb.DescribeBackupInput{
		BackupArn: &req.BackupArn,
	})
	if err != nil {
		return nil, err
	}

	bd := out.BackupDescription

	return &models.DescribeBackupOutput{
		BackupDescription: buildBackupDescriptionFromSDK(bd),
	}, nil
}

func (h *DynamoDBHandler) deleteBackup(ctx context.Context, body []byte) (any, error) {
	var req models.DeleteBackupInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DeleteBackup(ctx, &sdkdynamodb.DeleteBackupInput{
		BackupArn: &req.BackupArn,
	})
	if err != nil {
		return nil, err
	}

	bd := out.BackupDescription

	return &models.DeleteBackupOutput{
		BackupDescription: models.BackupDescription{
			BackupDetails: models.BackupDetails{
				BackupArn:              aws.ToString(bd.BackupDetails.BackupArn),
				BackupName:             aws.ToString(bd.BackupDetails.BackupName),
				BackupStatus:           string(bd.BackupDetails.BackupStatus),
				BackupType:             string(bd.BackupDetails.BackupType),
				BackupCreationDateTime: aws.ToTime(bd.BackupDetails.BackupCreationDateTime).UTC().Format(time.RFC3339),
				BackupSizeBytes:        aws.ToInt64(bd.BackupDetails.BackupSizeBytes),
			},
			SourceTableDetails: models.SourceTableDetails{
				TableName: aws.ToString(bd.SourceTableDetails.TableName),
				TableArn:  aws.ToString(bd.SourceTableDetails.TableArn),
				TableID:   aws.ToString(bd.SourceTableDetails.TableId),
			},
		},
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
		return nil, NewResourceNotFoundException("backup not found: " + req.BackupArn)
	}

	region := h.regionFromHandlerContext(ctx)

	db.mu.Lock("RestoreTableFromBackup")
	if _, rExists := db.Tables[region]; !rExists {
		db.Tables[region] = make(map[string]*Table)
	}

	if _, tExists := db.Tables[region][req.TargetTableName]; tExists {
		db.mu.Unlock()

		return nil, NewResourceInUseException(
			"table already exists: " + req.TargetTableName,
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

// selectPITRItems returns the items to restore for a RestoreTableToPointInTime
// call. Caller must hold sourceTable.mu.RLock.
//
// Behaviour:
//   - UseLatestRestorableTime or RestoreDateTime empty → current items.
//   - RestoreDateTime parseable → newest snapshot whose Taken <= RestoreDateTime.
//   - No matching snapshot (e.g. requested time is before the table was created
//     or the snapshot window has rotated past it) → nil, signalling the caller
//     to return a validation error.
func selectPITRItems(sourceTable *Table, req models.RestoreTableToPointInTimeInput) []map[string]any {
	if req.UseLatestRestorableTime || req.RestoreDateTime == "" {
		return deepCopyItems(sourceTable.Items)
	}

	t, err := time.Parse(time.RFC3339Nano, req.RestoreDateTime)
	if err != nil {
		// Fallback to seconds-since-epoch which the AWS SDK marshals when
		// using the JSON 1.0 number form.
		if secs, parseErr := strconv.ParseFloat(req.RestoreDateTime, 64); parseErr == nil {
			const nanosPerSec = float64(time.Second / time.Nanosecond)
			t = time.Unix(int64(secs), int64((secs-float64(int64(secs)))*nanosPerSec)).UTC()
		} else {
			return deepCopyItems(sourceTable.Items)
		}
	}

	// Newest snapshot at-or-before t. Snapshots are appended in time order so
	// scanning backwards is O(k) where k is the index from the end.
	for i := len(sourceTable.pitrSnapshots) - 1; i >= 0; i-- {
		snap := sourceTable.pitrSnapshots[i]
		if !snap.Taken.After(t) {
			return deepCopyItems(snap.Items)
		}
	}

	return nil
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
	// Pick the items snapshot to restore: when the caller asked for a specific
	// point in time, find the latest janitor snapshot at-or-before it; else
	// (UseLatestRestorableTime or no time supplied) use current items.
	itemsCopy := selectPITRItems(sourceTable, req)
	keySchema := make([]models.KeySchemaElement, len(sourceTable.KeySchema))
	copy(keySchema, sourceTable.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(sourceTable.AttributeDefinitions))
	copy(attrDefs, sourceTable.AttributeDefinitions)
	sourceTable.mu.RUnlock()

	if !pitrEnabled {
		return nil, NewValidationException(
			"point in time recovery is not enabled for table: " + req.SourceTableName,
		)
	}

	if itemsCopy == nil {
		return nil, NewValidationException(
			"requested RestoreDateTime is outside the available recovery window for table: " +
				req.SourceTableName,
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
			"table already exists: " + req.TargetTableName,
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

// buildBackupDescriptionFromSDK converts an SDK BackupDescription (as returned by the
// StorageBackend interface) into the wire-format models.BackupDescription.
func buildBackupDescriptionFromSDK(bd *sdktypes.BackupDescription) models.BackupDescription {
	if bd == nil {
		return models.BackupDescription{}
	}

	var details models.BackupDetails
	if bd.BackupDetails != nil {
		details = models.BackupDetails{
			BackupArn:              aws.ToString(bd.BackupDetails.BackupArn),
			BackupName:             aws.ToString(bd.BackupDetails.BackupName),
			BackupStatus:           string(bd.BackupDetails.BackupStatus),
			BackupType:             string(bd.BackupDetails.BackupType),
			BackupCreationDateTime: aws.ToTime(bd.BackupDetails.BackupCreationDateTime).UTC().Format(time.RFC3339),
			BackupSizeBytes:        aws.ToInt64(bd.BackupDetails.BackupSizeBytes),
		}
	}

	var src models.SourceTableDetails
	if bd.SourceTableDetails != nil {
		src = models.SourceTableDetails{
			TableName: aws.ToString(bd.SourceTableDetails.TableName),
			TableArn:  aws.ToString(bd.SourceTableDetails.TableArn),
			TableID:   aws.ToString(bd.SourceTableDetails.TableId),
		}

		// Preserve key schema from SDK representation.
		for _, ks := range bd.SourceTableDetails.KeySchema {
			src.KeySchema = append(src.KeySchema, models.KeySchemaElement{
				AttributeName: aws.ToString(ks.AttributeName),
				KeyType:       string(ks.KeyType),
			})
		}
	}

	return models.BackupDescription{
		BackupDetails:      details,
		SourceTableDetails: src,
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
