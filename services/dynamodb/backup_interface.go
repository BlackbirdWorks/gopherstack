package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// CreateBackup creates a point-in-time backup of the named DynamoDB table.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) CreateBackup(
	ctx context.Context,
	input *sdkdynamodb.CreateBackupInput,
) (*sdkdynamodb.CreateBackupOutput, error) {
	tableName := aws.ToString(input.TableName)
	backupName := aws.ToString(input.BackupName)

	if tableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if backupName == "" {
		return nil, NewValidationException("BackupName is required")
	}

	region := getRegionFromContext(ctx, db)

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock("CreateBackup")
	itemsCopy := deepCopyItems(table.Items)
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	tableArn := table.TableArn
	tableID := table.TableID
	table.mu.RUnlock()

	now := time.Now()
	bkpARN := backupARN(region, db.accountID, tableName, now)
	sizeBytes := estimateTableSizeBytes(itemsCopy)

	backup := &Backup{
		BackupArn:            bkpARN,
		BackupName:           backupName,
		BackupStatus:         models.BackupStatusAvailable,
		BackupType:           models.BackupTypeUser,
		TableName:            tableName,
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

	return &sdkdynamodb.CreateBackupOutput{
		BackupDetails: &sdktypes.BackupDetails{
			BackupArn:              aws.String(bkpARN),
			BackupName:             aws.String(backupName),
			BackupStatus:           sdktypes.BackupStatusAvailable,
			BackupType:             sdktypes.BackupTypeUser,
			BackupCreationDateTime: aws.Time(now.UTC()),
			BackupSizeBytes:        aws.Int64(sizeBytes),
		},
	}, nil
}

// DeleteBackup removes an existing backup by ARN and returns its description.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) DeleteBackup(
	_ context.Context,
	input *sdkdynamodb.DeleteBackupInput,
) (*sdkdynamodb.DeleteBackupOutput, error) {
	backupArn := aws.ToString(input.BackupArn)
	if backupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	db.mu.Lock("DeleteBackup")
	backup, exists := db.Backups[backupArn]
	if !exists {
		db.mu.Unlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("backup not found: %s", backupArn))
	}

	backupCopy := *backup
	backupCopy.BackupStatus = models.BackupStatusDeleted
	delete(db.Backups, backupArn)
	db.mu.Unlock()

	return &sdkdynamodb.DeleteBackupOutput{
		BackupDescription: buildSDKBackupDescription(&backupCopy),
	}, nil
}

// buildSDKBackupDetails converts an internal Backup into SDK BackupDetails.
func buildSDKBackupDetails(b *Backup) *sdktypes.BackupDetails {
	return &sdktypes.BackupDetails{
		BackupArn:              aws.String(b.BackupArn),
		BackupName:             aws.String(b.BackupName),
		BackupStatus:           sdktypes.BackupStatus(b.BackupStatus),
		BackupType:             sdktypes.BackupType(b.BackupType),
		BackupCreationDateTime: aws.Time(b.CreationDateTime.UTC()),
		BackupSizeBytes:        aws.Int64(b.SizeBytes),
	}
}

// buildSDKSourceTableDetails converts an internal Backup into SDK SourceTableDetails.
func buildSDKSourceTableDetails(b *Backup) *sdktypes.SourceTableDetails {
	sdkKeys := make([]sdktypes.KeySchemaElement, 0, len(b.KeySchema))
	for _, ks := range b.KeySchema {
		sdkKeys = append(sdkKeys, sdktypes.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       sdktypes.KeyType(ks.KeyType),
		})
	}

	return &sdktypes.SourceTableDetails{
		TableName: aws.String(b.TableName),
		TableId:   aws.String(b.TableID),
		TableArn:  aws.String(b.TableArn),
		KeySchema: sdkKeys,
		ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(models.DefaultReadCapacity),
			WriteCapacityUnits: aws.Int64(models.DefaultWriteCapacity),
		},
		TableCreationDateTime: aws.Time(b.CreationDateTime.UTC()),
	}
}

// buildSDKBackupDescription converts an internal Backup into a full SDK BackupDescription.
func buildSDKBackupDescription(b *Backup) *sdktypes.BackupDescription {
	return &sdktypes.BackupDescription{
		BackupDetails:      buildSDKBackupDetails(b),
		SourceTableDetails: buildSDKSourceTableDetails(b),
	}
}

// BatchExecuteStatement executes multiple PartiQL statements and returns their results.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) BatchExecuteStatement(
	ctx context.Context,
	input *sdkdynamodb.BatchExecuteStatementInput,
) (*sdkdynamodb.BatchExecuteStatementOutput, error) {
	runner := &partiQLRunner{backend: db}
	responses := make([]sdktypes.BatchStatementResponse, 0, len(input.Statements))

	for _, stmt := range input.Statements {
		params := make([]map[string]any, 0, len(stmt.Parameters))

		for _, p := range stmt.Parameters {
			// models.FromSDKAttributeValue always returns map[string]any or nil.
			if wireMap, ok := models.FromSDKAttributeValue(p).(map[string]any); ok {
				params = append(params, wireMap)
			}
		}

		req := executeStatementRequest{
			Statement:  aws.ToString(stmt.Statement),
			Parameters: params,
		}

		result, err := runner.executeStatement(ctx, req)
		if err != nil {
			responses = append(responses, sdktypes.BatchStatementResponse{
				Error: &sdktypes.BatchStatementError{
					Code:    sdktypes.BatchStatementErrorCodeEnum("StatementError"),
					Message: aws.String(err.Error()),
				},
			})

			continue
		}

		resp := sdktypes.BatchStatementResponse{}
		if len(result.Items) > 0 {
			sdkItem, convErr := models.ToSDKItem(result.Items[0])
			if convErr == nil {
				resp.Item = sdkItem
			}
		}

		responses = append(responses, resp)
	}

	return &sdkdynamodb.BatchExecuteStatementOutput{
		Responses: responses,
	}, nil
}
