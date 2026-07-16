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

// maxBatchExecuteStatements is the maximum number of PartiQL statements per
// BatchExecuteStatement call, matching the AWS service limit.
const maxBatchExecuteStatements = 25

// tableBackupSnapshot holds the fields captured from a Table under RLock for backup creation.
type tableBackupSnapshot struct {
	SSEType                string
	TableID                string
	Status                 string
	StreamViewType         string
	BillingMode            string
	TableArn               string
	SSEKMSMasterKeyArn     string
	KeySchema              []models.KeySchemaElement
	Items                  []map[string]any
	LocalSecondaryIndexes  []models.LocalSecondaryIndex
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex
	AttributeDefinitions   []models.AttributeDefinition
	ProvisionedThroughput  models.ProvisionedThroughputDescription
	SSEEnabled             bool
	StreamsEnabled         bool
}

func snapshotTableForBackup(table *Table) tableBackupSnapshot {
	table.mu.RLock("CreateBackup")
	defer table.mu.RUnlock()

	snap := tableBackupSnapshot{
		Items:                 deepCopyItems(table.Items),
		TableArn:              table.TableArn,
		TableID:               table.TableID,
		ProvisionedThroughput: table.ProvisionedThroughput,
		BillingMode:           table.BillingMode,
		SSEEnabled:            table.SSEEnabled,
		SSEType:               table.SSEType,
		SSEKMSMasterKeyArn:    table.SSEKMSMasterKeyArn,
		StreamsEnabled:        table.StreamsEnabled,
		StreamViewType:        table.StreamViewType,
		Status:                table.Status,
	}
	snap.KeySchema = make([]models.KeySchemaElement, len(table.KeySchema))
	copy(snap.KeySchema, table.KeySchema)
	snap.AttributeDefinitions = make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(snap.AttributeDefinitions, table.AttributeDefinitions)
	snap.GlobalSecondaryIndexes = make(
		[]models.GlobalSecondaryIndex,
		len(table.GlobalSecondaryIndexes),
	)
	copy(snap.GlobalSecondaryIndexes, table.GlobalSecondaryIndexes)
	snap.LocalSecondaryIndexes = make(
		[]models.LocalSecondaryIndex,
		len(table.LocalSecondaryIndexes),
	)
	copy(snap.LocalSecondaryIndexes, table.LocalSecondaryIndexes)

	return snap
}

// CreateBackup creates a point-in-time backup of the named DynamoDB table.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) CreateBackup(
	ctx context.Context,
	input *sdkdynamodb.CreateBackupInput,
) (*sdkdynamodb.CreateBackupOutput, error) {
	if input == nil {
		return nil, NewValidationException("CreateBackupInput must not be nil")
	}

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

	snap := snapshotTableForBackup(table)

	if snap.Status != models.TableStatusActive {
		return nil, NewValidationException(
			fmt.Sprintf(
				"table %q is not ACTIVE (status=%s); backups can only be created on ACTIVE tables",
				tableName, snap.Status,
			),
		)
	}

	// Check for duplicate backup name scoped to this table; AWS returns BackupInUseException.
	if db.duplicateBackupNameExistsRLocked(tableName, backupName) {
		return nil, NewBackupInUseException(
			fmt.Sprintf(
				"backup with name %q already exists for table %q",
				backupName,
				tableName,
			),
		)
	}

	now := time.Now()
	bkpARN := backupARN(region, db.accountID, tableName, now)
	sizeBytes := estimateTableSizeBytes(table)

	backup := &Backup{
		BackupArn: bkpARN, BackupName: backupName,
		BackupStatus: models.BackupStatusAvailable, BackupType: models.BackupTypeUser,
		TableName: tableName, TableArn: snap.TableArn, TableID: snap.TableID,
		CreationDateTime: now, Items: snap.Items,
		KeySchema: snap.KeySchema, AttributeDefinitions: snap.AttributeDefinitions,
		GlobalSecondaryIndexes: snap.GlobalSecondaryIndexes,
		LocalSecondaryIndexes:  snap.LocalSecondaryIndexes,
		ProvisionedThroughput:  snap.ProvisionedThroughput, BillingMode: snap.BillingMode,
		SSEEnabled: snap.SSEEnabled, SSEType: snap.SSEType,
		SSEKMSMasterKeyArn: snap.SSEKMSMasterKeyArn,
		StreamsEnabled:     snap.StreamsEnabled, StreamViewType: snap.StreamViewType,
		SizeBytes: sizeBytes,
	}

	db.insertBackupLocked(backup)

	return &sdkdynamodb.CreateBackupOutput{
		BackupDetails: &sdktypes.BackupDetails{
			BackupArn: aws.String(bkpARN), BackupName: aws.String(backupName),
			BackupStatus: sdktypes.BackupStatusAvailable, BackupType: sdktypes.BackupTypeUser,
			BackupCreationDateTime: aws.Time(now.UTC()), BackupSizeBytes: aws.Int64(sizeBytes),
		},
	}, nil
}

// duplicateBackupNameExistsRLocked reports whether a non-deleted backup with
// the given name already exists for tableName, under a defer-protected
// db.mu.RLock.
func (db *InMemoryDB) duplicateBackupNameExistsRLocked(tableName, backupName string) bool {
	db.mu.RLock("CreateBackup.checkDuplicate")
	defer db.mu.RUnlock()

	for _, existing := range db.backups.All() {
		if existing.TableName == tableName && existing.BackupName == backupName &&
			existing.BackupStatus != models.BackupStatusDeleted {
			return true
		}
	}

	return false
}

// insertBackupLocked stores backup and evicts the oldest entries beyond
// maxBackupsRetained, under a defer-protected db.mu.Lock.
func (db *InMemoryDB) insertBackupLocked(backup *Backup) {
	db.mu.Lock("CreateBackup")
	defer db.mu.Unlock()

	db.backups.Put(backup)
	evictOldestFromTable(
		db.backups,
		maxBackupsRetained,
		backupKeyFn,
		func(b *Backup) time.Time { return b.CreationDateTime },
	)
}

// DescribeBackup returns the full description of a backup by ARN.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) DescribeBackup(
	ctx context.Context,
	input *sdkdynamodb.DescribeBackupInput,
) (*sdkdynamodb.DescribeBackupOutput, error) {
	if input == nil {
		return nil, NewValidationException("DescribeBackupInput must not be nil")
	}

	backupArn := aws.ToString(input.BackupArn)
	if backupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	requestRegion := getRegionFromContext(ctx, db)
	if db.regionFromARN(backupArn) != requestRegion {
		return nil, NewResourceNotFoundException("backup not found: " + backupArn)
	}

	backupCopy, exists := db.backupCopyRLocked(backupArn)
	if !exists {
		return nil, NewResourceNotFoundException("backup not found: " + backupArn)
	}

	return &sdkdynamodb.DescribeBackupOutput{
		BackupDescription: buildSDKBackupDescription(&backupCopy),
	}, nil
}

// backupCopyRLocked returns a copy of the backup stored under backupArn (and
// whether it exists) under a defer-protected db.mu.RLock.
func (db *InMemoryDB) backupCopyRLocked(backupArn string) (Backup, bool) {
	db.mu.RLock("DescribeBackup")
	defer db.mu.RUnlock()

	backup, exists := db.backups.Get(backupArn)
	if !exists {
		return Backup{}, false
	}

	return *backup, true
}

// DeleteBackup removes an existing backup by ARN and returns its description.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) DeleteBackup(
	ctx context.Context,
	input *sdkdynamodb.DeleteBackupInput,
) (*sdkdynamodb.DeleteBackupOutput, error) {
	if input == nil {
		return nil, NewValidationException("DeleteBackupInput must not be nil")
	}

	backupArn := aws.ToString(input.BackupArn)
	if backupArn == "" {
		return nil, NewValidationException("BackupArn is required")
	}

	requestRegion := getRegionFromContext(ctx, db)
	if db.regionFromARN(backupArn) != requestRegion {
		return nil, NewResourceNotFoundException("backup not found: " + backupArn)
	}

	db.mu.Lock("DeleteBackup")
	defer db.mu.Unlock()

	backup, exists := db.backups.Get(backupArn)
	if !exists {
		return nil, NewResourceNotFoundException("backup not found: " + backupArn)
	}

	backupCopy := *backup
	backupCopy.BackupStatus = models.BackupStatusDeleted
	db.backups.Delete(backupArn)

	return &sdkdynamodb.DeleteBackupOutput{
		BackupDescription: buildSDKBackupDescription(&backupCopy),
	}, nil
}

// buildSDKBackupDetails converts an internal Backup into SDK BackupDetails.
// Returns nil if b is nil.
func buildSDKBackupDetails(b *Backup) *sdktypes.BackupDetails {
	if b == nil {
		return nil
	}

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
// Returns nil if b is nil.
func buildSDKSourceTableDetails(b *Backup) *sdktypes.SourceTableDetails {
	if b == nil {
		return nil
	}

	sdkKeys := make([]sdktypes.KeySchemaElement, 0, len(b.KeySchema))
	for _, ks := range b.KeySchema {
		sdkKeys = append(sdkKeys, sdktypes.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       sdktypes.KeyType(ks.KeyType),
		})
	}

	// Use the actual provisioned throughput captured at backup creation time.
	readCU := int64(b.ProvisionedThroughput.ReadCapacityUnits)
	if readCU == 0 {
		readCU = models.DefaultReadCapacity
	}

	writeCU := int64(b.ProvisionedThroughput.WriteCapacityUnits)
	if writeCU == 0 {
		writeCU = models.DefaultWriteCapacity
	}

	return &sdktypes.SourceTableDetails{
		TableName: aws.String(b.TableName),
		TableId:   aws.String(b.TableID),
		TableArn:  aws.String(b.TableArn),
		KeySchema: sdkKeys,
		ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCU),
			WriteCapacityUnits: aws.Int64(writeCU),
		},
		TableCreationDateTime: aws.Time(b.CreationDateTime.UTC()),
	}
}

// buildSDKBackupDescription converts an internal Backup into a full SDK BackupDescription.
// Returns nil if b is nil.
func buildSDKBackupDescription(b *Backup) *sdktypes.BackupDescription {
	if b == nil {
		return nil
	}

	return &sdktypes.BackupDescription{
		BackupDetails:      buildSDKBackupDetails(b),
		SourceTableDetails: buildSDKSourceTableDetails(b),
	}
}

// BatchExecuteStatement executes multiple PartiQL statements and returns their results.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
//
// AWS limit: at most maxBatchExecuteStatements statements per call.
// The ConsistentRead flag on each statement is forwarded to the underlying
// Query / Scan execution so strongly-consistent reads are honoured.
func (db *InMemoryDB) BatchExecuteStatement(
	ctx context.Context,
	input *sdkdynamodb.BatchExecuteStatementInput,
) (*sdkdynamodb.BatchExecuteStatementOutput, error) {
	if input == nil {
		return nil, NewValidationException("BatchExecuteStatementInput must not be nil")
	}

	if len(input.Statements) > maxBatchExecuteStatements {
		return nil, NewValidationException(
			fmt.Sprintf("too many statements: %d exceeds the limit of %d",
				len(input.Statements), maxBatchExecuteStatements),
		)
	}

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
			Statement:      aws.ToString(stmt.Statement),
			Parameters:     params,
			ConsistentRead: aws.ToBool(stmt.ConsistentRead),
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
			// BatchExecuteStatement returns at most one item per statement (AWS spec).
			// INSERT/UPDATE/DELETE return no item; SELECT returns the first matching item.
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
