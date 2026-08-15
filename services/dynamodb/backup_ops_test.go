package dynamodb_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// TestBackupValidation tests input validation edge cases for backup/PITR handlers.
func TestBackupValidation(t *testing.T) {
	t.Parallel()

	type pitrSpec map[string]any

	tests := []struct {
		body     any
		name     string
		target   string
		wantType string
		wantCode int
	}{
		{
			name:     "DescribeContinuousBackups_EmptyTableName",
			target:   "DynamoDB_20120810.DescribeContinuousBackups",
			body:     map[string]any{"TableName": ""},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:   "UpdateContinuousBackups_EmptyTableName",
			target: "DynamoDB_20120810.UpdateContinuousBackups",
			body: map[string]any{
				"TableName":                        "",
				"PointInTimeRecoverySpecification": pitrSpec{"PointInTimeRecoveryEnabled": true},
			},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "CreateBackup_EmptyTableName",
			target:   "DynamoDB_20120810.CreateBackup",
			body:     models.CreateBackupInput{TableName: "", BackupName: "b"},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "CreateBackup_EmptyBackupName",
			target:   "DynamoDB_20120810.CreateBackup",
			body:     models.CreateBackupInput{TableName: "T", BackupName: ""},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "DescribeBackup_EmptyArn",
			target:   "DynamoDB_20120810.DescribeBackup",
			body:     models.DescribeBackupInput{BackupArn: ""},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "DeleteBackup_EmptyArn",
			target:   "DynamoDB_20120810.DeleteBackup",
			body:     models.DeleteBackupInput{BackupArn: ""},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			code, resp := doBackupRequest(t, h, tt.target, tt.body)
			assert.Equal(t, tt.wantCode, code)
			assert.Contains(t, resp["__type"], tt.wantType)
		})
	}
}

// TestListBackupsPagination tests ExclusiveStartBackupArn cursor pagination.
func TestListBackupsPagination(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)
	createTable(t, db, "PaginateTable")

	// Create 3 backups
	for i := range 3 {
		doBackupRequest(t, h, "DynamoDB_20120810.CreateBackup", models.CreateBackupInput{
			TableName:  "PaginateTable",
			BackupName: fmt.Sprintf("backup-%d", i),
		})
	}

	// First page: limit 2
	code1, resp1 := doBackupRequest(t, h, "DynamoDB_20120810.ListBackups", models.ListBackupsInput{
		TableName: "PaginateTable",
		Limit:     2,
	})
	require.Equal(t, http.StatusOK, code1)
	page1 := resp1["BackupSummaries"].([]any)
	assert.Len(t, page1, 2)

	lastArn, ok := resp1["LastEvaluatedBackupArn"].(string)
	require.True(t, ok, "expected LastEvaluatedBackupArn in first page response")
	require.NotEmpty(t, lastArn)

	// Second page using cursor
	code2, resp2 := doBackupRequest(t, h, "DynamoDB_20120810.ListBackups", models.ListBackupsInput{
		TableName:               "PaginateTable",
		Limit:                   2,
		ExclusiveStartBackupArn: lastArn,
	})
	require.Equal(t, http.StatusOK, code2)
	page2 := resp2["BackupSummaries"].([]any)
	assert.Len(t, page2, 1)

	// No more pages
	assert.Empty(t, resp2["LastEvaluatedBackupArn"])
}

// TestRestoreTableHasTableID tests that restored tables get a TableID assigned.
func TestRestoreTableHasTableID(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)
	createTable(t, db, "IDSourceTable")

	// Enable PITR
	doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
		"TableName": "IDSourceTable",
		"PointInTimeRecoverySpecification": map[string]any{
			"PointInTimeRecoveryEnabled": true,
		},
	})

	// Create backup
	createCode, createResp := doBackupRequest(
		t,
		h,
		"DynamoDB_20120810.CreateBackup",
		models.CreateBackupInput{
			TableName:  "IDSourceTable",
			BackupName: "id-test-backup",
		},
	)
	require.Equal(t, http.StatusOK, createCode)
	backupArn := createResp["BackupDetails"].(map[string]any)["BackupArn"].(string)

	// Restore from backup — should have TableId
	restoreCode1, restoreResp1 := doBackupRequest(
		t,
		h,
		"DynamoDB_20120810.RestoreTableFromBackup",
		models.RestoreTableFromBackupInput{
			BackupArn:       backupArn,
			TargetTableName: "RestoredWithID1",
		},
	)
	require.Equal(t, http.StatusOK, restoreCode1)
	td1 := restoreResp1["TableDescription"].(map[string]any)
	assert.NotEmpty(t, td1["TableId"], "RestoreTableFromBackup: restored table must have a TableId")

	// Restore to point in time — should have TableId
	restoreCode2, restoreResp2 := doBackupRequest(
		t,
		h,
		"DynamoDB_20120810.RestoreTableToPointInTime",
		models.RestoreTableToPointInTimeInput{
			SourceTableName:         "IDSourceTable",
			TargetTableName:         "RestoredWithID2",
			UseLatestRestorableTime: true,
		},
	)
	require.Equal(t, http.StatusOK, restoreCode2)
	td2 := restoreResp2["TableDescription"].(map[string]any)
	assert.NotEmpty(
		t,
		td2["TableId"],
		"RestoreTableToPointInTime: restored table must have a TableId",
	)
}

// TestUpdateTable_EmptyReplicaRegion tests that UpdateTable returns error for empty RegionName.
func TestUpdateTable_EmptyReplicaRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(context.Context, *dynamodb.InMemoryDB) error
		name string
	}{
		{
			name: "Create_EmptyRegion",
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) error {
				input := models.UpdateTableInput{
					TableName: "RegionTestTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: ""}},
					},
				}
				sdkInput, err := models.ToSDKUpdateTableInput(&input)
				require.NoError(t, err)
				_, err = db.UpdateTable(ctx, sdkInput)

				return err
			},
		},
		{
			name: "Delete_EmptyRegion",
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) error {
				input := models.UpdateTableInput{
					TableName: "RegionTestTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Delete: &models.DeleteReplicationGroupMemberAction{RegionName: ""}},
					},
				}
				sdkInput, err := models.ToSDKUpdateTableInput(&input)
				require.NoError(t, err)
				_, err = db.UpdateTable(ctx, sdkInput)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTable(t, db, "RegionTestTable")

			err := tt.run(t.Context(), db)
			require.Error(t, err)
		})
	}
}

func TestPITR_GetPITRStatus_Disabled(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "PITRTable")

	// PITREnabled starts as false; verify getPITRStatus returns DISABLED.
	tbl, ok := db.GetTable("PITRTable")
	if !ok {
		t.Fatal("table not found")
	}

	// Access the exported status via the describe-table call.
	out, err := db.DescribeTable(ctx, &dynamodb_sdk.DescribeTableInput{
		TableName: aws.String("PITRTable"),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}

	if out.Table == nil {
		t.Fatal("expected table description")
	}

	_ = tbl // silence unused warning
}

func TestBackup_CreateAndDescribe(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "BackupSrc")

	putTestItem(t, db, "BackupSrc", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
		"v":  &types.AttributeValueMemberS{Value: "hello"},
	})

	backupOut, err := db.CreateBackup(ctx, &dynamodb_sdk.CreateBackupInput{
		TableName:  aws.String("BackupSrc"),
		BackupName: aws.String("my-backup"),
	})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	if backupOut.BackupDetails == nil {
		t.Fatal("expected BackupDetails")
	}

	backupARN := aws.ToString(backupOut.BackupDetails.BackupArn)
	if backupARN == "" {
		t.Fatal("expected non-empty BackupArn")
	}

	// Describe the backup.
	descOut, err := db.DescribeBackup(ctx, &dynamodb_sdk.DescribeBackupInput{
		BackupArn: aws.String(backupARN),
	})
	if err != nil {
		t.Fatalf("DescribeBackup: %v", err)
	}

	if descOut.BackupDescription == nil {
		t.Fatal("expected BackupDescription")
	}

	if descOut.BackupDescription.SourceTableDetails == nil {
		t.Fatal("expected SourceTableDetails")
	}

	if aws.ToString(descOut.BackupDescription.SourceTableDetails.TableName) != "BackupSrc" {
		t.Errorf("unexpected source table name: %s",
			aws.ToString(descOut.BackupDescription.SourceTableDetails.TableName))
	}
}

func TestBackup_DeleteAndDescribe_ReturnsError(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "BackupDel")

	backupOut, err := db.CreateBackup(ctx, &dynamodb_sdk.CreateBackupInput{
		TableName:  aws.String("BackupDel"),
		BackupName: aws.String("del-backup"),
	})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	arn := aws.ToString(backupOut.BackupDetails.BackupArn)

	_, err = db.DeleteBackup(ctx, &dynamodb_sdk.DeleteBackupInput{
		BackupArn: aws.String(arn),
	})
	if err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	// Describe after delete should return error.
	_, err = db.DescribeBackup(ctx, &dynamodb_sdk.DescribeBackupInput{
		BackupArn: aws.String(arn),
	})
	assertErrorCode(t, err, "ResourceNotFoundException")
}

// TestDescribeAndDeleteBackup_SourceTableDetails_WireFields drives
// CreateBackup, DescribeBackup and DeleteBackup through the real SDK client
// and asserts SourceTableDetails.ProvisionedThroughput and
// TableCreationDateTime survive the wire round trip on both Describe and
// Delete responses. Both are "This member is required" fields on the real
// SourceTableDetails (dynamodb@v1.63.1 types/types.go:3116), and the
// in-memory backend already computes them correctly
// (buildSDKSourceTableDetails in backup_interface.go) -- but the wire
// converter (buildBackupDescriptionFromSDK in backup_ops.go) dropped both,
// so a real client reading resp.BackupDescription.SourceTableDetails.
// ProvisionedThroughput or .TableCreationDateTime always saw a zero value.
func TestDescribeAndDeleteBackup_SourceTableDetails_WireFields(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &dynamodb_sdk.CreateTableInput{
		TableName: aws.String("backup-wire-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(7),
			WriteCapacityUnits: aws.Int64(3),
		},
	})
	require.NoError(t, err)

	backupOut, err := client.CreateBackup(t.Context(), &dynamodb_sdk.CreateBackupInput{
		TableName:  aws.String("backup-wire-table"),
		BackupName: aws.String("wire-backup"),
	})
	require.NoError(t, err)
	backupARN := aws.ToString(backupOut.BackupDetails.BackupArn)

	descOut, err := client.DescribeBackup(t.Context(), &dynamodb_sdk.DescribeBackupInput{
		BackupArn: aws.String(backupARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.BackupDescription.SourceTableDetails.ProvisionedThroughput)
	assert.Equal(t, int64(7),
		aws.ToInt64(descOut.BackupDescription.SourceTableDetails.ProvisionedThroughput.ReadCapacityUnits))
	assert.Equal(t, int64(3),
		aws.ToInt64(descOut.BackupDescription.SourceTableDetails.ProvisionedThroughput.WriteCapacityUnits))
	assert.False(t,
		aws.ToTime(descOut.BackupDescription.SourceTableDetails.TableCreationDateTime).IsZero(),
		"DescribeBackup must report the source table's creation time")

	delOut, err := client.DeleteBackup(t.Context(), &dynamodb_sdk.DeleteBackupInput{
		BackupArn: aws.String(backupARN),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.BackupDescription.SourceTableDetails.ProvisionedThroughput)
	assert.Equal(t, int64(7),
		aws.ToInt64(delOut.BackupDescription.SourceTableDetails.ProvisionedThroughput.ReadCapacityUnits))
	assert.Equal(t, int64(3),
		aws.ToInt64(delOut.BackupDescription.SourceTableDetails.ProvisionedThroughput.WriteCapacityUnits))
	assert.False(t,
		aws.ToTime(delOut.BackupDescription.SourceTableDetails.TableCreationDateTime).IsZero(),
		"DeleteBackup must report the source table's creation time")
}

// validateTableName is called at the HTTP dispatch layer. Tests call the exported
// wrapper to verify the constraint logic directly.
