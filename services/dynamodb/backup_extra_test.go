package dynamodb_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

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
	createCode, createResp := doBackupRequest(t, h, "DynamoDB_20120810.CreateBackup", models.CreateBackupInput{
		TableName:  "IDSourceTable",
		BackupName: "id-test-backup",
	})
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
	assert.NotEmpty(t, td2["TableId"], "RestoreTableToPointInTime: restored table must have a TableId")
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

			err := tt.run(context.Background(), db)
			require.Error(t, err)
		})
	}
}
