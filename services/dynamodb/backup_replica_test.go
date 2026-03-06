package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// doBackupRequest is a helper that sends a DynamoDB API request via the handler and decodes the response.
func doBackupRequest(t *testing.T, h *dynamodb.DynamoDBHandler, target string, body any) (int, map[string]any) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	w := httptest.NewRecorder()

	require.NoError(t, serveEchoHandler(h.Handler(), w, req))

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))

	return w.Code, result
}

// TestBackupOperations tests CreateBackup, DescribeBackup, ListBackups, DeleteBackup.
func TestBackupOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.DynamoDBHandler)
		validate func(*testing.T, *dynamodb.DynamoDBHandler)
		name     string
	}{
		{
			name: "CreateBackup_Success",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "BackupSource")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.CreateBackup", models.CreateBackupInput{
					TableName:  "BackupSource",
					BackupName: "my-backup",
				})
				require.Equal(t, http.StatusOK, code)
				details, ok := resp["BackupDetails"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, details["BackupArn"])
				assert.Equal(t, "my-backup", details["BackupName"])
				assert.Equal(t, models.BackupStatusAvailable, details["BackupStatus"])
			},
		},
		{
			name: "CreateBackup_TableNotFound",
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.CreateBackup", models.CreateBackupInput{
					TableName:  "NonExistent",
					BackupName: "bad-backup",
				})
				require.Equal(t, http.StatusBadRequest, code)
				assert.Contains(t, resp["__type"], "ResourceNotFoundException")
			},
		},
		{
			name: "DescribeBackup_Success",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "DescribeBackupTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				// First create a backup
				createCode, createResp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{
						TableName:  "DescribeBackupTable",
						BackupName: "describe-backup",
					},
				)
				require.Equal(t, http.StatusOK, createCode)
				backupArn := createResp["BackupDetails"].(map[string]any)["BackupArn"].(string)

				// Now describe it
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeBackup", models.DescribeBackupInput{
					BackupArn: backupArn,
				})
				require.Equal(t, http.StatusOK, code)
				bd := resp["BackupDescription"].(map[string]any)
				details := bd["BackupDetails"].(map[string]any)
				assert.Equal(t, backupArn, details["BackupArn"])
				assert.Equal(t, "describe-backup", details["BackupName"])
			},
		},
		{
			name: "DescribeBackup_NotFound",
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeBackup", models.DescribeBackupInput{
					BackupArn: "arn:aws:dynamodb:us-east-1:123456789012:table/T/backup/000",
				})
				require.Equal(t, http.StatusBadRequest, code)
				assert.Contains(t, resp["__type"], "ResourceNotFoundException")
			},
		},
		{
			name: "ListBackups_FilterByTable",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "ListTable")
				createTable(t, db, "OtherTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				// Create backups for both tables
				doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{TableName: "ListTable", BackupName: "b1"},
				)
				doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{TableName: "OtherTable", BackupName: "b2"},
				)

				// List filtered
				code, resp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.ListBackups",
					models.ListBackupsInput{TableName: "ListTable"},
				)
				require.Equal(t, http.StatusOK, code)
				summaries := resp["BackupSummaries"].([]any)
				require.Len(t, summaries, 1)
				assert.Equal(t, "ListTable", summaries[0].(map[string]any)["TableName"])
			},
		},
		{
			name: "DeleteBackup_Success",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "DeleteBackupTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				// Create then delete
				createCode, createResp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{
						TableName:  "DeleteBackupTable",
						BackupName: "to-delete",
					},
				)
				require.Equal(t, http.StatusOK, createCode)
				backupArn := createResp["BackupDetails"].(map[string]any)["BackupArn"].(string)

				delCode, delResp := doBackupRequest(t, h, "DynamoDB_20120810.DeleteBackup", models.DeleteBackupInput{
					BackupArn: backupArn,
				})
				require.Equal(t, http.StatusOK, delCode)
				bd := delResp["BackupDescription"].(map[string]any)
				details := bd["BackupDetails"].(map[string]any)
				assert.Equal(t, models.BackupStatusDeleted, details["BackupStatus"])

				// Verify gone
				descCode, _ := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.DescribeBackup",
					models.DescribeBackupInput{BackupArn: backupArn},
				)
				assert.Equal(t, http.StatusBadRequest, descCode)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			tt.validate(t, h)
		})
	}
}

// TestRestoreTableFromBackup tests creating a table from a backup snapshot.
func TestRestoreTableFromBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.DynamoDBHandler)
		validate func(*testing.T, *dynamodb.DynamoDBHandler)
		name     string
	}{
		{
			name: "RestoreTableFromBackup_Success",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "SourceTable")

				// Add an item to the source table
				_, err := db.PutItem(context.Background(), &dynamodb_sdk.PutItemInput{
					TableName: aws.String("SourceTable"),
					Item: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: "item1"},
					},
				})
				require.NoError(t, err)
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				// Create backup
				createCode, createResp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{
						TableName:  "SourceTable",
						BackupName: "source-backup",
					},
				)
				require.Equal(t, http.StatusOK, createCode)
				backupArn := createResp["BackupDetails"].(map[string]any)["BackupArn"].(string)

				// Restore to new table
				restoreCode, restoreResp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.RestoreTableFromBackup",
					models.RestoreTableFromBackupInput{
						BackupArn:       backupArn,
						TargetTableName: "RestoredTable",
					},
				)
				require.Equal(t, http.StatusOK, restoreCode)
				td := restoreResp["TableDescription"].(map[string]any)
				assert.Equal(t, "RestoredTable", td["TableName"])
				assert.Equal(t, models.TableStatusActive, td["TableStatus"])

				// Verify items were copied
				db := h.Backend.(*dynamodb.InMemoryDB)
				table, exists := db.GetTableInRegion("RestoredTable", "")
				require.True(t, exists)
				assert.Len(t, table.Items, 1)
			},
		},
		{
			name: "RestoreTableFromBackup_TargetExists",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "SourceTable2")
				createTable(t, db, "ExistingTarget")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				createCode, createResp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.CreateBackup",
					models.CreateBackupInput{
						TableName:  "SourceTable2",
						BackupName: "backup2",
					},
				)
				require.Equal(t, http.StatusOK, createCode)
				backupArn := createResp["BackupDetails"].(map[string]any)["BackupArn"].(string)

				restoreCode, _ := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.RestoreTableFromBackup",
					models.RestoreTableFromBackupInput{
						BackupArn:       backupArn,
						TargetTableName: "ExistingTarget",
					},
				)
				assert.Equal(t, http.StatusBadRequest, restoreCode)
			},
		},
		{
			name: "RestoreTableFromBackup_BackupNotFound",
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, _ := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.RestoreTableFromBackup",
					models.RestoreTableFromBackupInput{
						BackupArn:       "arn:aws:dynamodb:us-east-1:123:table/T/backup/0",
						TargetTableName: "NewTable",
					},
				)
				assert.Equal(t, http.StatusBadRequest, code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			tt.validate(t, h)
		})
	}
}

// TestRestoreTableToPointInTime tests PITR restore.
func TestRestoreTableToPointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.DynamoDBHandler)
		validate func(*testing.T, *dynamodb.DynamoDBHandler)
		name     string
	}{
		{
			name: "RestoreTableToPointInTime_PITRDisabled",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "PITRSource")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.RestoreTableToPointInTime",
					models.RestoreTableToPointInTimeInput{
						SourceTableName:         "PITRSource",
						TargetTableName:         "PITRTarget",
						UseLatestRestorableTime: true,
					},
				)
				require.Equal(t, http.StatusBadRequest, code)
				assert.Contains(t, resp["__type"], "ValidationException")
			},
		},
		{
			name: "RestoreTableToPointInTime_PITREnabled",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "PITREnabledSource")
				// Enable PITR
				doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
					"TableName": "PITREnabledSource",
					"PointInTimeRecoverySpecification": map[string]any{
						"PointInTimeRecoveryEnabled": true,
					},
				})
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(
					t,
					h,
					"DynamoDB_20120810.RestoreTableToPointInTime",
					models.RestoreTableToPointInTimeInput{
						SourceTableName:         "PITREnabledSource",
						TargetTableName:         "PITRTarget2",
						UseLatestRestorableTime: true,
					},
				)
				require.Equal(t, http.StatusOK, code)
				td := resp["TableDescription"].(map[string]any)
				assert.Equal(t, "PITRTarget2", td["TableName"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			tt.validate(t, h)
		})
	}
}

// TestContinuousBackupsPerTable tests per-table PITR state management.
func TestContinuousBackupsPerTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.DynamoDBHandler)
		validate func(*testing.T, *dynamodb.DynamoDBHandler)
		name     string
	}{
		{
			name: "DescribeContinuousBackups_DefaultDisabled",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTable(t, h.Backend.(*dynamodb.InMemoryDB), "PITRTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeContinuousBackups", map[string]any{
					"TableName": "PITRTable",
				})
				require.Equal(t, http.StatusOK, code)
				cbd := resp["ContinuousBackupsDescription"].(map[string]any)
				pitr := cbd["PointInTimeRecoveryDescription"].(map[string]any)
				assert.Equal(t, "DISABLED", pitr["PointInTimeRecoveryStatus"])
			},
		},
		{
			name: "UpdateContinuousBackups_Enable_Persists",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTable(t, h.Backend.(*dynamodb.InMemoryDB), "PITRPersistTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				// Enable PITR
				updateCode, _ := doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
					"TableName": "PITRPersistTable",
					"PointInTimeRecoverySpecification": map[string]any{
						"PointInTimeRecoveryEnabled": true,
					},
				})
				require.Equal(t, http.StatusOK, updateCode)

				// Verify it persisted
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeContinuousBackups", map[string]any{
					"TableName": "PITRPersistTable",
				})
				require.Equal(t, http.StatusOK, code)
				cbd := resp["ContinuousBackupsDescription"].(map[string]any)
				pitr := cbd["PointInTimeRecoveryDescription"].(map[string]any)
				assert.Equal(t, "ENABLED", pitr["PointInTimeRecoveryStatus"])
			},
		},
		{
			name: "UpdateContinuousBackups_EnableThenDisable",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTable(t, h.Backend.(*dynamodb.InMemoryDB), "PITRToggleTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
					"TableName":                        "PITRToggleTable",
					"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": true},
				})
				doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
					"TableName":                        "PITRToggleTable",
					"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": false},
				})

				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeContinuousBackups", map[string]any{
					"TableName": "PITRToggleTable",
				})
				require.Equal(t, http.StatusOK, code)
				cbd := resp["ContinuousBackupsDescription"].(map[string]any)
				pitr := cbd["PointInTimeRecoveryDescription"].(map[string]any)
				assert.Equal(t, "DISABLED", pitr["PointInTimeRecoveryStatus"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			tt.validate(t, h)
		})
	}
}

// TestGlobalTablesV2_ReplicaManagement tests UpdateTable with ReplicaUpdates.
func TestGlobalTablesV2_ReplicaManagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.InMemoryDB)
		validate func(*testing.T, *dynamodb.InMemoryDB, any, error)
		run      func(context.Context, *dynamodb.InMemoryDB) (any, error)
		name     string
	}{
		{
			name: "CreateReplica_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "GlobalTable")
			},
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) (any, error) {
				input := models.UpdateTableInput{
					TableName: "GlobalTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: "eu-west-1"}},
					},
				}
				sdkInput, err := models.ToSDKUpdateTableInput(&input)
				if err != nil {
					return nil, err
				}

				return db.UpdateTable(ctx, sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				out := result.(*dynamodb_sdk.UpdateTableOutput)
				require.NotNil(t, out.TableDescription)
				require.Len(t, out.TableDescription.Replicas, 1)
				assert.Equal(t, "eu-west-1", *out.TableDescription.Replicas[0].RegionName)
				assert.Equal(t, types.ReplicaStatusActive, out.TableDescription.Replicas[0].ReplicaStatus)
			},
		},
		{
			name: "CreateReplica_Idempotent",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "IdempotentTable")
			},
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) (any, error) {
				input := models.UpdateTableInput{
					TableName: "IdempotentTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: "us-west-2"}},
					},
				}
				sdkInput, _ := models.ToSDKUpdateTableInput(&input)
				_, _ = db.UpdateTable(ctx, sdkInput)

				// Add again — should not duplicate
				return db.UpdateTable(ctx, sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				out := result.(*dynamodb_sdk.UpdateTableOutput)
				require.Len(t, out.TableDescription.Replicas, 1)
			},
		},
		{
			name: "DeleteReplica_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "DeleteReplicaTable")
				// Add a replica first
				input := models.UpdateTableInput{
					TableName: "DeleteReplicaTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: "ap-southeast-1"}},
					},
				}
				sdkInput, _ := models.ToSDKUpdateTableInput(&input)
				_, _ = db.UpdateTable(context.Background(), sdkInput)
			},
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) (any, error) {
				input := models.UpdateTableInput{
					TableName: "DeleteReplicaTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Delete: &models.DeleteReplicationGroupMemberAction{RegionName: "ap-southeast-1"}},
					},
				}
				sdkInput, _ := models.ToSDKUpdateTableInput(&input)

				return db.UpdateTable(ctx, sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				out := result.(*dynamodb_sdk.UpdateTableOutput)
				assert.Empty(t, out.TableDescription.Replicas)
			},
		},
		{
			name: "DescribeTable_IncludesReplicas",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "DescribeReplicaTable")
				input := models.UpdateTableInput{
					TableName: "DescribeReplicaTable",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: "us-west-2"}},
					},
				}
				sdkInput, _ := models.ToSDKUpdateTableInput(&input)
				_, _ = db.UpdateTable(context.Background(), sdkInput)
			},
			run: func(ctx context.Context, db *dynamodb.InMemoryDB) (any, error) {
				return db.DescribeTable(ctx, &dynamodb_sdk.DescribeTableInput{
					TableName: aws.String("DescribeReplicaTable"),
				})
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				out := result.(*dynamodb_sdk.DescribeTableOutput)
				require.Len(t, out.Table.Replicas, 1)
				assert.Equal(t, "us-west-2", *out.Table.Replicas[0].RegionName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()

			if tt.setup != nil {
				tt.setup(t, db)
			}

			result, err := tt.run(context.Background(), db)
			tt.validate(t, db, result, err)
		})
	}
}

// TestDescribeTableReplicaAutoScaling tests DescribeTableReplicaAutoScaling via HTTP.
func TestDescribeTableReplicaAutoScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.DynamoDBHandler)
		validate func(*testing.T, *dynamodb.DynamoDBHandler)
		name     string
	}{
		{
			name: "DescribeTableReplicaAutoScaling_NoReplicas",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTable(t, h.Backend.(*dynamodb.InMemoryDB), "AutoScaleTable")
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeTableReplicaAutoScaling", map[string]any{
					"TableName": "AutoScaleTable",
				})
				require.Equal(t, http.StatusOK, code)
				desc := resp["TableAutoScalingDescription"].(map[string]any)
				assert.Equal(t, "AutoScaleTable", desc["TableName"])
				// No replicas configured
				assert.Nil(t, desc["Replicas"])
			},
		},
		{
			name: "DescribeTableReplicaAutoScaling_WithReplica",
			setup: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				db := h.Backend.(*dynamodb.InMemoryDB)
				createTable(t, db, "AutoScaleWithReplica")
				// Add replica
				input := models.UpdateTableInput{
					TableName: "AutoScaleWithReplica",
					ReplicaUpdates: []models.ReplicaUpdate{
						{Create: &models.CreateReplicationGroupMemberAction{RegionName: "eu-central-1"}},
					},
				}
				sdkInput, _ := models.ToSDKUpdateTableInput(&input)
				_, _ = db.UpdateTable(context.Background(), sdkInput)
			},
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, resp := doBackupRequest(t, h, "DynamoDB_20120810.DescribeTableReplicaAutoScaling", map[string]any{
					"TableName": "AutoScaleWithReplica",
				})
				require.Equal(t, http.StatusOK, code)
				desc := resp["TableAutoScalingDescription"].(map[string]any)
				replicas := desc["Replicas"].([]any)
				require.Len(t, replicas, 1)
				assert.Equal(t, "eu-central-1", replicas[0].(map[string]any)["RegionName"])
			},
		},
		{
			name: "DescribeTableReplicaAutoScaling_TableNotFound",
			validate: func(t *testing.T, h *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, _ := doBackupRequest(t, h, "DynamoDB_20120810.DescribeTableReplicaAutoScaling", map[string]any{
					"TableName": "NoSuchTable",
				})
				assert.Equal(t, http.StatusBadRequest, code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			tt.validate(t, h)
		})
	}
}
