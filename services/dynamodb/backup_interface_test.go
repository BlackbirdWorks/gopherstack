package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// ── CreateBackup ──────────────────────────────────────────────────────────────

func TestInMemoryDB_CreateBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, db *dynamodb.InMemoryDB)
		input      *sdk.CreateBackupInput
		name       string
		errContain string
		wantErr    bool
	}{
		{
			name: "success_basic",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Books", "pk")
			},
			input: &sdk.CreateBackupInput{
				TableName:  aws.String("Books"),
				BackupName: aws.String("books-backup-01"),
			},
		},
		{
			name:       "missing_table_name",
			input:      &sdk.CreateBackupInput{TableName: aws.String(""), BackupName: aws.String("b")},
			wantErr:    true,
			errContain: "TableName",
		},
		{
			name:       "missing_backup_name",
			input:      &sdk.CreateBackupInput{TableName: aws.String("T"), BackupName: aws.String("")},
			wantErr:    true,
			errContain: "BackupName",
		},
		{
			name:    "table_does_not_exist",
			input:   &sdk.CreateBackupInput{TableName: aws.String("Ghost"), BackupName: aws.String("snap")},
			wantErr: true,
		},
		{
			name: "returns_backup_details",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Items", "pk")
			},
			input: &sdk.CreateBackupInput{
				TableName:  aws.String("Items"),
				BackupName: aws.String("items-snap"),
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

			out, err := db.CreateBackup(t.Context(), tt.input)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errContain != "" {
					assert.Contains(t, err.Error(), tt.errContain)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			require.NotNil(t, out.BackupDetails)
			assert.NotEmpty(t, aws.ToString(out.BackupDetails.BackupArn))
			assert.Equal(t, aws.ToString(tt.input.BackupName), aws.ToString(out.BackupDetails.BackupName))
			assert.Equal(t, types.BackupStatusAvailable, out.BackupDetails.BackupStatus)
			assert.Equal(t, types.BackupTypeUser, out.BackupDetails.BackupType)
			assert.NotNil(t, out.BackupDetails.BackupCreationDateTime)
		})
	}
}

// ── DeleteBackup ──────────────────────────────────────────────────────────────

func TestInMemoryDB_DeleteBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, db *dynamodb.InMemoryDB) string
		name        string
		overrideArn string
		errContain  string
		wantErr     bool
	}{
		{
			name: "success_deletes_existing",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) string {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				out, err := db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
					TableName:  aws.String("T"),
					BackupName: aws.String("snap"),
				})
				require.NoError(t, err)

				return aws.ToString(out.BackupDetails.BackupArn)
			},
		},
		{
			name:        "empty_backup_arn",
			overrideArn: "",
			wantErr:     true,
			errContain:  "BackupArn",
		},
		{
			name:        "nonexistent_arn",
			overrideArn: "arn:aws:dynamodb:us-east-1:123456789012:table/T/backup/nosuchbackup",
			wantErr:     true,
		},
		{
			name: "returns_backup_description",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) string {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				out, err := db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
					TableName:  aws.String("T"),
					BackupName: aws.String("to-delete"),
				})
				require.NoError(t, err)

				return aws.ToString(out.BackupDetails.BackupArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			backupArn := tt.overrideArn

			if tt.setup != nil {
				backupArn = tt.setup(t, db)
			}

			out, err := db.DeleteBackup(t.Context(), &sdk.DeleteBackupInput{
				BackupArn: &backupArn,
			})

			if tt.wantErr {
				require.Error(t, err)

				if tt.errContain != "" {
					assert.Contains(t, err.Error(), tt.errContain)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			require.NotNil(t, out.BackupDescription)
			require.NotNil(t, out.BackupDescription.BackupDetails)
			assert.Equal(t, backupArn, aws.ToString(out.BackupDescription.BackupDetails.BackupArn))
			assert.Equal(t, types.BackupStatusDeleted, out.BackupDescription.BackupDetails.BackupStatus)
		})
	}
}

// ── BatchExecuteStatement ─────────────────────────────────────────────────────

func TestInMemoryDB_BatchExecuteStatement(t *testing.T) {
	t.Parallel()

	// setupTable creates a table "T" with items {"pk":{"S":"a"}}, {"pk":{"S":"b"}}.
	setupTable := func(t *testing.T, db *dynamodb.InMemoryDB) {
		t.Helper()
		createTableHelper(t, db, "T", "pk")

		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("T"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "a"},
				"v":  &types.AttributeValueMemberS{Value: "hello"},
			},
		})
		require.NoError(t, err)

		_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("T"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "b"},
				"v":  &types.AttributeValueMemberS{Value: "world"},
			},
		})
		require.NoError(t, err)
	}

	tests := []struct {
		setup       func(t *testing.T, db *dynamodb.InMemoryDB)
		input       *sdk.BatchExecuteStatementInput
		name        string
		wantErrCode string
		wantLen     int
		wantErr     bool
	}{
		{
			name:    "empty_statements",
			input:   &sdk.BatchExecuteStatementInput{Statements: []types.BatchStatementRequest{}},
			wantLen: 0,
		},
		{
			name:  "select_existing_row",
			setup: setupTable,
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`SELECT pk, v FROM "T" WHERE pk = 'a'`)},
				},
			},
			wantLen: 1,
		},
		{
			name:  "select_nonexistent_row_returns_empty_response",
			setup: setupTable,
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`SELECT pk FROM "T" WHERE pk = 'zzz'`)},
				},
			},
			wantLen: 1,
		},
		{
			name:  "invalid_statement_returns_error_in_response",
			setup: setupTable,
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`NOT A VALID PARTIQL STATEMENT`)},
				},
			},
			wantLen:     1,
			wantErrCode: "StatementError",
		},
		{
			name:  "multiple_statements_mixed",
			setup: setupTable,
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`SELECT pk, v FROM "T" WHERE pk = 'a'`)},
					{Statement: aws.String(`SELECT pk FROM "T" WHERE pk = 'b'`)},
				},
			},
			wantLen: 2,
		},
		{
			name:  "select_with_parameter",
			setup: setupTable,
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{
						Statement:  aws.String(`SELECT pk, v FROM "T" WHERE pk = ?`),
						Parameters: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}},
					},
				},
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			out, err := db.BatchExecuteStatement(t.Context(), tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Len(t, out.Responses, tt.wantLen)

			if tt.wantErrCode != "" {
				require.NotEmpty(t, out.Responses)
				require.NotNil(t, out.Responses[0].Error)
				assert.Equal(t, tt.wantErrCode, string(out.Responses[0].Error.Code))
			}
		})
	}
}

// ── StorageBackend interface compliance ────────────────────────────────────────

// TestStorageBackend_InterfaceCompliance verifies that InMemoryDB satisfies the
// full StorageBackend interface including the three newly added operations.
func TestStorageBackend_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ dynamodb.StorageBackend = (*dynamodb.InMemoryDB)(nil)
}

// ── CreateBackup / DeleteBackup round-trip ─────────────────────────────────────

func TestCreateDeleteBackup_RoundTrip(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "RoundTrip", "pk")

	ctx := context.Background()

	// Create backup.
	createOut, err := db.CreateBackup(ctx, &sdk.CreateBackupInput{
		TableName:  aws.String("RoundTrip"),
		BackupName: aws.String("rt-snap"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut)
	require.NotNil(t, createOut.BackupDetails)
	backupArn := aws.ToString(createOut.BackupDetails.BackupArn)
	assert.NotEmpty(t, backupArn)

	// Delete backup.
	deleteOut, err := db.DeleteBackup(ctx, &sdk.DeleteBackupInput{
		BackupArn: &backupArn,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteOut)
	assert.Equal(t, backupArn, aws.ToString(deleteOut.BackupDescription.BackupDetails.BackupArn))
	assert.Equal(t, types.BackupStatusDeleted, deleteOut.BackupDescription.BackupDetails.BackupStatus)

	// Second delete should fail (already deleted).
	_, err = db.DeleteBackup(ctx, &sdk.DeleteBackupInput{BackupArn: &backupArn})
	require.Error(t, err)
}

// ── BatchExecuteStatement DML ──────────────────────────────────────────────────

// TestInMemoryDB_BatchExecuteStatement_DML tests INSERT/UPDATE/DELETE PartiQL via BatchExecuteStatement.
func TestInMemoryDB_BatchExecuteStatement_DML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, db *dynamodb.InMemoryDB)
		verify   func(t *testing.T, db *dynamodb.InMemoryDB)
		input    *sdk.BatchExecuteStatementInput
		name     string
		wantLen  int
		wantErrs int
	}{
		{
			name: "insert_via_partiql",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
			},
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`INSERT INTO "T" VALUE {'pk': 'new-item', 'v': 'inserted'}`)},
				},
			},
			wantLen: 1,
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				out, err := db.GetItem(t.Context(), &sdk.GetItemInput{
					TableName: aws.String("T"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "new-item"},
					},
				})
				require.NoError(t, err)
				require.NotNil(t, out.Item)
			},
		},
		{
			name: "update_via_partiql",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "x"},
						"v":  &types.AttributeValueMemberS{Value: "old"},
					},
				})
				require.NoError(t, err)
			},
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`UPDATE "T" SET v='new' WHERE pk='x'`)},
				},
			},
			wantLen: 1,
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				out, err := db.GetItem(t.Context(), &sdk.GetItemInput{
					TableName: aws.String("T"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "x"},
					},
				})
				require.NoError(t, err)
				v, ok := out.Item["v"].(*types.AttributeValueMemberS)
				require.True(t, ok)
				assert.Equal(t, "new", v.Value)
			},
		},
		{
			name: "delete_via_partiql",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "to-del"},
					},
				})
				require.NoError(t, err)
			},
			input: &sdk.BatchExecuteStatementInput{
				Statements: []types.BatchStatementRequest{
					{Statement: aws.String(`DELETE FROM "T" WHERE pk='to-del'`)},
				},
			},
			wantLen: 1,
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				out, err := db.GetItem(t.Context(), &sdk.GetItemInput{
					TableName: aws.String("T"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "to-del"},
					},
				})
				require.NoError(t, err)
				assert.Empty(t, out.Item)
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

			out, err := db.BatchExecuteStatement(t.Context(), tt.input)
			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Len(t, out.Responses, tt.wantLen)

			errCount := 0
			for _, resp := range out.Responses {
				if resp.Error != nil {
					errCount++
				}
			}
			assert.Equal(t, tt.wantErrs, errCount)

			if tt.verify != nil {
				tt.verify(t, db)
			}
		})
	}
}

// ── Handler Reset and Purge ───────────────────────────────────────────────────

func TestHandler_Reset_ClearsState(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createTableHelper(t, db, "T", "pk")

	_, err := db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("T"),
		BackupName: aws.String("snap"),
	})
	require.NoError(t, err)

	handler.Reset()

	_, err = db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
		TableName: aws.String("T"),
	})
	require.Error(t, err)
}

func TestHandler_Purge_DelegatesToBackend(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createTableHelper(t, db, "Purgeable", "pk")

	require.NotPanics(t, func() {
		handler.Purge(time.Now().Add(24 * time.Hour))
	})
}

// ── Handler with Authorization region extraction ──────────────────────────────

func TestHandler_RegionFromAuthHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authHeader string
		regionHdr  string
		wantCode   int
	}{
		{
			name: "valid_sigv4_auth",
			// AWS SigV4 Credential header: AKID/date/region/service/type
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20230525/eu-west-1/dynamodb/" +
				"aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=abc",
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "x_amz_region_header",
			regionHdr: "ap-southeast-2",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:     "no_region_headers",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(db)

			body := []byte(`{"TableName":"NoSuchTable"}`)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			if tt.regionHdr != "" {
				req.Header.Set("X-Amz-Region", tt.regionHdr)
			}

			ctxWithLogger := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctxWithLogger)

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_TagOperations_Via_Handler verifies TagResource / UntagResource / ListTagsOfResource
// through the HTTP handler to improve dispatch coverage.
func TestHandler_TagOperations_Via_Handler(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createTableHelper(t, db, "Tagged", "pk")
	descOut, err := db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
		TableName: aws.String("Tagged"),
	})
	require.NoError(t, err)
	tableARN := aws.ToString(descOut.Table.TableArn)

	doHandlerReq := func(target string, payload any) *httptest.ResponseRecorder {
		t.Helper()
		body, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("X-Amz-Target", target)
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		ctxLog := logger.Save(req.Context(), slog.Default())
		req = req.WithContext(ctxLog)
		rec := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rec)
		require.NoError(t, handler.Handler()(c))

		return rec
	}

	rec := doHandlerReq("DynamoDB_20120810.TagResource", map[string]any{
		"ResourceArn": tableARN,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doHandlerReq("DynamoDB_20120810.ListTagsOfResource", map[string]any{
		"ResourceArn": tableARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doHandlerReq("DynamoDB_20120810.UntagResource", map[string]any{
		"ResourceArn": tableARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_TableMutationOps_Via_Handler tests UpdateTable / UpdateTimeToLive / DescribeTimeToLive
// through the HTTP handler to improve dispatchTableOps coverage.
func TestHandler_TableMutationOps_Via_Handler(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)
	createTableHelper(t, db, "MutTable", "pk")

	doHandlerReq := func(target string, payload any) *httptest.ResponseRecorder {
		t.Helper()
		body, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("X-Amz-Target", target)
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		ctxLog := logger.Save(req.Context(), slog.Default())
		req = req.WithContext(ctxLog)
		rec := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rec)
		require.NoError(t, handler.Handler()(c))

		return rec
	}

	// UpdateTimeToLive
	rec := doHandlerReq("DynamoDB_20120810.UpdateTimeToLive", map[string]any{
		"TableName": "MutTable",
		"TimeToLiveSpecification": map[string]any{
			"AttributeName": "ttl",
			"Enabled":       true,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeTimeToLive
	rec = doHandlerReq("DynamoDB_20120810.DescribeTimeToLive", map[string]any{
		"TableName": "MutTable",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_UpdateTable_Via_Handler adds UpdateTable coverage via handler dispatch.
func TestHandler_UpdateTable_Via_Handler(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)
	createTableHelper(t, db, "UpdateMe", "pk")

	body, err := json.Marshal(map[string]any{
		"TableName":   "UpdateMe",
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTable")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	ctxLog := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctxLog)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_RestoreTableToPointInTime_Coverage adds coverage for PITR error paths.
func TestHandler_RestoreTableToPointInTime_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, db *dynamodb.InMemoryDB)
		payload  map[string]any
		name     string
		wantCode int
	}{
		{
			name: "pitr_not_enabled",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Source", "pk")
			},
			payload: map[string]any{
				"SourceTableName": "Source",
				"TargetTableName": "Restored",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "target_table_already_exists",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "PITRSrc", "pk")
				createTableHelper(t, db, "PITRDst", "pk")
				// Enable PITR on the source table via handler.
				handler := dynamodb.NewHandler(db)
				body, _ := json.Marshal(map[string]any{
					"TableName": "PITRSrc",
					"PointInTimeRecoverySpecification": map[string]any{
						"PointInTimeRecoveryEnabled": true,
					},
				})
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
				req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateContinuousBackups")
				req.Header.Set("Content-Type", "application/x-amz-json-1.0")
				ctxLog := logger.Save(req.Context(), slog.Default())
				req = req.WithContext(ctxLog)
				rec := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				require.NoError(t, handler.Handler()(c))
				require.Equal(t, http.StatusOK, rec.Code)
			},
			payload: map[string]any{
				"SourceTableName": "PITRSrc",
				"TargetTableName": "PITRDst",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(db)
			if tt.setup != nil {
				tt.setup(t, db)
			}

			body, err := json.Marshal(tt.payload)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.RestoreTableToPointInTime")
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			ctxLog := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctxLog)

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			require.NoError(t, handler.Handler()(c))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_BackupOps_InvalidJSON covers JSON unmarshal error paths in backup handlers.
func TestHandler_BackupOps_InvalidJSON(t *testing.T) {
	t.Parallel()

	targets := []string{
		"DynamoDB_20120810.CreateBackup",
		"DynamoDB_20120810.DeleteBackup",
		"DynamoDB_20120810.DescribeBackup",
		"DynamoDB_20120810.ListBackups",
		"DynamoDB_20120810.RestoreTableFromBackup",
		"DynamoDB_20120810.RestoreTableToPointInTime",
	}

	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(db)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{bad json")))
			req.Header.Set("X-Amz-Target", target)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			ctxLog := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctxLog)

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			require.NoError(t, handler.Handler()(c))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestInMemoryDB_ListBackups_BackupTypeFilter tests the backup type filter in ListBackups.
func TestInMemoryDB_ListBackups_BackupTypeFilter(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createTableHelper(t, db, "T", "pk")

	// Create two backups.
	_, err := db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("T"),
		BackupName: aws.String("snap1"),
	})
	require.NoError(t, err)

	_, err = db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("T"),
		BackupName: aws.String("snap2"),
	})
	require.NoError(t, err)

	// List with BackupType filter via handler.
	body, marshalErr := json.Marshal(map[string]any{
		"TableName":  "T",
		"BackupType": "USER",
	})
	require.NoError(t, marshalErr)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListBackups")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	ctxLog := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctxLog)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["BackupSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2)
}

// TestInMemoryDB_PutResourcePolicy_EmptyPolicy tests the empty policy validation.
func TestInMemoryDB_PutResourcePolicy_EmptyPolicy(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)
	createTableHelper(t, db, "T", "pk")
	descOut, err := db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
		TableName: aws.String("T"),
	})
	require.NoError(t, err)
	tableARN := aws.ToString(descOut.Table.TableArn)

	// Empty policy should return validation error.
	body, marshalErr := json.Marshal(map[string]any{
		"ResourceArn": tableARN,
		"Policy":      "",
	})
	require.NoError(t, marshalErr)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutResourcePolicy")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	ctxLog := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctxLog)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestInMemoryDB_Close tests that Close() can be called without panicking.
func TestInMemoryDB_Close(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "T", "pk")

	require.NotPanics(t, func() {
		db.Close()
	})
}

// TestInMemoryDB_Purge_WithOldEntries tests that Purge deletes tables, backups, and streams
// that are older than the cutoff time.
func TestInMemoryDB_Purge_WithOldEntries(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Create a table.
	createTableHelper(t, db, "OldTable", "pk")

	// Create a backup.
	_, err := db.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("OldTable"),
		BackupName: aws.String("old-backup"),
	})
	require.NoError(t, err)

	// Purge with a future cutoff — should delete the table and backup.
	db.Purge(time.Now().Add(24 * time.Hour))

	// Table should now be gone.
	_, err = db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
		TableName: aws.String("OldTable"),
	})
	require.Error(t, err)
}

// TestInMemoryDB_DeleteItem_ReturnValues tests DeleteItem with ReturnConsumedCapacity and
// ReturnItemCollectionMetrics to cover those output branches.
func TestInMemoryDB_DeleteItem_ReturnValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, db *dynamodb.InMemoryDB)
		conditionExpr      string
		name               string
		wantReturnedItem   bool
		wantConsumedCap    bool
		wantCollectionSize bool
		wantErr            bool
	}{
		{
			name: "return_all_old_with_consumed_capacity",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "del1"},
						"v":  &types.AttributeValueMemberS{Value: "val"},
					},
				})
				require.NoError(t, err)
			},
			wantReturnedItem: true,
			wantConsumedCap:  true,
		},
		{
			name: "return_all_old_with_item_collection_metrics",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "del2"},
					},
				})
				require.NoError(t, err)
			},
			wantReturnedItem:   true,
			wantCollectionSize: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			pkVal := "del1"
			if tt.wantCollectionSize {
				pkVal = "del2"
			}

			input := &sdk.DeleteItemInput{
				TableName: aws.String("T"),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: pkVal},
				},
				ReturnValues: types.ReturnValueAllOld,
			}

			if tt.wantConsumedCap {
				input.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
			}

			if tt.wantCollectionSize {
				input.ReturnItemCollectionMetrics = types.ReturnItemCollectionMetricsSize
			}

			out, err := db.DeleteItem(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)

			if tt.wantReturnedItem {
				assert.NotEmpty(t, out.Attributes)
			}

			if tt.wantConsumedCap {
				assert.NotNil(t, out.ConsumedCapacity)
			}

			if tt.wantCollectionSize {
				assert.NotNil(t, out.ItemCollectionMetrics)
			}
		})
	}
}

// TestInMemoryDB_BatchGetItem_LimitExceeded tests the batch size limit.
func TestInMemoryDB_BatchGetItem_LimitExceeded(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "T", "pk")

	// Build 101 keys (over the 100 item limit).
	keys := make([]map[string]types.AttributeValue, 101)
	for i := range keys {
		keys[i] = map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("key-%d", i)},
		}
	}

	_, err := db.BatchGetItem(t.Context(), &sdk.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"T": {Keys: keys},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Batch size limit exceeded")
}

// TestInMemoryDB_DeleteItem_ConditionalExpression covers the checkDeleteCondition path.
func TestInMemoryDB_DeleteItem_ConditionalExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, db *dynamodb.InMemoryDB)
		condition string
		name      string
		pkVal     string
		wantErr   bool
	}{
		{
			name: "condition_passes",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "cond1"},
						"v":  &types.AttributeValueMemberS{Value: "exists"},
					},
				})
				require.NoError(t, err)
			},
			condition: "attribute_exists(v)",
			pkVal:     "cond1",
		},
		{
			name: "condition_fails",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "cond2"},
					},
				})
				require.NoError(t, err)
			},
			condition: "attribute_exists(nonexistent)",
			pkVal:     "cond2",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "T", "pk")
			if tt.setup != nil {
				tt.setup(t, db)
			}

			_, err := db.DeleteItem(t.Context(), &sdk.DeleteItemInput{
				TableName: aws.String("T"),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: tt.pkVal},
				},
				ConditionExpression: aws.String(tt.condition),
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestInMemoryDB_UpdateItem_InvalidExpression covers the doUpdate error path.
func TestInMemoryDB_UpdateItem_InvalidExpression(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "T", "pk")
	_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String("T"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "x"},
		},
	})
	require.NoError(t, err)

	// An invalid update expression should return an error.
	_, err = db.UpdateItem(t.Context(), &sdk.UpdateItemInput{
		TableName: aws.String("T"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "x"},
		},
		UpdateExpression: aws.String("INVALID EXPRESSION SYNTAX !!@@##"),
	})
	require.Error(t, err)
}
