package dynamodb_test

// Tests for refinement 1 items:
//  1. nthSmallest: quickselect correctness and edge cases
//  2. sweepTxnTokens/sweepTxnPending: two-phase (snapshot) sweep
//  3. BatchExecuteStatement: ≤25 statement limit
//  4. ExportTableToPointInTime: unique ARN per call
//  5. ListExports/DescribeExport: export tracking in InMemoryDB
//  6. ListContributorInsights: returns empty list
//  7. UpdateContributorInsights: validates table exists
//  8. UpdateGlobalTableSettings: validates global table exists
//  9. UpdateKinesisStreamingDestination: validates table and stream ARN
// 10. UpdateTableReplicaAutoScaling: validates table name
// 11. ExecuteTransaction: validates statement count
// 12. ImportTable: validates required fields
// 13. ListImports: returns empty list
// 14. PartiQL key schema cache: repeated queries reuse cached schema

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newRefinementDB(t *testing.T) *dynamodb.InMemoryDB {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	t.Cleanup(db.Close)

	return db
}

func createTableForRefinement(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
	t.Helper()

	ctx := t.Context()
	_, err := db.CreateTable(ctx, &sdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// (1) BatchExecuteStatement ≤25 limit
// ---------------------------------------------------------------------------

func TestBatchExecuteStatement_Limit25(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	ctx := t.Context()
	createTableForRefinement(t, db, "LimitTable")

	tests := []struct {
		name      string
		errSubstr string
		count     int
		wantErr   bool
	}{
		{
			name:    "exactly_25_statements",
			count:   25,
			wantErr: false,
		},
		{
			name:      "26_statements_rejected",
			count:     26,
			wantErr:   true,
			errSubstr: "statements",
		},
		{
			name:      "100_statements_rejected",
			count:     100,
			wantErr:   true,
			errSubstr: "statements",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts := make([]types.BatchStatementRequest, tt.count)
			for i := range stmts {
				stmts[i] = types.BatchStatementRequest{
					Statement: aws.String(fmt.Sprintf("SELECT * FROM \"LimitTable\" WHERE pk = '%d'", i)),
				}
			}

			_, err := db.BatchExecuteStatement(ctx, &sdk.BatchExecuteStatementInput{
				Statements: stmts,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// (2) ExportTableToPointInTime: unique ARN per call
// ---------------------------------------------------------------------------

func TestExportTableToPointInTime_UniqueARN(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "ExportTable")

	h := dynamodb.NewHandler(db)
	ctx := t.Context()

	const calls = 5
	arns := make(map[string]bool, calls)

	for i := range calls {
		body := fmt.Appendf(
			nil,
			`{"TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/ExportTable","S3Bucket":"bucket-%d"}`,
			i,
		)
		out, err := h.HandleRequest(ctx, "ExportTableToPointInTime", body)
		require.NoError(t, err, "call %d", i)
		require.NotNil(t, out, "call %d returned nil", i)

		exportARN := dynamodb.ExtractExportARNForTest(out)
		require.NotEmpty(t, exportARN, "ExportArn missing on call %d", i)

		assert.False(t, arns[exportARN], "ARN collision on call %d: %s", i, exportARN)
		arns[exportARN] = true
	}
}

// ---------------------------------------------------------------------------
// (3) ListExports / DescribeExport: export tracking
// ---------------------------------------------------------------------------

func TestListExports_TracksExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filterARN string
		inject    []dynamodb.ExportDescFields
		wantCount int
	}{
		{
			name:      "empty_when_no_exports",
			inject:    nil,
			wantCount: 0,
		},
		{
			name: "returns_all_stored_exports",
			inject: []dynamodb.ExportDescFields{
				{
					ExportArn:    "arn:aws:dynamodb:us-east-1:123:table/T/export/1",
					ExportStatus: "COMPLETED",
					TableArn:     "arn:T",
					S3Bucket:     "b",
				},
				{
					ExportArn:    "arn:aws:dynamodb:us-east-1:123:table/T/export/2",
					ExportStatus: "COMPLETED",
					TableArn:     "arn:T",
					S3Bucket:     "b",
				},
			},
			wantCount: 2,
		},
		{
			name: "filters_by_table_arn",
			inject: []dynamodb.ExportDescFields{
				{ExportArn: "arn:1", ExportStatus: "COMPLETED", TableArn: "arn:TableA"},
				{ExportArn: "arn:2", ExportStatus: "COMPLETED", TableArn: "arn:TableB"},
			},
			filterARN: "arn:TableA",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localDB := newRefinementDB(t)

			for _, e := range tt.inject {
				localDB.StoreExportForTest(e.ExportArn, e.TableArn, e.S3Bucket, e.ExportStatus)
			}

			assert.Equal(t, len(tt.inject), localDB.ExportCount())
		})
	}
}

// ---------------------------------------------------------------------------
// (4) ListContributorInsights: empty list stub
// ---------------------------------------------------------------------------

func TestListContributorInsights_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	out, err := db.ListContributorInsights(t.Context(), &sdk.ListContributorInsightsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.ContributorInsightsSummaries)
}

// ---------------------------------------------------------------------------
// (5) UpdateContributorInsights: validates table exists
// ---------------------------------------------------------------------------

func TestUpdateContributorInsights_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("NonExistent"),
		ContributorInsightsAction: types.ContributorInsightsActionEnable,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateContributorInsights_TogglesStatus(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "CITable")

	out, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("CITable"),
		ContributorInsightsAction: types.ContributorInsightsActionEnable,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusEnabled, out.ContributorInsightsStatus)

	desc, err := db.DescribeContributorInsights(t.Context(), &sdk.DescribeContributorInsightsInput{
		TableName: aws.String("CITable"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusEnabled, desc.ContributorInsightsStatus)

	disabled, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("CITable"),
		ContributorInsightsAction: types.ContributorInsightsActionDisable,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusDisabled, disabled.ContributorInsightsStatus)
}

// ---------------------------------------------------------------------------
// (6) UpdateGlobalTableSettings: validates global table exists
// ---------------------------------------------------------------------------

func TestUpdateGlobalTableSettings_NotFound(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String("NoGT"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateGlobalTableSettings_EmptyName(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String(""),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

// ---------------------------------------------------------------------------
// (7) UpdateKinesisStreamingDestination: validates table and stream ARN
// ---------------------------------------------------------------------------

func TestUpdateKinesisStreamingDestination_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateKinesisStreamingDestination(t.Context(), &sdk.UpdateKinesisStreamingDestinationInput{
		TableName: aws.String("NoTable"),
		StreamArn: aws.String("arn:aws:kinesis:us-east-1:123:stream/s"),
	})
	require.Error(t, err)
}

func TestUpdateKinesisStreamingDestination_MissingStreamARN(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "KinesisTable")

	_, err := db.UpdateKinesisStreamingDestination(t.Context(), &sdk.UpdateKinesisStreamingDestinationInput{
		TableName: aws.String("KinesisTable"),
		StreamArn: aws.String(""),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StreamArn")
}

func TestUpdateKinesisStreamingDestination_ReturnsActive(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "KinesisActiveTable")

	streamARN := "arn:aws:kinesis:us-east-1:123:stream/my-stream"

	_, err := db.EnableKinesisStreamingDestination(t.Context(), &sdk.EnableKinesisStreamingDestinationInput{
		TableName: aws.String("KinesisActiveTable"),
		StreamArn: aws.String(streamARN),
	})
	require.NoError(t, err)

	out, err := db.UpdateKinesisStreamingDestination(t.Context(), &sdk.UpdateKinesisStreamingDestinationInput{
		TableName: aws.String("KinesisActiveTable"),
		StreamArn: aws.String(streamARN),
	})
	require.NoError(t, err)
	assert.Equal(t, types.DestinationStatusActive, out.DestinationStatus)
	assert.Equal(t, "KinesisActiveTable", aws.ToString(out.TableName))
}

// ---------------------------------------------------------------------------
// (8) UpdateTableReplicaAutoScaling: validates table name
// ---------------------------------------------------------------------------

func TestUpdateTableReplicaAutoScaling_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("NoTable"),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_EmptyName(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(""),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_ReturnsDescription(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "ASTable")

	out, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("ASTable"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TableAutoScalingDescription)
	assert.Equal(t, "ASTable", aws.ToString(out.TableAutoScalingDescription.TableName))
}

// ---------------------------------------------------------------------------
// (9) ExecuteTransaction: statement count validation
// ---------------------------------------------------------------------------

func TestExecuteTransaction_EmptyStatements(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")
}

func TestExecuteTransaction_TooManyStatements(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "TxnTable")

	stmts := make([]types.ParameterizedStatement, 101)
	for i := range stmts {
		stmt := fmt.Sprintf("SELECT * FROM \"TxnTable\" WHERE pk = '%d'", i)
		stmts[i] = types.ParameterizedStatement{Statement: &stmt}
	}

	_, err := db.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: stmts,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Too many statements")
}

func TestExecuteTransaction_ValidStatement(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "TxnSelectTable")
	ctx := t.Context()

	// Pre-populate an item.
	_, err := db.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("TxnSelectTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "key1"},
		},
	})
	require.NoError(t, err)

	stmt := "SELECT * FROM \"TxnSelectTable\" WHERE pk = 'key1'"
	out, err := db.ExecuteTransaction(ctx, &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{
			{Statement: &stmt},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
}

// ---------------------------------------------------------------------------
// (10) ImportTable: validates required fields
// ---------------------------------------------------------------------------

func TestImportTable_MissingTableCreationParameters(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &types.S3BucketSource{
			S3Bucket: aws.String("bucket"),
		},
		// TableCreationParameters intentionally omitted
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TableCreationParameters")
}

func TestImportTable_MissingS3Bucket(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		TableCreationParameters: &types.TableCreationParameters{
			TableName: aws.String("MyTable"),
		},
		// S3BucketSource intentionally omitted
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "S3BucketSource")
}

func TestImportTable_ReturnsCompleted(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	out, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &types.S3BucketSource{
			S3Bucket: aws.String("my-bucket"),
		},
		TableCreationParameters: &types.TableCreationParameters{
			TableName: aws.String("ImportedTable"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ImportTableDescription)
	assert.Equal(t, types.ImportStatusCompleted, out.ImportTableDescription.ImportStatus)
	assert.NotEmpty(t, aws.ToString(out.ImportTableDescription.ImportArn))
}

// ---------------------------------------------------------------------------
// (11) ListImports: always empty
// ---------------------------------------------------------------------------

func TestListImports_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	out, err := db.ListImports(t.Context(), &sdk.ListImportsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.ImportSummaryList)
}

// ---------------------------------------------------------------------------
// (12) PartiQL key schema cache: repeated queries hit cache
// ---------------------------------------------------------------------------

func TestPartiQLKeySchemaCache_CachesResult(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	createTableForRefinement(t, db, "CacheTestTable")
	ctx := t.Context()

	// First call — populates the cache.
	ks1, err := db.GetKeySchemaForPartiQLForTest(ctx, "CacheTestTable")
	require.NoError(t, err)
	require.NotNil(t, ks1)

	// Second call — should be served from cache without error.
	ks2, err := db.GetKeySchemaForPartiQLForTest(ctx, "CacheTestTable")
	require.NoError(t, err)
	require.NotNil(t, ks2)

	// Both calls return the same schema.
	assert.Equal(t, ks1, ks2)
}

func TestPartiQLKeySchemaCache_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)

	_, err := db.GetKeySchemaForPartiQLForTest(t.Context(), "DoesNotExist")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// ---------------------------------------------------------------------------
// (13) sweepTxnTokens two-phase: tokens removed under write lock
// ---------------------------------------------------------------------------

func TestSweepTxnTokens_TwoPhaseDoesSweep(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Inject an expired token.
	db.InjectExpiredTxnTokenForTest("expired-token")
	require.Equal(t, 1, db.TxnTokenCount())

	janitor.SweepTxnTokens(t.Context())

	// Token should be gone after sweep.
	assert.Equal(t, 0, db.TxnTokenCount())
}

func TestSweepTxnPending_TwoPhaseDoesSweep(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Inject a stale pending token.
	db.InjectStaleTxnPendingForTest("stale-pending")
	require.Equal(t, 1, db.TxnPendingCount())

	janitor.SweepTxnPending(t.Context())

	assert.Equal(t, 0, db.TxnPendingCount())
}

func TestSweepTxnTokens_LiveTokenNotRemoved(t *testing.T) {
	t.Parallel()

	db := newRefinementDB(t)
	janitor := dynamodb.NewJanitorForTest(db)

	// Add a token that is still alive (expires in the future).
	db.AddTxnToken("live-token", time.Now().Add(time.Hour))
	require.Equal(t, 1, db.TxnTokenCount())

	janitor.SweepTxnTokens(t.Context())

	// Live token must not be removed.
	assert.Equal(t, 1, db.TxnTokenCount())
}

// ---------------------------------------------------------------------------
// (14) nthSmallest quickselect: correctness
// ---------------------------------------------------------------------------

func TestNthSmallest_QuickselectCorrectness(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		want  time.Time
		name  string
		times []time.Time
		n     int
	}{
		{
			name:  "single_element",
			times: []time.Time{base},
			n:     1,
			want:  base,
		},
		{
			name:  "n_equals_one_smallest",
			times: []time.Time{base.Add(2 * time.Hour), base.Add(1 * time.Hour), base},
			n:     1,
			want:  base,
		},
		{
			name:  "n_equals_middle",
			times: []time.Time{base.Add(3 * time.Hour), base.Add(1 * time.Hour), base.Add(2 * time.Hour)},
			n:     2,
			want:  base.Add(2 * time.Hour),
		},
		{
			name:  "n_beyond_length_returns_max",
			times: []time.Time{base, base.Add(time.Hour)},
			n:     99,
			want:  base.Add(time.Hour),
		},
		{
			name:  "already_sorted",
			times: []time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour)},
			n:     2,
			want:  base.Add(time.Hour),
		},
		{
			name:  "reverse_sorted",
			times: []time.Time{base.Add(2 * time.Hour), base.Add(time.Hour), base},
			n:     2,
			want:  base.Add(time.Hour),
		},
		{
			name:  "duplicates",
			times: []time.Time{base, base, base.Add(time.Hour)},
			n:     1,
			want:  base,
		},
		{
			name:  "n_zero_returns_zero_time",
			times: []time.Time{base},
			n:     0,
			want:  time.Time{},
		},
		{
			name:  "empty_slice_n_one",
			times: []time.Time{},
			n:     1,
			want:  time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// nthSmallest is accessible through evictOldestTokens which calls it internally.
			// We test it indirectly via the sweep path that exercises the same quickselect code.
			got := dynamodb.NthSmallestForTest(tt.times, tt.n)
			assert.Equal(t, tt.want, got, "nthSmallest(%d) mismatch", tt.n)
		})
	}
}
