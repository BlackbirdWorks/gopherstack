package dynamodb_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestMetrics_ExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		metric    string
		wantErr   bool
	}{
		{name: "select latency", statement: `SELECT * FROM "mtab"`, metric: "SuccessfulRequestLatency"},
		{name: "select returned items", statement: `SELECT * FROM "mtab"`, metric: "ReturnedItemCount"},
		{name: "insert latency", statement: `INSERT INTO "mtab" VALUE {'pk': 'x'}`, metric: "SuccessfulRequestLatency"},
		{name: "missing table user error", statement: `SELECT * FROM "nosuch"`, metric: "UserErrors", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			db.SetDefaultRegion("us-east-1")
			rec := &metricRecorder{}
			db.SetMetricEmitter(rec)

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(db))

			_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String("mtab"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			_, err = client.ExecuteStatement(
				t.Context(),
				&sdk.ExecuteStatementInput{Statement: aws.String(tt.statement)},
			)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			dims := map[string]string{"TableName": "mtab", "Operation": "ExecuteStatement"}
			if tt.metric == "UserErrors" {
				dims = map[string]string{}
			}

			assert.True(t, rec.has(tt.metric, dims))
		})
	}
}

func TestMetrics_StreamReturned(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("StreamMetrics", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "StreamMetrics", "NEW_AND_OLD_IMAGES"))

	_, err = db.PutItem(ctx, makePutItem("StreamMetrics", "pk", "a"))
	require.NoError(t, err)

	rec := &metricRecorder{}
	db.SetMetricEmitter(rec)

	records := drainStreamRecords(t, db, "StreamMetrics")
	require.Len(t, records, 1)

	table, ok := db.GetTable("StreamMetrics")
	require.True(t, ok)

	label := table.StreamARN[strings.LastIndex(table.StreamARN, "/")+1:]
	dims := map[string]string{"TableName": "StreamMetrics", "StreamLabel": label}

	assert.True(t, rec.has("ReturnedRecordsCount", dims))
	assert.True(t, rec.has("ReturnedBytes", dims))
}
