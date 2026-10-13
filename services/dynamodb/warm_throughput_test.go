package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func warmTableInput(name string, tableWarm, gsiWarm *dynamodbtypes.WarmThroughput) *dynamodbsdk.CreateTableInput {
	return &dynamodbsdk.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		WarmThroughput: tableWarm,
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("gsi"),
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String("gk"), KeyType: dynamodbtypes.KeyTypeHash},
			},
			Projection:     &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			WarmThroughput: gsiWarm,
		}},
	}
}

func TestRealClient_WarmThroughput(t *testing.T) {
	t.Parallel()

	wt := func(r, w int64) *dynamodbtypes.WarmThroughput {
		return &dynamodbtypes.WarmThroughput{ReadUnitsPerSecond: aws.Int64(r), WriteUnitsPerSecond: aws.Int64(w)}
	}

	tests := []struct {
		tableWarm   *dynamodbtypes.WarmThroughput
		gsiWarm     *dynamodbtypes.WarmThroughput
		updateWarm  *dynamodbtypes.WarmThroughput
		gsiUpdate   *dynamodbtypes.WarmThroughput
		name        string
		wantTableRW [2]int64
		wantGSIRW   [2]int64
	}{
		{
			name: "create values", tableWarm: wt(15000, 5000), gsiWarm: wt(13000, 4500),
			wantTableRW: [2]int64{15000, 5000}, wantGSIRW: [2]int64{13000, 4500},
		},
		{
			name: "update table", tableWarm: wt(15000, 5000), gsiWarm: wt(13000, 4500),
			updateWarm:  wt(20000, 8000),
			wantTableRW: [2]int64{20000, 8000}, wantGSIRW: [2]int64{13000, 4500},
		},
		{
			name: "update gsi", tableWarm: wt(15000, 5000), gsiWarm: wt(13000, 4500),
			gsiUpdate:   wt(16000, 6000),
			wantTableRW: [2]int64{15000, 5000}, wantGSIRW: [2]int64{16000, 6000},
		},
		{
			name: "partial table update", tableWarm: wt(15000, 5000), gsiWarm: wt(13000, 4500),
			updateWarm:  &dynamodbtypes.WarmThroughput{ReadUnitsPerSecond: aws.Int64(18000)},
			wantTableRW: [2]int64{18000, 5000}, wantGSIRW: [2]int64{13000, 4500},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			created, err := client.CreateTable(ctx, warmTableInput("warm", tt.tableWarm, tt.gsiWarm))
			require.NoError(t, err)
			require.NotNil(t, created.TableDescription.WarmThroughput)
			assert.Equal(t, dynamodbtypes.TableStatusActive, created.TableDescription.WarmThroughput.Status)
			require.NotNil(t, created.TableDescription.GlobalSecondaryIndexes[0].WarmThroughput)

			if tt.updateWarm != nil || tt.gsiUpdate != nil {
				in := &dynamodbsdk.UpdateTableInput{TableName: aws.String("warm"), WarmThroughput: tt.updateWarm}
				if tt.gsiUpdate != nil {
					in.GlobalSecondaryIndexUpdates = []dynamodbtypes.GlobalSecondaryIndexUpdate{{
						Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{
							IndexName: aws.String("gsi"), WarmThroughput: tt.gsiUpdate,
						},
					}}
				}

				_, err = client.UpdateTable(ctx, in)
				require.NoError(t, err)
			}

			desc, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{TableName: aws.String("warm")})
			require.NoError(t, err)

			tw := desc.Table.WarmThroughput
			require.NotNil(t, tw)
			assert.Equal(
				t,
				tt.wantTableRW,
				[2]int64{aws.ToInt64(tw.ReadUnitsPerSecond), aws.ToInt64(tw.WriteUnitsPerSecond)},
			)
			assert.Equal(t, dynamodbtypes.TableStatusActive, tw.Status)

			gw := desc.Table.GlobalSecondaryIndexes[0].WarmThroughput
			require.NotNil(t, gw)
			assert.Equal(
				t,
				tt.wantGSIRW,
				[2]int64{aws.ToInt64(gw.ReadUnitsPerSecond), aws.ToInt64(gw.WriteUnitsPerSecond)},
			)
			assert.Equal(t, dynamodbtypes.IndexStatusActive, gw.Status)
		})
	}
}

func TestRealClient_WarmThroughputOmittedAndValidated(t *testing.T) {
	t.Parallel()

	t.Run("omitted when unset", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

		_, err := client.CreateTable(t.Context(), warmTableInput("plain", nil, nil))
		require.NoError(t, err)

		desc, err := client.DescribeTable(t.Context(), &dynamodbsdk.DescribeTableInput{TableName: aws.String("plain")})
		require.NoError(t, err)
		assert.Nil(t, desc.Table.WarmThroughput)
		assert.Nil(t, desc.Table.GlobalSecondaryIndexes[0].WarmThroughput)
	})

	t.Run("empty gsi warm throughput rejected", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

		_, err := client.CreateTable(t.Context(),
			warmTableInput("bad", nil, &dynamodbtypes.WarmThroughput{}))
		require.ErrorContains(t, err, "ValidationException")
	})
}
