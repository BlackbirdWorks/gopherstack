// Package dynamodb_test covers gopherstack-l3vv part (b): UpdateGlobalTableSettings
// cached ReplicaSettingsUpdate[].ReplicaProvisionedReadCapacityUnits into
// gt.ReplicaSettings and echoed it back, but never wrote it through to the
// real replica Table's ProvisionedThroughput.ReadCapacityUnits --
// DescribeGlobalTableSettings reads that value from the real table
// (replicaTableCapacityRLocked), not from the cache, so the two ops could
// permanently disagree after a single UpdateGlobalTableSettings call. This
// test drives the real aws-sdk-go-v2 client and proves DescribeTable on the
// replica, and DescribeGlobalTableSettings, both see the new value.
package dynamodb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newTestDynamoDBClientInRegion is newTestDynamoDBClient parameterized by
// region: dynamodb's RouteMatcher/handler derives the acting region from the
// request's SigV4 signature (handler.go's ExtractRegionFromRequest), so a
// replica in a non-default region needs its own, differently-configured client.
func newTestDynamoDBClientInRegion(t *testing.T, h *dynamodb.DynamoDBHandler, region string) *sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func TestGlobalTableSettings_UpdateWritesThroughToReplicaTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tableName   string
		region      string
		otherRegion string
		initialRCU  int64
		updatedRCU  int64
	}{
		{
			name:        "replica RCU write-through agrees on DescribeTable and DescribeGlobalTableSettings",
			tableName:   "gt-writethrough-table",
			region:      "us-east-1",
			otherRegion: "eu-west-1",
			initialRCU:  5,
			updatedRCU:  123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			t.Cleanup(backend.Close)
			handler := dynamodb.NewHandler(backend)

			defaultClient := newTestDynamoDBClientInRegion(t, handler, tt.region)

			_, err := defaultClient.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String(tt.tableName),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				BillingMode: types.BillingModeProvisioned,
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(tt.initialRCU),
					WriteCapacityUnits: aws.Int64(tt.initialRCU),
				},
			})
			require.NoError(t, err)

			_, err = defaultClient.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
				GlobalTableName: aws.String(tt.tableName),
				ReplicationGroup: []types.Replica{
					{RegionName: aws.String(tt.region)},
					{RegionName: aws.String(tt.otherRegion)},
				},
			})
			require.NoError(t, err)

			_, err = defaultClient.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
				GlobalTableName: aws.String(tt.tableName),
				ReplicaSettingsUpdate: []types.ReplicaSettingsUpdate{
					{
						RegionName:                          aws.String(tt.region),
						ReplicaProvisionedReadCapacityUnits: aws.Int64(tt.updatedRCU),
					},
				},
			})
			require.NoError(t, err)

			replicaClient := newTestDynamoDBClientInRegion(t, handler, tt.region)
			descTable, err := replicaClient.DescribeTable(t.Context(), &sdk.DescribeTableInput{
				TableName: aws.String(tt.tableName),
			})
			require.NoError(t, err)
			require.NotNil(t, descTable.Table.ProvisionedThroughput)
			require.NotNil(t, descTable.Table.ProvisionedThroughput.ReadCapacityUnits)
			assert.Equal(t, tt.updatedRCU, *descTable.Table.ProvisionedThroughput.ReadCapacityUnits,
				"DescribeTable on the replica should see the RCU UpdateGlobalTableSettings wrote through")

			descSettings, err := defaultClient.DescribeGlobalTableSettings(
				t.Context(),
				&sdk.DescribeGlobalTableSettingsInput{GlobalTableName: aws.String(tt.tableName)},
			)
			require.NoError(t, err)

			replica := findReplicaSettings(t, descSettings.ReplicaSettings, tt.region)
			require.NotNil(t, replica.ReplicaProvisionedReadCapacityUnits)
			assert.Equal(t, tt.updatedRCU, *replica.ReplicaProvisionedReadCapacityUnits,
				"DescribeGlobalTableSettings should agree with DescribeTable, not diverge")
		})
	}
}
