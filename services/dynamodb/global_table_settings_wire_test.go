// Package dynamodb_test covers gopherstack-rrtz item 4: legacy Global
// Tables v1's DescribeGlobalTableSettings was inconsistent with
// UpdateGlobalTableSettings -- Update echoed ReplicaBillingModeSummary and
// ReplicaTableClassSummary, Describe did not, at both the backend
// (global_tables.go) and the wire-handler layer (handler_global_tables.go
// had two separate, differently-narrow conversion structs). This test
// drives the real aws-sdk-go-v2 client over HTTP and checks the two ops
// agree on the same replica's settings.
package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestGlobalTableSettings_DescribeAgreesWithUpdate(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "gt-settings-table")

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-settings-table"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("ap-southeast-1")},
		},
	})
	require.NoError(t, err)

	updateOut, err := client.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName:                          aws.String("gt-settings-table"),
		GlobalTableBillingMode:                   types.BillingModeProvisioned,
		GlobalTableProvisionedWriteCapacityUnits: aws.Int64(77),
		ReplicaSettingsUpdate: []types.ReplicaSettingsUpdate{
			{
				RegionName:        aws.String("ap-southeast-1"),
				ReplicaTableClass: types.TableClassStandardInfrequentAccess,
			},
		},
	})
	require.NoError(t, err)

	updateReplica := findReplicaSettings(t, updateOut.ReplicaSettings, "ap-southeast-1")
	require.NotNil(t, updateReplica.ReplicaBillingModeSummary)
	assert.Equal(t, types.BillingModeProvisioned, updateReplica.ReplicaBillingModeSummary.BillingMode)
	require.NotNil(t, updateReplica.ReplicaTableClassSummary)
	assert.Equal(t, types.TableClassStandardInfrequentAccess, updateReplica.ReplicaTableClassSummary.TableClass)
	require.NotNil(t, updateReplica.ReplicaProvisionedWriteCapacityUnits)
	assert.Equal(t, int64(77), *updateReplica.ReplicaProvisionedWriteCapacityUnits)

	descOut, err := client.DescribeGlobalTableSettings(t.Context(), &sdk.DescribeGlobalTableSettingsInput{
		GlobalTableName: aws.String("gt-settings-table"),
	})
	require.NoError(t, err)

	descReplica := findReplicaSettings(t, descOut.ReplicaSettings, "ap-southeast-1")

	// The two ops must agree: same billing mode and table class, since both
	// now read the same stored global-table state instead of Describe
	// silently omitting the fields Update reported.
	require.NotNil(t, descReplica.ReplicaBillingModeSummary)
	assert.Equal(
		t,
		updateReplica.ReplicaBillingModeSummary.BillingMode,
		descReplica.ReplicaBillingModeSummary.BillingMode,
	)
	require.NotNil(t, descReplica.ReplicaTableClassSummary)
	assert.Equal(
		t,
		updateReplica.ReplicaTableClassSummary.TableClass,
		descReplica.ReplicaTableClassSummary.TableClass,
	)
}

// TestGlobalTableSettings_AutoScaling verifies UpdateGlobalTableSettings's
// autoscaling-settings-update fields (GlobalTableProvisionedWriteCapacity
// AutoScalingSettingsUpdate, ReplicaProvisionedReadCapacityAutoScaling
// SettingsUpdate, GlobalTableGlobalSecondaryIndexSettingsUpdate's and
// ReplicaGlobalSecondaryIndexSettingsUpdate's per-GSI
// ProvisionedReadCapacityAutoScalingSettingsUpdate/
// ProvisionedWriteCapacityAutoScalingSettingsUpdate) are stored and echoed
// back on both UpdateGlobalTableSettingsOutput and
// DescribeGlobalTableSettingsOutput, instead of being accepted then
// silently dropped (api_op_UpdateGlobalTableSettings.go).
func TestGlobalTableSettings_AutoScaling(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("gt-as-table"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi-as-1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-as-table"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("eu-west-1")},
		},
	})
	require.NoError(t, err)

	writeAutoScaling := types.AutoScalingSettingsUpdate{
		MinimumUnits: aws.Int64(5),
		MaximumUnits: aws.Int64(50),
	}
	readAutoScaling := types.AutoScalingSettingsUpdate{
		MinimumUnits: aws.Int64(2),
		MaximumUnits: aws.Int64(20),
	}
	gsiReadAutoScaling := types.AutoScalingSettingsUpdate{
		MinimumUnits: aws.Int64(1),
		MaximumUnits: aws.Int64(10),
	}
	gsiWriteAutoScaling := types.AutoScalingSettingsUpdate{
		MinimumUnits: aws.Int64(3),
		MaximumUnits: aws.Int64(30),
	}

	updateOut, err := client.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String("gt-as-table"),
		GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate: &writeAutoScaling,
		GlobalTableGlobalSecondaryIndexSettingsUpdate: []types.GlobalTableGlobalSecondaryIndexSettingsUpdate{
			{
				IndexName: aws.String("gsi-as-1"),
				ProvisionedWriteCapacityAutoScalingSettingsUpdate: &gsiWriteAutoScaling,
			},
		},
		ReplicaSettingsUpdate: []types.ReplicaSettingsUpdate{
			{
				RegionName: aws.String("us-east-1"),
				ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate: &readAutoScaling,
				ReplicaGlobalSecondaryIndexSettingsUpdate: []types.ReplicaGlobalSecondaryIndexSettingsUpdate{
					{
						IndexName: aws.String("gsi-as-1"),
						ProvisionedReadCapacityAutoScalingSettingsUpdate: &gsiReadAutoScaling,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	upReplica := findReplicaSettings(t, updateOut.ReplicaSettings, "us-east-1")
	assertAutoScalingSettings(t, upReplica.ReplicaProvisionedWriteCapacityAutoScalingSettings, writeAutoScaling)
	assertAutoScalingSettings(t, upReplica.ReplicaProvisionedReadCapacityAutoScalingSettings, readAutoScaling)
	require.NotEmpty(t, upReplica.ReplicaGlobalSecondaryIndexSettings)
	upGSI := upReplica.ReplicaGlobalSecondaryIndexSettings[0]
	assertAutoScalingSettings(t, upGSI.ProvisionedReadCapacityAutoScalingSettings, gsiReadAutoScaling)
	assertAutoScalingSettings(t, upGSI.ProvisionedWriteCapacityAutoScalingSettings, gsiWriteAutoScaling)

	// The global-table-level write autoscaling setting must apply uniformly
	// to every replica, including one the update call didn't mention.
	otherReplica := findReplicaSettings(t, updateOut.ReplicaSettings, "eu-west-1")
	assertAutoScalingSettings(t, otherReplica.ReplicaProvisionedWriteCapacityAutoScalingSettings, writeAutoScaling)

	descOut, err := client.DescribeGlobalTableSettings(t.Context(), &sdk.DescribeGlobalTableSettingsInput{
		GlobalTableName: aws.String("gt-as-table"),
	})
	require.NoError(t, err)

	descReplica := findReplicaSettings(t, descOut.ReplicaSettings, "us-east-1")
	assertAutoScalingSettings(t, descReplica.ReplicaProvisionedWriteCapacityAutoScalingSettings, writeAutoScaling)
	assertAutoScalingSettings(t, descReplica.ReplicaProvisionedReadCapacityAutoScalingSettings, readAutoScaling)
	require.NotEmpty(t, descReplica.ReplicaGlobalSecondaryIndexSettings)
	descGSI := descReplica.ReplicaGlobalSecondaryIndexSettings[0]
	assertAutoScalingSettings(t, descGSI.ProvisionedReadCapacityAutoScalingSettings, gsiReadAutoScaling)
	assertAutoScalingSettings(t, descGSI.ProvisionedWriteCapacityAutoScalingSettings, gsiWriteAutoScaling)
}

func assertAutoScalingSettings(
	t *testing.T, got *types.AutoScalingSettingsDescription, want types.AutoScalingSettingsUpdate,
) {
	t.Helper()

	require.NotNil(t, got)
	require.NotNil(t, got.MinimumUnits)
	require.NotNil(t, got.MaximumUnits)
	assert.Equal(t, *want.MinimumUnits, *got.MinimumUnits)
	assert.Equal(t, *want.MaximumUnits, *got.MaximumUnits)
}

func findReplicaSettings(
	t *testing.T, settings []types.ReplicaSettingsDescription, region string,
) types.ReplicaSettingsDescription {
	t.Helper()

	for _, rs := range settings {
		if aws.ToString(rs.RegionName) == region {
			return rs
		}
	}

	require.FailNowf(t, "no ReplicaSettingsDescription found", "region %q", region)

	return types.ReplicaSettingsDescription{}
}

func TestGlobalTableSettings_GSISettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tableName  string
		gsiName    string
		initialRCU int64
		updatedRCU int64
	}{
		{
			name:       "describe and update GSI settings on replica",
			tableName:  "gt-gsi-table-1",
			gsiName:    "gsi-index-1",
			initialRCU: 5,
			updatedRCU: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			t.Cleanup(backend.Close)
			client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

			// Create table with GSI
			_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String(tt.tableName),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String(tt.gsiName),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("sk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
						ProvisionedThroughput: &types.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(tt.initialRCU),
							WriteCapacityUnits: aws.Int64(5),
						},
					},
				},
				BillingMode: types.BillingModeProvisioned,
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			})
			require.NoError(t, err)

			_, err = client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
				GlobalTableName: aws.String(tt.tableName),
				ReplicationGroup: []types.Replica{
					{RegionName: aws.String("us-east-1")},
					{RegionName: aws.String("eu-west-1")},
				},
			})
			require.NoError(t, err)

			// Describe settings before update
			descOut1, err := client.DescribeGlobalTableSettings(t.Context(), &sdk.DescribeGlobalTableSettingsInput{
				GlobalTableName: aws.String(tt.tableName),
			})
			require.NoError(t, err)
			replica1 := findReplicaSettings(t, descOut1.ReplicaSettings, "us-east-1")
			require.NotEmpty(t, replica1.ReplicaGlobalSecondaryIndexSettings)
			assert.Equal(t, tt.gsiName, *replica1.ReplicaGlobalSecondaryIndexSettings[0].IndexName)
			assert.Equal(
				t,
				tt.initialRCU,
				*replica1.ReplicaGlobalSecondaryIndexSettings[0].ProvisionedReadCapacityUnits,
			)

			// Update GSI settings on replica
			updateOut, err := client.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
				GlobalTableName: aws.String(tt.tableName),
				ReplicaSettingsUpdate: []types.ReplicaSettingsUpdate{
					{
						RegionName: aws.String("us-east-1"),
						ReplicaGlobalSecondaryIndexSettingsUpdate: []types.ReplicaGlobalSecondaryIndexSettingsUpdate{
							{
								IndexName:                    aws.String(tt.gsiName),
								ProvisionedReadCapacityUnits: aws.Int64(tt.updatedRCU),
							},
						},
					},
				},
			})
			require.NoError(t, err)
			upReplica := findReplicaSettings(t, updateOut.ReplicaSettings, "us-east-1")
			require.NotEmpty(t, upReplica.ReplicaGlobalSecondaryIndexSettings)
			assert.Equal(
				t,
				tt.updatedRCU,
				*upReplica.ReplicaGlobalSecondaryIndexSettings[0].ProvisionedReadCapacityUnits,
			)

			// Describe again to verify persisted read capacity
			descOut2, err := client.DescribeGlobalTableSettings(t.Context(), &sdk.DescribeGlobalTableSettingsInput{
				GlobalTableName: aws.String(tt.tableName),
			})
			require.NoError(t, err)
			replica2 := findReplicaSettings(t, descOut2.ReplicaSettings, "us-east-1")
			require.NotEmpty(t, replica2.ReplicaGlobalSecondaryIndexSettings)
			assert.Equal(
				t,
				tt.updatedRCU,
				*replica2.ReplicaGlobalSecondaryIndexSettings[0].ProvisionedReadCapacityUnits,
			)
		})
	}
}
