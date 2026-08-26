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

func TestUpdateTableReplicaAutoScaling_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("NoTable"),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_EmptyName(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(""),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_ReturnsDescription(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "ASTable")

	out, err := db.UpdateTableReplicaAutoScaling(
		t.Context(),
		&sdk.UpdateTableReplicaAutoScalingInput{
			TableName: aws.String("ASTable"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.TableAutoScalingDescription)
	assert.Equal(t, "ASTable", aws.ToString(out.TableAutoScalingDescription.TableName))
}

// TestUpdateTableReplicaAutoScaling_ProvisionedWriteCapacityAutoScalingUpdate_SurvivesWireConversion
// verifies that UpdateTableReplicaAutoScalingInput.ProvisionedWriteCapacityAutoScalingUpdate
// reaches the backend and is reflected back on both the Update response and a
// subsequent DescribeTableReplicaAutoScaling call. updateTableReplicaAutoScalingInput
// previously declared only TableName, so a real client's autoscaling
// configuration -- the entire point of the operation -- never reached
// InMemoryDB.UpdateTableReplicaAutoScaling, which already applies it correctly
// once it arrives.
func TestUpdateTableReplicaAutoScaling_ProvisionedWriteCapacityAutoScalingUpdate_SurvivesWireConversion(
	t *testing.T,
) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-as-table"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("eu-west-1")},
		},
	})
	require.NoError(t, err)

	out, err := client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("gt-as-table"),
		ProvisionedWriteCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
			MinimumUnits:        aws.Int64(5),
			MaximumUnits:        aws.Int64(500),
			AutoScalingDisabled: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	require.NotNil(t, out.TableAutoScalingDescription)
	require.Len(t, out.TableAutoScalingDescription.Replicas, 1)

	updateSettings := out.TableAutoScalingDescription.Replicas[0].
		ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(
		t,
		updateSettings,
		"ProvisionedWriteCapacityAutoScalingUpdate must survive the wire round-trip",
	)
	assert.Equal(t, int64(5), aws.ToInt64(updateSettings.MinimumUnits))
	assert.Equal(t, int64(500), aws.ToInt64(updateSettings.MaximumUnits))

	desc, err := client.DescribeTableReplicaAutoScaling(t.Context(), &sdk.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String("gt-as-table"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.TableAutoScalingDescription)
	require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)

	descSettings := desc.TableAutoScalingDescription.Replicas[0].
		ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(t, descSettings, "settings must also survive on DescribeTableReplicaAutoScaling")
	assert.Equal(t, int64(5), aws.ToInt64(descSettings.MinimumUnits))
	assert.Equal(t, int64(500), aws.ToInt64(descSettings.MaximumUnits))
}

func TestUpdateTableReplicaAutoScaling_GlobalSecondaryIndexUpdates_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		indexName string
		minUnits  int64
		maxUnits  int64
		disabled  bool
	}{
		{
			name:      "single_gsi_autoscaling",
			indexName: "gsi-1",
			minUnits:  10,
			maxUnits:  1000,
			disabled:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

			_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
				GlobalTableName: aws.String("gt-gsi-table"),
				ReplicationGroup: []types.Replica{
					{RegionName: aws.String("us-east-1")},
					{RegionName: aws.String("eu-west-1")},
				},
			})
			require.NoError(t, err)

			out, err := client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
				TableName: aws.String("gt-gsi-table"),
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexAutoScalingUpdate{
					{
						IndexName: aws.String(tt.indexName),
						ProvisionedWriteCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
							MinimumUnits:        aws.Int64(tt.minUnits),
							MaximumUnits:        aws.Int64(tt.maxUnits),
							AutoScalingDisabled: aws.Bool(tt.disabled),
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, out.TableAutoScalingDescription)
			require.Len(t, out.TableAutoScalingDescription.Replicas, 1)
			require.Len(t, out.TableAutoScalingDescription.Replicas[0].GlobalSecondaryIndexes, 1)

			gsi := out.TableAutoScalingDescription.Replicas[0].GlobalSecondaryIndexes[0]
			assert.Equal(t, tt.indexName, aws.ToString(gsi.IndexName))
			require.NotNil(t, gsi.ProvisionedWriteCapacityAutoScalingSettings)
			assert.Equal(t, tt.minUnits, aws.ToInt64(gsi.ProvisionedWriteCapacityAutoScalingSettings.MinimumUnits))
			assert.Equal(t, tt.maxUnits, aws.ToInt64(gsi.ProvisionedWriteCapacityAutoScalingSettings.MaximumUnits))

			desc, err := client.DescribeTableReplicaAutoScaling(t.Context(), &sdk.DescribeTableReplicaAutoScalingInput{
				TableName: aws.String("gt-gsi-table"),
			})
			require.NoError(t, err)
			require.NotNil(t, desc.TableAutoScalingDescription)
			require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)
			require.Len(t, desc.TableAutoScalingDescription.Replicas[0].GlobalSecondaryIndexes, 1)

			descGsi := desc.TableAutoScalingDescription.Replicas[0].GlobalSecondaryIndexes[0]
			assert.Equal(t, tt.indexName, aws.ToString(descGsi.IndexName))
			require.NotNil(t, descGsi.ProvisionedWriteCapacityAutoScalingSettings)
			assert.Equal(t, tt.minUnits, aws.ToInt64(descGsi.ProvisionedWriteCapacityAutoScalingSettings.MinimumUnits))
			assert.Equal(t, tt.maxUnits, aws.ToInt64(descGsi.ProvisionedWriteCapacityAutoScalingSettings.MaximumUnits))
		})
	}
}
