package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestUpdateTableReplicaAutoScaling_ScalingPolicyAndRoleArn_SurvivesWireConversion
// verifies AutoScalingRoleArn and ScalingPolicyUpdate (a real, wire-serialized
// input member per api_op_UpdateTableReplicaAutoScaling.go's
// AutoScalingSettingsUpdate) round-trip through Update and Describe as
// AutoScalingRoleArn/ScalingPolicies on the response -- echoed exactly as the
// caller supplied them, not fabricated (this backend has no IAM/scaling-policy
// engine behind them).
func TestUpdateTableReplicaAutoScaling_ScalingPolicyAndRoleArn_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-policy-table"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("eu-west-1")},
		},
	})
	require.NoError(t, err)

	out, err := client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("gt-policy-table"),
		ProvisionedWriteCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
			MinimumUnits:       aws.Int64(5),
			MaximumUnits:       aws.Int64(500),
			AutoScalingRoleArn: aws.String("arn:aws:iam::123456789012:role/DynamoDBAutoscaleRole"),
			ScalingPolicyUpdate: &types.AutoScalingPolicyUpdate{
				PolicyName: aws.String("my-write-policy"),
				TargetTrackingScalingPolicyConfiguration: &types.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
					TargetValue:      aws.Float64(70.0),
					DisableScaleIn:   aws.Bool(true),
					ScaleInCooldown:  aws.Int32(60),
					ScaleOutCooldown: aws.Int32(30),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.TableAutoScalingDescription.Replicas, 1)

	settings := out.TableAutoScalingDescription.Replicas[0].ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(t, settings)
	assert.Equal(t, "arn:aws:iam::123456789012:role/DynamoDBAutoscaleRole", aws.ToString(settings.AutoScalingRoleArn))
	require.Len(t, settings.ScalingPolicies, 1)
	policy := settings.ScalingPolicies[0]
	assert.Equal(t, "my-write-policy", aws.ToString(policy.PolicyName))
	require.NotNil(t, policy.TargetTrackingScalingPolicyConfiguration)
	assert.InDelta(t, 70.0, aws.ToFloat64(policy.TargetTrackingScalingPolicyConfiguration.TargetValue), 0.001)
	assert.True(t, aws.ToBool(policy.TargetTrackingScalingPolicyConfiguration.DisableScaleIn))
	assert.Equal(t, int32(60), aws.ToInt32(policy.TargetTrackingScalingPolicyConfiguration.ScaleInCooldown))
	assert.Equal(t, int32(30), aws.ToInt32(policy.TargetTrackingScalingPolicyConfiguration.ScaleOutCooldown))

	desc, err := client.DescribeTableReplicaAutoScaling(t.Context(), &sdk.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String("gt-policy-table"),
	})
	require.NoError(t, err)
	require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)

	descSettings := desc.TableAutoScalingDescription.Replicas[0].ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(t, descSettings)
	assert.Equal(
		t,
		"arn:aws:iam::123456789012:role/DynamoDBAutoscaleRole",
		aws.ToString(descSettings.AutoScalingRoleArn),
		"AutoScalingRoleArn must also survive on DescribeTableReplicaAutoScaling",
	)
	require.Len(t, descSettings.ScalingPolicies, 1)
	assert.Equal(t, "my-write-policy", aws.ToString(descSettings.ScalingPolicies[0].PolicyName))
}

// TestUpdateTableReplicaAutoScaling_ReplicaUpdates_ReadCapacity_RoundTrip
// verifies ReplicaUpdates -- previously entirely dropped at the wire layer
// (handler_autoscaling.go's updateTableReplicaAutoScalingInput declared no
// field for it) -- now reaches the backend and is reflected back as
// ReplicaProvisionedReadCapacityAutoScalingSettings on that replica alone.
func TestUpdateTableReplicaAutoScaling_ReplicaUpdates_ReadCapacity_RoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-replica-read"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("eu-west-1")},
			{RegionName: aws.String("ap-southeast-2")},
		},
	})
	require.NoError(t, err)

	out, err := client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("gt-replica-read"),
		ReplicaUpdates: []types.ReplicaAutoScalingUpdate{
			{
				RegionName: aws.String("eu-west-1"),
				ReplicaProvisionedReadCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
					MinimumUnits: aws.Int64(3),
					MaximumUnits: aws.Int64(300),
				},
				ReplicaGlobalSecondaryIndexUpdates: []types.ReplicaGlobalSecondaryIndexAutoScalingUpdate{
					{
						IndexName: aws.String("gsi-1"),
						ProvisionedReadCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
							MinimumUnits: aws.Int64(1),
							MaximumUnits: aws.Int64(100),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	byRegion := replicaAutoScalingByRegion(out.TableAutoScalingDescription.Replicas)

	untouched := byRegion["ap-southeast-2"]
	require.NotNil(t, untouched)
	assert.Nil(t, untouched.ReplicaProvisionedReadCapacityAutoScalingSettings,
		"a replica not named in ReplicaUpdates must not get another replica's read settings")

	euWest := byRegion["eu-west-1"]
	require.NotNil(t, euWest)
	require.NotNil(t, euWest.ReplicaProvisionedReadCapacityAutoScalingSettings)
	assert.Equal(t, int64(3), aws.ToInt64(euWest.ReplicaProvisionedReadCapacityAutoScalingSettings.MinimumUnits))
	assert.Equal(t, int64(300), aws.ToInt64(euWest.ReplicaProvisionedReadCapacityAutoScalingSettings.MaximumUnits))
	require.Len(t, euWest.GlobalSecondaryIndexes, 1)
	assert.Equal(t, "gsi-1", aws.ToString(euWest.GlobalSecondaryIndexes[0].IndexName))
	require.NotNil(t, euWest.GlobalSecondaryIndexes[0].ProvisionedReadCapacityAutoScalingSettings)
	assert.Equal(
		t,
		int64(1),
		aws.ToInt64(euWest.GlobalSecondaryIndexes[0].ProvisionedReadCapacityAutoScalingSettings.MinimumUnits),
	)

	desc, err := client.DescribeTableReplicaAutoScaling(t.Context(), &sdk.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String("gt-replica-read"),
	})
	require.NoError(t, err)

	descByRegion := replicaAutoScalingByRegion(desc.TableAutoScalingDescription.Replicas)
	descEuWest := descByRegion["eu-west-1"]
	require.NotNil(t, descEuWest)
	require.NotNil(t, descEuWest.ReplicaProvisionedReadCapacityAutoScalingSettings)
	assert.Equal(t, int64(3), aws.ToInt64(descEuWest.ReplicaProvisionedReadCapacityAutoScalingSettings.MinimumUnits))
}

func replicaAutoScalingByRegion(
	replicas []types.ReplicaAutoScalingDescription,
) map[string]*types.ReplicaAutoScalingDescription {
	out := make(map[string]*types.ReplicaAutoScalingDescription, len(replicas))
	for i := range replicas {
		out[aws.ToString(replicas[i].RegionName)] = &replicas[i]
	}

	return out
}

// TestUpdateTableReplicaAutoScaling_ReplicaUpdates_UnknownRegion_ResourceNotFound
// verifies a ReplicaUpdates entry naming a region that isn't one of the
// table's replicas is rejected with ResourceNotFoundException -- a real,
// documented error for this op (confirmed against
// awsAwsjson10_deserializeOpErrorUpdateTableReplicaAutoScaling in the pinned SDK).
func TestUpdateTableReplicaAutoScaling_ReplicaUpdates_UnknownRegion_ResourceNotFound(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-no-replica"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("gt-no-replica"),
		ReplicaUpdates: []types.ReplicaAutoScalingUpdate{
			{
				RegionName: aws.String("ap-southeast-1"),
				ReplicaProvisionedReadCapacityAutoScalingUpdate: &types.AutoScalingSettingsUpdate{
					MinimumUnits: aws.Int64(1),
					MaximumUnits: aws.Int64(10),
				},
			},
		},
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

// TestUpdateTableReplicaAutoScaling_Validation covers the table-driven
// validation error cases: billing mode gate, and MinimumUnits > MaximumUnits.
func TestUpdateTableReplicaAutoScaling_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update      *types.AutoScalingSettingsUpdate
		name        string
		billingMode types.BillingMode
	}{
		{
			name:        "on_demand_table_rejects_autoscaling_settings",
			billingMode: types.BillingModePayPerRequest,
			update: &types.AutoScalingSettingsUpdate{
				MinimumUnits: aws.Int64(5),
				MaximumUnits: aws.Int64(50),
			},
		},
		{
			name:        "minimum_greater_than_maximum",
			billingMode: types.BillingModeProvisioned,
			update: &types.AutoScalingSettingsUpdate{
				MinimumUnits: aws.Int64(500),
				MaximumUnits: aws.Int64(5),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			tableName := "as-validation-" + tt.name

			rc, wc := int64(5), int64(5)
			createInput := &sdk.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: tt.billingMode,
			}
			if tt.billingMode == types.BillingModeProvisioned {
				createInput.ProvisionedThroughput = &types.ProvisionedThroughput{
					ReadCapacityUnits:  &rc,
					WriteCapacityUnits: &wc,
				}
			}
			_, err := client.CreateTable(t.Context(), createInput)
			require.NoError(t, err)

			_, err = client.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
				TableName: aws.String(tableName),
				ProvisionedWriteCapacityAutoScalingUpdate: tt.update,
			})
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "expected a smithy.APIError")
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
