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

func newWitnessTable(t *testing.T, name string) *dynamodbsdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	keySchema, attrDefs := wireTestKeySchema()

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName: aws.String(name), KeySchema: keySchema, AttributeDefinitions: attrDefs,
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

func TestUpdateTable_ReplicaOverrides(t *testing.T) {
	t.Parallel()

	client := newWitnessTable(t, "replica-overrides")
	ctx := t.Context()

	_, err := client.UpdateTable(ctx, &dynamodbsdk.UpdateTableInput{
		TableName: aws.String("replica-overrides"),
		ReplicaUpdates: []dynamodbtypes.ReplicationGroupUpdate{{
			Create: &dynamodbtypes.CreateReplicationGroupMemberAction{
				RegionName:     aws.String("eu-west-1"),
				KMSMasterKeyId: aws.String("alias/replica"),
				OnDemandThroughputOverride: &dynamodbtypes.OnDemandThroughputOverride{
					MaxReadRequestUnits: aws.Int64(500),
				},
				TableClassOverride: dynamodbtypes.TableClassStandardInfrequentAccess,
			},
		}},
	})
	require.NoError(t, err)

	desc, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{TableName: aws.String("replica-overrides")})
	require.NoError(t, err)
	require.Len(t, desc.Table.Replicas, 1)

	r := desc.Table.Replicas[0]
	assert.Equal(t, "alias/replica", aws.ToString(r.KMSMasterKeyId))
	assert.EqualValues(t, 500, aws.ToInt64(r.OnDemandThroughputOverride.MaxReadRequestUnits))
	assert.Equal(t, dynamodbtypes.TableClassStandardInfrequentAccess, r.ReplicaTableClassSummary.TableClass)

	_, err = client.UpdateTable(ctx, &dynamodbsdk.UpdateTableInput{
		TableName: aws.String("replica-overrides"),
		ReplicaUpdates: []dynamodbtypes.ReplicationGroupUpdate{{
			Update: &dynamodbtypes.UpdateReplicationGroupMemberAction{
				RegionName:     aws.String("eu-west-1"),
				KMSMasterKeyId: aws.String("alias/rotated"),
				OnDemandThroughputOverride: &dynamodbtypes.OnDemandThroughputOverride{
					MaxReadRequestUnits: aws.Int64(900),
				},
			},
		}},
	})
	require.NoError(t, err)

	desc, err = client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{TableName: aws.String("replica-overrides")})
	require.NoError(t, err)
	assert.Equal(t, "alias/rotated", aws.ToString(desc.Table.Replicas[0].KMSMasterKeyId))
	assert.EqualValues(t, 900, aws.ToInt64(desc.Table.Replicas[0].OnDemandThroughputOverride.MaxReadRequestUnits))
}

func TestUpdateTable_GlobalTableWitness(t *testing.T) {
	t.Parallel()

	createReplica := dynamodbtypes.ReplicationGroupUpdate{
		Create: &dynamodbtypes.CreateReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")},
	}
	witness := []dynamodbtypes.GlobalTableWitnessGroupUpdate{{
		Create: &dynamodbtypes.CreateGlobalTableWitnessGroupMemberAction{RegionName: aws.String("us-west-2")},
	}}
	two := []dynamodbtypes.GlobalTableWitnessGroupUpdate{witness[0], {
		Create: &dynamodbtypes.CreateGlobalTableWitnessGroupMemberAction{RegionName: aws.String("ap-south-1")},
	}}

	deleteWitness := []dynamodbtypes.GlobalTableWitnessGroupUpdate{{
		Delete: &dynamodbtypes.DeleteGlobalTableWitnessGroupMemberAction{RegionName: aws.String("us-west-2")},
	}}

	tests := []struct {
		input   *dynamodbsdk.UpdateTableInput
		name    string
		wantErr bool
	}{
		{
			name: "strong_with_replica",
			input: &dynamodbsdk.UpdateTableInput{
				MultiRegionConsistency:    dynamodbtypes.MultiRegionConsistencyStrong,
				ReplicaUpdates:            []dynamodbtypes.ReplicationGroupUpdate{createReplica},
				GlobalTableWitnessUpdates: witness,
			},
		},
		{
			name: "eventual_rejected", wantErr: true,
			input: &dynamodbsdk.UpdateTableInput{
				ReplicaUpdates:            []dynamodbtypes.ReplicationGroupUpdate{createReplica},
				GlobalTableWitnessUpdates: witness,
			},
		},
		{
			name: "without_replica_create_rejected", wantErr: true,
			input: &dynamodbsdk.UpdateTableInput{
				MultiRegionConsistency:    dynamodbtypes.MultiRegionConsistencyStrong,
				GlobalTableWitnessUpdates: witness,
			},
		},
		{
			name: "two_witnesses_rejected", wantErr: true,
			input: &dynamodbsdk.UpdateTableInput{
				MultiRegionConsistency:    dynamodbtypes.MultiRegionConsistencyStrong,
				ReplicaUpdates:            []dynamodbtypes.ReplicationGroupUpdate{createReplica},
				GlobalTableWitnessUpdates: two,
			},
		},
		{
			name: "delete_unknown_rejected", wantErr: true,
			input: &dynamodbsdk.UpdateTableInput{
				GlobalTableWitnessUpdates: deleteWitness,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWitnessTable(t, "witness-table")
			tt.input.TableName = aws.String("witness-table")

			out, err := client.UpdateTable(t.Context(), tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			witnesses := out.TableDescription.GlobalTableWitnesses
			require.Len(t, witnesses, 1)
			assert.Equal(t, "us-west-2", aws.ToString(witnesses[0].RegionName))
			assert.Equal(t, dynamodbtypes.WitnessStatusActive, witnesses[0].WitnessStatus)

			desc, err := client.DescribeTable(t.Context(), &dynamodbsdk.DescribeTableInput{
				TableName: aws.String("witness-table"),
			})
			require.NoError(t, err)
			require.Len(t, desc.Table.GlobalTableWitnesses, 1)

			_, err = client.UpdateTable(t.Context(), &dynamodbsdk.UpdateTableInput{
				TableName: aws.String("witness-table"),
				ReplicaUpdates: []dynamodbtypes.ReplicationGroupUpdate{{
					Delete: &dynamodbtypes.DeleteReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")},
				}},
				GlobalTableWitnessUpdates: deleteWitness,
			})
			require.NoError(t, err)

			desc, err = client.DescribeTable(t.Context(), &dynamodbsdk.DescribeTableInput{
				TableName: aws.String("witness-table"),
			})
			require.NoError(t, err)
			assert.Empty(t, desc.Table.GlobalTableWitnesses)
		})
	}
}
