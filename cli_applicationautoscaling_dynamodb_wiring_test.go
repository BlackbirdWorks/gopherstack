package main

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdkddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	aasbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// Drives the real composition root, not hand-wired backends, so deleting
// applicationautoscaling's SetAppConfig call breaks this test.
func TestInitializeServices_ApplicationAutoscalingDynamoDBWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	ddbH, ok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler)
	require.True(t, ok, "DynamoDB handler must be registered")

	aasH, ok := byName["ApplicationAutoscaling"].(*aasbackend.Handler)
	require.True(t, ok, "ApplicationAutoscaling handler must be registered")

	ctx := t.Context()

	tableName := "aas-ddb-wiring-table"
	_, err = ddbH.Backend.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []sdkddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: sdkddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []sdkddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: sdkddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: sdkddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &sdkddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = aasH.Backend.RegisterScalableTarget(
		"dynamodb", "table/"+tableName, "dynamodb:table:WriteCapacityUnits",
		aws.Int32(5), aws.Int32(500), nil, "", nil,
	)
	require.NoError(t, err)

	desc, err := ddbH.Backend.DescribeTableReplicaAutoScaling(ctx, &sdkddb.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)

	settings := desc.TableAutoScalingDescription.Replicas[0].ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(
		t,
		settings,
		"RegisterScalableTarget must have pushed capacity into DynamoDB's own autoscaling state",
	)
	require.Equal(t, int64(5), aws.ToInt64(settings.MinimumUnits))
	require.Equal(t, int64(500), aws.ToInt64(settings.MaximumUnits))

	// Reverse direction: a DynamoDB-native update shows up on DescribeScalableTargets.
	_, err = ddbH.Backend.UpdateTableReplicaAutoScaling(ctx, &sdkddb.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(tableName),
		ReplicaUpdates: []sdkddbtypes.ReplicaAutoScalingUpdate{
			{
				RegionName: aws.String("us-east-1"),
				ReplicaProvisionedReadCapacityAutoScalingUpdate: &sdkddbtypes.AutoScalingSettingsUpdate{
					MinimumUnits: aws.Int64(2),
					MaximumUnits: aws.Int64(200),
				},
			},
		},
	})
	require.NoError(t, err)

	targets, _, err := aasH.Backend.DescribeScalableTargets(aasbackend.DescribeScalableTargetsFilter{
		ServiceNamespace: "dynamodb",
		ResourceIDs:      []string{"table/" + tableName},
	})
	require.NoError(t, err)

	var found *aasbackend.ScalableTarget

	for _, tgt := range targets {
		if tgt.ScalableDimension == "dynamodb:table:ReadCapacityUnits" {
			found = tgt
		}
	}

	require.NotNil(t, found, "UpdateTableReplicaAutoScaling must be visible via DescribeScalableTargets")
	require.Equal(t, int32(2), found.MinCapacity)
	require.Equal(t, int32(200), found.MaxCapacity)
}
