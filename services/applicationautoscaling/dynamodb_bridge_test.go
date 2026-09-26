package applicationautoscaling_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// fakeDynamoDBSibling satisfies siblingServices structurally, mirroring *CLI.
type fakeDynamoDBSibling struct {
	ddbHandler service.Registerable
}

func (f *fakeDynamoDBSibling) GetDynamoDBHandler() service.Registerable { return f.ddbHandler }

// newTestDDBSDKClient stands up the real aws-sdk-go-v2 dynamodb client against
// an httptest server running h.
func newTestDDBSDKClient(t *testing.T, h *ddbbackend.DynamoDBHandler) *ddbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ddbsdk.NewFromConfig(cfg, func(o *ddbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newWiredBackends builds a DynamoDB and an ApplicationAutoScaling
// backend/client pair, wired together via SetAppConfig.
func newWiredBackends(t *testing.T) (*ddbsdk.Client, *aassdk.Client) {
	t.Helper()

	ddbHandler := ddbbackend.NewHandler(ddbbackend.NewInMemoryDB())
	ddbClient := newTestDDBSDKClient(t, ddbHandler)

	aasBk := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	aasBk.SetAppConfig(&fakeDynamoDBSibling{ddbHandler: ddbHandler})
	aasClient := newTestAASSDKClient(t, applicationautoscaling.NewHandler(aasBk))

	return ddbClient, aasClient
}

// createProvisionedTable creates a minimal PROVISIONED-billing table --
// DynamoDB rejects autoscaling settings against a PAY_PER_REQUEST table.
func createProvisionedTable(t *testing.T, ddbClient *ddbsdk.Client, name string) {
	t.Helper()

	_, err := ddbClient.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
}

// createOnDemandTable creates a minimal PAY_PER_REQUEST-billing table.
func createOnDemandTable(t *testing.T, ddbClient *ddbsdk.Client, name string) {
	t.Helper()

	_, err := ddbClient.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

// On-demand tables get AAS's own ValidationException, not DynamoDB's wrapped error.
func TestRegisterScalableTarget_DynamoDB_PayPerRequestRejected(t *testing.T) {
	t.Parallel()

	ddbClient, aasClient := newWiredBackends(t)
	createOnDemandTable(t, ddbClient, "ondemand-table")

	_, err := aasClient.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/ondemand-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		MinCapacity:       aws.Int32(5),
		MaxCapacity:       aws.Int32(500),
	})
	require.Error(t, err)

	var vErr *aastypes.ValidationException
	require.ErrorAs(t, err, &vErr)
	assert.Contains(t, aws.ToString(vErr.Message), "PAY_PER_REQUEST table mode is not scalable")
	assert.NotContains(t, err.Error(), "amazonaws.dynamodb", "must not leak DynamoDB's own error namespace")
}

// A table switched to on-demand after registration is rejected at PutScalingPolicy.
func TestPutScalingPolicy_DynamoDB_PayPerRequestRejected(t *testing.T) {
	t.Parallel()

	ddbClient, aasClient := newWiredBackends(t)
	createProvisionedTable(t, ddbClient, "switched-table")

	ctx := t.Context()

	_, err := aasClient.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/switched-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		MinCapacity:       aws.Int32(5),
		MaxCapacity:       aws.Int32(500),
	})
	require.NoError(t, err)

	_, err = ddbClient.UpdateTable(ctx, &ddbsdk.UpdateTableInput{
		TableName:   aws.String("switched-table"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = aasClient.PutScalingPolicy(ctx, &aassdk.PutScalingPolicyInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/switched-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		PolicyName:        aws.String("switched-policy"),
		PolicyType:        aastypes.PolicyTypeTargetTrackingScaling,
		TargetTrackingScalingPolicyConfiguration: &aastypes.TargetTrackingScalingPolicyConfiguration{
			TargetValue: aws.Float64(70),
			PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
				PredefinedMetricType: aastypes.MetricTypeDynamoDBWriteCapacityUtilization,
			},
		},
	})
	require.Error(t, err)

	var vErr *aastypes.ValidationException
	require.ErrorAs(t, err, &vErr)
	assert.Contains(t, aws.ToString(vErr.Message), "PAY_PER_REQUEST table mode is not scalable")
	assert.NotContains(t, err.Error(), "amazonaws.dynamodb", "must not leak DynamoDB's own error namespace")
}

// RegisterScalableTarget(ns=dynamodb) must push capacity into DynamoDB's own
// autoscaling state, so DescribeTableReplicaAutoScaling agrees.
func TestRegisterScalableTarget_DynamoDB_ReflectsInDescribeTableReplicaAutoScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dimension aastypes.ScalableDimension
		read      bool
		minCap    int32
		maxCap    int32
	}{
		{dimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits, read: false, minCap: 5, maxCap: 500},
		{dimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits, read: true, minCap: 3, maxCap: 300},
	}

	for _, tt := range tests {
		t.Run(string(tt.dimension), func(t *testing.T) {
			t.Parallel()

			ddbClient, aasClient := newWiredBackends(t)
			createProvisionedTable(t, ddbClient, "wire-table")

			_, err := aasClient.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
				ResourceId:        aws.String("table/wire-table"),
				ScalableDimension: tt.dimension,
				MinCapacity:       aws.Int32(tt.minCap),
				MaxCapacity:       aws.Int32(tt.maxCap),
			})
			require.NoError(t, err)

			desc, err := ddbClient.DescribeTableReplicaAutoScaling(
				t.Context(),
				&ddbsdk.DescribeTableReplicaAutoScalingInput{
					TableName: aws.String("wire-table"),
				},
			)
			require.NoError(t, err)
			require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)

			replica := desc.TableAutoScalingDescription.Replicas[0]

			settings := replica.ReplicaProvisionedWriteCapacityAutoScalingSettings
			if tt.read {
				settings = replica.ReplicaProvisionedReadCapacityAutoScalingSettings
			}

			require.NotNil(t, settings)
			assert.Equal(t, int64(tt.minCap), aws.ToInt64(settings.MinimumUnits))
			assert.Equal(t, int64(tt.maxCap), aws.ToInt64(settings.MaximumUnits))
		})
	}
}

func TestRegisterScalableTarget_DynamoDB_TableDoesNotExist(t *testing.T) {
	t.Parallel()

	_, aasClient := newWiredBackends(t)

	_, err := aasClient.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/no-such-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(10),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DynamoDB table does not exist: table/no-such-table")

	var vErr *aastypes.ValidationException
	require.ErrorAs(t, err, &vErr)
}

// A capacity-only RegisterScalableTarget call must not wipe out a policy
// PutScalingPolicy configured earlier (same clobber-bug class as gopherstack-1vv2).
func TestRegisterScalableTarget_DynamoDB_DoesNotClobberScalingPolicy(t *testing.T) {
	t.Parallel()

	ddbClient, aasClient := newWiredBackends(t)
	createProvisionedTable(t, ddbClient, "carry-forward-table")

	ctx := t.Context()

	_, err := aasClient.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/carry-forward-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		MinCapacity:       aws.Int32(5),
		MaxCapacity:       aws.Int32(500),
	})
	require.NoError(t, err)

	_, err = aasClient.PutScalingPolicy(ctx, &aassdk.PutScalingPolicyInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/carry-forward-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		PolicyName:        aws.String("carry-forward-policy"),
		PolicyType:        aastypes.PolicyTypeTargetTrackingScaling,
		TargetTrackingScalingPolicyConfiguration: &aastypes.TargetTrackingScalingPolicyConfiguration{
			TargetValue: aws.Float64(70),
			PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
				PredefinedMetricType: aastypes.MetricTypeDynamoDBWriteCapacityUtilization,
			},
		},
	})
	require.NoError(t, err)

	_, err = aasClient.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/carry-forward-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
		MinCapacity:       aws.Int32(10),
		MaxCapacity:       aws.Int32(1000),
	})
	require.NoError(t, err)

	desc, err := ddbClient.DescribeTableReplicaAutoScaling(ctx, &ddbsdk.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String("carry-forward-table"),
	})
	require.NoError(t, err)
	require.Len(t, desc.TableAutoScalingDescription.Replicas, 1)

	settings := desc.TableAutoScalingDescription.Replicas[0].ReplicaProvisionedWriteCapacityAutoScalingSettings
	require.NotNil(t, settings)
	assert.Equal(t, int64(10), aws.ToInt64(settings.MinimumUnits))
	assert.Equal(t, int64(1000), aws.ToInt64(settings.MaximumUnits))
	require.Len(t, settings.ScalingPolicies, 1, "the earlier PutScalingPolicy call must survive")
	assert.Equal(t, "carry-forward-policy", aws.ToString(settings.ScalingPolicies[0].PolicyName))
}

// The reverse direction: settings set via DynamoDB's own API must show up on
// DescribeScalableTargets.
func TestUpdateTableReplicaAutoScaling_DynamoDB_ReflectsInDescribeScalableTargets(t *testing.T) {
	t.Parallel()

	ddbClient, aasClient := newWiredBackends(t)
	createProvisionedTable(t, ddbClient, "vv-table")

	_, err := ddbClient.UpdateTableReplicaAutoScaling(t.Context(), &ddbsdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("vv-table"),
		ProvisionedWriteCapacityAutoScalingUpdate: &ddbtypes.AutoScalingSettingsUpdate{
			MinimumUnits: aws.Int64(3),
			MaximumUnits: aws.Int64(300),
		},
	})
	require.NoError(t, err)

	out, err := aasClient.DescribeScalableTargets(t.Context(), &aassdk.DescribeScalableTargetsInput{
		ServiceNamespace: aastypes.ServiceNamespaceDynamodb,
		ResourceIds:      []string{"table/vv-table"},
	})
	require.NoError(t, err)
	require.Len(t, out.ScalableTargets, 1)

	target := out.ScalableTargets[0]
	assert.Equal(t, "table/vv-table", aws.ToString(target.ResourceId))
	assert.Equal(t, aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits, target.ScalableDimension)
	assert.Equal(t, int32(3), aws.ToInt32(target.MinCapacity))
	assert.Equal(t, int32(300), aws.ToInt32(target.MaxCapacity))
}

func TestUpdateTableReplicaAutoScaling_DynamoDB_ReflectsInDescribeScalingPolicies(t *testing.T) {
	t.Parallel()

	ddbClient, aasClient := newWiredBackends(t)
	createProvisionedTable(t, ddbClient, "vv-policy-table")

	_, err := ddbClient.UpdateTableReplicaAutoScaling(t.Context(), &ddbsdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("vv-policy-table"),
		ProvisionedWriteCapacityAutoScalingUpdate: &ddbtypes.AutoScalingSettingsUpdate{
			MinimumUnits: aws.Int64(1),
			MaximumUnits: aws.Int64(100),
			ScalingPolicyUpdate: &ddbtypes.AutoScalingPolicyUpdate{
				PolicyName: aws.String("native-policy"),
				TargetTrackingScalingPolicyConfiguration: &ddbtypes.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
					TargetValue: aws.Float64(65),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := aasClient.DescribeScalingPolicies(t.Context(), &aassdk.DescribeScalingPoliciesInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/vv-policy-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableWriteCapacityUnits,
	})
	require.NoError(t, err)
	require.Len(t, out.ScalingPolicies, 1)

	p := out.ScalingPolicies[0]
	assert.Equal(t, "native-policy", aws.ToString(p.PolicyName))
	require.NotNil(t, p.TargetTrackingScalingPolicyConfiguration)
	assert.InDelta(t, 65.0, aws.ToFloat64(p.TargetTrackingScalingPolicyConfiguration.TargetValue), 0.001)
}

// The bridge degrades gracefully when the DynamoDB sibling isn't wired.
func TestRegisterScalableTarget_DynamoDB_NoSiblingWired(t *testing.T) {
	t.Parallel()

	aasBk := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	aasClient := newTestAASSDKClient(t, applicationautoscaling.NewHandler(aasBk))

	_, err := aasClient.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
		ResourceId:        aws.String("table/unwired-table"),
		ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(10),
	})
	require.NoError(t, err)
}
