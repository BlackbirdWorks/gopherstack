package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	sagemakertypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func createSageMakerRuntimeClient(t *testing.T) *sagemakerruntime.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sagemakerruntime.NewFromConfig(cfg, func(o *sagemakerruntime.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_SageMakerRuntime_InvokeEndpoint(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	admin := createSageMakerIntegClient(t)
	client := createSageMakerRuntimeClient(t)
	suffix := uuid.NewString()[:8]
	model, cfgName, endpointName := "rt-model-"+suffix, "rt-cfg-"+suffix, "rt-ep-"+suffix

	_, err := admin.CreateModel(ctx, &sagemakersdk.CreateModelInput{
		ModelName:        aws.String(model),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/sagemaker-role"),
		PrimaryContainer: &sagemakertypes.ContainerDefinition{
			Image: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"),
		},
	})
	require.NoError(t, err)

	_, err = admin.CreateEndpointConfig(ctx, &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String(cfgName),
		ProductionVariants: []sagemakertypes.ProductionVariant{{
			VariantName:          aws.String("AllTraffic"),
			ModelName:            aws.String(model),
			InstanceType:         sagemakertypes.ProductionVariantInstanceTypeMlM5Large,
			InitialInstanceCount: aws.Int32(1),
		}},
	})
	require.NoError(t, err)

	_, err = admin.CreateEndpoint(ctx, &sagemakersdk.CreateEndpointInput{
		EndpointName:       aws.String(endpointName),
		EndpointConfigName: aws.String(cfgName),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, descErr := admin.DescribeEndpoint(
			ctx, &sagemakersdk.DescribeEndpointInput{EndpointName: aws.String(endpointName)},
		)

		return descErr == nil && out.EndpointStatus == sagemakertypes.EndpointStatusInService
	}, 30*time.Second, 200*time.Millisecond, "endpoint should reach InService")

	_, err = client.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{
		EndpointName: aws.String(endpointName),
		Body:         []byte("{}"),
	})
	require.NoError(t, err)
}
