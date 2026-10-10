package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeEndpoint_DesiredServerlessConfigWire(t *testing.T) {
	t.Parallel()

	client := newTestSageMakerClient(t, newTestHandler(t))

	_, err := client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("cfg"),
		ProductionVariants: []smtypes.ProductionVariant{{
			VariantName: aws.String("v1"),
			ModelName:   aws.String("m"),
			ServerlessConfig: &smtypes.ProductionVariantServerlessConfig{
				MemorySizeInMB: aws.Int32(2048),
				MaxConcurrency: aws.Int32(7),
			},
		}},
	})
	require.NoError(t, err)

	_, err = client.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
		EndpointName:       aws.String("ep"),
		EndpointConfigName: aws.String("cfg"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEndpoint(t.Context(), &sagemakersdk.DescribeEndpointInput{
		EndpointName: aws.String("ep"),
	})
	require.NoError(t, err)
	require.Len(t, out.ProductionVariants, 1)
	require.NotNil(t, out.ProductionVariants[0].DesiredServerlessConfig)
	assert.Equal(t, int32(7), aws.ToInt32(out.ProductionVariants[0].DesiredServerlessConfig.MaxConcurrency))
	require.Len(t, out.ProductionVariants[0].VariantStatus, 1)
	assert.Equal(t, smtypes.VariantStatusCreating, out.ProductionVariants[0].VariantStatus[0].Status)
	assert.NotNil(t, out.ProductionVariants[0].VariantStatus[0].StartTime)
}
