//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	sagemakertypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSageMakerIntegClient returns a SageMaker client pointed at the shared test container.
func createSageMakerIntegClient(t *testing.T) *sagemakersdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return sagemakersdk.NewFromConfig(cfg, func(o *sagemakersdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_SageMaker_ModelLifecycle tests the full SageMaker model CRUD lifecycle.
func TestIntegration_SageMaker_ModelLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		modelName string
		image     string
		roleARN   string
	}{
		{
			name:      "full_lifecycle",
			modelName: "integ-model-" + uuid.NewString()[:8],
			image:     "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
			roleARN:   "arn:aws:iam::000000000000:role/sagemaker-role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createSageMakerIntegClient(t)

			// Create model.
			_, err := client.CreateModel(ctx, &sagemakersdk.CreateModelInput{
				ModelName:        aws.String(tt.modelName),
				ExecutionRoleArn: aws.String(tt.roleARN),
				PrimaryContainer: &sagemakertypes.ContainerDefinition{
					Image: aws.String(tt.image),
				},
			})
			require.NoError(t, err, "CreateModel should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteModel(cleanupCtx, &sagemakersdk.DeleteModelInput{
					ModelName: aws.String(tt.modelName),
				})
			})

			// Describe model.
			descOut, err := client.DescribeModel(ctx, &sagemakersdk.DescribeModelInput{
				ModelName: aws.String(tt.modelName),
			})
			require.NoError(t, err, "DescribeModel should succeed")
			assert.Equal(t, tt.modelName, aws.ToString(descOut.ModelName))
			require.NotNil(t, descOut.PrimaryContainer)
			assert.Equal(t, tt.image, aws.ToString(descOut.PrimaryContainer.Image))
			assert.Equal(t, "arn:aws:iam::000000000000:role/sagemaker-role", aws.ToString(descOut.ExecutionRoleArn))

			// List models with filter.
			listOut, err := client.ListModels(ctx, &sagemakersdk.ListModelsInput{
				NameContains: aws.String("integ-model"),
			})
			require.NoError(t, err, "ListModels should succeed")

			found := false
			for _, m := range listOut.Models {
				if aws.ToString(m.ModelName) == tt.modelName {
					found = true

					break
				}
			}

			assert.True(t, found, "created model should appear in list")

			// Delete model.
			_, err = client.DeleteModel(ctx, &sagemakersdk.DeleteModelInput{
				ModelName: aws.String(tt.modelName),
			})
			require.NoError(t, err, "DeleteModel should succeed")

			// Verify model is gone.
			listOut2, err := client.ListModels(ctx, &sagemakersdk.ListModelsInput{
				NameContains: aws.String(tt.modelName),
			})
			require.NoError(t, err, "ListModels after delete should succeed")

			for _, m := range listOut2.Models {
				assert.NotEqual(t, tt.modelName, aws.ToString(m.ModelName), "deleted model should not appear in list")
			}
		})
	}
}
