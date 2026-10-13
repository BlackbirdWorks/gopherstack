package comprehend_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	comprehendsdk "github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

func TestFlywheelIteration_TrainedModelFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		modelType types.ModelType
		wantSub   string
	}{
		{name: "classifier", modelType: types.ModelTypeDocumentClassifier, wantSub: ":document-classifier/"},
		{name: "recognizer", modelType: types.ModelTypeEntityRecognizer, wantSub: ":entity-recognizer/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestComprehendSDKClient(t, h)

			fw, err := client.CreateFlywheel(t.Context(), &comprehendsdk.CreateFlywheelInput{
				FlywheelName:      aws.String("iter-" + tt.name),
				DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				DataLakeS3Uri:     aws.String("s3://bucket/prefix"),
				ModelType:         tt.modelType,
			})
			require.NoError(t, err)

			started, err := client.StartFlywheelIteration(t.Context(), &comprehendsdk.StartFlywheelIterationInput{
				FlywheelArn: fw.FlywheelArn,
			})
			require.NoError(t, err)

			describe := func() *types.FlywheelIterationProperties {
				out, descErr := client.DescribeFlywheelIteration(
					t.Context(),
					&comprehendsdk.DescribeFlywheelIterationInput{
						FlywheelArn:         fw.FlywheelArn,
						FlywheelIterationId: started.FlywheelIterationId,
					},
				)
				require.NoError(t, descErr)

				return out.FlywheelIterationProperties
			}

			evaluating := describe()
			assert.Equal(t, types.FlywheelIterationStatusEvaluating, evaluating.Status)
			require.NotNil(t, evaluating.TrainedModelArn)
			assert.Contains(t, *evaluating.TrainedModelArn, tt.wantSub)
			require.NotNil(t, evaluating.TrainedModelMetrics)
			assert.Nil(t, evaluating.EvaluatedModelMetrics)

			completed := describe()
			assert.Equal(t, types.FlywheelIterationStatusCompleted, completed.Status)
			assert.Equal(t, evaluating.TrainedModelArn, completed.EvaluatedModelArn)
			require.NotNil(t, completed.EvaluatedModelMetrics)
			assert.NotNil(t, completed.EvaluatedModelMetrics.AverageAccuracy)

			model, err := client.DescribeDocumentClassifier(t.Context(), &comprehendsdk.DescribeDocumentClassifierInput{
				DocumentClassifierArn: completed.TrainedModelArn,
			})
			if tt.modelType == types.ModelTypeDocumentClassifier {
				require.NoError(t, err)
				assert.Equal(t, fw.FlywheelArn, model.DocumentClassifierProperties.FlywheelArn)
			} else {
				require.Error(t, err)
			}
		})
	}
}
