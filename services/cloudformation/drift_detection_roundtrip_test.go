package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/require"
)

// TestDetectStackResourceDrift_RoundTrip drives DetectStackResourceDrift
// through the real aws-sdk-go-v2 client. gopherstack used to return
// DetectStackDrift's response shape (StackDriftDetectionId) for this op;
// the real required output member is StackResourceDrift
// (cloudformation@v1.76.1 api_op_DetectStackResourceDrift.go:55-63), so an
// unfixed handler decodes a zero-value StackResourceDrift here.
func TestDetectStackResourceDrift_RoundTrip(t *testing.T) {
	t.Parallel()

	const template = `{"Resources":{
		"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"b","AccessControl":"Private"}}
	}}`

	t.Run("in sync", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)

		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName:    aws.String("drift-rt-stack"),
			TemplateBody: aws.String(template),
		})
		require.NoError(t, err)

		out, err := client.DetectStackResourceDrift(t.Context(), &cfnsdk.DetectStackResourceDriftInput{
			StackName:         aws.String("drift-rt-stack"),
			LogicalResourceId: aws.String("Bucket"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.StackResourceDrift)

		drift := out.StackResourceDrift
		require.NotNil(
			t,
			drift.LogicalResourceId,
			"unfixed handler emits StackDriftDetectionId; SDK decodes nothing into StackResourceDrift",
		)
		require.Equal(t, "Bucket", aws.ToString(drift.LogicalResourceId))
		require.Equal(t, "AWS::S3::Bucket", aws.ToString(drift.ResourceType))
		require.Equal(t, types.StackResourceDriftStatusInSync, drift.StackResourceDriftStatus)
		require.NotNil(t, drift.Timestamp)
		require.NotEmpty(t, aws.ToString(drift.StackId))
	})

	t.Run("modified", func(t *testing.T) {
		t.Parallel()

		backend, client := newTestHandlerAndClientWithBackend(t)

		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName:    aws.String("drift-rt-stack-mod"),
			TemplateBody: aws.String(template),
		})
		require.NoError(t, err)

		backend.ForceModifyResourceProperties("drift-rt-stack-mod", "Bucket", map[string]any{
			"BucketName":    "b",
			"AccessControl": "PublicRead",
		})

		out, err := client.DetectStackResourceDrift(t.Context(), &cfnsdk.DetectStackResourceDriftInput{
			StackName:         aws.String("drift-rt-stack-mod"),
			LogicalResourceId: aws.String("Bucket"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.StackResourceDrift)

		drift := out.StackResourceDrift
		require.Equal(t, types.StackResourceDriftStatusModified, drift.StackResourceDriftStatus)
		require.NotEmpty(t, drift.PropertyDifferences)
		require.NotEmpty(t, aws.ToString(drift.ActualProperties))
		require.NotEmpty(t, aws.ToString(drift.ExpectedProperties))
	})
}
