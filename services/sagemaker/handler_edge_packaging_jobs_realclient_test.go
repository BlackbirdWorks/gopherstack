package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListEdgePackagingJobs_CompilationJobName_RealClient covers
// gopherstack-6flj's PARITY-gap targeting queue: ListEdgePackagingJobs never
// appears by name in services/sagemaker/PARITY.md. Real
// types.EdgePackagingJobSummary declares CompilationJobName
// (aws-sdk-go-v2/service/sagemaker/types/types.go), and DescribeEdgePackagingJob
// already surfaces the backend-tracked value; the List summary omitted it
// entirely, so a real client's list decoded every item's CompilationJobName as
// nil regardless of what was set at creation.
func TestListEdgePackagingJobs_CompilationJobName_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"edge-job-one", "edge-job-two"}
	compilationJobs := []string{"comp-job-alpha", "comp-job-beta"}

	for i, name := range names {
		_, err := client.CreateEdgePackagingJob(t.Context(), &sagemakersdk.CreateEdgePackagingJobInput{
			EdgePackagingJobName: aws.String(name),
			ModelName:            aws.String("some-model"),
			ModelVersion:         aws.String("1.0"),
			RoleArn:              aws.String("arn:aws:iam::000000000000:role/TestRole"),
			CompilationJobName:   aws.String(compilationJobs[i]),
			OutputConfig: &smtypes.EdgeOutputConfig{
				S3OutputLocation: aws.String("s3://bucket/edge-out"),
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListEdgePackagingJobs(t.Context(), &sagemakersdk.ListEdgePackagingJobsInput{})
	require.NoError(t, err)
	require.Len(t, out.EdgePackagingJobSummaries, 2)

	got := make(map[string]string, len(out.EdgePackagingJobSummaries))
	for _, s := range out.EdgePackagingJobSummaries {
		got[aws.ToString(s.EdgePackagingJobName)] = aws.ToString(s.CompilationJobName)
	}

	assert.Equal(t, "comp-job-alpha", got["edge-job-one"])
	assert.Equal(t, "comp-job-beta", got["edge-job-two"])
}
