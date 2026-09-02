package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListInferenceRecommendationsJobs_SiblingFields_RealClient covers
// gopherstack-6flj's PARITY-gap targeting queue: ListInferenceRecommendationsJobs
// never appears by name in services/sagemaker/PARITY.md. Real
// types.InferenceRecommendationsJob declares JobDescription and RoleArn as
// REQUIRED members (api_op_ListInferenceRecommendationsJobs.go /
// deserializers.go awsAwsjson11_deserializeDocumentInferenceRecommendationsJob).
// DescribeInferenceRecommendationsJob already surfaces both from the backend's
// tracked InferenceRecommendationsJob.JobDescription/RoleArn, but the List
// summary omitted both entirely -- a real client's list decoded them as empty
// strings on every item regardless of what was set at creation.
func TestListInferenceRecommendationsJobs_SiblingFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"irj-one", "irj-two"}
	descriptions := []string{"first recommendation job", "second recommendation job"}
	roleArns := []string{
		"arn:aws:iam::000000000000:role/RoleOne",
		"arn:aws:iam::000000000000:role/RoleTwo",
	}

	for i, name := range names {
		_, err := client.CreateInferenceRecommendationsJob(
			t.Context(),
			&sagemakersdk.CreateInferenceRecommendationsJobInput{
				JobName:        aws.String(name),
				JobType:        smtypes.RecommendationJobTypeDefault,
				JobDescription: aws.String(descriptions[i]),
				RoleArn:        aws.String(roleArns[i]),
				InputConfig: &smtypes.RecommendationJobInputConfig{
					ModelPackageVersionArn: aws.String(
						"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg/1",
					),
				},
			},
		)
		require.NoError(t, err)
	}

	out, err := client.ListInferenceRecommendationsJobs(
		t.Context(),
		&sagemakersdk.ListInferenceRecommendationsJobsInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.InferenceRecommendationsJobs, 2)

	gotDescriptions := make(map[string]string, len(out.InferenceRecommendationsJobs))
	gotRoleArns := make(map[string]string, len(out.InferenceRecommendationsJobs))

	for _, j := range out.InferenceRecommendationsJobs {
		gotDescriptions[aws.ToString(j.JobName)] = aws.ToString(j.JobDescription)
		gotRoleArns[aws.ToString(j.JobName)] = aws.ToString(j.RoleArn)
	}

	assert.Equal(t, "first recommendation job", gotDescriptions["irj-one"])
	assert.Equal(t, "second recommendation job", gotDescriptions["irj-two"])
	assert.Equal(t, roleArns[0], gotRoleArns["irj-one"])
	assert.Equal(t, roleArns[1], gotRoleArns["irj-two"])
}
