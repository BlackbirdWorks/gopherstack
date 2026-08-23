package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeAIWorkloadConfig_Tags_RealClient proves Tags round-trips
// through the real aws-sdk-go-v2 client's own deserializer.
// DescribeAIWorkloadConfigOutput.Tags is []types.Tag (api_op_DescribeAIWorkloadConfig.go),
// a JSON array of {Key,Value} objects -- the backend previously embedded its
// internal map[string]string representation directly, which serializes as a
// JSON object and fails a real client's array deserialization outright.
func TestHandler_DescribeAIWorkloadConfig_Tags_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateAIWorkloadConfig(t.Context(), &sagemakersdk.CreateAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("wc-tags"),
		Tags: []smtypes.Tag{
			{Key: aws.String("owner"), Value: aws.String("alice")},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeAIWorkloadConfig(t.Context(), &sagemakersdk.DescribeAIWorkloadConfigInput{
		AIWorkloadConfigName: aws.String("wc-tags"),
	})
	require.NoError(t, err, "a map-shaped Tags field would fail deserialization into []types.Tag here")

	require.Len(t, out.Tags, 1)
	assert.Equal(t, "owner", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "alice", aws.ToString(out.Tags[0].Value))
}
