package bedrock_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestCreateModelInvocationJob_OversizedBody_TypesAsInternalServerException
// drives an oversized CreateModelInvocationJob request through the real
// aws-sdk-go-v2 client so the body exceeds httputils.MaxRequestBodyBytes
// (16MB). handleCreateModelInvocationJob's own ReadBody call then fails
// before any JSON parsing happens (services/bedrock/handler_model_invocation_jobs.go).
// Before the fix that path wrote {"__type":"InternalFailure",...}: no
// CreateModelInvocationJob op in bedrock@v1.66.4's deserializeOpError switch
// has a case for "InternalFailure" (every one of the 108 ops only has
// "InternalServerException"), so the real client fell through to an untyped
// smithy.GenericAPIError and errors.As into types.InternalServerException
// failed.
func TestCreateModelInvocationJob_OversizedBody_TypesAsInternalServerException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	huge := strings.Repeat("a", 17*1024*1024)

	_, err := client.CreateModelInvocationJob(t.Context(), &bedrocksdk.CreateModelInvocationJobInput{
		JobName: aws.String(huge),
		ModelId: aws.String("anthropic.claude-v2"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/test"),
		InputDataConfig: &types.ModelInvocationJobInputDataConfigMemberS3InputDataConfig{
			Value: types.ModelInvocationJobS3InputDataConfig{S3Uri: aws.String("s3://bucket/in")},
		},
		OutputDataConfig: &types.ModelInvocationJobOutputDataConfigMemberS3OutputDataConfig{
			Value: types.ModelInvocationJobS3OutputDataConfig{S3Uri: aws.String("s3://bucket/out")},
		},
	})

	require.Error(t, err)

	var typed *types.InternalServerException

	require.ErrorAs(t, err, &typed)
}
