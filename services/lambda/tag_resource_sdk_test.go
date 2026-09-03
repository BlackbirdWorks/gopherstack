package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real AWS: lambda's TagResourceInput.Tags is map[string]string, a plain
// JSON object under the "Tags" body key (aws-sdk-go-v2/service/lambda@v1.101.2
// serializers.go:6822-6834, awsRestjson1_serializeOpDocumentTagResourceInput),
// unlike stepfunctions' array-of-{key,value}. This emulator's map-shaped
// Tags is genuinely correct here.
func Test_SDKRoundTrip_Lambda_TagResource_UntagResource_ListTags(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	created, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("tag-rt-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &lambdasdk.TagResourceInput{
		Resource: created.FunctionArn,
		Tags:     map[string]string{"env": "prod", "team": "infra"},
	})
	require.NoError(t, err)

	listed, err := client.ListTags(ctx, &lambdasdk.ListTagsInput{
		Resource: created.FunctionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, listed.Tags)

	_, err = client.UntagResource(ctx, &lambdasdk.UntagResourceInput{
		Resource: created.FunctionArn,
		TagKeys:  []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTags(ctx, &lambdasdk.ListTagsInput{
		Resource: created.FunctionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, afterUntag.Tags)
}
