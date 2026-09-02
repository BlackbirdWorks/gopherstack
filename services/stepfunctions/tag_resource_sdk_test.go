package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// Real AWS: sfn's TagResourceInput.Tags is []types.Tag, an array of
// {"key","value"} objects (aws-sdk-go-v2/service/sfn@v1.45.4
// serializers.go:3140-3145, awsAwsjson10_serializeOpDocumentTagResourceInput),
// not the JSON object map the emulator previously required.
func Test_SDKRoundTrip_TagResource_UntagResource_ListTagsForResource(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	smName := "tag-rt-sm-" + uuid.NewString()[:8]
	created, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(validPassDef),
		RoleArn:    aws.String(validRoleARN),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sfnsdk.TagResourceInput{
		ResourceArn: created.StateMachineArn,
		Tags: []sfntypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &sfnsdk.ListTagsForResourceInput{
		ResourceArn: created.StateMachineArn,
	})
	require.NoError(t, err)

	got := make(map[string]string, len(listed.Tags))
	for _, tag := range listed.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got)

	_, err = client.UntagResource(ctx, &sfnsdk.UntagResourceInput{
		ResourceArn: created.StateMachineArn,
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(ctx, &sfnsdk.ListTagsForResourceInput{
		ResourceArn: created.StateMachineArn,
	})
	require.NoError(t, err)

	gotAfter := make(map[string]string, len(afterUntag.Tags))
	for _, tag := range afterUntag.Tags {
		gotAfter[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod"}, gotAfter)
}
