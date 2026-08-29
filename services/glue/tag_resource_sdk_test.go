package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// Real AWS: glue's TagResourceInput.TagsToAdd is map[string]string, a plain
// JSON object body field (aws-sdk-go-v2/service/glue@v1.152.0
// serializers.go:37549-37564, awsAwsjson11_serializeOpDocumentTagResourceInput),
// matching this emulator's map-shaped TagsToAdd exactly.
func Test_SDKRoundTrip_Glue_TagResource_UntagResource_GetTags(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("tag-rt-db")},
	})
	require.NoError(t, err)

	dbARN := "arn:aws:glue:" + testRegion + ":" + testAccountID + ":database/tag-rt-db"

	_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
		ResourceArn: aws.String(dbARN),
		TagsToAdd:   map[string]string{"env": "prod", "team": "infra"},
	})
	require.NoError(t, err)

	got, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(dbARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got.Tags)

	_, err = client.UntagResource(ctx, &gluesdk.UntagResourceInput{
		ResourceArn:  aws.String(dbARN),
		TagsToRemove: []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(dbARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, afterUntag.Tags)
}
