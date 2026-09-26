package kafkaconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagUntagListTagsForResource(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, minimalCreateConnectorInput("tag-me"))
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &kafkaconnectsdk.TagResourceInput{
		ResourceArn: created.ConnectorArn,
		Tags:        map[string]string{"env": "test", "owner": "terraform"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(
		ctx,
		&kafkaconnectsdk.ListTagsForResourceInput{ResourceArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "test", "owner": "terraform"}, listOut.Tags)

	_, err = client.UntagResource(ctx, &kafkaconnectsdk.UntagResourceInput{
		ResourceArn: created.ConnectorArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut, err = client.ListTagsForResource(
		ctx,
		&kafkaconnectsdk.ListTagsForResourceInput{ResourceArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"owner": "terraform"}, listOut.Tags)
}

func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.TagResource(t.Context(), &kafkaconnectsdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:connector/nope/abc"),
		Tags:        map[string]string{"a": "b"},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

func TestCreateConnectorWithTags(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	input := minimalCreateConnectorInput("tagged-on-create")
	input.Tags = map[string]string{"Environment": "test"}

	created, err := client.CreateConnector(ctx, input)
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(
		ctx,
		&kafkaconnectsdk.ListTagsForResourceInput{ResourceArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"Environment": "test"}, listOut.Tags)
}
