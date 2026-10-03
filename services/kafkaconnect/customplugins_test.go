package kafkaconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalCreateCustomPluginInput(name string) *kafkaconnectsdk.CreateCustomPluginInput {
	return &kafkaconnectsdk.CreateCustomPluginInput{
		ContentType: types.CustomPluginContentTypeZip,
		Location: &types.CustomPluginLocation{
			S3Location: &types.S3Location{
				BucketArn: aws.String("arn:aws:s3:::my-plugin-bucket"),
				FileKey:   aws.String("plugins/my-plugin.zip"),
			},
		},
		Name: aws.String(name),
	}
}

func TestCreateCustomPlugin(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	out, err := client.CreateCustomPlugin(t.Context(), minimalCreateCustomPluginInput("plugin-one"))
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.CustomPluginArn), "custom-plugin/plugin-one/")
	assert.Equal(t, types.CustomPluginStateActive, out.CustomPluginState)
	assert.EqualValues(t, 1, out.Revision)
}

func TestCreateCustomPlugin_DuplicateNameReturnsConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("dup-plugin"))
	require.NoError(t, err)

	_, err = client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("dup-plugin"))
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}

func TestDescribeCustomPlugin(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("describe-plugin"))
	require.NoError(t, err)

	out, err := client.DescribeCustomPlugin(
		ctx,
		&kafkaconnectsdk.DescribeCustomPluginInput{CustomPluginArn: created.CustomPluginArn},
	)
	require.NoError(t, err)
	assert.Equal(t, "describe-plugin", aws.ToString(out.Name))
	assert.Equal(t, types.CustomPluginStateActive, out.CustomPluginState)
	require.NotNil(t, out.LatestRevision)
	assert.EqualValues(t, 1, out.LatestRevision.Revision)
	require.NotNil(t, out.LatestRevision.Location)
	require.NotNil(t, out.LatestRevision.Location.S3Location)
	assert.Equal(t, "plugins/my-plugin.zip", aws.ToString(out.LatestRevision.Location.S3Location.FileKey))
	require.NotNil(t, out.LatestRevision.FileDescription)
	assert.NotEmpty(t, aws.ToString(out.LatestRevision.FileDescription.FileMd5))
}

func TestDescribeCustomPlugin_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeCustomPlugin(t.Context(), &kafkaconnectsdk.DescribeCustomPluginInput{
		CustomPluginArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:custom-plugin/nope/abc"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

func TestListCustomPlugins(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("list-plugin-a"))
	require.NoError(t, err)
	_, err = client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("list-plugin-b"))
	require.NoError(t, err)

	out, err := client.ListCustomPlugins(ctx, &kafkaconnectsdk.ListCustomPluginsInput{})
	require.NoError(t, err)
	assert.Len(t, out.CustomPlugins, 2)
}

func TestDeleteCustomPlugin(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateCustomPlugin(ctx, minimalCreateCustomPluginInput("delete-plugin"))
	require.NoError(t, err)

	out, err := client.DeleteCustomPlugin(
		ctx,
		&kafkaconnectsdk.DeleteCustomPluginInput{CustomPluginArn: created.CustomPluginArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.CustomPluginStateDeleting, out.CustomPluginState)

	_, err = client.DescribeCustomPlugin(
		ctx,
		&kafkaconnectsdk.DescribeCustomPluginInput{CustomPluginArn: created.CustomPluginArn},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
