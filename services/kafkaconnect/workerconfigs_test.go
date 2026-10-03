package kafkaconnect_test

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalCreateWorkerConfigurationInput(name string) *kafkaconnectsdk.CreateWorkerConfigurationInput {
	content := base64.StdEncoding.EncodeToString([]byte("key.converter=org.apache.kafka.connect.json.JsonConverter\n"))

	return &kafkaconnectsdk.CreateWorkerConfigurationInput{
		Name:                  aws.String(name),
		PropertiesFileContent: aws.String(content),
	}
}

func TestCreateWorkerConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	out, err := client.CreateWorkerConfiguration(t.Context(), minimalCreateWorkerConfigurationInput("wc-one"))
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.WorkerConfigurationArn), "worker-configuration/wc-one/")
	assert.Equal(t, types.WorkerConfigurationStateActive, out.WorkerConfigurationState)
	require.NotNil(t, out.LatestRevision)
	assert.EqualValues(t, 1, out.LatestRevision.Revision)
}

func TestCreateWorkerConfiguration_DuplicateNameReturnsConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateWorkerConfiguration(ctx, minimalCreateWorkerConfigurationInput("dup-wc"))
	require.NoError(t, err)

	_, err = client.CreateWorkerConfiguration(ctx, minimalCreateWorkerConfigurationInput("dup-wc"))
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}

func TestDescribeWorkerConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	input := minimalCreateWorkerConfigurationInput("describe-wc")

	created, err := client.CreateWorkerConfiguration(ctx, input)
	require.NoError(t, err)

	out, err := client.DescribeWorkerConfiguration(ctx, &kafkaconnectsdk.DescribeWorkerConfigurationInput{
		WorkerConfigurationArn: created.WorkerConfigurationArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "describe-wc", aws.ToString(out.Name))
	require.NotNil(t, out.LatestRevision)
	assert.Equal(t, aws.ToString(input.PropertiesFileContent), aws.ToString(out.LatestRevision.PropertiesFileContent))
}

func TestDescribeWorkerConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeWorkerConfiguration(t.Context(), &kafkaconnectsdk.DescribeWorkerConfigurationInput{
		WorkerConfigurationArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:worker-configuration/nope/abc"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

func TestListWorkerConfigurations(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateWorkerConfiguration(ctx, minimalCreateWorkerConfigurationInput("list-wc-a"))
	require.NoError(t, err)
	_, err = client.CreateWorkerConfiguration(ctx, minimalCreateWorkerConfigurationInput("list-wc-b"))
	require.NoError(t, err)

	out, err := client.ListWorkerConfigurations(ctx, &kafkaconnectsdk.ListWorkerConfigurationsInput{})
	require.NoError(t, err)
	assert.Len(t, out.WorkerConfigurations, 2)
}

func TestDeleteWorkerConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateWorkerConfiguration(ctx, minimalCreateWorkerConfigurationInput("delete-wc"))
	require.NoError(t, err)

	out, err := client.DeleteWorkerConfiguration(ctx, &kafkaconnectsdk.DeleteWorkerConfigurationInput{
		WorkerConfigurationArn: created.WorkerConfigurationArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.WorkerConfigurationStateDeleting, out.WorkerConfigurationState)

	_, err = client.DescribeWorkerConfiguration(ctx, &kafkaconnectsdk.DescribeWorkerConfigurationInput{
		WorkerConfigurationArn: created.WorkerConfigurationArn,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
