package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestProvisionedThroughput_ModelArnIsAccountless proves ModelArn,
// DesiredModelArn and FoundationModelArn on a provisioned throughput are
// built account-less, matching bedrock@v1.66.4's FoundationModelArn shape
// pattern (service-2.json: "arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}::
// foundation-model/..." -- two colons before the resource, no account
// segment). The expected ARN is built independently of the handler's own
// foundationModelARN helper so a regression in that helper cannot pass.
func TestProvisionedThroughput_ModelArnIsAccountless(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "titan text", modelID: "amazon.titan-text-express-v1"},
		{name: "claude v2", modelID: "anthropic.claude-v2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestBedrockClient(t, h)

			wantArn := "arn:aws:bedrock:us-east-1::foundation-model/" + tt.modelID

			createOut, err := client.CreateProvisionedModelThroughput(
				t.Context(),
				&bedrocksdk.CreateProvisionedModelThroughputInput{
					ModelId:              aws.String(tt.modelID),
					ModelUnits:           aws.Int32(1),
					ProvisionedModelName: aws.String("pmt-" + tt.name),
				},
			)
			require.NoError(t, err)

			getOut, err := client.GetProvisionedModelThroughput(
				t.Context(),
				&bedrocksdk.GetProvisionedModelThroughputInput{
					ProvisionedModelId: createOut.ProvisionedModelArn,
				},
			)
			require.NoError(t, err)

			assert.Equal(t, wantArn, aws.ToString(getOut.ModelArn))
			assert.Equal(t, wantArn, aws.ToString(getOut.DesiredModelArn))
			assert.Equal(t, wantArn, aws.ToString(getOut.FoundationModelArn))
		})
	}
}

// TestProvisionedThroughput_UpdateDesiredModelArnIsAccountless proves
// UpdateProvisionedModelThroughput rebuilds DesiredModelArn account-less too
// (same FoundationModelArn shape as create -- see
// TestProvisionedThroughput_ModelArnIsAccountless).
func TestProvisionedThroughput_UpdateDesiredModelArnIsAccountless(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	client := newTestBedrockClient(t, h)

	createOut, err := client.CreateProvisionedModelThroughput(
		t.Context(),
		&bedrocksdk.CreateProvisionedModelThroughputInput{
			ModelId:              aws.String("amazon.titan-text-express-v1"),
			ModelUnits:           aws.Int32(1),
			ProvisionedModelName: aws.String("pmt-update"),
		},
	)
	require.NoError(t, err)

	const desiredModelID = "anthropic.claude-v2"

	_, err = client.UpdateProvisionedModelThroughput(
		t.Context(),
		&bedrocksdk.UpdateProvisionedModelThroughputInput{
			ProvisionedModelId: createOut.ProvisionedModelArn,
			DesiredModelId:     aws.String(desiredModelID),
		},
	)
	require.NoError(t, err)

	getOut, err := client.GetProvisionedModelThroughput(
		t.Context(),
		&bedrocksdk.GetProvisionedModelThroughputInput{
			ProvisionedModelId: createOut.ProvisionedModelArn,
		},
	)
	require.NoError(t, err)

	wantArn := "arn:aws:bedrock:us-east-1::foundation-model/" + desiredModelID
	assert.Equal(t, wantArn, aws.ToString(getOut.DesiredModelArn))
}
