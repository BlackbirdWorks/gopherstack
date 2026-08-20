package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestPutWebhook_AuthenticationConfigurationRoundTrip proves the real SDK
// client can read back AllowedIPRange through ListWebhooks after PutWebhook.
// Real types.WebhookAuthConfiguration
// (awsAwsjson11_deserializeDocumentWebhookAuthConfiguration /
// awsAwsjson11_serializeDocumentWebhookAuthConfiguration) uses the
// capitalized wire keys "AllowedIPRange"/"SecretToken" -- unlike every other
// member of WebhookDefinition, which is lowerCamelCase. Before the fix,
// gopherstack both parsed and emitted lowercase "allowedIPRange", so a real
// client's request never populated AllowedIPRange server-side and a real
// client's response never saw it echoed back.
func TestPutWebhook_AuthenticationConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodePipelineClient(t, h)

	_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
		Pipeline: &types.PipelineDeclaration{
			Name:    aws.String("webhook-auth-pipeline"),
			RoleArn: aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
			ArtifactStore: &types.ArtifactStore{
				Type:     types.ArtifactStoreTypeS3,
				Location: aws.String("my-artifact-bucket"),
			},
			Stages: []types.StageDeclaration{
				{
					Name: aws.String("Source"),
					Actions: []types.ActionDeclaration{
						{
							Name: aws.String("SourceAction"),
							ActionTypeId: &types.ActionTypeId{
								Category: types.ActionCategorySource,
								Owner:    types.ActionOwnerAws,
								Provider: aws.String("S3"),
								Version:  aws.String("1"),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	putOut, err := client.PutWebhook(t.Context(), &cpsdk.PutWebhookInput{
		Webhook: &types.WebhookDefinition{
			Name:           aws.String("ip-webhook"),
			TargetPipeline: aws.String("webhook-auth-pipeline"),
			TargetAction:   aws.String("SourceAction"),
			Authentication: types.WebhookAuthenticationTypeIp,
			AuthenticationConfiguration: &types.WebhookAuthConfiguration{
				AllowedIPRange: aws.String("10.0.0.0/8"),
			},
			Filters: []types.WebhookFilterRule{
				{JsonPath: aws.String("$.ref")},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.Webhook.Definition.AuthenticationConfiguration)
	assert.Equal(t, "10.0.0.0/8", aws.ToString(putOut.Webhook.Definition.AuthenticationConfiguration.AllowedIPRange))

	listOut, err := client.ListWebhooks(t.Context(), &cpsdk.ListWebhooksInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Webhooks, 1)
	require.NotNil(t, listOut.Webhooks[0].Definition.AuthenticationConfiguration)
	assert.Equal(t,
		"10.0.0.0/8",
		aws.ToString(listOut.Webhooks[0].Definition.AuthenticationConfiguration.AllowedIPRange),
	)
}
