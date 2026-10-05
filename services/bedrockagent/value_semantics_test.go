package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdate_OmittedDescriptionIsCleared(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::123456789012:role/r"

	cases := []struct {
		run  func(t *testing.T, c *bedrockagentsdk.Client) (before, after string)
		name string
	}{
		{
			name: "knowledge base",
			run: func(t *testing.T, c *bedrockagentsdk.Client) (string, string) {
				t.Helper()

				kbCfg := &types.KnowledgeBaseConfiguration{Type: types.KnowledgeBaseTypeVector}
				created, err := c.CreateKnowledgeBase(t.Context(), &bedrockagentsdk.CreateKnowledgeBaseInput{
					Name: aws.String("kb"), RoleArn: aws.String(role), Description: aws.String("d"),
					KnowledgeBaseConfiguration: kbCfg,
				})
				require.NoError(t, err)

				id := created.KnowledgeBase.KnowledgeBaseId
				_, err = c.UpdateKnowledgeBase(t.Context(), &bedrockagentsdk.UpdateKnowledgeBaseInput{
					KnowledgeBaseId: id, Name: aws.String("kb"), RoleArn: aws.String(role),
					KnowledgeBaseConfiguration: kbCfg,
				})
				require.NoError(t, err)

				got, err := c.GetKnowledgeBase(t.Context(), &bedrockagentsdk.GetKnowledgeBaseInput{KnowledgeBaseId: id})
				require.NoError(t, err)

				return aws.ToString(created.KnowledgeBase.Description), aws.ToString(got.KnowledgeBase.Description)
			},
		},
		{
			name: "prompt",
			run: func(t *testing.T, c *bedrockagentsdk.Client) (string, string) {
				t.Helper()

				created, err := c.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("p"), Description: aws.String("d"),
				})
				require.NoError(t, err)

				_, err = c.UpdatePrompt(t.Context(), &bedrockagentsdk.UpdatePromptInput{
					PromptIdentifier: created.Id, Name: aws.String("p"),
				})
				require.NoError(t, err)

				got, err := c.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{PromptIdentifier: created.Id})
				require.NoError(t, err)

				return aws.ToString(created.Description), aws.ToString(got.Description)
			},
		},
		{
			name: "flow",
			run: func(t *testing.T, c *bedrockagentsdk.Client) (string, string) {
				t.Helper()

				created, err := c.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name: aws.String("f"), ExecutionRoleArn: aws.String(role), Description: aws.String("d"),
				})
				require.NoError(t, err)

				_, err = c.UpdateFlow(t.Context(), &bedrockagentsdk.UpdateFlowInput{
					FlowIdentifier: created.Id, Name: aws.String("f"), ExecutionRoleArn: aws.String(role),
				})
				require.NoError(t, err)

				got, err := c.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{FlowIdentifier: created.Id})
				require.NoError(t, err)

				return aws.ToString(created.Description), aws.ToString(got.Description)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			before, after := tc.run(t, newTestHandlerAndClient(t))
			assert.Equal(t, "d", before)
			assert.Empty(t, after)
		})
	}
}
