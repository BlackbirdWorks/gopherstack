package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestGetPipelineState_StageLatestExecutionAndDefaultInbound(t *testing.T) {
	t.Parallel()

	h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodePipelineClient(t, h)
	newTestPipelineForState(t, client, "state-pipe")

	exec, err := client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
		Name: aws.String("state-pipe"),
	})
	require.NoError(t, err)

	out, err := client.GetPipelineState(t.Context(), &cpsdk.GetPipelineStateInput{Name: aws.String("state-pipe")})
	require.NoError(t, err)
	require.Len(t, out.StageStates, 1)

	le := out.StageStates[0].LatestExecution
	require.NotNil(t, le)
	assert.Equal(t, aws.ToString(exec.PipelineExecutionId), aws.ToString(le.PipelineExecutionId))
	assert.Equal(t, types.StageExecutionStatusSucceeded, le.Status)
	assert.Nil(t, out.StageStates[0].InboundTransitionState)
}

func TestCodePipeline_ErrorMessagesAndTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(*cpsdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "pipeline_not_found",
			call: func(c *cpsdk.Client) error {
				_, err := c.GetPipeline(t.Context(), &cpsdk.GetPipelineInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "PipelineNotFoundException",
			wantMsg:  "Account '123456789012' does not have a pipeline with name 'nope'",
		},
		{
			name: "start_not_found",
			call: func(c *cpsdk.Client) error {
				_, err := c.StartPipelineExecution(
					t.Context(),
					&cpsdk.StartPipelineExecutionInput{Name: aws.String("nope")},
				)

				return err
			},
			wantCode: "PipelineNotFoundException",
			wantMsg:  "does not have a pipeline with name 'nope'",
		},
		{
			name: "bad_list_token",
			call: func(c *cpsdk.Client) error {
				_, err := c.ListPipelines(t.Context(), &cpsdk.ListPipelinesInput{NextToken: aws.String("garbage")})

				return err
			},
			wantCode: "InvalidNextTokenException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)

			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
		})
	}
}
