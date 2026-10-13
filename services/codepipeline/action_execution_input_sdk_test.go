package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListActionExecutions_InputAndVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    map[string]string
		role   string
		wantRe string
	}{
		{name: "configuration", cfg: map[string]string{"Owner": "o", "Repo": "r"}, role: "", wantRe: "us-east-1"},
		{name: "role and region", cfg: nil, role: "arn:aws:iam::000000000000:role/act", wantRe: "us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCodePipelineClient(t, h)

			p := samplePipeline("p")
			p.Stages[0].Actions[0].Configuration = tt.cfg
			p.Stages[0].Actions[0].RoleArn = tt.role
			_, err := h.Backend.CreatePipeline(t.Context(), p, nil)
			require.NoError(t, err)

			_, err = client.StartPipelineExecution(
				t.Context(),
				&cpsdk.StartPipelineExecutionInput{Name: aws.String("p")},
			)
			require.NoError(t, err)

			out, err := client.ListActionExecutions(
				t.Context(),
				&cpsdk.ListActionExecutionsInput{PipelineName: aws.String("p")},
			)
			require.NoError(t, err)
			require.NotEmpty(t, out.ActionExecutionDetails)

			d := out.ActionExecutionDetails[0]
			require.NotNil(t, d.Input)
			require.NotNil(t, d.Input.ActionTypeId)
			assert.Equal(t, "Source", string(d.Input.ActionTypeId.Category))
			assert.Equal(t, "GitHub", aws.ToString(d.Input.ActionTypeId.Provider))
			assert.Equal(t, tt.cfg, d.Input.Configuration)
			assert.Equal(t, tt.wantRe, aws.ToString(d.Input.Region))
			assert.Equal(t, tt.role, aws.ToString(d.Input.RoleArn))
			require.NotNil(t, d.PipelineVersion)
			assert.EqualValues(t, 1, aws.ToInt32(d.PipelineVersion))
		})
	}
}
