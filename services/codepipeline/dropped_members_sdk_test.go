package codepipeline_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_ListActionTypesAppliesRegionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{name: "no filter uses caller region", filter: "", want: []string{"us-provider"}},
		{name: "other region", filter: "eu-west-1", want: []string{"eu-provider"}},
		{name: "empty region", filter: "ap-south-1", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodePipelineClient(t, newTestHandler(t))

			for provider, region := range map[string]string{"us-provider": "us-east-1", "eu-provider": "eu-west-1"} {
				_, err := client.CreateCustomActionType(t.Context(), &cpsdk.CreateCustomActionTypeInput{
					Category:              types.ActionCategoryBuild,
					Provider:              aws.String(provider),
					Version:               aws.String("1"),
					InputArtifactDetails:  &types.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
					OutputArtifactDetails: &types.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
				}, func(o *cpsdk.Options) { o.Region = region })
				require.NoError(t, err)
			}

			in := &cpsdk.ListActionTypesInput{}
			if tt.filter != "" {
				in.RegionFilter = aws.String(tt.filter)
			}

			out, err := client.ListActionTypes(t.Context(), in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.ActionTypes))
			for _, at := range out.ActionTypes {
				got = append(got, aws.ToString(at.Id.Provider))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSDK_StartPipelineExecutionClientRequestTokenReplays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		first     string
		second    string
		wantEqual bool
	}{
		{name: "same token", first: "tok-1", second: "tok-1", wantEqual: true},
		{name: "different tokens", first: "tok-1", second: "tok-2", wantEqual: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCodePipelineClient(t, h)

			p := samplePipeline("tok-pipe")
			_, err := h.Backend.CreatePipeline(t.Context(), p, nil)
			require.NoError(t, err)

			a, err := client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
				Name: aws.String("tok-pipe"), ClientRequestToken: aws.String(tt.first),
			})
			require.NoError(t, err)

			b, err := client.StartPipelineExecution(t.Context(), &cpsdk.StartPipelineExecutionInput{
				Name: aws.String("tok-pipe"), ClientRequestToken: aws.String(tt.second),
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantEqual, aws.ToString(a.PipelineExecutionId) == aws.ToString(b.PipelineExecutionId))

			list, err := client.ListPipelineExecutions(t.Context(), &cpsdk.ListPipelineExecutionsInput{
				PipelineName: aws.String("tok-pipe"),
			})
			require.NoError(t, err)

			want := 1
			if !tt.wantEqual {
				want = 2
			}

			assert.Len(t, list.PipelineExecutionSummaries, want)
		})
	}
}
