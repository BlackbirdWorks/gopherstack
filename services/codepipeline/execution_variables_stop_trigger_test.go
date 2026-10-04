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

func TestPipelineExecution_VariablesSourceRevisionsStopTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		overrides     []types.PipelineVariable
		revisions     []types.SourceRevisionOverride
		stopReason    string
		wantVars      map[string]string
		wantRevisions []string
	}{
		{
			name:       "defaults_and_stop_reason",
			stopReason: "operator halt",
			wantVars:   map[string]string{"Env": "dev", "Region": "us-east-1"},
		},
		{
			name:      "override_replaces_default",
			overrides: []types.PipelineVariable{{Name: aws.String("Env"), Value: aws.String("prod")}},
			wantVars:  map[string]string{"Env": "prod", "Region": "us-east-1"},
		},
		{
			name: "source_revision_pinned_for_declared_action_only",
			revisions: []types.SourceRevisionOverride{
				{
					ActionName: aws.String("SourceAction"), RevisionType: types.SourceRevisionTypeCommitId,
					RevisionValue: aws.String("abc123"),
				},
				{
					ActionName: aws.String("NoSuchAction"), RevisionType: types.SourceRevisionTypeCommitId,
					RevisionValue: aws.String("zzz"),
				},
			},
			wantVars:      map[string]string{"Env": "dev", "Region": "us-east-1"},
			wantRevisions: []string{"abc123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{Pipeline: &types.PipelineDeclaration{
				Name:         aws.String("vars-pipeline"),
				RoleArn:      aws.String("arn:aws:iam::123456789012:role/pipeline-role"),
				PipelineType: types.PipelineTypeV2,
				ArtifactStore: &types.ArtifactStore{
					Type: types.ArtifactStoreTypeS3, Location: aws.String("my-artifact-bucket"),
				},
				Variables: []types.PipelineVariableDeclaration{
					{Name: aws.String("Env"), DefaultValue: aws.String("dev")},
					{Name: aws.String("Region"), DefaultValue: aws.String("us-east-1")},
				},
				Stages: []types.StageDeclaration{{
					Name: aws.String("Source"),
					Actions: []types.ActionDeclaration{{
						Name: aws.String("SourceAction"),
						ActionTypeId: &types.ActionTypeId{
							Category: types.ActionCategorySource, Owner: types.ActionOwnerAws,
							Provider: aws.String("S3"), Version: aws.String("1"),
						},
					}},
				}},
			}})
			require.NoError(t, err)

			started, err := client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{
				Name:            aws.String("vars-pipeline"),
				Variables:       tt.overrides,
				SourceRevisions: tt.revisions,
			})
			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName:        aws.String("vars-pipeline"),
				PipelineExecutionId: started.PipelineExecutionId,
			})
			require.NoError(t, err)

			gotVars := map[string]string{}
			for _, v := range got.PipelineExecution.Variables {
				gotVars[aws.ToString(v.Name)] = aws.ToString(v.ResolvedValue)
			}

			assert.Equal(t, tt.wantVars, gotVars)

			if tt.stopReason != "" {
				_, err = client.StopPipelineExecution(ctx, &cpsdk.StopPipelineExecutionInput{
					PipelineName:        aws.String("vars-pipeline"),
					PipelineExecutionId: started.PipelineExecutionId,
					Reason:              aws.String(tt.stopReason),
				})
				require.NoError(t, err)
			}

			list, err := client.ListPipelineExecutions(ctx, &cpsdk.ListPipelineExecutionsInput{
				PipelineName: aws.String("vars-pipeline"),
			})
			require.NoError(t, err)
			require.Len(t, list.PipelineExecutionSummaries, 1)

			sum := list.PipelineExecutionSummaries[0]

			if tt.stopReason != "" {
				require.NotNil(t, sum.StopTrigger)
				assert.Equal(t, tt.stopReason, aws.ToString(sum.StopTrigger.Reason))
			} else {
				assert.Nil(t, sum.StopTrigger)
			}

			gotRevs := make([]string, 0, len(sum.SourceRevisions))
			for _, r := range sum.SourceRevisions {
				gotRevs = append(gotRevs, aws.ToString(r.RevisionId))
			}

			assert.ElementsMatch(t, tt.wantRevisions, gotRevs)
		})
	}
}
