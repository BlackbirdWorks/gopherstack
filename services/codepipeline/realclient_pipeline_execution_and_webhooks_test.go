package codepipeline_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestRealClient_PipelineExecutionAndWebhooks drives every remaining uncovered op through
// a real aws-sdk-go-v2 codepipeline client: DeleteWebhook,
// DeregisterWebhookWithThirdParty, EnableStageTransition, GetActionType,
// GetJobDetails, GetPipelineExecution, ListActionTypes, ListRuleExecutions,
// ListRuleTypes, PollForJobs, PutActionRevision, PutApprovalResult,
// RegisterWebhookWithThirdParty, RollbackStage, TagResource, UntagResource,
// UpdateActionType.
func TestRealClient_PipelineExecutionAndWebhooks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "pipeline execution family: GetPipelineExecution, RollbackStage, EnableStageTransition, rules",
			run: func(t *testing.T) {
				t.Helper()

				h := codepipeline.NewHandler(
					codepipeline.NewInMemoryBackend("123456789012", "us-east-1"),
				)
				client := newTestCodePipelineClient(t, h)
				ctx := t.Context()

				newTestPipelineForState(t, client, "slice25-exec-pipeline")

				started, err := client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{
					Name: aws.String("slice25-exec-pipeline"),
				})
				require.NoError(t, err)
				execID := aws.ToString(started.PipelineExecutionId)
				require.NotEmpty(t, execID)

				got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
					PipelineName:        aws.String("slice25-exec-pipeline"),
					PipelineExecutionId: aws.String(execID),
				})
				require.NoError(t, err)
				require.NotNil(t, got.PipelineExecution)
				assert.Equal(t, execID, aws.ToString(got.PipelineExecution.PipelineExecutionId))
				assert.Equal(t, types.PipelineExecutionStatusSucceeded, got.PipelineExecution.Status)

				rolled, err := client.RollbackStage(ctx, &cpsdk.RollbackStageInput{
					PipelineName:              aws.String("slice25-exec-pipeline"),
					StageName:                 aws.String("Source"),
					TargetPipelineExecutionId: aws.String(execID),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(rolled.PipelineExecutionId))

				_, err = client.EnableStageTransition(ctx, &cpsdk.EnableStageTransitionInput{
					PipelineName:   aws.String("slice25-exec-pipeline"),
					StageName:      aws.String("Source"),
					TransitionType: types.StageTransitionTypeInbound,
				})
				require.NoError(t, err)

				ruleExecs, err := client.ListRuleExecutions(ctx, &cpsdk.ListRuleExecutionsInput{
					PipelineName: aws.String("slice25-exec-pipeline"),
				})
				require.NoError(t, err)
				assert.Empty(t, ruleExecs.RuleExecutionDetails)

				ruleTypes, err := client.ListRuleTypes(ctx, &cpsdk.ListRuleTypesInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, ruleTypes.RuleTypes)
			},
		},
		{name: "job family: PollForJobs, GetJobDetails", run: func(t *testing.T) {
			t.Helper()

			b := codepipeline.NewInMemoryBackend("123456789012", "us-east-1")
			h := codepipeline.NewHandler(b)
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			b.AddJobInternal(&codepipeline.Job{
				ID:     "slice25-job-1",
				Nonce:  "slice25-nonce",
				Status: "Queued",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Build",
					Owner:    "AWS",
					Provider: "CodeBuild",
					Version:  "1",
				},
			})

			polled, err := client.PollForJobs(ctx, &cpsdk.PollForJobsInput{
				ActionTypeId: &types.ActionTypeId{
					Category: types.ActionCategoryBuild,
					Owner:    types.ActionOwnerAws,
					Provider: aws.String("CodeBuild"),
					Version:  aws.String("1"),
				},
			})
			require.NoError(t, err)
			require.Len(t, polled.Jobs, 1)
			assert.Equal(t, "slice25-job-1", aws.ToString(polled.Jobs[0].Id))
			assert.Equal(t, "slice25-nonce", aws.ToString(polled.Jobs[0].Nonce))

			details, err := client.GetJobDetails(ctx, &cpsdk.GetJobDetailsInput{
				JobId: aws.String("slice25-job-1"),
			})
			require.NoError(t, err)
			require.NotNil(t, details.JobDetails)
			assert.Equal(t, "slice25-job-1", aws.ToString(details.JobDetails.Id))
			require.NotNil(t, details.JobDetails.Data)
			require.NotNil(t, details.JobDetails.Data.ActionTypeId)
			assert.Equal(t, "CodeBuild", aws.ToString(details.JobDetails.Data.ActionTypeId.Provider))
		}},
		{name: "custom action type family: GetActionType, UpdateActionType, ListActionTypes", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreateCustomActionType(ctx, &cpsdk.CreateCustomActionTypeInput{
				Category: types.ActionCategoryBuild,
				Provider: aws.String("slice25-provider"),
				Version:  aws.String("1"),
				InputArtifactDetails: &types.ArtifactDetails{
					MinimumCount: 0,
					MaximumCount: 5,
				},
				OutputArtifactDetails: &types.ArtifactDetails{
					MinimumCount: 0,
					MaximumCount: 5,
				},
			})
			require.NoError(t, err)

			got, err := client.GetActionType(ctx, &cpsdk.GetActionTypeInput{
				Category: types.ActionCategoryBuild,
				Owner:    aws.String("Custom"),
				Provider: aws.String("slice25-provider"),
				Version:  aws.String("1"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.ActionType)
			assert.Equal(t, "slice25-provider", aws.ToString(got.ActionType.Id.Provider))

			_, err = client.UpdateActionType(ctx, &cpsdk.UpdateActionTypeInput{
				ActionType: &types.ActionTypeDeclaration{
					Id: &types.ActionTypeIdentifier{
						Category: types.ActionCategoryBuild,
						Owner:    aws.String(string(types.ActionOwnerCustom)),
						Provider: aws.String("slice25-provider"),
						Version:  aws.String("1"),
					},
					Executor: &types.ActionTypeExecutor{
						Type: types.ExecutorTypeLambda,
						Configuration: &types.ExecutorConfiguration{
							LambdaExecutorConfiguration: &types.LambdaExecutorConfiguration{
								LambdaFunctionArn: aws.String(
									"arn:aws:lambda:us-east-1:123456789012:function:slice25-fn",
								),
							},
						},
					},
					InputArtifactDetails: &types.ActionTypeArtifactDetails{
						MinimumCount: 0,
						MaximumCount: 5,
					},
					OutputArtifactDetails: &types.ActionTypeArtifactDetails{
						MinimumCount: 0,
						MaximumCount: 5,
					},
					Description: aws.String("updated via typed client"),
				},
			})
			require.NoError(t, err)

			got, err = client.GetActionType(ctx, &cpsdk.GetActionTypeInput{
				Category: types.ActionCategoryBuild,
				Owner:    aws.String("Custom"),
				Provider: aws.String("slice25-provider"),
				Version:  aws.String("1"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated via typed client", aws.ToString(got.ActionType.Description))

			listOut, err := client.ListActionTypes(ctx, &cpsdk.ListActionTypesInput{})
			require.NoError(t, err)
			var found bool
			for _, at := range listOut.ActionTypes {
				if aws.ToString(at.Id.Provider) == "slice25-provider" {
					found = true
				}
			}
			assert.True(t, found, "created custom action type must appear in ListActionTypes")
		}},
		{name: "approval and revision family: PutActionRevision, PutApprovalResult", run: func(t *testing.T) {
			t.Helper()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: &types.PipelineDeclaration{
					Name:    aws.String("slice25-approval-pipeline"),
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
						{
							Name: aws.String("Approve"),
							Actions: []types.ActionDeclaration{
								{
									Name: aws.String("ApprovalAction"),
									ActionTypeId: &types.ActionTypeId{
										Category: types.ActionCategoryApproval,
										Owner:    types.ActionOwnerAws,
										Provider: aws.String("Manual"),
										Version:  aws.String("1"),
									},
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)

			revised, err := client.PutActionRevision(ctx, &cpsdk.PutActionRevisionInput{
				PipelineName: aws.String("slice25-approval-pipeline"),
				StageName:    aws.String("Source"),
				ActionName:   aws.String("SourceAction"),
				ActionRevision: &types.ActionRevision{
					RevisionId:       aws.String("rev-1"),
					RevisionChangeId: aws.String("change-1"),
					Created:          aws.Time(time.Now().UTC()),
				},
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(revised.PipelineExecutionId))

			state, err := client.GetPipelineState(ctx, &cpsdk.GetPipelineStateInput{
				Name: aws.String("slice25-approval-pipeline"),
			})
			require.NoError(t, err)

			var token string
			for _, stage := range state.StageStates {
				if aws.ToString(stage.StageName) != "Approve" {
					continue
				}

				for _, action := range stage.ActionStates {
					if aws.ToString(action.ActionName) == "ApprovalAction" &&
						action.LatestExecution != nil {
						token = aws.ToString(action.LatestExecution.Token)
					}
				}
			}
			require.NotEmpty(t, token, "approval action must have a pending token")

			approved, err := client.PutApprovalResult(ctx, &cpsdk.PutApprovalResultInput{
				PipelineName: aws.String("slice25-approval-pipeline"),
				StageName:    aws.String("Approve"),
				ActionName:   aws.String("ApprovalAction"),
				Token:        aws.String(token),
				Result: &types.ApprovalResult{
					Status:  types.ApprovalStatusApproved,
					Summary: aws.String("looks good"),
				},
			})
			require.NoError(t, err)
			assert.False(t, approved.ApprovedAt.IsZero())
		}},
		{
			name: "webhook family: RegisterWebhookWithThirdParty, DeregisterWebhookWithThirdParty, DeleteWebhook",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestCodePipelineClient(t, h)
				ctx := t.Context()

				_, err := client.PutWebhook(ctx, &cpsdk.PutWebhookInput{
					Webhook: &types.WebhookDefinition{
						Name:           aws.String("slice25-webhook"),
						TargetPipeline: aws.String("slice25-target-pipeline"),
						TargetAction:   aws.String("SourceAction"),
						Filters: []types.WebhookFilterRule{
							{JsonPath: aws.String("$.ref"), MatchEquals: aws.String("refs/heads/main")},
						},
						Authentication:              types.WebhookAuthenticationTypeUnauthenticated,
						AuthenticationConfiguration: &types.WebhookAuthConfiguration{},
					},
				})
				require.NoError(t, err)

				_, err = client.RegisterWebhookWithThirdParty(
					ctx,
					&cpsdk.RegisterWebhookWithThirdPartyInput{
						WebhookName: aws.String("slice25-webhook"),
					},
				)
				require.NoError(t, err)

				_, err = client.DeregisterWebhookWithThirdParty(
					ctx,
					&cpsdk.DeregisterWebhookWithThirdPartyInput{
						WebhookName: aws.String("slice25-webhook"),
					},
				)
				require.NoError(t, err)

				_, err = client.DeleteWebhook(ctx, &cpsdk.DeleteWebhookInput{
					Name: aws.String("slice25-webhook"),
				})
				require.NoError(t, err)

				listOut, err := client.ListWebhooks(ctx, &cpsdk.ListWebhooksInput{})
				require.NoError(t, err)
				for _, wh := range listOut.Webhooks {
					assert.NotEqual(t, "slice25-webhook", aws.ToString(wh.Definition.Name))
				}
			},
		},
		{name: "tags: TagResource, UntagResource", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: &types.PipelineDeclaration{
					Name:    aws.String("slice25-tags-pipeline"),
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

			got, err := client.GetPipeline(
				ctx,
				&cpsdk.GetPipelineInput{Name: aws.String("slice25-tags-pipeline")},
			)
			require.NoError(t, err)
			require.NotNil(t, got.Metadata)
			pipelineArn := aws.ToString(got.Metadata.PipelineArn)
			require.NotEmpty(t, pipelineArn)

			_, err = client.TagResource(ctx, &cpsdk.TagResourceInput{
				ResourceArn: aws.String(pipelineArn),
				Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
			})
			require.NoError(t, err)

			tagsOut, err := client.ListTagsForResource(ctx, &cpsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(pipelineArn),
			})
			require.NoError(t, err)
			require.Len(t, tagsOut.Tags, 1)
			assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))

			_, err = client.UntagResource(ctx, &cpsdk.UntagResourceInput{
				ResourceArn: aws.String(pipelineArn),
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			tagsOut, err = client.ListTagsForResource(ctx, &cpsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(pipelineArn),
			})
			require.NoError(t, err)
			assert.Empty(t, tagsOut.Tags)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
