package codepipeline_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func variableCheck(variable, op, value string) types.RuleDeclaration {
	return types.RuleDeclaration{
		Name: aws.String("check"),
		RuleTypeId: &types.RuleTypeId{
			Category: types.RuleCategoryRule, Owner: types.RuleOwnerAws,
			Provider: aws.String("VariableCheck"), Version: aws.String("1"),
		},
		Configuration: map[string]string{"Variable": variable, "Operator": op, "Value": value},
	}
}

func conditionPipeline(name string, mutate func(*types.StageDeclaration)) *types.PipelineDeclaration {
	p := sdkPipeline(name)
	p.PipelineType = types.PipelineTypeV2
	p.Variables = []types.PipelineVariableDeclaration{{Name: aws.String("Env"), DefaultValue: aws.String("dev")}}
	p.Stages[0].Actions[0].OutputArtifacts = []types.OutputArtifact{{Name: aws.String("SourceOut")}}
	deploy := types.StageDeclaration{
		Name: aws.String("Deploy"),
		Actions: []types.ActionDeclaration{{
			Name: aws.String("DeployAction"),
			ActionTypeId: &types.ActionTypeId{
				Category: types.ActionCategoryDeploy, Owner: types.ActionOwnerAws,
				Provider: aws.String("S3"), Version: aws.String("1"),
			},
		}},
	}
	mutate(&deploy)
	p.Stages = append(p.Stages, deploy)

	return p
}

func TestStageConditions_Engine(t *testing.T) {
	t.Parallel()

	entry := func(result types.Result, rule types.RuleDeclaration) func(*types.StageDeclaration) {
		return func(s *types.StageDeclaration) {
			s.BeforeEntry = &types.BeforeEntryConditions{Conditions: []types.Condition{
				{Result: result, Rules: []types.RuleDeclaration{rule}},
			}}
		}
	}

	tests := []struct {
		mutate        func(*types.StageDeclaration)
		name          string
		wantExec      types.PipelineExecutionStatus
		wantCondition types.ConditionExecutionStatus
		wantDeploy    bool
	}{
		{
			name:          "entry met",
			mutate:        entry(types.ResultFail, variableCheck("#{variables.Env}", "EQ", "dev")),
			wantExec:      types.PipelineExecutionStatusSucceeded,
			wantCondition: types.ConditionExecutionStatusSucceeded,
			wantDeploy:    true,
		},
		{
			name:          "entry not met fails",
			mutate:        entry(types.ResultFail, variableCheck("#{variables.Env}", "EQ", "prod")),
			wantExec:      types.PipelineExecutionStatusFailed,
			wantCondition: types.ConditionExecutionStatusFailed,
		},
		{
			name:          "entry not met skips",
			mutate:        entry(types.ResultSkip, variableCheck("#{variables.Env}", "EQ", "prod")),
			wantExec:      types.PipelineExecutionStatusSucceeded,
			wantCondition: types.ConditionExecutionStatusFailed,
		},
		{
			name:          "numeric operator",
			mutate:        entry(types.ResultFail, variableCheck("7", "GT", "5")),
			wantExec:      types.PipelineExecutionStatusSucceeded,
			wantCondition: types.ConditionExecutionStatusSucceeded,
			wantDeploy:    true,
		},
		{
			name:          "regex operator",
			mutate:        entry(types.ResultFail, variableCheck("release-42", "MATCHES", `^release-\d+$`)),
			wantExec:      types.PipelineExecutionStatusSucceeded,
			wantCondition: types.ConditionExecutionStatusSucceeded,
			wantDeploy:    true,
		},
		{
			name:          "unknown operator fails the rule",
			mutate:        entry(types.ResultFail, variableCheck("a", "WAT", "a")),
			wantExec:      types.PipelineExecutionStatusFailed,
			wantCondition: types.ConditionExecutionStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: conditionPipeline("cond", tt.mutate),
			})
			require.NoError(t, err)

			start, err := client.StartPipelineExecution(
				ctx,
				&cpsdk.StartPipelineExecutionInput{Name: aws.String("cond")},
			)
			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName: aws.String("cond"), PipelineExecutionId: start.PipelineExecutionId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantExec, got.PipelineExecution.Status)

			if tt.wantExec == types.PipelineExecutionStatusFailed {
				assert.NotEmpty(t, aws.ToString(got.PipelineExecution.StatusSummary))
			}

			state, err := client.GetPipelineState(ctx, &cpsdk.GetPipelineStateInput{Name: aws.String("cond")})
			require.NoError(t, err)

			cs := state.StageStates[1].BeforeEntryConditionState
			require.NotNil(t, cs)
			assert.Equal(t, tt.wantCondition, cs.LatestExecution.Status)
			require.Len(t, cs.ConditionStates, 1)
			require.Len(t, cs.ConditionStates[0].RuleStates, 1)

			rules, err := client.ListRuleExecutions(ctx, &cpsdk.ListRuleExecutionsInput{
				PipelineName: aws.String("cond"),
				Filter:       &types.RuleExecutionFilter{PipelineExecutionId: start.PipelineExecutionId},
			})
			require.NoError(t, err)
			require.Len(t, rules.RuleExecutionDetails, 1)
			assert.Equal(t, "Deploy", aws.ToString(rules.RuleExecutionDetails[0].StageName))
			assert.Equal(t, "check", aws.ToString(rules.RuleExecutionDetails[0].RuleName))

			actions, err := client.ListActionExecutions(
				ctx,
				&cpsdk.ListActionExecutionsInput{PipelineName: aws.String("cond")},
			)
			require.NoError(t, err)

			var deployRan bool

			for _, a := range actions.ActionExecutionDetails {
				deployRan = deployRan || aws.ToString(a.StageName) == "Deploy"
			}

			assert.Equal(t, tt.wantDeploy, deployRan)
		})
	}
}

func TestStageConditions_Override(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		conditionType types.ConditionType
		wantErr       string
	}{
		{name: "failed entry condition", conditionType: types.ConditionTypeBeforeEntry},
		{
			name:          "condition type with no run",
			conditionType: types.ConditionTypeOnSuccess,
			wantErr:       "ConditionNotOverridableException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: conditionPipeline("ovr", func(s *types.StageDeclaration) {
					s.BeforeEntry = &types.BeforeEntryConditions{Conditions: []types.Condition{{
						Result: types.ResultFail,
						Rules:  []types.RuleDeclaration{variableCheck("a", "EQ", "b")},
					}}}
				}),
			})
			require.NoError(t, err)

			start, err := client.StartPipelineExecution(
				ctx,
				&cpsdk.StartPipelineExecutionInput{Name: aws.String("ovr")},
			)
			require.NoError(t, err)

			_, err = client.OverrideStageCondition(ctx, &cpsdk.OverrideStageConditionInput{
				PipelineName: aws.String("ovr"), StageName: aws.String("Deploy"),
				PipelineExecutionId: start.PipelineExecutionId, ConditionType: tt.conditionType,
			})

			if tt.wantErr != "" {
				var cno *types.ConditionNotOverridableException
				require.ErrorAs(t, err, &cno)

				return
			}

			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName: aws.String("ovr"), PipelineExecutionId: start.PipelineExecutionId,
			})
			require.NoError(t, err)
			assert.Equal(t, types.PipelineExecutionStatusSucceeded, got.PipelineExecution.Status)

			state, err := client.GetPipelineState(ctx, &cpsdk.GetPipelineStateInput{Name: aws.String("ovr")})
			require.NoError(t, err)
			assert.Equal(t, types.ConditionExecutionStatusOverridden,
				state.StageStates[1].BeforeEntryConditionState.LatestExecution.Status)

			_, err = client.OverrideStageCondition(ctx, &cpsdk.OverrideStageConditionInput{
				PipelineName: aws.String("ovr"), StageName: aws.String("Deploy"),
				PipelineExecutionId: start.PipelineExecutionId, ConditionType: tt.conditionType,
			})

			var cno *types.ConditionNotOverridableException
			require.ErrorAs(t, err, &cno, "an overridden condition cannot be overridden again")
		})
	}
}

func TestStageConditions_SuccessAndFailureHandling(t *testing.T) {
	t.Parallel()

	failing := types.RuleDeclaration{
		Name: aws.String("check"),
		RuleTypeId: &types.RuleTypeId{
			Category: types.RuleCategoryRule, Owner: types.RuleOwnerAws,
			Provider: aws.String("VariableCheck"), Version: aws.String("1"),
		},
		Configuration: map[string]string{"Variable": "x", "Operator": "EQ", "Value": "y"},
	}

	tests := []struct {
		mutate     func(*types.StageDeclaration)
		name       string
		wantExec   types.PipelineExecutionStatus
		wantOnSucc bool
	}{
		{
			name: "success condition not met fails",
			mutate: func(s *types.StageDeclaration) {
				s.OnSuccess = &types.SuccessConditions{Conditions: []types.Condition{{
					Result: types.ResultFail, Rules: []types.RuleDeclaration{failing},
				}}}
			},
			wantExec:   types.PipelineExecutionStatusFailed,
			wantOnSucc: true,
		},
		{
			name: "success condition met proceeds",
			mutate: func(s *types.StageDeclaration) {
				ok := failing
				ok.Configuration = map[string]string{"Variable": "x", "Operator": "EQ", "Value": "x"}
				s.OnSuccess = &types.SuccessConditions{Conditions: []types.Condition{{
					Result: types.ResultFail, Rules: []types.RuleDeclaration{ok},
				}}}
			},
			wantExec:   types.PipelineExecutionStatusSucceeded,
			wantOnSucc: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(
				ctx,
				&cpsdk.CreatePipelineInput{Pipeline: conditionPipeline("succ", tt.mutate)},
			)
			require.NoError(t, err)

			start, err := client.StartPipelineExecution(
				ctx,
				&cpsdk.StartPipelineExecutionInput{Name: aws.String("succ")},
			)
			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName: aws.String("succ"), PipelineExecutionId: start.PipelineExecutionId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantExec, got.PipelineExecution.Status)

			state, err := client.GetPipelineState(ctx, &cpsdk.GetPipelineStateInput{Name: aws.String("succ")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantOnSucc, state.StageStates[1].OnSuccessConditionState != nil)
		})
	}
}

func TestPipelineExecution_ArtifactRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantRevision string
		revisions    []types.SourceRevisionOverride
	}{
		{
			name: "pinned source revision",
			revisions: []types.SourceRevisionOverride{{
				ActionName: aws.String("SourceAction"), RevisionType: types.SourceRevisionTypeCommitId,
				RevisionValue: aws.String("abc123"),
			}},
			wantRevision: "abc123",
		},
		{name: "no pinned revision"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codepipeline.NewHandler(codepipeline.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodePipelineClient(t, h)
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: conditionPipeline("art", func(*types.StageDeclaration) {}),
			})
			require.NoError(t, err)

			start, err := client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{
				Name: aws.String("art"), SourceRevisions: tt.revisions,
			})
			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName: aws.String("art"), PipelineExecutionId: start.PipelineExecutionId,
			})
			require.NoError(t, err)

			if tt.wantRevision == "" {
				assert.Empty(t, got.PipelineExecution.ArtifactRevisions)

				return
			}

			require.Len(t, got.PipelineExecution.ArtifactRevisions, 1)
			assert.Equal(t, "SourceOut", aws.ToString(got.PipelineExecution.ArtifactRevisions[0].Name))
			assert.Equal(t, tt.wantRevision, aws.ToString(got.PipelineExecution.ArtifactRevisions[0].RevisionId))
		})
	}
}

var errBuildRejected = errors.New("build rejected")

type flakyBuilder struct{ failures atomic.Int32 }

func (f *flakyBuilder) StartBuild(context.Context, string) error {
	if f.failures.Add(-1) >= 0 {
		return errBuildRejected
	}

	return nil
}

func buildStage(s *types.StageDeclaration) {
	s.Actions[0].ActionTypeId = &types.ActionTypeId{
		Category: types.ActionCategoryBuild, Owner: types.ActionOwnerAws,
		Provider: aws.String("CodeBuild"), Version: aws.String("1"),
	}
	s.Actions[0].Configuration = map[string]string{"ProjectName": "p"}
}

func TestStageConditions_FailureHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		onFailure types.FailureConditions
		name      string
		wantExec  types.PipelineExecutionStatus
		failures  int32
		wantRetry bool
	}{
		{
			name:      "retry succeeds on second attempt",
			onFailure: types.FailureConditions{Result: types.ResultRetry},
			failures:  1, wantExec: types.PipelineExecutionStatusSucceeded, wantRetry: true,
		},
		{
			name:      "retry exhausted after one attempt",
			onFailure: types.FailureConditions{Result: types.ResultRetry},
			failures:  2, wantExec: types.PipelineExecutionStatusFailed, wantRetry: true,
		},
		{
			name:      "fail result leaves stage failed",
			onFailure: types.FailureConditions{Result: types.ResultFail},
			failures:  1, wantExec: types.PipelineExecutionStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := codepipeline.NewInMemoryBackend("123456789012", "us-east-1")
			builder := &flakyBuilder{}
			be.SetCodeBuildBackend(builder)
			builder.failures.Store(tt.failures)

			client := newTestCodePipelineClient(t, codepipeline.NewHandler(be))
			ctx := t.Context()

			_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
				Pipeline: conditionPipeline("fail", func(s *types.StageDeclaration) {
					buildStage(s)
					s.OnFailure = &tt.onFailure
				}),
			})
			require.NoError(t, err)

			start, err := client.StartPipelineExecution(
				ctx,
				&cpsdk.StartPipelineExecutionInput{Name: aws.String("fail")},
			)
			require.NoError(t, err)

			got, err := client.GetPipelineExecution(ctx, &cpsdk.GetPipelineExecutionInput{
				PipelineName: aws.String("fail"), PipelineExecutionId: start.PipelineExecutionId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantExec, got.PipelineExecution.Status)

			state, err := client.GetPipelineState(ctx, &cpsdk.GetPipelineStateInput{Name: aws.String("fail")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantRetry, state.StageStates[1].RetryStageMetadata != nil)
		})
	}
}

func TestStageConditions_AutomatedRollback(t *testing.T) {
	t.Parallel()

	be := codepipeline.NewInMemoryBackend("123456789012", "us-east-1")
	builder := &flakyBuilder{}
	be.SetCodeBuildBackend(builder)

	client := newTestCodePipelineClient(t, codepipeline.NewHandler(be))
	ctx := t.Context()

	_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
		Pipeline: conditionPipeline("rb", func(s *types.StageDeclaration) {
			buildStage(s)
			s.OnFailure = &types.FailureConditions{Result: types.ResultRollback}
		}),
	})
	require.NoError(t, err)

	good, err := client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{Name: aws.String("rb")})
	require.NoError(t, err)

	builder.failures.Store(1)

	_, err = client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{Name: aws.String("rb")})
	require.NoError(t, err)

	list, err := client.ListPipelineExecutions(ctx, &cpsdk.ListPipelineExecutionsInput{PipelineName: aws.String("rb")})
	require.NoError(t, err)

	var rollback *types.PipelineExecutionSummary

	for i := range list.PipelineExecutionSummaries {
		if list.PipelineExecutionSummaries[i].ExecutionType == types.ExecutionTypeRollback {
			rollback = &list.PipelineExecutionSummaries[i]
		}
	}

	require.NotNil(t, rollback)
	assert.Equal(t, types.TriggerTypeAutomatedRollback, rollback.Trigger.TriggerType)
	assert.Equal(
		t,
		aws.ToString(good.PipelineExecutionId),
		aws.ToString(rollback.RollbackMetadata.RollbackTargetPipelineExecutionId),
	)
}

func TestStageConditions_SnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	be := codepipeline.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestCodePipelineClient(t, codepipeline.NewHandler(be))
	ctx := t.Context()

	_, err := client.CreatePipeline(ctx, &cpsdk.CreatePipelineInput{
		Pipeline: conditionPipeline("snap", func(s *types.StageDeclaration) {
			s.BeforeEntry = &types.BeforeEntryConditions{Conditions: []types.Condition{{
				Result: types.ResultFail, Rules: []types.RuleDeclaration{variableCheck("a", "EQ", "b")},
			}}}
		}),
	})
	require.NoError(t, err)

	_, err = client.StartPipelineExecution(ctx, &cpsdk.StartPipelineExecutionInput{Name: aws.String("snap")})
	require.NoError(t, err)

	restored := codepipeline.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(ctx, be.Snapshot(ctx)))

	rules, err := newTestCodePipelineClient(t, codepipeline.NewHandler(restored)).ListRuleExecutions(
		ctx, &cpsdk.ListRuleExecutionsInput{PipelineName: aws.String("snap")})
	require.NoError(t, err)
	require.Len(t, rules.RuleExecutionDetails, 1)
	assert.Equal(t, types.RuleExecutionStatusFailed, rules.RuleExecutionDetails[0].Status)
}

func TestStageConditions_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*types.StageDeclaration)
		name   string
	}{
		{
			name: "bad condition result",
			mutate: func(s *types.StageDeclaration) {
				s.BeforeEntry = &types.BeforeEntryConditions{Conditions: []types.Condition{{Result: "EXPLODE"}}}
			},
		},
		{
			name: "rule without name",
			mutate: func(s *types.StageDeclaration) {
				r := variableCheck("a", "EQ", "a")
				r.Name = aws.String("")
				s.OnSuccess = &types.SuccessConditions{
					Conditions: []types.Condition{{Rules: []types.RuleDeclaration{r}}},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodePipelineClient(t, codepipeline.NewHandler(
				codepipeline.NewInMemoryBackend("123456789012", "us-east-1")))

			_, err := client.CreatePipeline(t.Context(), &cpsdk.CreatePipelineInput{
				Pipeline: conditionPipeline("bad", tt.mutate),
			})

			var ise *types.InvalidStructureException
			require.ErrorAs(t, err, &ise)
		})
	}
}
