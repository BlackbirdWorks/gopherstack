package budgets_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const roleARN = "arn:aws:iam::123456789012:role/budget-exec"

func actionInput(name string, approval types.ApprovalModel, threshold float64) *bsdk.CreateBudgetActionInput {
	return &bsdk.CreateBudgetActionInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String(name),
		ActionType:       types.ActionTypeIam,
		ApprovalModel:    approval,
		NotificationType: types.NotificationTypeActual,
		ExecutionRoleArn: aws.String(roleARN),
		ActionThreshold: &types.ActionThreshold{
			ActionThresholdType: types.ThresholdTypePercentage, ActionThresholdValue: threshold,
		},
		Definition: &types.Definition{IamActionDefinition: &types.IamActionDefinition{
			PolicyArn: aws.String("arn:aws:iam::aws:policy/AWSDenyAll"), Roles: []string{"worker"},
		}},
		Subscribers: []types.Subscriber{email("ops@example.com")},
	}
}

func actionStatus(t *testing.T, f *fixture, name, id string) types.ActionStatus {
	t.Helper()

	got, err := f.client.DescribeBudgetAction(t.Context(), &bsdk.DescribeBudgetActionInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String(name), ActionId: aws.String(id),
	})
	require.NoError(t, err)

	return got.Action.Status
}

func execute(t *testing.T, f *fixture, name, id string, et types.ExecutionType) error {
	t.Helper()

	_, err := f.client.ExecuteBudgetAction(t.Context(), &bsdk.ExecuteBudgetActionInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String(name), ActionId: aws.String(id), ExecutionType: et,
	})

	return err
}

func TestActionManualApprovalFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	createBudget(t, f, "act", "100")

	created, err := f.client.CreateBudgetAction(t.Context(), actionInput("act", types.ApprovalModelManual, 80))
	require.NoError(t, err)

	id := aws.ToString(created.ActionId)
	assert.Equal(t, types.ActionStatusStandby, actionStatus(t, f, "act", id))

	require.Equal(
		t,
		"InvalidParameterException",
		apiCode(execute(t, f, "act", id, types.ExecutionTypeApproveBudgetAction)),
	)

	f.spend.set(95)
	assert.Equal(t, types.ActionStatusPending, actionStatus(t, f, "act", id))
	assert.Equal(t, 1, f.notifier.count())

	require.NoError(t, execute(t, f, "act", id, types.ExecutionTypeApproveBudgetAction))
	assert.Equal(t, types.ActionStatusExecutionSuccess, actionStatus(t, f, "act", id))

	require.NoError(t, execute(t, f, "act", id, types.ExecutionTypeReverseBudgetAction))
	assert.Equal(t, types.ActionStatusReverseSuccess, actionStatus(t, f, "act", id))

	f.spend.set(10)
	require.NoError(t, execute(t, f, "act", id, types.ExecutionTypeResetBudgetAction))
	assert.Equal(t, types.ActionStatusStandby, actionStatus(t, f, "act", id))
	assert.Equal(t, []string{"apply:APPLY_IAM_POLICY", "reverse:APPLY_IAM_POLICY"}, f.executor.runs)

	hist, err := f.client.DescribeBudgetActionHistories(t.Context(), &bsdk.DescribeBudgetActionHistoriesInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("act"), ActionId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ActionStatusStandby, hist.ActionHistories[0].Status)
	assert.Equal(t, types.EventTypeCreateAction, hist.ActionHistories[len(hist.ActionHistories)-1].EventType)
}

func TestActionAutomaticAndFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  types.ActionStatus
		fails bool
	}{
		{name: "success", want: types.ActionStatusExecutionSuccess},
		{name: "failure", fails: true, want: types.ActionStatusExecutionFailure},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.executor.fails = tc.fails
			createBudget(t, f, "auto", "100")

			created, err := f.client.CreateBudgetAction(t.Context(), actionInput("auto", types.ApprovalModelAuto, 50))
			require.NoError(t, err)

			id := aws.ToString(created.ActionId)
			f.spend.set(70)
			assert.Equal(t, tc.want, actionStatus(t, f, "auto", id))

			if tc.fails {
				f.executor.mu.Lock()
				f.executor.fails = false
				f.executor.mu.Unlock()
				require.NoError(t, execute(t, f, "auto", id, types.ExecutionTypeRetryBudgetAction))
				assert.Equal(t, types.ActionStatusExecutionSuccess, actionStatus(t, f, "auto", id))
			}
		})
	}
}

func TestActionCRUDAndValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	createBudget(t, f, "crud", "100")

	bad := actionInput("crud", types.ApprovalModelManual, 80)
	bad.ExecutionRoleArn = aws.String("arn:aws:iam::999999999999:role/other")
	_, err := f.client.CreateBudgetAction(ctx, bad)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	mismatch := actionInput("crud", types.ApprovalModelManual, 80)
	mismatch.ActionType = types.ActionTypeScp
	_, err = f.client.CreateBudgetAction(ctx, mismatch)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.CreateBudgetAction(ctx, actionInput("missing", types.ApprovalModelManual, 80))
	assert.Equal(t, "NotFoundException", apiCode(err))

	created, err := f.client.CreateBudgetAction(ctx, actionInput("crud", types.ApprovalModelManual, 80))
	require.NoError(t, err)

	id := created.ActionId

	upd, err := f.client.UpdateBudgetAction(ctx, &bsdk.UpdateBudgetActionInput{
		AccountId:  aws.String(testAccount),
		BudgetName: aws.String("crud"),
		ActionId:   id,
		ActionThreshold: &types.ActionThreshold{
			ActionThresholdType:  types.ThresholdTypeAbsoluteValue,
			ActionThresholdValue: 500,
		},
	})
	require.NoError(t, err)
	assert.InDelta(t, 80, upd.OldAction.ActionThreshold.ActionThresholdValue, 0)
	assert.InDelta(t, 500, upd.NewAction.ActionThreshold.ActionThresholdValue, 0)

	forBudget, err := f.client.DescribeBudgetActionsForBudget(ctx, &bsdk.DescribeBudgetActionsForBudgetInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("crud"),
	})
	require.NoError(t, err)
	assert.Len(t, forBudget.Actions, 1)

	forAccount, err := f.client.DescribeBudgetActionsForAccount(ctx, &bsdk.DescribeBudgetActionsForAccountInput{
		AccountId: aws.String(testAccount),
	})
	require.NoError(t, err)
	assert.Len(t, forAccount.Actions, 1)

	del, err := f.client.DeleteBudgetAction(ctx, &bsdk.DeleteBudgetActionInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("crud"), ActionId: id,
	})
	require.NoError(t, err)
	assert.Equal(t, id, del.Action.ActionId)

	_, err = f.client.DescribeBudgetAction(ctx, &bsdk.DescribeBudgetActionInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("crud"), ActionId: id,
	})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestDeleteBudgetRemovesActions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	createBudget(t, f, "gone", "100")

	_, err := f.client.CreateBudgetAction(t.Context(), actionInput("gone", types.ApprovalModelManual, 80))
	require.NoError(t, err)

	_, err = f.client.DeleteBudget(t.Context(), &bsdk.DeleteBudgetInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("gone"),
	})
	require.NoError(t, err)

	out, err := f.client.DescribeBudgetActionsForAccount(t.Context(), &bsdk.DescribeBudgetActionsForAccountInput{
		AccountId: aws.String(testAccount),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Actions)
}
