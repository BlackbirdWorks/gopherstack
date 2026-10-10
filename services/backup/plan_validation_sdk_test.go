package backup_test

import (
	"fmt"

	"testing"

	"github.com/blackbirdworks/gopherstack/services/backup"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateBackupPlanValidation(t *testing.T) {
	t.Parallel()

	rule := func(mut func(*types.BackupRuleInput)) []types.BackupRuleInput {
		r := types.BackupRuleInput{
			RuleName:              aws.String("r1"),
			TargetBackupVaultName: aws.String("v1"),
			ScheduleExpression:    aws.String("cron(0 12 * * ? *)"),
		}
		mut(&r)

		return []types.BackupRuleInput{r}
	}

	tests := []struct {
		name     string
		planName string
		wantCode string
		rules    []types.BackupRuleInput
	}{
		{name: "valid", planName: "p-1_a.b", rules: rule(func(*types.BackupRuleInput) {})},
		{
			name:     "plan_name_space",
			planName: "bad name",
			rules:    rule(func(*types.BackupRuleInput) {}),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "rule_name_symbol", planName: "p",
			rules:    rule(func(r *types.BackupRuleInput) { r.RuleName = aws.String("bad rule!") }),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "bad_cron", planName: "p",
			rules:    rule(func(r *types.BackupRuleInput) { r.ScheduleExpression = aws.String("cron(bogus)") }),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "rate_expression", planName: "p",
			rules:    rule(func(r *types.BackupRuleInput) { r.ScheduleExpression = aws.String("rate(1 hour)") }),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "short_start_window", planName: "p",
			rules:    rule(func(r *types.BackupRuleInput) { r.StartWindowMinutes = aws.Int64(5) }),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "cold_storage_gap", planName: "p",
			rules: rule(func(r *types.BackupRuleInput) {
				r.Lifecycle = &types.Lifecycle{
					MoveToColdStorageAfterDays: aws.Int64(10),
					DeleteAfterDays:            aws.Int64(20),
				}
			}),
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "cold_storage_ok", planName: "p",
			rules: rule(func(r *types.BackupRuleInput) {
				r.Lifecycle = &types.Lifecycle{
					MoveToColdStorageAfterDays: aws.Int64(30),
					DeleteAfterDays:            aws.Int64(120),
				}
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			_, err := client.CreateBackupPlan(t.Context(), &backupsdk.CreateBackupPlanInput{
				BackupPlan: &types.BackupPlanInput{BackupPlanName: aws.String(tt.planName), Rules: tt.rules},
			})

			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}

func TestHandler_TrailingSlashPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "plan_slash", path: "/backup/plans/%s/"},
		{name: "plan_plain", path: "/backup/plans/%s"},
		{name: "selections_slash", path: "/backup/plans/%s/selections/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			plan, err := b.CreateBackupPlanValidated(
				"p",
				[]backup.Rule{{RuleName: "r", TargetVaultName: "v"}},
				nil,
				nil,
			)
			require.NoError(t, err)

			rec := doRequest(t, backup.NewHandler(b), "GET", fmt.Sprintf(tt.path, plan.BackupPlanID), "")
			assert.Equal(t, 200, rec.Code, rec.Body.String())
		})
	}
}

func TestSDK_ListMaxResultsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		max     int32
		wantErr bool
	}{
		{name: "in_range", max: 10},
		{name: "upper", max: 1000},
		{name: "too_large", max: 1001, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			_, err := client.ListBackupPlans(
				t.Context(),
				&backupsdk.ListBackupPlansInput{MaxResults: aws.Int32(tt.max)},
			)

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidParameterValueException", apiErr.ErrorCode())
		})
	}
}

func TestSDK_StartBackupJobArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		role     string
		wantErr  bool
	}{
		{
			name:     "valid",
			resource: "arn:aws:dynamodb:us-east-1:123456789012:table/t",
			role:     "arn:aws:iam::123456789012:role/r",
		},
		{
			name:     "bad_role",
			resource: "arn:aws:dynamodb:us-east-1:123456789012:table/t",
			role:     "notarn",
			wantErr:  true,
		},
		{name: "bad_resource", resource: "notanarn", role: "arn:aws:iam::123456789012:role/r", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			mustVault(t, b, "v1")

			_, err := client.StartBackupJob(t.Context(), &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("v1"),
				ResourceArn:     aws.String(tt.resource),
				IamRoleArn:      aws.String(tt.role),
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidParameterValueException", apiErr.ErrorCode())
		})
	}
}
