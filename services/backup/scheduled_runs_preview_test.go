package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestBackend_ScheduledRunsPreview guards GetBackupPlanInput.MaxScheduledRunsPreview
// ("Number of future scheduled backup runs to preview. When set to 0 (default),
// no scheduled runs preview is included in the response" -- api_op_GetBackupPlan.go),
// which was previously accepted on the wire and completely dropped: GetBackupPlan
// never even read the query parameter.
func TestBackend_ScheduledRunsPreview(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		rules      []backup.Rule
		wantTimes  []time.Time
		wantTypes  []string
		maxResults int
	}{
		{
			name:       "zero returns nothing",
			rules:      []backup.Rule{{RuleName: "r1", ScheduleExpression: "cron(30 4 ? * * *)"}},
			maxResults: 0,
			wantTimes:  nil,
			wantTypes:  nil,
		},
		{
			name:       "explicit daily schedule",
			rules:      []backup.Rule{{RuleName: "r1", ScheduleExpression: "cron(30 4 ? * * *)"}},
			maxResults: 3,
			wantTimes: []time.Time{
				time.Date(2026, 1, 1, 4, 30, 0, 0, time.UTC),
				time.Date(2026, 1, 2, 4, 30, 0, 0, time.UTC),
				time.Date(2026, 1, 3, 4, 30, 0, 0, time.UTC),
			},
			wantTypes: []string{"SNAPSHOTS", "SNAPSHOTS", "SNAPSHOTS"},
		},
		{
			name:       "empty schedule uses the documented default",
			rules:      []backup.Rule{{RuleName: "r1"}},
			maxResults: 2,
			wantTimes: []time.Time{
				time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC),
				time.Date(2026, 1, 2, 5, 0, 0, 0, time.UTC),
			},
			wantTypes: []string{"SNAPSHOTS", "SNAPSHOTS"},
		},
		{
			name: "continuous backup rule reports the combined execution type",
			rules: []backup.Rule{
				{RuleName: "r1", ScheduleExpression: "cron(30 4 ? * * *)", EnableContinuousBackup: true},
			},
			maxResults: 1,
			wantTimes:  []time.Time{time.Date(2026, 1, 1, 4, 30, 0, 0, time.UTC)},
			wantTypes:  []string{"CONTINUOUS_AND_SNAPSHOTS"},
		},
		{
			name: "multiple rules are merged and sorted by time",
			rules: []backup.Rule{
				{RuleName: "late", ScheduleExpression: "cron(0 20 ? * * *)"},
				{RuleName: "early", ScheduleExpression: "cron(0 6 ? * * *)"},
			},
			maxResults: 2,
			wantTimes: []time.Time{
				time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC),
				time.Date(2026, 1, 1, 20, 0, 0, 0, time.UTC),
			},
			wantTypes: []string{"SNAPSHOTS", "SNAPSHOTS"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := &backup.Plan{Rules: tc.rules}
			got := backup.ScheduledRunsPreview(plan, tc.maxResults, now)

			require.Len(t, got, len(tc.wantTimes))

			for i, want := range tc.wantTimes {
				assert.True(
					t, got[i].ExecutionTime.Equal(want),
					"entry %d: got %v want %v", i, got[i].ExecutionTime, want,
				)
				assert.Equal(t, tc.wantTypes[i], got[i].RuleExecutionType)
			}
		})
	}
}

// TestGetBackupPlan_ScheduledRunsPreviewOverSDKClient drives GetBackupPlan through
// the real aws-sdk-go-v2 client and proves MaxScheduledRunsPreview reaches the
// backend and the response carries a real ScheduledRunsPreview -- the observable
// wire effect a typed client actually decodes, not just an internal computation.
func TestGetBackupPlan_ScheduledRunsPreviewOverSDKClient(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	createOut, err := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
		BackupPlan: &types.BackupPlanInput{
			BackupPlanName: aws.String("preview-plan"),
			Rules: []types.BackupRuleInput{
				{
					RuleName:              aws.String("r1"),
					TargetBackupVaultName: aws.String("preview-vault"),
					ScheduleExpression:    aws.String("cron(0 5 ? * * *)"),
				},
			},
		},
	})
	require.NoError(t, err)

	noPreview, err := client.GetBackupPlan(ctx, &backupsdk.GetBackupPlanInput{
		BackupPlanId: createOut.BackupPlanId,
	})
	require.NoError(t, err)
	assert.Empty(t, noPreview.ScheduledRunsPreview)

	withPreview, err := client.GetBackupPlan(ctx, &backupsdk.GetBackupPlanInput{
		BackupPlanId:            createOut.BackupPlanId,
		MaxScheduledRunsPreview: 4,
	})
	require.NoError(t, err)
	require.Len(t, withPreview.ScheduledRunsPreview, 4)

	for i := range 3 {
		cur := withPreview.ScheduledRunsPreview[i].ExecutionTime
		next := withPreview.ScheduledRunsPreview[i+1].ExecutionTime
		assert.Equal(t, 24*time.Hour, next.Sub(*cur), "daily schedule entries must be 24h apart")
	}

	assert.Equal(t, types.RuleExecutionTypeSnapshots, withPreview.ScheduledRunsPreview[0].RuleExecutionType)
}
