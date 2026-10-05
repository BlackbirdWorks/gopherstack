package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_LegalHoldCancelKeepsRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retainDays *int64
		name       string
		wantUntil  bool
	}{
		{retainDays: aws.Int64(30), name: "retain_days", wantUntil: true},
		{retainDays: nil, name: "no_retain", wantUntil: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			ctx := t.Context()

			created, err := client.CreateLegalHold(ctx, &backupsdk.CreateLegalHoldInput{
				Title: aws.String("hold"), Description: aws.String("court order"),
			})
			require.NoError(t, err)

			_, err = client.CancelLegalHold(ctx, &backupsdk.CancelLegalHoldInput{
				LegalHoldId:        created.LegalHoldId,
				CancelDescription:  aws.String("lifted"),
				RetainRecordInDays: tt.retainDays,
			})
			require.NoError(t, err)

			got, err := client.GetLegalHold(ctx, &backupsdk.GetLegalHoldInput{LegalHoldId: created.LegalHoldId})
			require.NoError(t, err)
			assert.Equal(t, types.LegalHoldStatusCanceled, got.Status)
			assert.Equal(t, "lifted", aws.ToString(got.CancelDescription))
			require.NotNil(t, got.CancellationDate)

			if tt.wantUntil {
				require.NotNil(t, got.RetainRecordUntil)
				assert.WithinDuration(t, time.Now().Add(30*24*time.Hour), *got.RetainRecordUntil, time.Minute)
			} else {
				assert.Nil(t, got.RetainRecordUntil)
			}

			list, err := client.ListLegalHolds(ctx, &backupsdk.ListLegalHoldsInput{})
			require.NoError(t, err)
			require.Len(t, list.LegalHolds, 1)
			assert.Equal(t, "court order", aws.ToString(list.LegalHolds[0].Description))
			assert.NotEmpty(t, aws.ToString(list.LegalHolds[0].LegalHoldArn))
			assert.NotNil(t, list.LegalHolds[0].CreationDate)
			assert.NotNil(t, list.LegalHolds[0].CancellationDate)
		})
	}
}

func TestSDK_LegalHoldIdempotencyToken(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	in := func(token string) *backupsdk.CreateLegalHoldInput {
		return &backupsdk.CreateLegalHoldInput{
			Title: aws.String("hold"), Description: aws.String("d"), IdempotencyToken: aws.String(token),
		}
	}

	first, err := client.CreateLegalHold(ctx, in("tok-1"))
	require.NoError(t, err)
	retry, err := client.CreateLegalHold(ctx, in("tok-1"))
	require.NoError(t, err)
	other, err := client.CreateLegalHold(ctx, in("tok-2"))
	require.NoError(t, err)

	assert.Equal(t, aws.ToString(first.LegalHoldId), aws.ToString(retry.LegalHoldId))
	assert.NotEqual(t, aws.ToString(first.LegalHoldId), aws.ToString(other.LegalHoldId))
}

func TestSDK_FrameworkAndReportPlanTagsAndTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *backupsdk.Client) (arn1, arn2 string, dupErr error)
		name string
	}{
		{name: "framework", run: func(t *testing.T, client *backupsdk.Client) (string, string, error) {
			t.Helper()

			in := func(token string) *backupsdk.CreateFrameworkInput {
				return &backupsdk.CreateFrameworkInput{
					FrameworkName:    aws.String("fw"),
					IdempotencyToken: aws.String(token),
					FrameworkTags:    map[string]string{"env": "dev"},
					FrameworkControls: []types.FrameworkControl{
						{ControlName: aws.String("BACKUP_RECOVERY_POINT_MANUAL_DELETION_DISABLED")},
					},
				}
			}

			first, err := client.CreateFramework(t.Context(), in("t1"))
			require.NoError(t, err)
			retry, err := client.CreateFramework(t.Context(), in("t1"))
			require.NoError(t, err)
			_, dupErr := client.CreateFramework(t.Context(), in("t2"))

			return aws.ToString(first.FrameworkArn), aws.ToString(retry.FrameworkArn), dupErr
		}},
		{name: "report_plan", run: func(t *testing.T, client *backupsdk.Client) (string, string, error) {
			t.Helper()

			in := func(token string) *backupsdk.CreateReportPlanInput {
				return &backupsdk.CreateReportPlanInput{
					ReportPlanName:        aws.String("rp"),
					IdempotencyToken:      aws.String(token),
					ReportPlanTags:        map[string]string{"env": "dev"},
					ReportDeliveryChannel: &types.ReportDeliveryChannel{S3BucketName: aws.String("bkt")},
					ReportSetting:         &types.ReportSetting{ReportTemplate: aws.String("BACKUP_JOB_REPORT")},
				}
			}

			first, err := client.CreateReportPlan(t.Context(), in("t1"))
			require.NoError(t, err)
			retry, err := client.CreateReportPlan(t.Context(), in("t1"))
			require.NoError(t, err)
			_, dupErr := client.CreateReportPlan(t.Context(), in("t2"))

			return aws.ToString(first.ReportPlanArn), aws.ToString(retry.ReportPlanArn), dupErr
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			arn1, arn2, dupErr := tt.run(t, client)

			assert.Equal(t, arn1, arn2)
			require.Error(t, dupErr)
			assert.Contains(t, dupErr.Error(), "AlreadyExistsException")

			tagsOut, err := client.ListTags(t.Context(), &backupsdk.ListTagsInput{ResourceArn: aws.String(arn1)})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "dev"}, tagsOut.Tags)
		})
	}
}

func TestSDK_StartBackupJobTagsLifecycleAndToken(t *testing.T) {
	t.Parallel()

	b, client := newRealClient(t)
	ctx := t.Context()

	mustVault(t, b, "vault-a")

	in := &backupsdk.StartBackupJobInput{
		BackupVaultName:   aws.String("vault-a"),
		ResourceArn:       aws.String("arn:aws:ec2:us-east-1:123456789012:volume/vol-1"),
		IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/r"),
		IdempotencyToken:  aws.String("job-token"),
		RecoveryPointTags: map[string]string{"team": "core"},
		Lifecycle:         &types.Lifecycle{DeleteAfterDays: aws.Int64(120), MoveToColdStorageAfterDays: aws.Int64(30)},
	}

	started, err := client.StartBackupJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.StartBackupJob(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(started.BackupJobId), aws.ToString(retry.BackupJobId))

	require.NoError(t, b.CompleteBackupJob(aws.ToString(started.BackupJobId)))

	desc, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{BackupJobId: started.BackupJobId})
	require.NoError(t, err)
	rpArn := aws.ToString(desc.RecoveryPointArn)
	require.NotEmpty(t, rpArn)

	tagsOut, err := client.ListTags(ctx, &backupsdk.ListTagsInput{ResourceArn: aws.String(rpArn)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "core"}, tagsOut.Tags)

	rp, err := client.DescribeRecoveryPoint(ctx, &backupsdk.DescribeRecoveryPointInput{
		BackupVaultName: aws.String("vault-a"), RecoveryPointArn: aws.String(rpArn),
	})
	require.NoError(t, err)
	require.NotNil(t, rp.Lifecycle)
	assert.EqualValues(t, 120, aws.ToInt64(rp.Lifecycle.DeleteAfterDays))
	require.NotNil(t, rp.CalculatedLifecycle)
	assert.NotNil(t, rp.CalculatedLifecycle.DeleteAt)

	_, err = client.TagResource(ctx, &backupsdk.TagResourceInput{
		ResourceArn: aws.String(rpArn), Tags: map[string]string{"extra": "1"},
	})
	require.NoError(t, err)
	_, err = client.UntagResource(ctx, &backupsdk.UntagResourceInput{
		ResourceArn: aws.String(rpArn), TagKeyList: []string{"team"},
	})
	require.NoError(t, err)

	tagsOut, err = client.ListTags(ctx, &backupsdk.ListTagsInput{ResourceArn: aws.String(rpArn)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"extra": "1"}, tagsOut.Tags)
}

func TestSDK_ListReportJobsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  backupsdk.ListReportJobsInput
		wantN  int
		future bool
	}{
		{name: "no_filter", input: backupsdk.ListReportJobsInput{}, wantN: 2},
		{name: "by_plan", input: backupsdk.ListReportJobsInput{ByReportPlanName: aws.String("plan-a")}, wantN: 1},
		{name: "status_match", input: backupsdk.ListReportJobsInput{ByStatus: aws.String("COMPLETED")}, wantN: 2},
		{name: "status_miss", input: backupsdk.ListReportJobsInput{ByStatus: aws.String("RUNNING")}, wantN: 0},
		{name: "created_after_future", input: backupsdk.ListReportJobsInput{}, wantN: 0, future: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			b.StartReportJob("plan-a")
			b.StartReportJob("plan-b")

			in := tt.input
			if tt.future {
				in.ByCreationAfter = aws.Time(time.Now().Add(time.Hour))
			}

			out, err := client.ListReportJobs(t.Context(), &in)
			require.NoError(t, err)
			assert.Len(t, out.ReportJobs, tt.wantN)
		})
	}
}

func TestSDK_ListRestoreJobsByProtectedResourceFilters(t *testing.T) {
	t.Parallel()

	const resource = "arn:aws:ec2:us-east-1:123456789012:volume/vol-9"

	tests := []struct {
		name  string
		input backupsdk.ListRestoreJobsByProtectedResourceInput
		wantN int
	}{
		{name: "no_filter", input: backupsdk.ListRestoreJobsByProtectedResourceInput{}, wantN: 1},
		{name: "status_match", input: backupsdk.ListRestoreJobsByProtectedResourceInput{
			ByStatus: types.RestoreJobStatusCompleted,
		}, wantN: 1},
		{name: "status_miss", input: backupsdk.ListRestoreJobsByProtectedResourceInput{
			ByStatus: types.RestoreJobStatusRunning,
		}, wantN: 0},
		{name: "rp_created_after_future", input: backupsdk.ListRestoreJobsByProtectedResourceInput{
			ByRecoveryPointCreationDateAfter: aws.Time(time.Now().Add(time.Hour)),
		}, wantN: 0},
		{name: "rp_created_before_future", input: backupsdk.ListRestoreJobsByProtectedResourceInput{
			ByRecoveryPointCreationDateBefore: aws.Time(time.Now().Add(time.Hour)),
		}, wantN: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			ctx := t.Context()

			mustVault(t, b, "rv-vault")
			mustRP(t, b, "rv-vault", "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-9", resource, "EBS")

			_, err := client.StartRestoreJob(ctx, &backupsdk.StartRestoreJobInput{
				RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:123456789012:recovery-point:rp-9"),
				IamRoleArn:       aws.String("arn:aws:iam::123456789012:role/r"),
				Metadata:         map[string]string{"k": "v"},
				IdempotencyToken: aws.String("restore-token"),
			})
			require.NoError(t, err)

			in := tt.input
			in.ResourceArn = aws.String(resource)

			out, err := client.ListRestoreJobsByProtectedResource(ctx, &in)
			require.NoError(t, err)
			assert.Len(t, out.RestoreJobs, tt.wantN)
		})
	}
}

func TestSDK_StartRestoreJobIdempotencyToken(t *testing.T) {
	t.Parallel()

	b, client := newRealClient(t)
	ctx := t.Context()

	mustVault(t, b, "rv-vault")
	mustRP(
		t, b, "rv-vault", "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-1",
		"arn:aws:ec2:::volume/v", "EBS",
	)

	in := &backupsdk.StartRestoreJobInput{
		RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:123456789012:recovery-point:rp-1"),
		IamRoleArn:       aws.String("arn:aws:iam::123456789012:role/r"),
		Metadata:         map[string]string{"k": "v"},
		IdempotencyToken: aws.String("tok"),
	}

	first, err := client.StartRestoreJob(ctx, in)
	require.NoError(t, err)
	retry, err := client.StartRestoreJob(ctx, in)
	require.NoError(t, err)

	assert.Equal(t, aws.ToString(first.RestoreJobId), aws.ToString(retry.RestoreJobId))
}

func TestSDK_FrameworkDescribeAndListMembers(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateFramework(ctx, &backupsdk.CreateFrameworkInput{
		FrameworkName:    aws.String("fw-members"),
		IdempotencyToken: aws.String("tok-d"),
		FrameworkControls: []types.FrameworkControl{
			{ControlName: aws.String("BACKUP_RECOVERY_POINT_MANUAL_DELETION_DISABLED")},
			{ControlName: aws.String("BACKUP_RECOVERY_POINT_ENCRYPTED")},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeFramework(ctx, &backupsdk.DescribeFrameworkInput{
		FrameworkName: aws.String("fw-members"),
	})
	require.NoError(t, err)
	assert.Equal(t, "tok-d", aws.ToString(desc.IdempotencyToken))

	list, err := client.ListFrameworks(ctx, &backupsdk.ListFrameworksInput{})
	require.NoError(t, err)
	require.Len(t, list.Frameworks, 1)
	assert.EqualValues(t, 2, list.Frameworks[0].NumberOfControls)
	assert.Equal(t, "COMPLETED", aws.ToString(list.Frameworks[0].DeploymentStatus))
}
