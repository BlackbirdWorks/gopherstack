package emrserverless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrserverlesstypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for emrserverless's four flagged List ops,
// verified via cmd/structfielddiff against emrserverless@v1.44.4.
// ListApplications and ListSessions already matched ApplicationSummary/
// SessionSummary exactly. ListJobRuns' JobRunSummary and ListJobRunAttempts'
// JobRunAttemptSummary were both missing the optional "type" member
// (SPARK/HIVE, derivable from the job's own JobDriver); JobRunSummary was
// also missing attemptCreatedAt/attemptUpdatedAt (this backend never models
// retries, so the single synthesized attempt's timestamps are the job run's
// own -- same convention ListJobRunAttempts already used for its one
// synthesized attempt).
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("list applications exact", func(t *testing.T) {
		t.Parallel()

		backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
		client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
		ctx := t.Context()

		_, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
			Name:         aws.String("lss-app-" + uuid.NewString()[:8]),
			ReleaseLabel: aws.String("emr-6.6.0"),
			Type:         aws.String("SPARK"),
		})
		require.NoError(t, err)

		out, err := client.ListApplications(ctx, &emrserverlesssdk.ListApplicationsInput{})
		require.NoError(t, err)
		require.Len(t, out.Applications, 1)
		a := out.Applications[0]
		assert.NotEmpty(t, aws.ToString(a.Id))
		assert.NotEmpty(t, aws.ToString(a.Arn))
		assert.Equal(t, "emr-6.6.0", aws.ToString(a.ReleaseLabel))
		assert.NotNil(t, a.CreatedAt)
	})

	t.Run("list sessions exact", func(t *testing.T) {
		t.Parallel()

		backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
		client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
		ctx := t.Context()

		appOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
			Name:         aws.String("lss-app-" + uuid.NewString()[:8]),
			ReleaseLabel: aws.String("emr-6.6.0"),
			Type:         aws.String("SPARK"),
		})
		require.NoError(t, err)

		roleArn := "arn:aws:iam::" + emrRTAccountID + ":role/test-role"

		_, err = client.StartSession(ctx, &emrserverlesssdk.StartSessionInput{
			ApplicationId:    appOut.ApplicationId,
			ExecutionRoleArn: aws.String(roleArn),
			ClientToken:      aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		out, err := client.ListSessions(ctx, &emrserverlesssdk.ListSessionsInput{
			ApplicationId: appOut.ApplicationId,
		})
		require.NoError(t, err)
		require.Len(t, out.Sessions, 1)
		s := out.Sessions[0]
		assert.NotEmpty(t, aws.ToString(s.SessionId))
		assert.NotEmpty(t, aws.ToString(s.Arn))
		assert.Equal(t, roleArn, aws.ToString(s.ExecutionRoleArn))
	})

	t.Run("list job runs gains type and attempt timestamps", func(t *testing.T) {
		t.Parallel()

		backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
		client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
		ctx := t.Context()

		appOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
			Name:         aws.String("lss-app-" + uuid.NewString()[:8]),
			ReleaseLabel: aws.String("emr-6.6.0"),
			Type:         aws.String("SPARK"),
		})
		require.NoError(t, err)

		_, err = client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
			ApplicationId:    appOut.ApplicationId,
			ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
			JobDriver: &emrserverlesstypes.JobDriverMemberSparkSubmit{
				Value: emrserverlesstypes.SparkSubmit{
					EntryPoint: aws.String("s3://bucket/script.py"),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListJobRuns(ctx, &emrserverlesssdk.ListJobRunsInput{
			ApplicationId: appOut.ApplicationId,
		})
		require.NoError(t, err)
		require.Len(t, out.JobRuns, 1)
		jr := out.JobRuns[0]
		assert.Equal(t, "SPARK", aws.ToString(jr.Type))
		assert.NotNil(t, jr.AttemptCreatedAt)
		assert.NotNil(t, jr.AttemptUpdatedAt)
	})

	t.Run("list job run attempts gains type", func(t *testing.T) {
		t.Parallel()

		backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
		client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
		ctx := t.Context()

		appOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
			Name:         aws.String("lss-app-" + uuid.NewString()[:8]),
			ReleaseLabel: aws.String("emr-6.6.0"),
			Type:         aws.String("SPARK"),
		})
		require.NoError(t, err)

		jobOut, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
			ApplicationId:    appOut.ApplicationId,
			ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
			JobDriver: &emrserverlesstypes.JobDriverMemberHive{
				Value: emrserverlesstypes.Hive{
					Query: aws.String("SELECT 1"),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListJobRunAttempts(ctx, &emrserverlesssdk.ListJobRunAttemptsInput{
			ApplicationId: appOut.ApplicationId,
			JobRunId:      jobOut.JobRunId,
		})
		require.NoError(t, err)
		require.Len(t, out.JobRunAttempts, 1)
		assert.Equal(t, "HIVE", aws.ToString(out.JobRunAttempts[0].Type))
	})
}
