package emrserverless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// newStartedJobRun creates an application and starts a job run on it,
// returning their real-client-issued IDs.
func newStartedJobRun(t *testing.T, client *emrserverlesssdk.Client) (string, string) {
	t.Helper()
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		ClientToken:  aws.String(t.Name() + "-create-app"),
		Name:         aws.String(t.Name() + "-app"),
		ReleaseLabel: aws.String("emr-6.9.0"),
		Type:         aws.String("SPARK"),
	})
	require.NoError(t, err)

	started, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    app.ApplicationId,
		ClientToken:      aws.String(t.Name() + "-start-job-run"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
	})
	require.NoError(t, err)

	return aws.ToString(app.ApplicationId), aws.ToString(started.JobRunId)
}

// TestGetJobRunAttemptParameter proves GetJobRunInput.Attempt (dropped from
// the wire entirely before this fix) is read and validated: attempt 0 (the
// only attempt this backend ever produces, see ListJobRunAttempts) succeeds,
// any other attempt number fails since it was never created.
func TestGetJobRunAttemptParameter(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	applicationID, jobRunID := newStartedJobRun(t, client)

	_, err := client.GetJobRun(ctx, &emrserverlesssdk.GetJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
		Attempt:       aws.Int32(0),
	})
	require.NoError(t, err, "attempt 0 is this job run's own (only) attempt")

	_, err = client.GetJobRun(ctx, &emrserverlesssdk.GetJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
		Attempt:       aws.Int32(2),
	})
	require.Error(t, err, "attempt 2 was never created by this single-attempt backend")
}

// TestGetDashboardForJobRunParameters proves GetDashboardForJobRunInput's
// Attempt and AccessSystemProfileLogs (both dropped from the wire entirely
// before this fix) affect behavior: Attempt is validated identically to
// GetJobRun's, and AccessSystemProfileLogs is echoed into the synthesized
// dashboard URL.
func TestGetDashboardForJobRunParameters(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	applicationID, jobRunID := newStartedJobRun(t, client)

	withoutLogs, err := client.GetDashboardForJobRun(ctx, &emrserverlesssdk.GetDashboardForJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
	})
	require.NoError(t, err)
	assert.NotContains(t, aws.ToString(withoutLogs.Url), "accessSystemProfileLogs")

	withLogs, err := client.GetDashboardForJobRun(ctx, &emrserverlesssdk.GetDashboardForJobRunInput{
		ApplicationId:           aws.String(applicationID),
		JobRunId:                aws.String(jobRunID),
		AccessSystemProfileLogs: aws.Bool(true),
		Attempt:                 aws.Int32(0),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(withLogs.Url), "accessSystemProfileLogs=true")

	_, err = client.GetDashboardForJobRun(ctx, &emrserverlesssdk.GetDashboardForJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
		Attempt:       aws.Int32(5),
	})
	require.Error(t, err, "attempt 5 was never created by this single-attempt backend")
}
