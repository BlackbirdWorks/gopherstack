package emrserverless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrserverlesstypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// newTestEMRServerlessSDKClient stands up the real aws-sdk-go-v2
// emrserverless client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.

// TestGetJobRun_ReleaseLabelSurvivesEmptyApplicationReleaseLabel proves the
// required JobRun.ReleaseLabel member (emrserverless@v1.44.4
// types/types.go, "This member is required.") survives a real SDK client
// round trip when the owning application was created with an explicit empty
// ReleaseLabel -- a state CreateApplicationInput's own client-side
// validator (validateOpCreateApplicationInput) permits, since it only
// null-checks the *string pointer, never its content.
//
// Before the fix, jobRunToMap/jobRunSummaryToMap only set "releaseLabel"
// when jr.ReleaseLabel != "", so an application created this way produced a
// GetJobRun/ListJobRuns response missing the required key entirely, and a
// real client's typed ReleaseLabel field decoded to nil -- gopherstack-r80d
// batch 20.
func TestGetJobRun_ReleaseLabelSurvivesEmptyApplicationReleaseLabel(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		ClientToken: aws.String("create-app-empty-release-label"),
		Name:        aws.String("empty-release-label-app"),
		// Explicit empty string: passes client-side validation (only the
		// pointer is null-checked), reaching the backend.
		ReleaseLabel: aws.String(""),
		Type:         aws.String("SPARK"),
	})
	require.NoError(t, err)

	started, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    app.ApplicationId,
		ClientToken:      aws.String("start-job-run-empty-release-label"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
	})
	require.NoError(t, err)

	got, err := client.GetJobRun(ctx, &emrserverlesssdk.GetJobRunInput{
		ApplicationId: app.ApplicationId,
		JobRunId:      started.JobRunId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.JobRun.ReleaseLabel, "ReleaseLabel is required by the real SDK")
	require.Empty(t, *got.JobRun.ReleaseLabel)

	listed, err := client.ListJobRuns(ctx, &emrserverlesssdk.ListJobRunsInput{
		ApplicationId: app.ApplicationId,
	})
	require.NoError(t, err)
	require.Len(t, listed.JobRuns, 1)
	require.NotNil(t, listed.JobRuns[0].ReleaseLabel, "ReleaseLabel is required by the real SDK")
	require.Empty(t, *listed.JobRuns[0].ReleaseLabel)
}

// TestListJobRunAttempts_ReleaseLabelAndStateDetailsMirrorJobRun proves the
// required JobRunAttemptSummary.ReleaseLabel and .StateDetails members
// (both "This member is required.") survive a real SDK client round trip,
// carrying the job run's own already-tracked values rather than a
// hardcoded empty string.
//
// Before the fix, ListJobRunAttempts' single synthesised attempt always set
// ReleaseLabel/StateDetails to "" regardless of what the backing JobRun
// actually held (the comment claimed neither was "tracked by the backend",
// which was false -- JobRun.ReleaseLabel and JobRun.StateDetails are both
// already stored). The wire key was present either way (an empty string,
// not an omitted key), so this is a data-fidelity bug rather than a
// dropped-key one, but it means real client-observable data was discarded
// for no reason -- gopherstack-r80d batch 20.
func TestListJobRunAttempts_ReleaseLabelAndStateDetailsMirrorJobRun(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		ClientToken:  aws.String("create-app-attempts"),
		Name:         aws.String("attempts-app"),
		ReleaseLabel: aws.String("emr-6.6.0"),
		Type:         aws.String("SPARK"),
	})
	require.NoError(t, err)

	started, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    app.ApplicationId,
		ClientToken:      aws.String("start-job-run-attempts"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
	})
	require.NoError(t, err)

	_, err = client.CancelJobRun(ctx, &emrserverlesssdk.CancelJobRunInput{
		ApplicationId: app.ApplicationId,
		JobRunId:      started.JobRunId,
	})
	require.NoError(t, err)

	attempts, err := client.ListJobRunAttempts(ctx, &emrserverlesssdk.ListJobRunAttemptsInput{
		ApplicationId: app.ApplicationId,
		JobRunId:      started.JobRunId,
	})
	require.NoError(t, err)
	require.Len(t, attempts.JobRunAttempts, 1)

	attempt := attempts.JobRunAttempts[0]
	require.NotNil(t, attempt.ReleaseLabel, "ReleaseLabel is required by the real SDK")
	require.Equal(t, "emr-6.6.0", *attempt.ReleaseLabel)
	require.NotNil(t, attempt.StateDetails, "StateDetails is required by the real SDK")
	require.Equal(t, "Job run cancelled by user request", *attempt.StateDetails)
}

// TestGetJobRun_JobDriverKeyAlwaysPresent documents (fixed but NOT counted
// in the campaign's bug tally) that JobRun.JobDriver -- required per
// emrserverless@v1.44.4 types/types.go -- now always occupies the
// "jobDriver" wire key, even when StartJobRun was called without one (a
// state validateOpStartJobRunInput permits: JobDriver is only validated
// when non-nil). This cannot be proven to change any real client's
// observed decode: awsRestjson1_deserializeDocumentJobDriver's switch over
// the "jobDriver" object's keys assigns nothing when the object has none,
// and the deserializer's outer switch skips the field entirely when the
// key is absent -- both paths leave the typed JobDriver field nil. The
// test asserts the (identical, unprovable) real-client-visible outcome
// under both configurations, documenting the shape rather than a
// regression a client could ever observe.
func TestGetJobRun_JobDriverKeyAlwaysPresent(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		ClientToken:  aws.String("create-app-jobdriver"),
		Name:         aws.String("jobdriver-app"),
		ReleaseLabel: aws.String("emr-6.6.0"),
		Type:         aws.String("SPARK"),
	})
	require.NoError(t, err)

	started, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    app.ApplicationId,
		ClientToken:      aws.String("start-job-run-no-driver"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
	})
	require.NoError(t, err)

	got, err := client.GetJobRun(ctx, &emrserverlesssdk.GetJobRunInput{
		ApplicationId: app.ApplicationId,
		JobRunId:      started.JobRunId,
	})
	require.NoError(t, err)
	require.Nil(t, got.JobRun.JobDriver)

	startedWithDriver, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    app.ApplicationId,
		ClientToken:      aws.String("start-job-run-with-driver"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
		JobDriver: &emrserverlesstypes.JobDriverMemberSparkSubmit{
			Value: emrserverlesstypes.SparkSubmit{EntryPoint: aws.String("s3://bucket/job.py")},
		},
	})
	require.NoError(t, err)

	gotWithDriver, err := client.GetJobRun(ctx, &emrserverlesssdk.GetJobRunInput{
		ApplicationId: app.ApplicationId,
		JobRunId:      startedWithDriver.JobRunId,
	})
	require.NoError(t, err)
	require.NotNil(t, gotWithDriver.JobRun.JobDriver)
}
