package integration_test

import (
	"testing"

	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newSchedulerClient(t *testing.T) *scheduler.CloudSchedulerClient {
	t.Helper()

	client, err := scheduler.NewCloudSchedulerClient(t.Context(), gcpGRPCOpts(t, "8124")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_Scheduler_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newSchedulerClient(t)
	ctx := t.Context()
	location := "projects/" + gcpProject + "/locations/us-central1"

	job, err := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{
		Parent: location,
		Job: &schedulerpb.Job{
			Name:     location + "/jobs/" + gcpName("it-job"),
			Schedule: "0 9 * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
				Uri: "http://127.0.0.1:9/tick", HttpMethod: schedulerpb.HttpMethod_POST,
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, schedulerpb.Job_ENABLED, job.GetState())

	upd, err := client.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
		Job:        &schedulerpb.Job{Name: job.GetName(), Description: "nightly"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "nightly", upd.GetDescription())

	paused, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: job.GetName()})
	require.NoError(t, err)
	assert.Equal(t, schedulerpb.Job_PAUSED, paused.GetState())

	resumed, err := client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: job.GetName()})
	require.NoError(t, err)
	assert.Equal(t, schedulerpb.Job_ENABLED, resumed.GetState())

	ran, err := client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)
	assert.Equal(t, job.GetName(), ran.GetName())

	require.NoError(t, client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: job.GetName()}))
}

func TestIntegration_GCP_Scheduler_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newSchedulerClient(t)

	_, err := client.GetJob(t.Context(), &schedulerpb.GetJobRequest{
		Name: "projects/" + gcpProject + "/locations/us-central1/jobs/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
