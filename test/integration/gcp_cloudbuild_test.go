package integration_test

import (
	"context"
	"testing"
	"time"

	cloudbuild "cloud.google.com/go/cloudbuild/apiv1/v2"
	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const cloudBuildPort = "8136"

func newCloudBuildClient(t *testing.T) *cloudbuild.Client {
	t.Helper()

	c, err := cloudbuild.NewClient(t.Context(), gcpGRPCOpts(t, cloudBuildPort)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func isTerminalBuild(s cloudbuildpb.Build_Status) bool {
	switch s {
	case cloudbuildpb.Build_SUCCESS, cloudbuildpb.Build_FAILURE, cloudbuildpb.Build_INTERNAL_ERROR,
		cloudbuildpb.Build_TIMEOUT, cloudbuildpb.Build_CANCELLED:
		return true
	default:
		return false
	}
}

// TestIntegration_GCP_CloudBuild_BuildAndNotifications queues a build and follows it through the cloud-builds
// Pub/Sub topic. Whether the steps can run depends on a container runtime in the emulator, so only the
// terminal-status contract is asserted.
func TestIntegration_GCP_CloudBuild_BuildAndNotifications(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	proj := gcpName("it-cb")
	cb := newCloudBuildClient(t)
	ps, err := pubsub.NewClient(ctx, proj, gcpGRPCOpts(t, "8085")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ps.Close() })

	topic := "projects/" + proj + "/topics/cloud-builds"
	sub := "projects/" + proj + "/subscriptions/" + gcpName("it-cb-sub")

	_, err = ps.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic})
	require.NoError(t, err)
	_, err = ps.SubscriptionAdminClient.CreateSubscription(
		ctx,
		&pubsubpb.Subscription{Name: sub, Topic: topic},
	)
	require.NoError(t, err)

	op, err := cb.CreateBuild(ctx, &cloudbuildpb.CreateBuildRequest{
		ProjectId: proj,
		Build: &cloudbuildpb.Build{
			Steps: []*cloudbuildpb.BuildStep{
				{Name: "busybox", Entrypoint: "echo", Args: []string{"hi $_WHO"}},
			},
			Substitutions: map[string]string{"_WHO": "it"},
		},
	})
	require.NoError(t, err)

	built, err := op.Wait(ctx)
	require.NoError(t, err)
	assert.True(t, isTerminalBuild(built.GetStatus()), built.GetStatus().String())
	assert.Equal(t, []string{"hi it"}, built.GetSteps()[0].GetArgs())

	if built.GetStatus() == cloudbuildpb.Build_FAILURE {
		assert.NotEmpty(t, built.GetStatusDetail())
	}

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	seen := map[string]bool{}

	require.NoError(t, ps.Subscriber(sub).Receive(rctx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()

		seen[m.Attributes["status"]] = true

		if m.Attributes["buildId"] == built.GetId() &&
			m.Attributes["status"] == built.GetStatus().String() {
			cancel()
		}
	}))
	assert.True(t, seen["QUEUED"], "QUEUED is published first")
	assert.True(t, seen[built.GetStatus().String()])

	got, err := cb.GetBuild(ctx, &cloudbuildpb.GetBuildRequest{ProjectId: proj, Id: built.GetId()})
	require.NoError(t, err)
	assert.Equal(t, built.GetStatus(), got.GetStatus())

	_, err = cb.GetBuild(ctx, &cloudbuildpb.GetBuildRequest{ProjectId: proj, Id: "missing"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestIntegration_GCP_CloudBuild_TriggersAndRESTClient(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	proj := gcpName("it-cbt")
	parent := "projects/" + proj + "/locations/global"

	rest, err := cloudbuild.NewRESTClient(ctx,
		option.WithEndpoint("http://"+gcpAddr(t, cloudBuildPort)), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rest.Close() })

	name := gcpName("trig")
	// The REST client builds the path from project_id; parent selects the location.
	created, err := rest.CreateBuildTrigger(ctx, &cloudbuildpb.CreateBuildTriggerRequest{
		ProjectId: proj, Parent: parent,
		Trigger: &cloudbuildpb.BuildTrigger{
			Name: name, Tags: []string{"it"},
			BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: &cloudbuildpb.Build{
				Steps: []*cloudbuildpb.BuildStep{{Name: "busybox", Args: []string{"true"}}},
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, parent+"/triggers/"+created.GetId(), created.GetResourceName())

	got, err := rest.GetBuildTrigger(
		ctx,
		&cloudbuildpb.GetBuildTriggerRequest{ProjectId: proj, TriggerId: created.GetId()},
	)
	require.NoError(t, err)
	assert.Equal(t, name, got.GetName())

	_, err = rest.CreateBuildTrigger(
		ctx,
		&cloudbuildpb.CreateBuildTriggerRequest{ProjectId: proj, Parent: parent, Trigger: created},
	)
	require.ErrorContains(t, err, "409", "the REST client maps ALREADY_EXISTS to HTTP 409")

	upd, err := rest.UpdateBuildTrigger(ctx, &cloudbuildpb.UpdateBuildTriggerRequest{
		ProjectId: proj, TriggerId: created.GetId(),
		Trigger:    &cloudbuildpb.BuildTrigger{Description: "patched"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "patched", upd.GetDescription())

	run, err := rest.RunBuildTrigger(
		ctx,
		&cloudbuildpb.RunBuildTriggerRequest{ProjectId: proj, TriggerId: created.GetId()},
	)
	require.NoError(t, err)

	built, err := run.Wait(ctx)
	require.NoError(t, err)
	assert.Equal(t, created.GetId(), built.GetBuildTriggerId())
	assert.True(t, isTerminalBuild(built.GetStatus()))

	require.NoError(
		t,
		rest.DeleteBuildTrigger(
			ctx,
			&cloudbuildpb.DeleteBuildTriggerRequest{ProjectId: proj, TriggerId: created.GetId()},
		),
	)

	_, err = rest.GetBuildTrigger(
		ctx,
		&cloudbuildpb.GetBuildTriggerRequest{ProjectId: proj, TriggerId: created.GetId()},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestIntegration_GCP_CloudBuild_WorkerPools(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	cb := newCloudBuildClient(t)
	parent := "projects/" + gcpName("it-cbp") + "/locations/us-central1"

	op, err := cb.CreateWorkerPool(ctx, &cloudbuildpb.CreateWorkerPoolRequest{
		Parent: parent, WorkerPoolId: "pool1",
		WorkerPool: &cloudbuildpb.WorkerPool{
			DisplayName: "it",
			Config: &cloudbuildpb.WorkerPool_PrivatePoolV1Config{
				PrivatePoolV1Config: &cloudbuildpb.PrivatePoolV1Config{
					WorkerConfig: &cloudbuildpb.PrivatePoolV1Config_WorkerConfig{
						MachineType: "e2-standard-4",
						DiskSizeGb:  100,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	pool, err := op.Wait(ctx)
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.WorkerPool_RUNNING, pool.GetState())
	assert.Equal(
		t,
		"e2-standard-4",
		pool.GetPrivatePoolV1Config().GetWorkerConfig().GetMachineType(),
	)

	del, err := cb.DeleteWorkerPool(
		ctx,
		&cloudbuildpb.DeleteWorkerPoolRequest{Name: pool.GetName()},
	)
	require.NoError(t, err)
	require.NoError(t, del.Wait(ctx))

	_, err = cb.GetWorkerPool(ctx, &cloudbuildpb.GetWorkerPoolRequest{Name: pool.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
