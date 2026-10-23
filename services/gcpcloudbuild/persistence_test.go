package gcpcloudbuild_test

import (
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0), "slow": blocks(nil)})
	regional := "projects/" + project + "/locations/us-central1"

	done := e.run(t, buildOf(step("img")))

	trig, err := e.client.CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
		Parent: parent,
		Trigger: &cloudbuildpb.BuildTrigger{
			Name: "t", BuildTemplate: &cloudbuildpb.BuildTrigger_Filename{Filename: "cloudbuild.yaml"},
		},
	})
	require.NoError(t, err)

	pop, err := e.client.CreateWorkerPool(t.Context(), &cloudbuildpb.CreateWorkerPoolRequest{
		Parent: regional, WorkerPoolId: "p1", WorkerPool: &cloudbuildpb.WorkerPool{DisplayName: "d"},
	})
	require.NoError(t, err)

	_, err = pop.Wait(t.Context())
	require.NoError(t, err)

	running, err := e.client.CreateBuild(
		t.Context(),
		&cloudbuildpb.CreateBuildRequest{Parent: parent, Build: buildOf(step("slow"))},
	)
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// A fresh backend has no runtime, so restored work can only be inspected, never resumed.
	fresh := gcpcloudbuild.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetBuild(&cloudbuildpb.GetBuildRequest{Name: done.GetName()})
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus())

	meta, err := running.Metadata()
	require.NoError(t, err)

	interrupted, err := fresh.GetBuild(
		&cloudbuildpb.GetBuildRequest{Name: meta.GetBuild().GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.Build_INTERNAL_ERROR, interrupted.GetStatus())
	assert.Contains(t, interrupted.GetStatusDetail(), "restart")

	gotTrig, err := fresh.GetBuildTrigger(
		&cloudbuildpb.GetBuildTriggerRequest{Name: trig.GetResourceName()},
	)
	require.NoError(t, err)
	assert.Equal(t, "t", gotTrig.GetName())

	pool, err := fresh.GetWorkerPool(regional + "/workerPools/p1")
	require.NoError(t, err)
	assert.Equal(t, "d", pool.GetDisplayName())

	require.NoError(t, fresh.Restore(t.Context(), snap), "restore is repeatable")

	retried, err := fresh.RetryBuild(&cloudbuildpb.RetryBuildRequest{Name: done.GetName()})
	require.NoError(t, err)
	assert.NotEmpty(t, retried.GetName(), "the original request survives a restore")
	fresh.Wait()

	e.backend.Close()
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	b := gcpcloudbuild.NewInMemoryBackend()
	require.Error(t, b.Restore(t.Context(), []byte("not json")))
}
