package gcpbatch_test

import (
	"testing"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tm := gcpbatch.Timing{Schedule: time.Second, Start: time.Second, TaskRun: time.Minute, Delete: time.Hour}
	src := newEnv(t, withTiming(tm))
	src.create(t, "keep", basicJob(2))
	src.create(t, "gone", basicJob(1))
	src.clock.Advance(3 * time.Second)

	_, err := src.client.DeleteJob(t.Context(), &batchpb.DeleteJobRequest{Name: parent + "/jobs/gone"})
	require.NoError(t, err)

	want := src.get(t, "keep")
	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t, withTiming(tm))
	dst.clock.Advance(3 * time.Second)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got := dst.get(t, "keep")
	assert.True(t, proto.Equal(want, got), "job survives a restore")
	assert.Equal(t, batchpb.JobStatus_DELETION_IN_PROGRESS, dst.get(t, "gone").GetStatus().GetState())
	assert.Len(t, dst.tasks(t, "keep"), 2)

	dst.clock.Advance(time.Minute)
	assert.Equal(t, int64(2), dst.get(t, "keep").GetStatus().GetTaskGroups()["group0"].GetCounts()["SUCCEEDED"])
}

func TestRestoreFailsInterruptedEngineJobs(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"hang:1": blocks()})
	src := newEnv(t, withRuntime(rt))
	src.create(t, "run", basicJob(1, containerRunnable("hang:1")))

	require.Eventually(t, func() bool {
		return src.get(t, "run").GetStatus().GetState() == batchpb.JobStatus_RUNNING
	}, time.Second*5, time.Millisecond*5)

	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))
	assert.Equal(t, batchpb.JobStatus_FAILED, dst.get(t, "run").GetStatus().GetState())
	assert.Equal(t, []string{"FAILED"}, taskStates(dst.tasks(t, "run")))
}
