package fsx_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataRepositoryTask_ReachesTerminalState proves DataRepositoryTask no
// longer hangs at EXECUTING/CANCELING forever (gopherstack ledger
// burn-down, 2026-09-18 items_still_open entry). CreateDataRepositoryTask
// leaves a task EXECUTING with no code path ever advancing it; this backend
// now settles EXECUTING -> SUCCEEDED after a short modeled duration
// (dataRepositoryTaskCompletionDelay), and CANCELING -> CANCELED on the
// sweep after Cancel is issued.
func TestDataRepositoryTask_ReachesTerminalState(t *testing.T) {
	t.Parallel()

	t.Run("executing_settles_at_succeeded", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestFSxClient(t, h)

		fsOut := createTestLustreFS(t, client)

		created, err := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
			FileSystemId: fsOut.FileSystem.FileSystemId,
			Type:         types.DataRepositoryTaskTypeExport,
			Report:       &types.CompletionReport{Enabled: aws.Bool(false)},
		})
		require.NoError(t, err)
		require.Equal(t, types.DataRepositoryTaskLifecycleExecuting, created.DataRepositoryTask.Lifecycle)

		taskID := created.DataRepositoryTask.TaskId

		immediate, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
			TaskIds: []string{aws.ToString(taskID)},
		})
		require.NoError(t, err)
		require.Len(t, immediate.DataRepositoryTasks, 1)
		assert.Equal(t, types.DataRepositoryTaskLifecycleExecuting, immediate.DataRepositoryTasks[0].Lifecycle)
		assert.Nil(t, immediate.DataRepositoryTasks[0].EndTime)

		// require.Eventually, not synctest, deliberately: this drives a real
		// httptest.NewServer through the typed SDK client (needed to prove
		// the wire shape, not just an internal struct), and wrapping that in
		// synctest.Test deadlocks -- the server's accept/read/write
		// goroutines join the bubble but block on real network I/O, which
		// synctest never counts as "durably blocked", so its fake clock
		// never advances (same finding services/sagemaker recorded as
		// gopherstack-k3ae).
		var final *types.DataRepositoryTask
		require.Eventually(t, func() bool {
			out, describeErr := client.DescribeDataRepositoryTasks(
				t.Context(),
				&fsxsdk.DescribeDataRepositoryTasksInput{TaskIds: []string{aws.ToString(taskID)}},
			)
			require.NoError(t, describeErr)
			require.Len(t, out.DataRepositoryTasks, 1)

			if out.DataRepositoryTasks[0].Lifecycle != types.DataRepositoryTaskLifecycleSucceeded {
				return false
			}

			final = &out.DataRepositoryTasks[0]

			return true
		}, 5*time.Second, 50*time.Millisecond)

		require.NotNil(t, final)
		require.NotNil(t, final.EndTime)
		require.NotNil(t, final.Status)
		assert.Equal(t, int64(1), aws.ToInt64(final.Status.TotalCount))
		assert.Equal(t, int64(1), aws.ToInt64(final.Status.SucceededCount))
		assert.Equal(t, int64(0), aws.ToInt64(final.Status.FailedCount))
		assert.NotNil(t, final.Status.LastUpdatedTime)
	})

	t.Run("canceling_settles_at_canceled", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestFSxClient(t, h)

		fsOut := createTestLustreFS(t, client)

		created, err := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
			FileSystemId: fsOut.FileSystem.FileSystemId,
			Type:         types.DataRepositoryTaskTypeExport,
			Report:       &types.CompletionReport{Enabled: aws.Bool(false)},
			Paths:        []string{"/a", "/b"},
		})
		require.NoError(t, err)
		taskID := created.DataRepositoryTask.TaskId

		cancelOut, err := client.CancelDataRepositoryTask(t.Context(), &fsxsdk.CancelDataRepositoryTaskInput{
			TaskId: taskID,
		})
		require.NoError(t, err)
		assert.Equal(t, types.DataRepositoryTaskLifecycleCanceling, cancelOut.Lifecycle)

		out, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
			TaskIds: []string{aws.ToString(taskID)},
		})
		require.NoError(t, err)
		require.Len(t, out.DataRepositoryTasks, 1)

		got := out.DataRepositoryTasks[0]
		assert.Equal(t, types.DataRepositoryTaskLifecycleCanceled, got.Lifecycle)
		require.NotNil(t, got.EndTime)
		require.NotNil(t, got.Status)
		assert.Equal(t, int64(2), aws.ToInt64(got.Status.TotalCount))
		assert.Equal(t, int64(0), aws.ToInt64(got.Status.SucceededCount))
		assert.Equal(t, int64(0), aws.ToInt64(got.Status.FailedCount))
	})
}
