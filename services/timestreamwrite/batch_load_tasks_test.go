package timestreamwrite_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestBatchLoadStatusConstants verifies exported status constants.
func TestBatchLoadStatusConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "CREATED", timestreamwrite.BatchLoadStatusCreated)
	assert.Equal(t, "IN_PROGRESS", timestreamwrite.BatchLoadStatusInProgress)
	assert.Equal(t, "FAILED", timestreamwrite.BatchLoadStatusFailed)
	assert.Equal(t, "SUCCEEDED", timestreamwrite.BatchLoadStatusSucceeded)
	assert.Equal(t, "PROGRESS_STOPPED", timestreamwrite.BatchLoadStatusProgressStopped)
	assert.Equal(t, "PENDING_RESUME", timestreamwrite.BatchLoadStatusPendingResume)
}

// TestInMemoryBackend_BatchLoadTaskCountExport verifies BatchLoadTaskCount export.
func TestInMemoryBackend_BatchLoadTaskCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "task-001",
		TargetDatabaseName: "db1",
		TargetTableName:    "tbl1",
		TaskStatus:         timestreamwrite.BatchLoadStatusCreated,
		CreationTime:       time.Now(),
		LastUpdatedTime:    time.Now(),
	})
	assert.Equal(t, 1, timestreamwrite.BatchLoadTaskCount(b))
}

// TestInMemoryBackend_AddBatchLoadTaskInternal seeds a batch load task directly.
func TestInMemoryBackend_AddBatchLoadTaskInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "seeded-task",
		TargetDatabaseName: "db",
		TargetTableName:    "tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusFailed,
		CreationTime:       now,
		LastUpdatedTime:    now,
	})

	task, err := b.DescribeBatchLoadTask("seeded-task")
	require.NoError(t, err)
	assert.Equal(t, timestreamwrite.BatchLoadStatusFailed, task.TaskStatus)
}

// TestInMemoryBackend_ResumeBatchLoadTask_FromFailed verifies resume from FAILED.
func TestInMemoryBackend_ResumeBatchLoadTask_FromFailed(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "rbt-db", ARN: "arn:xxx", CreationTime: now, LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "rbt-db", TableName: "rbt-tbl", ARN: "arn:xxx2",
		TableStatus: "ACTIVE", CreationTime: now, LastUpdatedTime: now,
	})

	task, err := b.CreateBatchLoadTask("rbt-db", "rbt-tbl", nil, nil)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task.TaskID, timestreamwrite.BatchLoadStatusFailed)
	require.NoError(t, err)

	err = b.ResumeBatchLoadTask(task.TaskID)
	require.NoError(t, err)

	resumed, err := b.DescribeBatchLoadTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, timestreamwrite.BatchLoadStatusCreated, resumed.TaskStatus)
}

// TestInMemoryBackend_ResumeBatchLoadTask_InvalidState verifies error when task
// is not resumable.
func TestInMemoryBackend_ResumeBatchLoadTask_InvalidState(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:     "in-progress-task",
		TaskStatus: timestreamwrite.BatchLoadStatusInProgress,
	})

	err := b.ResumeBatchLoadTask("in-progress-task")
	require.Error(t, err)
	require.ErrorIs(t, err, timestreamwrite.ErrInvalidBatchLoadStatus)
}

// TestInMemoryBackend_ResumeBatchLoadTask_ResumableStates verifies that only
// FAILED and PROGRESS_STOPPED tasks are resumable; PENDING_RESUME is NOT
// resumable (it is an internal intermediate state).
func TestInMemoryBackend_ResumeBatchLoadTask_ResumableStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "FAILED_is_resumable", status: timestreamwrite.BatchLoadStatusFailed, wantErr: false},
		{name: "PROGRESS_STOPPED_is_resumable", status: timestreamwrite.BatchLoadStatusProgressStopped, wantErr: false},
		{name: "PENDING_RESUME_is_not_resumable", status: timestreamwrite.BatchLoadStatusPendingResume, wantErr: true},
		{name: "IN_PROGRESS_is_not_resumable", status: timestreamwrite.BatchLoadStatusInProgress, wantErr: true},
		{name: "SUCCEEDED_is_not_resumable", status: timestreamwrite.BatchLoadStatusSucceeded, wantErr: true},
		{name: "CREATED_is_not_resumable", status: timestreamwrite.BatchLoadStatusCreated, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
				TaskID:             "resume-test-task",
				TargetDatabaseName: "any-db",
				TargetTableName:    "any-tbl",
				TaskStatus:         tt.status,
			})

			err := b.ResumeBatchLoadTask("resume-test-task")
			if tt.wantErr {
				require.ErrorIs(t, err, timestreamwrite.ErrInvalidBatchLoadStatus)
			} else {
				require.NoError(t, err)
				resumed, descErr := b.DescribeBatchLoadTask("resume-test-task")
				require.NoError(t, descErr)
				assert.Equal(t, timestreamwrite.BatchLoadStatusCreated, resumed.TaskStatus)
			}
		})
	}
}

// TestInMemoryBackend_ListBatchLoadTasks_SortedByCreationTime verifies that
// tasks are returned in creation-time order (oldest first).
func TestInMemoryBackend_ListBatchLoadTasks_SortedByCreationTime(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(
		&timestreamwrite.Database{
			DatabaseName: "sort-db",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/sort-db",
		},
	)
	b.AddTableInternal(
		&timestreamwrite.Table{
			DatabaseName: "sort-db",
			TableName:    "sort-tbl",
			TableStatus:  "ACTIVE",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/sort-db/table/sort-tbl",
		},
	)

	now := time.Now()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "z-task", TargetDatabaseName: "sort-db", TargetTableName: "sort-tbl",
		TaskStatus: "CREATED", CreationTime: now.Add(1 * time.Second),
	})
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "a-task", TargetDatabaseName: "sort-db", TargetTableName: "sort-tbl",
		TaskStatus: "CREATED", CreationTime: now,
	})

	tasks := b.ListBatchLoadTasks("")
	require.Len(t, tasks, 2)
	// Older task (a-task) should come first.
	assert.Equal(t, "a-task", tasks[0].TaskID)
	assert.Equal(t, "z-task", tasks[1].TaskID)
}

// TestInMemoryBackend_BatchLoadProgressReport verifies BatchLoadProgressReport
// is stored and returned via DescribeBatchLoadTask.
func TestInMemoryBackend_BatchLoadProgressReport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	pr := &timestreamwrite.BatchLoadProgressReport{
		RecordsProcessed: 500,
		RecordsIngested:  480,
	}
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "prb-task",
		TargetDatabaseName: "pr-db",
		TargetTableName:    "pr-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusInProgress,
		CreationTime:       time.Now(),
		LastUpdatedTime:    time.Now(),
		ProgressReport:     pr,
	})

	task, err := b.DescribeBatchLoadTask("prb-task")
	require.NoError(t, err)
	require.NotNil(t, task.ProgressReport)
	assert.Equal(t, int64(500), task.ProgressReport.RecordsProcessed)
	assert.Equal(t, int64(480), task.ProgressReport.RecordsIngested)
}
