package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// newTestBackendForML creates a backend for direct ML task run testing.
func newTestBackendForML(t *testing.T) *glue.InMemoryBackend {
	t.Helper()

	return glue.NewInMemoryBackend(testAccountID, testRegion)
}

// createMLTransformDirect creates an ML transform in the backend and returns its ID.
func createMLTransformDirect(t *testing.T, b *glue.InMemoryBackend, name string) string {
	t.Helper()

	m, err := b.CreateMLTransform(name, "desc", "role-arn", nil, glue.MLTransformParameter{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, m.TransformID)

	return m.TransformID
}

// TestBackend_StartMLEvaluationTaskRun exercises the backend ML evaluation run.
func TestBackend_StartMLEvaluationTaskRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transformID string
		wantErr     bool
	}{
		{
			name:        "unknown_transform_returns_error",
			transformID: "no-such-transform",
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackendForML(t)
			_, err := b.StartMLEvaluationTaskRun(tc.transformID)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("known_transform_returns_run", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "eval-transform")

		run, err := b.StartMLEvaluationTaskRun(transformID)
		require.NoError(t, err)
		assert.NotEmpty(t, run.TaskRunID)
		assert.Equal(t, transformID, run.TransformID)
		assert.Equal(t, "EVALUATION", run.TaskType)
		assert.Equal(t, "RUNNING", run.Status)
	})
}

// TestBackend_StartMLLabelingSetGenerationTaskRun exercises labeling set generation.
func TestBackend_StartMLLabelingSetGenerationTaskRun(t *testing.T) {
	t.Parallel()

	t.Run("creates_labeling_run", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "label-transform")

		run, err := b.StartMLLabelingSetGenerationTaskRun(transformID)
		require.NoError(t, err)
		assert.NotEmpty(t, run.TaskRunID)
		assert.Equal(t, "LABELING_SET_GENERATION", run.TaskType)
	})
}

// TestBackend_StartExportImportLabels exercises export/import label runs.
func TestBackend_StartExportImportLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fn           func(b *glue.InMemoryBackend, id, path string) (*glue.MLTaskRun, error)
		wantTaskType string
	}{
		{
			name: "export_labels",
			fn: func(b *glue.InMemoryBackend, id, path string) (*glue.MLTaskRun, error) {
				return b.StartExportLabelsTaskRun(id, path)
			},
			wantTaskType: "EXPORT_LABELS",
		},
		{
			name: "import_labels",
			fn: func(b *glue.InMemoryBackend, id, path string) (*glue.MLTaskRun, error) {
				return b.StartImportLabelsTaskRun(id, path)
			},
			wantTaskType: "IMPORT_LABELS",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackendForML(t)
			transformID := createMLTransformDirect(t, b, tc.name+"-transform")

			run, err := tc.fn(b, transformID, "s3://bucket/path/")
			require.NoError(t, err)
			assert.NotEmpty(t, run.TaskRunID)
			assert.Equal(t, tc.wantTaskType, run.TaskType)
			assert.Equal(t, "RUNNING", run.Status)
		})
	}
}

// TestBackend_GetMLTaskRun exercises backend task run retrieval.
func TestBackend_GetMLTaskRun(t *testing.T) {
	t.Parallel()

	t.Run("not_found_returns_error", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		_, err := b.GetMLTaskRun("t-missing", "r-missing")
		assert.Error(t, err)
	})

	t.Run("found_returns_run", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "get-run-transform")

		started, err := b.StartMLEvaluationTaskRun(transformID)
		require.NoError(t, err)

		got, err := b.GetMLTaskRun(transformID, started.TaskRunID)
		require.NoError(t, err)
		assert.Equal(t, started.TaskRunID, got.TaskRunID)
		assert.Equal(t, transformID, got.TransformID)
	})
}

// TestBackend_GetMLTaskRuns exercises listing task runs per transform.
func TestBackend_GetMLTaskRuns(t *testing.T) {
	t.Parallel()

	t.Run("unknown_transform_returns_error", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		_, err := b.GetMLTaskRuns("t-missing")
		assert.Error(t, err)
	})

	t.Run("empty_runs_for_new_transform", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "empty-transform")

		runs, err := b.GetMLTaskRuns(transformID)
		require.NoError(t, err)
		assert.Empty(t, runs)
	})

	t.Run("multiple_runs_all_returned", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "multi-transform")

		for range 4 {
			_, err := b.StartMLEvaluationTaskRun(transformID)
			require.NoError(t, err)
		}

		runs, err := b.GetMLTaskRuns(transformID)
		require.NoError(t, err)
		assert.Len(t, runs, 4)
	})

	t.Run("isolation_between_transforms", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		id1 := createMLTransformDirect(t, b, "transform-a")
		id2 := createMLTransformDirect(t, b, "transform-b")

		for range 2 {
			_, err := b.StartMLEvaluationTaskRun(id1)
			require.NoError(t, err)
		}
		_, err := b.StartMLEvaluationTaskRun(id2)
		require.NoError(t, err)

		runs1, err := b.GetMLTaskRuns(id1)
		require.NoError(t, err)
		assert.Len(t, runs1, 2)

		runs2, err := b.GetMLTaskRuns(id2)
		require.NoError(t, err)
		assert.Len(t, runs2, 1)
	})
}

// TestBackend_CancelMLTaskRun exercises cancelling a task run.
func TestBackend_CancelMLTaskRun(t *testing.T) {
	t.Parallel()

	t.Run("cancel_not_found_returns_error", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		err := b.CancelMLTaskRun("t-missing", "r-missing")
		assert.Error(t, err)
	})

	t.Run("cancel_transitions_to_stopped", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		transformID := createMLTransformDirect(t, b, "cancel-transform")

		started, err := b.StartMLEvaluationTaskRun(transformID)
		require.NoError(t, err)

		err = b.CancelMLTaskRun(transformID, started.TaskRunID)
		require.NoError(t, err)

		got, err := b.GetMLTaskRun(transformID, started.TaskRunID)
		require.NoError(t, err)
		assert.Equal(t, "STOPPED", got.Status)
		assert.Greater(t, got.CompletedOn, float64(0))
	})
}

// TestBackend_ListDataQualityEvaluationRuns exercises listing evaluation runs.
func TestBackend_ListDataQualityEvaluationRuns(t *testing.T) {
	t.Parallel()

	t.Run("empty_returns_empty", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		runs := b.ListDataQualityEvaluationRuns()
		assert.Empty(t, runs)
	})

	t.Run("seeded_runs_returned", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		b.AddDataQualityEvalRunInternal(&glue.DataQualityEvaluationRun{RunID: "run-1", Status: "SUCCEEDED"})
		b.AddDataQualityEvalRunInternal(&glue.DataQualityEvaluationRun{RunID: "run-2", Status: "RUNNING"})

		runs := b.ListDataQualityEvaluationRuns()
		assert.Len(t, runs, 2)
	})
}

// TestBackend_ListDataQualityResults exercises listing data quality results.
func TestBackend_ListDataQualityResults(t *testing.T) {
	t.Parallel()

	t.Run("empty_returns_empty", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		results := b.ListDataQualityResults()
		assert.Empty(t, results)
	})

	t.Run("seeded_results_returned_sorted", func(t *testing.T) {
		t.Parallel()

		b := newTestBackendForML(t)
		b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "res-z", Score: 0.5})
		b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "res-a", Score: 0.9})

		results := b.ListDataQualityResults()
		require.Len(t, results, 2)
		assert.Equal(t, "res-a", results[0].ResultID)
		assert.Equal(t, "res-z", results[1].ResultID)
	})
}

// TestBackend_Reset_ClearsMLTaskRuns verifies Reset removes ML task runs.
func TestBackend_Reset_ClearsMLTaskRuns(t *testing.T) {
	t.Parallel()

	b := newTestBackendForML(t)
	transformID := createMLTransformDirect(t, b, "reset-transform")

	_, err := b.StartMLEvaluationTaskRun(transformID)
	require.NoError(t, err)

	assert.Equal(t, 1, glue.MLTaskRunCount(b))

	b.Reset()

	assert.Equal(t, 0, glue.MLTaskRunCount(b))

	_, err = b.GetMLTaskRun(transformID, "any-run-id")
	assert.Error(t, err)
}

// TestBackend_MLTaskRunCount verifies MLTaskRunCount returns total runs across transforms.
func TestBackend_MLTaskRunCount(t *testing.T) {
	t.Parallel()

	b := newTestBackendForML(t)
	assert.Equal(t, 0, glue.MLTaskRunCount(b))

	id1 := createMLTransformDirect(t, b, "count-transform-1")
	id2 := createMLTransformDirect(t, b, "count-transform-2")

	_, err := b.StartMLEvaluationTaskRun(id1)
	require.NoError(t, err)
	assert.Equal(t, 1, glue.MLTaskRunCount(b))

	_, err = b.StartMLLabelingSetGenerationTaskRun(id1)
	require.NoError(t, err)
	assert.Equal(t, 2, glue.MLTaskRunCount(b))

	_, err = b.StartMLEvaluationTaskRun(id2)
	require.NoError(t, err)
	assert.Equal(t, 3, glue.MLTaskRunCount(b))
}
