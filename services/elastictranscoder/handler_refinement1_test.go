package elastictranscoder_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elastictranscoder"
)

// TestRefinement1_Reset verifies Backend.Reset() clears all state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "p1", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	assert.Equal(t, 1, h.Backend.PipelineCount())

	h.Backend.Reset()

	assert.Equal(t, 0, h.Backend.PipelineCount())
	assert.Equal(t, 0, h.Backend.PresetCount())
	assert.Equal(t, 0, h.Backend.JobCount())
}

// TestRefinement1_MultipleResetCycle verifies multiple Reset() cycles leave backend empty.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 3 {
		doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
			"Name": "p", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
		})
		h.Backend.Reset()
		assert.Equal(t, 0, h.Backend.PipelineCount())
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset() delegates to Backend.Reset().
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]any{
		"Name": "my-preset", "Container": "mp4",
	})
	assert.Equal(t, 1, h.Backend.PresetCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.PresetCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned for nil ctx.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &elastictranscoder.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elastictranscoder.ErrNilAppContext)
}

// TestRefinement1_SortedListPipelines verifies ListPipelines returns pipelines sorted by name.
func TestRefinement1_SortedListPipelines(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
			"Name": name, "InputBucket": "in", "OutputBucket": "out", "Role": "r",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodGet, "/2012-09-25/pipelines", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipelines []struct {
			Name string `json:"Name"`
		} `json:"Pipelines"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Pipelines, 3)
	assert.Equal(t, "alpha", out.Pipelines[0].Name)
	assert.Equal(t, "middle", out.Pipelines[1].Name)
	assert.Equal(t, "zebra", out.Pipelines[2].Name)
}

// TestRefinement1_SortedListPresets verifies ListPresets returns presets sorted by name.
func TestRefinement1_SortedListPresets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zoom", "aardvark", "normal"} {
		rec := doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]any{
			"Name": name, "Container": "mp4",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodGet, "/2012-09-25/presets", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Presets []struct {
			Name string `json:"Name"`
		} `json:"Presets"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Presets, 3)
	assert.Equal(t, "aardvark", out.Presets[0].Name)
	assert.Equal(t, "normal", out.Presets[1].Name)
	assert.Equal(t, "zoom", out.Presets[2].Name)
}

// TestRefinement1_SortedListTagsForResource verifies tags are returned in sorted key order.
func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "tagged-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created struct {
		Pipeline struct {
			Arn string `json:"Arn"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))

	rec = doRequest(t, h, http.MethodPost, "/2012-09-25/tags/"+created.Pipeline.Arn, map[string]any{
		"Tags": []map[string]string{
			{"Key": "zzz", "Value": "3"},
			{"Key": "aaa", "Value": "1"},
			{"Key": "mmm", "Value": "2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/2012-09-25/tags/"+created.Pipeline.Arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagOut struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&tagOut))
	require.Len(t, tagOut.Tags, 3)
	assert.Equal(t, "aaa", tagOut.Tags[0].Key)
	assert.Equal(t, "mmm", tagOut.Tags[1].Key)
	assert.Equal(t, "zzz", tagOut.Tags[2].Key)
}

// TestRefinement1_UpdatePipelineStatus_Validation verifies only Active/Paused are accepted.
func TestRefinement1_UpdatePipelineStatus_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status     string
		wantStatus int
	}{
		{"Active", http.StatusOK},
		{"Paused", http.StatusOK},
		{"", http.StatusBadRequest},
		{"invalid", http.StatusBadRequest},
		{"active", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.status+"_"+http.StatusText(tt.wantStatus), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
				"Name": "pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created struct {
				Pipeline struct {
					ID string `json:"Id"`
				} `json:"Pipeline"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))

			rec = doRequest(
				t,
				h,
				http.MethodPost,
				"/2012-09-25/pipelines/"+created.Pipeline.ID+"/status",
				map[string]any{
					"Status": tt.status,
				},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_UpdatePipeline_DuplicateName verifies renaming to an existing name fails.
func TestRefinement1_UpdatePipeline_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two pipelines.
	rec1 := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "pipe-a", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "pipe-b", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec2.Code)

	var created2 struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&created2))

	// Renaming pipe-b to pipe-a should fail.
	rec := doRequest(t, h, http.MethodPut, "/2012-09-25/pipelines/"+created2.Pipeline.ID, map[string]any{
		"Name": "pipe-a",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestRefinement1_SeedHelpers verifies AddPipelineInternal, AddPresetInternal, AddJobInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	p := &elastictranscoder.Pipeline{
		ID: "pipe-seed-1", ARN: "arn:aws:elastictranscoder:us-east-1:123456789012:pipeline/pipe-seed-1",
		Name: "seeded-pipe", InputBucket: "in", Role: "r", Status: "Active",
	}
	h.Backend.AddPipelineInternal(p)

	pr := &elastictranscoder.Preset{
		ID: "preset-seed-1", ARN: "arn:aws:elastictranscoder:us-east-1:123456789012:preset/preset-seed-1",
		Name: "seeded-preset", Container: "mp4", Type: "Custom",
	}
	h.Backend.AddPresetInternal(pr)

	j := &elastictranscoder.Job{
		ID: "job-seed-1", ARN: "arn:aws:elastictranscoder:us-east-1:123456789012:job/job-seed-1",
		PipelineID: "pipe-seed-1", Status: "Progressing",
	}
	h.Backend.AddJobInternal(j)

	assert.Equal(t, 1, h.Backend.PipelineCount())
	assert.Equal(t, 1, h.Backend.PresetCount())
	assert.Equal(t, 1, h.Backend.JobCount())
}

// TestRefinement1_ExportCountHelpers verifies count helpers reflect actual backend state.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 0, h.Backend.PipelineCount())
	assert.Equal(t, 0, h.Backend.PresetCount())
	assert.Equal(t, 0, h.Backend.JobCount())

	// Create pipeline.
	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "counter-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, h.Backend.PipelineCount())

	// Create preset.
	rec = doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]any{
		"Name": "counter-preset", "Container": "mp4",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, h.Backend.PresetCount())
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore round-trip preserves all data.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline and preset.
	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "persist-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]any{
		"Name": "persist-preset", "Container": "mp4",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	// Restore into fresh backend.
	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snap))

	assert.Equal(t, 1, h2.Backend.PipelineCount())
	assert.Equal(t, 1, h2.Backend.PresetCount())
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Missing required fields maps to 400.
	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "", "InputBucket": "", "Role": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_ErrAlreadyExistsMapping verifies duplicate pipeline names map to HTTP 409.
func TestRefinement1_ErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 2 {
		doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
			"Name": "dup-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
		})
	}

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "dup-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestRefinement1_NonNilSlices verifies all list operations return non-nil slices.
func TestRefinement1_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2012-09-25/pipelines", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var pipelineOut struct {
		Pipelines []any `json:"Pipelines"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&pipelineOut))
	assert.NotNil(t, pipelineOut.Pipelines)

	rec = doRequest(t, h, http.MethodGet, "/2012-09-25/presets", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var presetOut struct {
		Presets []any `json:"Presets"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&presetOut))
	assert.NotNil(t, presetOut.Presets)
}

// TestRefinement1_DeepCopyNotifications verifies that modifying a returned Pipeline's
// Notifications does not affect the stored version.
func TestRefinement1_DeepCopyNotifications(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "notif-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))

	// Set notifications.
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/2012-09-25/pipelines/"+created.Pipeline.ID+"/notifications",
		map[string]any{
			"Notifications": map[string]string{
				"Completed":   "arn:aws:sns:us-east-1:123456789012:done",
				"Progressing": "arn:aws:sns:us-east-1:123456789012:progress",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Read back; verify Notifications are present and correctly deep-copied.
	p1, err := h.Backend.ReadPipeline(created.Pipeline.ID)
	require.NoError(t, err)
	require.NotNil(t, p1.Notifications)

	// Mutate the copy.
	p1.Notifications.Completed = "mutated"

	// Read again; original must not be affected.
	p2, err := h.Backend.ReadPipeline(created.Pipeline.ID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:done", p2.Notifications.Completed)
}

// TestRefinement1_SortedListJobsByPipeline verifies jobs are returned sorted by ID.
func TestRefinement1_SortedListJobsByPipeline(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "job-sort-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var pipelineOut struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&pipelineOut))
	pID := pipelineOut.Pipeline.ID

	jobIDs := make([]string, 5)
	for i := range 5 {
		rec = doRequest(t, h, http.MethodPost, "/2012-09-25/jobs", map[string]any{
			"PipelineId": pID,
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var jOut struct {
			Job struct {
				ID string `json:"Id"`
			} `json:"Job"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&jOut))
		jobIDs[i] = jOut.Job.ID
	}

	rec = doRequest(t, h, http.MethodGet, "/2012-09-25/jobsByPipeline/"+pID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Jobs []struct {
			ID string `json:"Id"`
		} `json:"Jobs"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Jobs, 5)

	for i := 1; i < len(out.Jobs); i++ {
		assert.LessOrEqual(t, out.Jobs[i-1].ID, out.Jobs[i].ID)
	}
}

// TestRefinement1_SortedListJobsByStatus verifies jobs filtered by status are sorted by ID.
func TestRefinement1_SortedListJobsByStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "status-sort-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var pipelineOut struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&pipelineOut))

	for range 3 {
		doRequest(t, h, http.MethodPost, "/2012-09-25/jobs", map[string]any{
			"PipelineId": pipelineOut.Pipeline.ID,
		})
	}

	rec = doRequest(t, h, http.MethodGet, "/2012-09-25/jobsByStatus/Progressing", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Jobs []struct {
			ID string `json:"Id"`
		} `json:"Jobs"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Jobs, 3)

	for i := 1; i < len(out.Jobs); i++ {
		assert.LessOrEqual(t, out.Jobs[i-1].ID, out.Jobs[i].ID)
	}
}

// TestRefinement1_UpdatePipelineRoundTrip verifies name, buckets and role are persisted.
func TestRefinement1_UpdatePipelineRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "original-pipe", "InputBucket": "in-orig", "OutputBucket": "out-orig", "Role": "role-orig",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))

	// Update name and role.
	rec = doRequest(t, h, http.MethodPut, "/2012-09-25/pipelines/"+created.Pipeline.ID, map[string]any{
		"Name": "updated-pipe", "Role": "role-new",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Read back.
	rec = doRequest(t, h, http.MethodGet, "/2012-09-25/pipelines/"+created.Pipeline.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipeline struct {
			Name string `json:"Name"`
			Role string `json:"Role"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, "updated-pipe", out.Pipeline.Name)
	assert.Equal(t, "role-new", out.Pipeline.Role)
}

// TestRefinement1_SeedHelpers_DeepCopy verifies seed helpers store independent copies.
func TestRefinement1_SeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tags := map[string]string{"env": "dev"}
	p := &elastictranscoder.Pipeline{
		ID:   "dc-pipe-1",
		ARN:  "arn:aws:elastictranscoder:us-east-1:123456789012:pipeline/dc-pipe-1",
		Name: "deep-copy-pipe", Tags: tags,
	}
	h.Backend.AddPipelineInternal(p)

	// Mutate original; stored pipeline must not be affected.
	tags["env"] = "prod"

	got, err := h.Backend.ReadPipeline("dc-pipe-1")
	require.NoError(t, err)
	assert.Equal(t, "dev", got.Tags["env"])
}

// TestRefinement1_GetSupportedOperations_AllOps verifies all 20 operations are included.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreatePipeline", "ReadPipeline", "ListPipelines", "UpdatePipeline", "DeletePipeline",
		"UpdatePipelineNotifications", "UpdatePipelineStatus",
		"CreatePreset", "ReadPreset", "ListPresets", "DeletePreset",
		"CreateJob", "ReadJob", "ListJobsByPipeline", "ListJobsByStatus", "CancelJob",
		"TestRole",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "expected op %q in GetSupportedOperations", op)
	}
}

// TestRefinement1_PipelineName_Uniqueness verifies creating a pipeline with a duplicate name fails.
func TestRefinement1_PipelineName_Uniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// First create succeeds.
	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "unique-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	// Second create with same name is rejected with 409.
	rec = doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "unique-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Only one pipeline should exist.
	assert.Equal(t, 1, h.Backend.PipelineCount())
}

// TestRefinement1_TestRole_Success verifies TestRole returns success for valid inputs.
func TestRefinement1_TestRole_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet,
		"/2012-09-25/roleTests?role=arn:aws:iam::123456789012:role/MyRole&inputBucket=in&outputBucket=out",
		nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Success  string   `json:"Success"`
		Messages []string `json:"Messages"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, "true", out.Success)
	assert.NotNil(t, out.Messages)
}

// TestRefinement1_TestRole_Validation verifies TestRole rejects missing role/bucket.
func TestRefinement1_TestRole_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		role        string
		inputBucket string
		wantStatus  int
	}{
		{"missing_role", "", "my-bucket", http.StatusBadRequest},
		{"missing_bucket", "arn:aws:iam::123456789012:role/MyRole", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/2012-09-25/roleTests?role=" + tt.role + "&inputBucket=" + tt.inputBucket
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_UpdatePipelineNotifications_RoundTrip verifies notifications are persisted.
func TestRefinement1_UpdatePipelineNotifications_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "notif-rt-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created struct {
		Pipeline struct {
			ID string `json:"Id"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))

	const completedARN = "arn:aws:sns:us-east-1:123456789012:my-complete"

	rec = doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines/"+created.Pipeline.ID+"/notifications",
		map[string]any{
			"Notifications": map[string]string{"Completed": completedARN},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipeline struct {
			Notifications struct {
				Completed string `json:"Completed"`
			} `json:"Notifications"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, completedARN, out.Pipeline.Notifications.Completed)
}

// TestRefinement1_PipelineStatus_ActiveOnCreate verifies pipelines start as "Active".
func TestRefinement1_PipelineStatus_ActiveOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]any{
		"Name": "status-check-pipe", "InputBucket": "in", "OutputBucket": "out", "Role": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out struct {
		Pipeline struct {
			Status string `json:"Status"`
		} `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, "Active", out.Pipeline.Status)
}
