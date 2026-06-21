package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestRefinement1_ProviderInit_NilCtx ensures Init returns ErrNilAppContext on nil context.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mediaconvert.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, mediaconvert.ErrNilAppContext)
}

// TestRefinement1_BackendReset ensures Reset clears all state.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateQueue("q1", "", "", "", nil)
	require.NoError(t, err)
	require.Equal(t, 1, mediaconvert.QueueCount(b))

	b.Reset()

	require.Equal(t, 0, mediaconvert.QueueCount(b))
	require.Equal(t, 0, mediaconvert.JobCount(b))
	require.Equal(t, 0, mediaconvert.JobTemplateCount(b))
	require.Equal(t, 0, mediaconvert.PresetCount(b))
	require.Equal(t, 0, mediaconvert.CertificateCount(b))
}

// TestRefinement1_HandlerReset ensures Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)
	_, err := b.CreatePreset("p1", "", "", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, mediaconvert.PresetCount(b))

	h.Reset()

	require.Equal(t, 0, mediaconvert.PresetCount(b))
}

// TestRefinement1_AccountID ensures AccountID returns the configured value.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend("111122223333", "eu-west-1")
	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_HandlerOpsLen ensures a minimum set of operations is registered.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.GreaterOrEqual(t, mediaconvert.HandlerOpsLen(h), 28)
}

// TestRefinement1_ErrValidationMappedTo400 verifies ErrValidation → 400.
func TestRefinement1_ErrValidationMappedTo400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "create_queue_invalid_status",
			method: http.MethodPut,
			path:   "/2017-08-29/queues/test-q-for-valid",
			body:   map[string]any{"status": "INVALID_STATUS"},
		},
		{
			name:   "cancel_job_already_canceled",
			method: http.MethodDelete,
			path:   "/2017-08-29/jobs/placeholder", // set up below
			body:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "cancel_job_already_canceled" {
				// Create and cancel a job first, then attempt double cancel.
				rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs",
					map[string]any{"role": "arn:aws:iam::123:role/role"})
				require.Equal(t, http.StatusCreated, rec.Code)

				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				job := resp["job"].(map[string]any)
				id := job["id"].(string)

				// First cancel succeeds.
				rec2 := doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+id, nil)
				require.Equal(t, http.StatusNoContent, rec2.Code)

				// Second cancel should fail with 400.
				rec3 := doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+id, nil)
				assert.Equal(t, http.StatusBadRequest, rec3.Code)

				return
			}

			if tt.name == "create_queue_invalid_status" {
				// Queue must exist for UpdateQueue to reach the status validation.
				doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "test-q-for-valid"})
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_CreateQueue_WithTags verifies tags are stored at creation time.
func TestRefinement1_CreateQueue_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name": "tagged-queue",
		"tags": map[string]string{"env": "prod", "team": "infra"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	queue := resp["queue"].(map[string]any)
	tags := queue["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

// TestRefinement1_CreateJobTemplate_WithTags verifies tags are stored at creation time.
func TestRefinement1_CreateJobTemplate_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name": "tagged-template",
		"tags": map[string]string{"cost-center": "12345"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jt := resp["jobTemplate"].(map[string]any)
	tags := jt["tags"].(map[string]any)
	assert.Equal(t, "12345", tags["cost-center"])
}

// TestRefinement1_CreatePreset_WithTags verifies tags are stored at creation time.
func TestRefinement1_CreatePreset_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{
		"name": "tagged-preset",
		"tags": map[string]string{"project": "alpha"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	p := resp["preset"].(map[string]any)
	tags := p["tags"].(map[string]any)
	assert.Equal(t, "alpha", tags["project"])
}

// TestRefinement1_CreateJob_WithTags verifies tags are stored at creation time.
func TestRefinement1_CreateJob_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": "arn:aws:iam::123:role/role",
		"tags": map[string]string{"run": "nightly"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	j := resp["job"].(map[string]any)
	tags := j["tags"].(map[string]any)
	assert.Equal(t, "nightly", tags["run"])
}

// TestRefinement1_UpdateQueue_InvalidStatus verifies bad status returns 400.
func TestRefinement1_UpdateQueue_InvalidStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus int
	}{
		{name: "invalid_status", status: "RUNNING", wantStatus: http.StatusBadRequest},
		{name: "active_status", status: "ACTIVE", wantStatus: http.StatusOK},
		{name: "paused_status", status: "PAUSED", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "test-q"})
			rec := doRequest(t, h, http.MethodPut, "/2017-08-29/queues/test-q", map[string]any{"status": tt.status})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_CancelJob_InvalidStatus verifies only SUBMITTED/PROGRESSING jobs can be canceled.
func TestRefinement1_CancelJob_InvalidStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/role", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	require.NoError(t, b.CancelJob(j.ID))
	require.ErrorIs(t, b.CancelJob(j.ID), mediaconvert.ErrValidation)
}

// TestRefinement1_DeepCopySettings ensures modifying returned settings does not affect stored data.
func TestRefinement1_DeepCopySettings(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	settings := map[string]any{"codec": "H.264", "nested": map[string]any{"bitrate": 4000}}

	p, err := b.CreatePreset("deep-copy-test", "", "", settings, nil)
	require.NoError(t, err)

	// Mutate the returned copy.
	p.Settings["codec"] = "H.265"
	p.Settings["injected"] = "EVIL"

	// Original should be unaffected.
	p2, err := b.GetPreset("deep-copy-test")
	require.NoError(t, err)
	assert.Equal(t, "H.264", p2.Settings["codec"])
	assert.NotContains(t, p2.Settings, "injected")
}

// TestRefinement1_SeedHelpers verifies all Add*Internal seed helpers work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seedFunc func(b *mediaconvert.InMemoryBackend)
		check    func(b *mediaconvert.InMemoryBackend)
		name     string
	}{
		{
			name: "add_queue_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddQueueInternal(&mediaconvert.Queue{Name: "seed-q", Status: "ACTIVE", Arn: "arn:test:q"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.QueueCount(b))
			},
		},
		{
			name: "add_job_template_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddJobTemplateInternal(&mediaconvert.JobTemplate{Name: "seed-jt", Arn: "arn:test:jt"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.JobTemplateCount(b))
			},
		},
		{
			name: "add_job_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddJobInternal(&mediaconvert.Job{ID: "seed-job-id", Status: "SUBMITTED", Arn: "arn:test:job"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.JobCount(b))
			},
		},
		{
			name: "add_preset_internal",
			seedFunc: func(b *mediaconvert.InMemoryBackend) {
				b.AddPresetInternal(&mediaconvert.Preset{Name: "seed-preset", Arn: "arn:test:preset"})
			},
			check: func(b *mediaconvert.InMemoryBackend) {
				assert.Equal(t, 1, mediaconvert.PresetCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			tt.seedFunc(b)
			tt.check(b)
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore round-trip.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b1.CreateQueue("q1", "desc", "", "", map[string]string{"env": "test"})
	require.NoError(t, err)
	_, err = b1.CreatePreset("p1", "preset1", "", nil, nil)
	require.NoError(t, err)
	_, err = b1.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	_, err = b1.CreateJobTemplate("jt1", "tmpl", "", "", 0, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b1.AssociateCertificate("arn:aws:acm:us-east-1:123:cert/abc"))
	b1.PutPolicy("ALLOWED", "ALLOWED", "DISALLOWED")

	snap := b1.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := mediaconvert.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, testAccountID, b2.AccountID())
	assert.Equal(t, testRegion, b2.Region())
	assert.Equal(t, 1, mediaconvert.QueueCount(b2))
	assert.Equal(t, 1, mediaconvert.PresetCount(b2))
	assert.Equal(t, 1, mediaconvert.JobCount(b2))
	assert.Equal(t, 1, mediaconvert.JobTemplateCount(b2))
	assert.Equal(t, 1, mediaconvert.CertificateCount(b2))

	p, err := b2.GetPolicy()
	require.NoError(t, err)
	assert.Equal(t, "ALLOWED", p.HTTPInputs)
	assert.Equal(t, "DISALLOWED", p.S3Inputs)
}

// TestRefinement1_ListQueues_SortedByName verifies list is sorted.
func TestRefinement1_ListQueues_SortedByName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for _, name := range []string{"c-queue", "a-queue", "b-queue"} {
		_, err := b.CreateQueue(name, "", "", "", nil)
		require.NoError(t, err)
	}

	queues := b.ListQueues()
	require.Len(t, queues, 3)
	assert.Equal(t, "a-queue", queues[0].Name)
	assert.Equal(t, "b-queue", queues[1].Name)
	assert.Equal(t, "c-queue", queues[2].Name)
}

// TestRefinement1_ListPresets_SortedByName verifies list is sorted.
func TestRefinement1_ListPresets_SortedByName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for _, name := range []string{"z-preset", "a-preset", "m-preset"} {
		_, err := b.CreatePreset(name, "", "", nil, nil)
		require.NoError(t, err)
	}

	presets := b.ListPresets()
	require.Len(t, presets, 3)
	assert.Equal(t, "a-preset", presets[0].Name)
	assert.Equal(t, "m-preset", presets[1].Name)
	assert.Equal(t, "z-preset", presets[2].Name)
}

// TestRefinement1_ListJobs_SortedNewestFirst verifies jobs are sorted by creation time.
func TestRefinement1_ListJobs_SortedNewestFirst(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for range 3 {
		_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
		require.NoError(t, err)
	}

	jobs := b.ListJobs()
	require.Len(t, jobs, 3)

	for i := 1; i < len(jobs); i++ {
		assert.GreaterOrEqual(t, jobs[i-1].CreatedAt, jobs[i].CreatedAt)
	}
}

// TestRefinement1_AssociateCertificate_EmptyARN verifies empty ARN returns ErrValidation.
func TestRefinement1_AssociateCertificate_EmptyARN(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.AssociateCertificate("")
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestRefinement1_CreateResourceShare_EmptyJobID verifies empty jobID returns ErrValidation.
func TestRefinement1_CreateResourceShare_EmptyJobID(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateResourceShare("")
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestRefinement1_ARNsFormat verifies ARNs are correctly formatted.
func TestRefinement1_ARNsFormat(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	q, err := b.CreateQueue("arn-test-queue", "", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, q.Arn, "mediaconvert")
	assert.Contains(t, q.Arn, testRegion)
	assert.Contains(t, q.Arn, testAccountID)

	jt, err := b.CreateJobTemplate("arn-test-jt", "", "", "", 0, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, jt.Arn, "jobTemplates/arn-test-jt")

	p, err := b.CreatePreset("arn-test-preset", "", "", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, p.Arn, "presets/arn-test-preset")

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Contains(t, j.Arn, "jobs/")
}

// TestRefinement1_QueueType verifies queue type is always CUSTOM.
func TestRefinement1_QueueType(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	q, err := b.CreateQueue("type-queue", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "CUSTOM", q.Type)
}

// TestRefinement1_JobInitialStatus verifies initial job status is SUBMITTED.
func TestRefinement1_JobInitialStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "SUBMITTED", j.Status)
}

// TestRefinement1_JobsCanceledAfterCancel verifies job status after cancel.
func TestRefinement1_JobsCanceledAfterCancel(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, b.CancelJob(j.ID))

	j2, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELED", j2.Status)
}

// TestRefinement1_UpdateJobTemplate_Settings verifies settings deep copy on update.
func TestRefinement1_UpdateJobTemplate_Settings(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJobTemplate("jt-settings", "", "", "", 0, nil, nil)
	require.NoError(t, err)

	newSettings := map[string]any{"key": "val1"}

	jt, err := b.UpdateJobTemplate("jt-settings", "", "", "", nil, newSettings)
	require.NoError(t, err)

	// Mutate the returned copy.
	jt.Settings["key"] = "MUTATED"

	// Backend should be unaffected.
	jt2, err := b.GetJobTemplate("jt-settings")
	require.NoError(t, err)
	assert.Equal(t, "val1", jt2.Settings["key"])
}

// TestRefinement1_QueueDefaultStatus verifies default queue status is ACTIVE.
func TestRefinement1_QueueDefaultStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	q, err := b.CreateQueue("default-status-queue", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", q.Status)
}

// TestRefinement1_QueueDefaultPricingPlan verifies default pricing plan is ON_DEMAND.
func TestRefinement1_QueueDefaultPricingPlan(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	q, err := b.CreateQueue("default-pricing-queue", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "ON_DEMAND", q.PricingPlan)
}

// TestRefinement1_ListJobTemplates_SortedByName verifies sort order.
func TestRefinement1_ListJobTemplates_SortedByName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for _, name := range []string{"z-tmpl", "a-tmpl", "m-tmpl"} {
		_, err := b.CreateJobTemplate(name, "", "", "", 0, nil, nil)
		require.NoError(t, err)
	}

	templates := b.ListJobTemplates()
	require.Len(t, templates, 3)
	assert.Equal(t, "a-tmpl", templates[0].Name)
	assert.Equal(t, "m-tmpl", templates[1].Name)
	assert.Equal(t, "z-tmpl", templates[2].Name)
}

// TestRefinement1_HandlersReturnEmptySlices verifies all list operations return empty slices (not nil).
func TestRefinement1_HandlersReturnEmptySlices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		key  string
	}{
		{name: "list_queues", path: "/2017-08-29/queues", key: "queues"},
		{name: "list_job_templates", path: "/2017-08-29/jobTemplates", key: "jobTemplates"},
		{name: "list_jobs", path: "/2017-08-29/jobs", key: "jobs"},
		{name: "list_presets", path: "/2017-08-29/presets", key: "presets"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			list, ok := resp[tt.key]
			require.True(t, ok, "key %q missing from response", tt.key)
			// JSON unmarshals empty arrays as []any{}, not nil.
			arr, ok := list.([]any)
			require.True(t, ok)
			assert.Empty(t, arr)
		})
	}
}

// TestRefinement1_CreateJob_MissingRole_Returns400 verifies role validation at backend level.
func TestRefinement1_CreateJob_MissingRole_Returns400(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("", "", "", nil, nil, nil, "")
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestRefinement1_CreatePreset_EmptyName_Returns400 verifies name validation at backend level.
func TestRefinement1_CreatePreset_EmptyName_Returns400(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreatePreset("", "", "", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestRefinement1_PersistenceRestoreEmptySnapshot verifies empty snapshot is handled gracefully.
func TestRefinement1_PersistenceRestoreEmptySnapshot(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, 0, mediaconvert.QueueCount(b))
}

// TestRefinement1_PersistenceInvalidJSON verifies invalid JSON returns error.
func TestRefinement1_PersistenceInvalidJSON(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte(`not-json`))
	require.Error(t, err)
}

// TestRefinement1_GetJobsQueryResultsAlwaysEmpty verifies stub returns empty list.
func TestRefinement1_GetJobsQueryResultsAlwaysEmpty(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	results := b.GetJobsQueryResults("any-query-id")
	assert.Empty(t, results)
}

// TestRefinement1_DeletePreset_CleansUpTags verifies tags are removed on deletion.
func TestRefinement1_DeletePreset_CleansUpTags(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	p, err := b.CreatePreset("tagged-preset", "", "", nil, map[string]string{"k": "v"})
	require.NoError(t, err)

	require.NoError(t, b.DeletePreset("tagged-preset"))
	assert.Empty(t, b.GetTags(p.Arn))
}
