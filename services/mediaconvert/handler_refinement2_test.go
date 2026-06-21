package mediaconvert_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// r2Backend creates a fresh backend for refinement-2 tests.
func r2Backend() *mediaconvert.InMemoryBackend {
	return mediaconvert.NewInMemoryBackend("111122223333", "us-east-1")
}

// r2Do performs an HTTP request against the handler and returns the recorder.
func r2Do(t *testing.T, h *mediaconvert.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(bodyBytes)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// r2ExtractOp returns the operation name for the given method+path.
func r2ExtractOp(h *mediaconvert.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}

// TestRefinement2_UpdatePreset_Success verifies PUT /presets/{name} returns 200.
func TestRefinement2_UpdatePreset_Success(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreatePreset("my-preset", "original", "Cat1", nil, nil)
	require.NoError(t, err)

	h := mediaconvert.NewHandler(b)
	rec := r2Do(t, h, http.MethodPut, "/2017-08-29/presets/my-preset",
		map[string]any{"description": "updated desc", "category": "NewCat"})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	preset, ok := out["preset"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated desc", preset["description"])
	assert.Equal(t, "NewCat", preset["category"])
}

// TestRefinement2_UpdatePreset_NotFound returns 404 for unknown preset.
func TestRefinement2_UpdatePreset_NotFound(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	rec := r2Do(t, h, http.MethodPut, "/2017-08-29/presets/no-such-preset", map[string]any{})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement2_UpdatePreset_Direct validates the backend method directly.
func TestRefinement2_UpdatePreset_Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		desc        string
		category    string
		wantDesc    string
		wantCat     string
		wantErr     bool
		setupPreset bool
	}{
		{
			name:        "update_description",
			setupPreset: true,
			desc:        "new desc",
			category:    "",
			wantDesc:    "new desc",
			wantCat:     "Old",
		},
		{
			name:        "update_category",
			setupPreset: true,
			desc:        "",
			category:    "NewCat",
			wantDesc:    "orig",
			wantCat:     "NewCat",
		},
		{
			name:        "not_found",
			setupPreset: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r2Backend()

			if tt.setupPreset {
				_, err := b.CreatePreset("p1", "orig", "Old", nil, nil)
				require.NoError(t, err)
			}

			p, err := b.UpdatePreset("p1", tt.desc, tt.category, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "not found")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, p.Description)
			assert.Equal(t, tt.wantCat, p.Category)
		})
	}
}

// TestRefinement2_UpdatePreset_Settings verifies settings are updated.
func TestRefinement2_UpdatePreset_Settings(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreatePreset("p-settings", "", "", nil, nil)
	require.NoError(t, err)

	settings := map[string]any{"codec": "AAC"}
	p, err := b.UpdatePreset("p-settings", "", "", settings)
	require.NoError(t, err)
	assert.Equal(t, "AAC", p.Settings["codec"])
}

// TestRefinement2_PutPolicy_Success verifies PUT /policy sets and returns a policy.
func TestRefinement2_PutPolicy_Success(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	rec := r2Do(t, h, http.MethodPut, "/2017-08-29/policy",
		map[string]any{"policy": map[string]any{
			"httpInputs":  "ALLOWED",
			"httpsInputs": "ALLOWED",
			"s3Inputs":    "ALLOWED",
		}})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	pol, ok := out["policy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ALLOWED", pol["httpInputs"])
}

// TestRefinement2_PutPolicy_EmptyBody sets an empty policy successfully.
func TestRefinement2_PutPolicy_EmptyBody(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	rec := r2Do(t, h, http.MethodPut, "/2017-08-29/policy", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement2_GetPolicyAfterPut verifies policy persists after PutPolicy.
func TestRefinement2_GetPolicyAfterPut(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	r2Do(t, h, http.MethodPut, "/2017-08-29/policy",
		map[string]any{"policy": map[string]any{"s3Inputs": "DISABLED"}})

	rec := r2Do(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	pol := out["policy"].(map[string]any)
	assert.Equal(t, "DISABLED", pol["s3Inputs"])
}

// TestRefinement2_QueueJobCounts verifies ProgressingJobsCount/SubmittedJobsCount are computed.
func TestRefinement2_QueueJobCounts(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreateQueue("test-q", "", "", "", nil)
	require.NoError(t, err)

	j1, err := b.CreateJob("arn:aws:iam::123:role/r", "test-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "test-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	err = b.CancelJob(j1.ID)
	require.NoError(t, err)

	q, err := b.GetQueue("test-q")
	require.NoError(t, err)

	assert.Equal(t, 1, q.SubmittedJobsCount)
	assert.Equal(t, 0, q.ProgressingJobsCount)
}

// TestRefinement2_QueueCountsInList verifies job counts appear in ListQueues.
func TestRefinement2_QueueCountsInList(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreateQueue("list-q", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "list-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	queues := b.ListQueues()
	require.Len(t, queues, 1)
	assert.Equal(t, 1, queues[0].SubmittedJobsCount)
}

// TestRefinement2_CreateJob_WithUserMetadata verifies userMetadata is stored.
func TestRefinement2_CreateJob_WithUserMetadata(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil,
		map[string]string{"key1": "val1"}, "")
	require.NoError(t, err)
	assert.Equal(t, "val1", j.UserMetadata["key1"])

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "val1", got.UserMetadata["key1"])
}

// TestRefinement2_CreateJob_Timing verifies SubmitTime is set.
func TestRefinement2_CreateJob_Timing(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	require.NotNil(t, j.Timing)
	assert.Greater(t, j.Timing.SubmitTime, float64(0))
}

// TestRefinement2_CreateJob_BillingTagsSource verifies field is stored.
func TestRefinement2_CreateJob_BillingTagsSource(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "JOB")
	require.NoError(t, err)
	assert.Equal(t, "JOB", j.BillingTagsSource)
}

// TestRefinement2_CreateJob_QueueArn verifies QueueArn is set when queue exists.
func TestRefinement2_CreateJob_QueueArn(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	q, err := b.CreateQueue("my-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "my-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	assert.Equal(t, q.Arn, j.QueueArn)
	assert.Equal(t, "my-q", j.Queue)
}

// TestRefinement2_CreateJob_UnknownQueue returns ErrNotFound.
func TestRefinement2_CreateJob_UnknownQueue(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "no-such-queue", "", nil, nil, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestRefinement2_AssociateCertificate_InvalidARN returns ErrValidation.
func TestRefinement2_AssociateCertificate_InvalidARN(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	err := b.AssociateCertificate("not-an-arn")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "arn:")
}

// TestRefinement2_DisassociateCertificate_InvalidARN returns error.
func TestRefinement2_DisassociateCertificate_InvalidARN(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	err := b.DisassociateCertificate("not-an-arn")
	require.Error(t, err)
}

// TestRefinement2_ListJobsOutput_TotalCount verifies totalCount in list response.
func TestRefinement2_ListJobsOutput_TotalCount(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	h := mediaconvert.NewHandler(b)

	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	rec := r2Do(t, h, http.MethodGet, "/2017-08-29/jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(2), out["totalCount"], 0)
}

// TestRefinement2_SnapshotDeepCopy verifies mutations after Snapshot don't corrupt snapshot.
func TestRefinement2_SnapshotDeepCopy(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	_, err := b.CreateQueue("snap-q", "", "", "", map[string]string{"k": "v"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	err = b.DeleteQueue("snap-q")
	require.NoError(t, err)

	b2 := r2Backend()
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	q, err := b2.GetQueue("snap-q")
	require.NoError(t, err)
	assert.Equal(t, "snap-q", q.Name)
}

// TestRefinement2_ParseRoute_PutPolicy verifies routing to PutPolicy.
func TestRefinement2_ParseRoute_PutPolicy(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	assert.Equal(t, "PutPolicy", r2ExtractOp(h, http.MethodPut, "/2017-08-29/policy"))
}

// TestRefinement2_ParseRoute_UpdatePreset verifies routing to UpdatePreset.
func TestRefinement2_ParseRoute_UpdatePreset(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	assert.Equal(t, "UpdatePreset", r2ExtractOp(h, http.MethodPut, "/2017-08-29/presets/my-preset"))
}

// TestRefinement2_GetSupportedOperations_NewOps verifies new ops are present.
func TestRefinement2_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PutPolicy")
	assert.Contains(t, ops, "UpdatePreset")
}

// TestRefinement2_CloneJob_UserMetadata ensures cloneJob deep-copies userMetadata.
func TestRefinement2_CloneJob_UserMetadata(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil,
		map[string]string{"a": "1"}, "")
	require.NoError(t, err)

	j.UserMetadata["a"] = "mutated"

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "1", got.UserMetadata["a"])
}

// TestRefinement2_CloneJob_Timing ensures Timing is deep-copied.
func TestRefinement2_CloneJob_Timing(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	j.Timing.SubmitTime = 0

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Timing)
	assert.Greater(t, got.Timing.SubmitTime, float64(0))
}

// TestRefinement2_AccelerationStatus verifies default AccelerationStatus is set.
func TestRefinement2_AccelerationStatus(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "NOT_APPLICABLE", j.AccelerationStatus)
}

// TestRefinement2_JobTiming_ViaHTTP verifies timing in HTTP response.
func TestRefinement2_JobTiming_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	rec := r2Do(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	job := out["job"].(map[string]any)
	timing := job["timing"].(map[string]any)
	assert.Greater(t, timing["submitTime"], float64(0))
}

// TestRefinement2_CreateJob_UnknownQueue_Via_HTTP returns 404 when queue not found.
func TestRefinement2_CreateJob_UnknownQueue_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	rec := r2Do(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r", "queue": "missing"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement2_UpdatePreset_LastUpdated verifies LastUpdated changes.
func TestRefinement2_UpdatePreset_LastUpdated(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	original, err := b.CreatePreset("lu-preset", "", "", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdatePreset("lu-preset", "new", "", nil)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, updated.LastUpdated, original.LastUpdated)
}

// TestRefinement2_ListQueues_JobCounts_Via_HTTP verifies HTTP /queues includes counts.
func TestRefinement2_ListQueues_JobCounts_Via_HTTP(t *testing.T) {
	t.Parallel()

	b := r2Backend()
	h := mediaconvert.NewHandler(b)

	_, err := b.CreateQueue("count-q", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "count-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	rec := r2Do(t, h, http.MethodGet, "/2017-08-29/queues", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queues, ok := out["queues"].([]any)
	require.True(t, ok)
	require.Len(t, queues, 1)
	q := queues[0].(map[string]any)
	assert.InDelta(t, float64(1), q["submittedJobsCount"], 0)
}

// TestRefinement2_ParseRoute_GetPolicy verifies GET /policy → GetPolicy.
func TestRefinement2_ParseRoute_GetPolicy(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r2Backend())
	assert.Equal(t, "GetPolicy", r2ExtractOp(h, http.MethodGet, "/2017-08-29/policy"))
	assert.Equal(t, "DeletePolicy", r2ExtractOp(h, http.MethodDelete, "/2017-08-29/policy"))
}
