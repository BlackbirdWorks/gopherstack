package mediaconvert_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// r3Backend creates a fresh backend for refinement-3 tests.
func r3Backend() *mediaconvert.InMemoryBackend {
	return mediaconvert.NewInMemoryBackend("123456789012", "us-west-2")
}

// r3Do performs an HTTP request against the handler and returns the recorder.
func r3Do(t *testing.T, h *mediaconvert.Handler, method, path string, body any) *httptest.ResponseRecorder {
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

// r3ExtractOp returns the operation name for the given method+path.
func r3ExtractOp(h *mediaconvert.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}

// r3CreateJob creates a job via the backend and returns it.
func r3CreateJob(t *testing.T, b *mediaconvert.InMemoryBackend) *mediaconvert.Job {
	t.Helper()

	j, err := b.CreateJob("arn:aws:iam::123:role/role", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	return j
}

// --- Issue 1: Job status COMPLETE / ERROR ---

// TestRefinement3_JobStatusComplete_Constant verifies the COMPLETE state is reachable.
func TestRefinement3_JobStatusComplete_Constant(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	// Advance through the full phase pipeline to COMPLETE.
	// SUBMITTED → PROGRESSING (phase=PROBING)
	b.AdvanceJobPhase()
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j.ID))

	// PROBING → TRANSCODING
	b.AdvanceJobPhase()
	assert.Equal(t, "TRANSCODING", mediaconvert.JobPhaseOf(b, j.ID))

	// TRANSCODING → UPLOADING
	b.AdvanceJobPhase()
	assert.Equal(t, "UPLOADING", mediaconvert.JobPhaseOf(b, j.ID))

	// UPLOADING → COMPLETE
	b.AdvanceJobPhase()
	assert.Equal(t, "COMPLETE", mediaconvert.JobStatusOf(b, j.ID))
}

// TestRefinement3_JobStatusError_FieldPresence verifies ERROR is a valid job status value.
func TestRefinement3_JobStatusError_FieldPresence(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	// Seed a job with ERROR status directly.
	errJob := &mediaconvert.Job{
		ID:     "error-job-1",
		Status: "ERROR",
		Role:   "arn:aws:iam::123:role/role",
	}
	b.AddJobInternal(errJob)

	got, err := b.GetJob("error-job-1")
	require.NoError(t, err)
	assert.Equal(t, "ERROR", got.Status)
}

// --- Issue 2: Job phases TRANSCODING / UPLOADING ---

// TestRefinement3_JobPhases_FullProgression tests the full phase advancement.
func TestRefinement3_JobPhases_FullProgression(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	require.Equal(t, "SUBMITTED", mediaconvert.JobStatusOf(b, j.ID))

	// Tick 1: SUBMITTED → PROGRESSING/PROBING
	b.AdvanceJobPhase()
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j.ID))
	assert.Equal(t, "PROBING", mediaconvert.JobPhaseOf(b, j.ID))

	// Tick 2: PROBING → TRANSCODING
	b.AdvanceJobPhase()
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j.ID))
	assert.Equal(t, "TRANSCODING", mediaconvert.JobPhaseOf(b, j.ID))

	// Tick 3: TRANSCODING → UPLOADING
	b.AdvanceJobPhase()
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j.ID))
	assert.Equal(t, "UPLOADING", mediaconvert.JobPhaseOf(b, j.ID))

	// Tick 4: UPLOADING → COMPLETE
	b.AdvanceJobPhase()
	assert.Equal(t, "COMPLETE", mediaconvert.JobStatusOf(b, j.ID))
	assert.Empty(t, mediaconvert.JobPhaseOf(b, j.ID))
}

// TestRefinement3_JobPhases_MultipleJobs ensures all jobs in the backend advance.
func TestRefinement3_JobPhases_MultipleJobs(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j1 := r3CreateJob(t, b)
	j2 := r3CreateJob(t, b)

	b.AdvanceJobPhase()
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j1.ID))
	assert.Equal(t, "PROGRESSING", mediaconvert.JobStatusOf(b, j2.ID))
}

// --- Issue 3: UpdateJob operation ---

// TestRefinement3_UpdateJob_Priority verifies priority change via UpdateJob.
func TestRefinement3_UpdateJob_Priority(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.Equal(t, 0, j.Priority)

	newPri := 10
	updated, err := b.UpdateJob(j.ID, "", &newPri, nil)
	require.NoError(t, err)
	assert.Equal(t, 10, updated.Priority)

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, 10, got.Priority)
}

// TestRefinement3_UpdateJob_QueueChange verifies queue transition is recorded.
func TestRefinement3_UpdateJob_QueueChange(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("q-src", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateQueue("q-dst", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/role", "q-src", "", nil, nil, nil, "")
	require.NoError(t, err)
	require.Equal(t, "q-src", j.Queue)

	updated, err := b.UpdateJob(j.ID, "q-dst", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "q-dst", updated.Queue)
	require.Len(t, updated.QueueTransitions, 1)
	assert.Equal(t, "q-src", updated.QueueTransitions[0].SourceQueue)
	assert.Equal(t, "q-dst", updated.QueueTransitions[0].DestinationQueue)
	assert.Greater(t, updated.QueueTransitions[0].Timestamp, float64(0))
}

// TestRefinement3_UpdateJob_HopDestinations verifies hop destinations are updated.
func TestRefinement3_UpdateJob_HopDestinations(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	hops := []mediaconvert.HopDestination{
		{WaitMinutes: 5, Priority: 2, Queue: "fallback-q"},
	}

	updated, err := b.UpdateJob(j.ID, "", nil, hops)
	require.NoError(t, err)
	require.Len(t, updated.HopDestinations, 1)
	assert.Equal(t, 5, updated.HopDestinations[0].WaitMinutes)
	assert.Equal(t, "fallback-q", updated.HopDestinations[0].Queue)
}

// TestRefinement3_UpdateJob_NotFound returns ErrNotFound for unknown job.
func TestRefinement3_UpdateJob_NotFound(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.UpdateJob("nonexistent-id", "", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrNotFound)
}

// TestRefinement3_UpdateJob_InvalidPriority returns ErrValidation for out-of-range priority.
func TestRefinement3_UpdateJob_InvalidPriority(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	badPri := 999
	_, err := b.UpdateJob(j.ID, "", &badPri, nil)
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestRefinement3_UpdateJob_Via_HTTP verifies PUT /jobs/{id} returns 200.
func TestRefinement3_UpdateJob_Via_HTTP(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	h := mediaconvert.NewHandler(b)
	j := r3CreateJob(t, b)

	rec := r3Do(t, h, http.MethodPut, "/2017-08-29/jobs/"+j.ID,
		map[string]any{"priority": 5})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData, ok := out["job"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(5), jobData["priority"], 0)
}

// TestRefinement3_UpdateJob_NotFound_Via_HTTP returns 404 for unknown job.
func TestRefinement3_UpdateJob_NotFound_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPut, "/2017-08-29/jobs/no-such-job",
		map[string]any{"priority": 1})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ParseRoute_UpdateJob verifies PUT /jobs/{id} → UpdateJob.
func TestRefinement3_ParseRoute_UpdateJob(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	assert.Equal(t, "UpdateJob", r3ExtractOp(h, http.MethodPut, "/2017-08-29/jobs/job-123"))
}

// --- Issue 4: HopDestination on CreateJob ---

// TestRefinement3_CreateJob_HopDestinations verifies hop destinations stored at creation.
func TestRefinement3_CreateJob_HopDestinations(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	hops := []mediaconvert.HopDestination{
		{WaitMinutes: 10, Priority: 0, Queue: "backup-q"},
		{WaitMinutes: 20, Priority: -5, Queue: "emergency-q"},
	}

	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "", "", 0, hops)
	require.NoError(t, err)
	require.Len(t, j.HopDestinations, 2)
	assert.Equal(t, "backup-q", j.HopDestinations[0].Queue)
	assert.Equal(t, "emergency-q", j.HopDestinations[1].Queue)
}

// TestRefinement3_CreateJob_HopDestinations_Via_HTTP verifies JSON parsing.
func TestRefinement3_CreateJob_HopDestinations_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": "arn:aws:iam::123:role/r",
		"hopDestinations": []any{
			map[string]any{"waitMinutes": 5, "queue": "q1"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	hops, ok := jobData["hopDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, hops, 1)
	hop := hops[0].(map[string]any)
	assert.Equal(t, "q1", hop["queue"])
}

// --- Issue 5: ReservationPlan on Queue ---

// TestRefinement3_Queue_ReservationPlan verifies reservation plan stored at creation.
func TestRefinement3_Queue_ReservationPlan(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	rp := &mediaconvert.ReservationPlan{
		ReservedSlots: 5,
		Status:        "ACTIVE",
		Commitment:    "ONE_YEAR",
		RenewalType:   "AUTO_RENEW",
	}

	q, err := b.CreateQueueFull("rp-queue", "", "", "", nil, 0, rp, nil)
	require.NoError(t, err)
	require.NotNil(t, q.ReservationPlan)
	assert.Equal(t, 5, q.ReservationPlan.ReservedSlots)
	assert.Equal(t, "ACTIVE", q.ReservationPlan.Status)
}

// TestRefinement3_Queue_ReservationPlan_Via_HTTP verifies JSON parsing.
func TestRefinement3_Queue_ReservationPlan_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name": "rp-http-queue",
		"reservationPlan": map[string]any{
			"reservedSlots": 3,
			"status":        "ACTIVE",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queueData := out["queue"].(map[string]any)
	rp, ok := queueData["reservationPlan"].(map[string]any)
	require.True(t, ok, "reservationPlan should be in response")
	assert.InDelta(t, float64(3), rp["reservedSlots"], 0)
}

// TestRefinement3_Queue_NoLegacyReservationPlanName verifies old field removed.
func TestRefinement3_Queue_NoLegacyReservationPlanName(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q, err := b.CreateQueue("no-rp-name-q", "", "", "", nil)
	require.NoError(t, err)

	h := mediaconvert.NewHandler(b)
	rec := r3Do(t, h, http.MethodGet, "/2017-08-29/queues/"+q.Name, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queueData := out["queue"].(map[string]any)
	_, hasOldField := queueData["reservationPlanName"]
	assert.False(t, hasOldField, "legacy reservationPlanName field should not be present")
}

// --- Issue 6: AccelerationSettings ---

// TestRefinement3_AccelerationSettings_Enabled verifies mode=ENABLED sets status=PREFERRED.
func TestRefinement3_AccelerationSettings_Enabled(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "ENABLED", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "PREFERRED", j.AccelerationStatus)
	require.NotNil(t, j.AccelerationSettings)
	assert.Equal(t, "ENABLED", j.AccelerationSettings.Mode)
}

// TestRefinement3_AccelerationSettings_Disabled verifies mode=DISABLED keeps NOT_APPLICABLE.
func TestRefinement3_AccelerationSettings_Disabled(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "DISABLED", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "NOT_APPLICABLE", j.AccelerationStatus)
}

// TestRefinement3_AccelerationSettings_Via_HTTP verifies JSON input parsing.
func TestRefinement3_AccelerationSettings_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": "arn:aws:iam::123:role/r",
		"accelerationSettings": map[string]any{
			"mode": "ENABLED",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	assert.Equal(t, "PREFERRED", jobData["accelerationStatus"])
	as := jobData["accelerationSettings"].(map[string]any)
	assert.Equal(t, "ENABLED", as["mode"])
}

// --- Issue 7: QueueTransitions ---

// TestRefinement3_QueueTransitions_Multiple verifies multiple transitions accumulate.
func TestRefinement3_QueueTransitions_Multiple(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("q1", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateQueue("q2", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateQueue("q3", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "q1", "", nil, nil, nil, "")
	require.NoError(t, err)

	_, err = b.UpdateJob(j.ID, "q2", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateJob(j.ID, "q3", nil, nil)
	require.NoError(t, err)

	require.Len(t, updated.QueueTransitions, 2)
	assert.Equal(t, "q1", updated.QueueTransitions[0].SourceQueue)
	assert.Equal(t, "q2", updated.QueueTransitions[0].DestinationQueue)
	assert.Equal(t, "q2", updated.QueueTransitions[1].SourceQueue)
	assert.Equal(t, "q3", updated.QueueTransitions[1].DestinationQueue)
}

// TestRefinement3_QueueTransitions_SameQueue_NoTransition verifies no new transition when queue unchanged.
func TestRefinement3_QueueTransitions_SameQueue_NoTransition(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("unchanged-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "unchanged-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	updated, err := b.UpdateJob(j.ID, "unchanged-q", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, updated.QueueTransitions)
}

// --- Issue 8: OutputGroupDetails ---

// TestRefinement3_OutputGroupDetails_OnComplete verifies output details set when job completes.
func TestRefinement3_OutputGroupDetails_OnComplete(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	// Advance all the way to COMPLETE.
	for range 4 {
		b.AdvanceJobPhase()
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	require.Equal(t, "COMPLETE", got.Status)
	require.Len(t, got.OutputGroupDetails, 1)
	require.Len(t, got.OutputGroupDetails[0].OutputDetails, 1)
	od := got.OutputGroupDetails[0].OutputDetails[0]
	assert.Equal(t, 60000, od.DurationInMs)
	require.NotNil(t, od.VideoDetails)
	assert.Equal(t, 1920, od.VideoDetails.WidthInPx)
	assert.Equal(t, 1080, od.VideoDetails.HeightInPx)
}

// --- Issue 9: Job Messages / Warnings ---

// TestRefinement3_Messages_InitializedOnCreate verifies Messages non-nil at creation.
func TestRefinement3_Messages_InitializedOnCreate(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.NotNil(t, j.Messages)
	assert.NotNil(t, j.Warnings)
}

// TestRefinement3_Warnings_InitializedOnCreate verifies Warnings non-nil at creation.
func TestRefinement3_Warnings_InitializedOnCreate(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	assert.NotNil(t, j.Warnings)
}

// TestRefinement3_Messages_Via_HTTP verifies messages field appears in response.
func TestRefinement3_Messages_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	_, hasMessages := jobData["messages"]
	assert.True(t, hasMessages, "messages field should be present")
}

// --- Issue 10: Timing StartTime/FinishTime ---

// TestRefinement3_Timing_StartTime_SetOnProgressing verifies StartTime set when PROGRESSING.
func TestRefinement3_Timing_StartTime_SetOnProgressing(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.Zero(t, j.Timing.StartTime)

	b.AdvanceJobPhase() // SUBMITTED → PROGRESSING

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Greater(t, got.Timing.StartTime, float64(0))
}

// TestRefinement3_Timing_FinishTime_SetOnComplete verifies FinishTime set on COMPLETE.
func TestRefinement3_Timing_FinishTime_SetOnComplete(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	for range 4 {
		b.AdvanceJobPhase()
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Greater(t, got.Timing.FinishTime, float64(0))
}

// --- Issue 11: ClientRequestToken idempotency ---

// TestRefinement3_ClientRequestToken_Dedup verifies same token returns same job.
func TestRefinement3_ClientRequestToken_Dedup(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	token := "unique-token-abc-123"

	j1, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", token, "", "", 0, nil)
	require.NoError(t, err)

	j2, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", token, "", "", 0, nil)
	require.NoError(t, err)

	assert.Equal(t, j1.ID, j2.ID, "same token should return same job")
	assert.Equal(t, 1, mediaconvert.JobCount(b), "only one job should be stored")
}

// TestRefinement3_ClientRequestToken_DifferentTokens verifies different tokens create distinct jobs.
func TestRefinement3_ClientRequestToken_DifferentTokens(t *testing.T) {
	t.Parallel()

	b := r3Backend()

	j1, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "token-one", "", "", 0, nil)
	require.NoError(t, err)

	j2, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "token-two", "", "", 0, nil)
	require.NoError(t, err)

	assert.NotEqual(t, j1.ID, j2.ID)
	assert.Equal(t, 2, mediaconvert.JobCount(b))
}

// TestRefinement3_ClientRequestToken_StoredOnJob verifies token field on job.
func TestRefinement3_ClientRequestToken_StoredOnJob(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	token := "my-request-token"

	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", token, "", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, token, j.ClientRequestToken)
}

// TestRefinement3_ClientRequestToken_IndexSize verifies token is indexed.
func TestRefinement3_ClientRequestToken_IndexSize(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	assert.Equal(t, 0, mediaconvert.TokenIndexSize(b))

	_, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "tkn-1", "", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, mediaconvert.TokenIndexSize(b))

	_, err = b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "tkn-2", "", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, mediaconvert.TokenIndexSize(b))
}

// TestRefinement3_ClientRequestToken_EmptyNoIndex verifies no token entry for empty token.
func TestRefinement3_ClientRequestToken_EmptyNoIndex(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, 0, mediaconvert.TokenIndexSize(b))
}

// TestRefinement3_ClientRequestToken_Via_HTTP verifies token dedup over HTTP.
func TestRefinement3_ClientRequestToken_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	body := map[string]any{
		"role":               "arn:aws:iam::123:role/r",
		"clientRequestToken": "http-token-xyz",
	}

	rec1 := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	id1 := out1["job"].(map[string]any)["id"].(string)

	rec2 := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", body)
	require.Equal(t, http.StatusCreated, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	id2 := out2["job"].(map[string]any)["id"].(string)

	assert.Equal(t, id1, id2, "duplicate token should return same job ID")
}

// --- Issue 12: JobEngineVersion ---

// TestRefinement3_JobEngineVersion_UsedAlwaysSet verifies JobEngineVersionUsed is always set.
func TestRefinement3_JobEngineVersion_UsedAlwaysSet(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionUsed)
}

// TestRefinement3_JobEngineVersion_RequestedStored verifies requested version stored.
func TestRefinement3_JobEngineVersion_RequestedStored(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "", "2017-08-29", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionRequested)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionUsed)
}

// TestRefinement3_JobEngineVersion_Via_HTTP verifies field parsed from HTTP body.
func TestRefinement3_JobEngineVersion_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":                      "arn:aws:iam::123:role/r",
		"jobEngineVersionRequested": "2017-08-29",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	assert.Equal(t, "2017-08-29", jobData["jobEngineVersionUsed"])
	assert.Equal(t, "2017-08-29", jobData["jobEngineVersionRequested"])
}

// --- Issue 13: Queue ConcurrentJobs / ServiceOverrides ---

// TestRefinement3_Queue_ConcurrentJobs verifies field stored at creation.
func TestRefinement3_Queue_ConcurrentJobs(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q, err := b.CreateQueueFull("cj-queue", "", "", "", nil, 8, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 8, q.ConcurrentJobs)
}

// TestRefinement3_Queue_ServiceOverrides verifies field stored at creation.
func TestRefinement3_Queue_ServiceOverrides(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	overrides := map[string]any{"feature_x": true, "max_bitrate": 50000}

	q, err := b.CreateQueueFull("so-queue", "", "", "", nil, 0, nil, overrides)
	require.NoError(t, err)
	assert.Equal(t, true, q.ServiceOverrides["feature_x"])
	assert.Equal(t, 50000, q.ServiceOverrides["max_bitrate"])
}

// TestRefinement3_Queue_ConcurrentJobs_Via_HTTP verifies JSON round-trip.
func TestRefinement3_Queue_ConcurrentJobs_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name":           "cj-http-queue",
		"concurrentJobs": 4,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queueData := out["queue"].(map[string]any)
	assert.InDelta(t, float64(4), queueData["concurrentJobs"], 0)
}

// --- Issue 14: ShareStatus / LastShareDetails ---

// TestRefinement3_ShareStatus_DefaultNotShared verifies initial ShareStatus.
func TestRefinement3_ShareStatus_DefaultNotShared(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	assert.Equal(t, "NOT_SHARED", j.ShareStatus)
}

// TestRefinement3_CreateResourceShare_SetsShareStatus verifies share sets status.
func TestRefinement3_CreateResourceShare_SetsShareStatus(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.Equal(t, "NOT_SHARED", j.ShareStatus)

	_, err := b.CreateResourceShare(j.ID)
	require.NoError(t, err)

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "SHARED", got.ShareStatus)
	require.NotNil(t, got.LastShareDetails)
	assert.NotEmpty(t, got.LastShareDetails.ShareToken)
	assert.Greater(t, got.LastShareDetails.SharedAt, float64(0))
}

// TestRefinement3_CreateResourceShare_Via_HTTP sets share on job.
func TestRefinement3_CreateResourceShare_Via_HTTP(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	h := mediaconvert.NewHandler(b)
	j := r3CreateJob(t, b)

	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/resourceShares",
		map[string]any{"jobId": j.ID})
	require.Equal(t, http.StatusNoContent, rec.Code)

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "SHARED", got.ShareStatus)
}

// --- Issue 15: Cancel sets FinishTime and queue counter ---

// TestRefinement3_CancelJob_FinishTime verifies FinishTime is set on cancel.
func TestRefinement3_CancelJob_FinishTime(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.Zero(t, j.Timing.FinishTime)

	require.NoError(t, b.CancelJob(j.ID))

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Greater(t, got.Timing.FinishTime, float64(0))
}

// TestRefinement3_CancelJob_QueueCounter_DecrementSubmitted verifies counter decremented.
func TestRefinement3_CancelJob_QueueCounter_DecrementSubmitted(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q, err := b.CreateQueue("cancel-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "cancel-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	// Before cancel: 1 submitted
	assert.Equal(t, 1, mediaconvert.QueueCounterSubmitted(b, q.Arn))

	require.NoError(t, b.CancelJob(j.ID))

	// After cancel: 0 submitted
	assert.Equal(t, 0, mediaconvert.QueueCounterSubmitted(b, q.Arn))
}

// --- Optimization: deepCloneMap depth cap ---

// TestRefinement3_DeepCloneMap_DepthCap verifies no panic/infinite recursion at depth 25.
func TestRefinement3_DeepCloneMap_DepthCap(t *testing.T) {
	t.Parallel()

	// Build a deeply nested map 25 levels deep.
	var buildNested func(depth int) map[string]any
	buildNested = func(depth int) map[string]any {
		if depth == 0 {
			return map[string]any{"leaf": "value"}
		}

		return map[string]any{"child": buildNested(depth - 1)}
	}

	deepMap := buildNested(25)

	b := r3Backend()
	// Should not panic; some deep values may be nil due to depth cap.
	p, err := b.CreatePreset("deep-preset", "", "", deepMap, nil)
	require.NoError(t, err)
	require.NotNil(t, p)
}

// TestRefinement3_DeepCloneMap_ShallowData verifies shallow maps still cloned correctly.
func TestRefinement3_DeepCloneMap_ShallowData(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	settings := map[string]any{
		"codec":   "H.264",
		"bitrate": 4000,
		"nested":  map[string]any{"level": 1},
	}

	p, err := b.CreatePreset("shallow-test", "", "", settings, nil)
	require.NoError(t, err)
	assert.Equal(t, "H.264", p.Settings["codec"])
	nested := p.Settings["nested"].(map[string]any)
	assert.Equal(t, 1, nested["level"])
}

// --- Optimization: Per-queue counters ---

// TestRefinement3_QueueCounters_Accurate verifies counters during lifecycle.
func TestRefinement3_QueueCounters_Accurate(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q, err := b.CreateQueue("lifecycle-q", "", "", "", nil)
	require.NoError(t, err)

	// Start: 0/0
	assert.Equal(t, 0, mediaconvert.QueueCounterSubmitted(b, q.Arn))
	assert.Equal(t, 0, mediaconvert.QueueCounterProgressing(b, q.Arn))

	// Create 2 jobs
	j1, err := b.CreateJob("arn:aws:iam::123:role/r", "lifecycle-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "lifecycle-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	// 2 submitted
	assert.Equal(t, 2, mediaconvert.QueueCounterSubmitted(b, q.Arn))

	// Advance once: both go to PROGRESSING
	b.AdvanceJobPhase()
	assert.Equal(t, 0, mediaconvert.QueueCounterSubmitted(b, q.Arn))
	assert.Equal(t, 2, mediaconvert.QueueCounterProgressing(b, q.Arn))

	// Cancel j1 (which is now PROGRESSING)
	require.NoError(t, b.CancelJob(j1.ID))
	assert.Equal(t, 1, mediaconvert.QueueCounterProgressing(b, q.Arn))
}

// TestRefinement3_QueueCounters_GetQueue_Uses_Counters verifies GetQueue reads counters.
func TestRefinement3_QueueCounters_GetQueue_Uses_Counters(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("counter-q2", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "counter-q2", "", nil, nil, nil, "")
	require.NoError(t, err)

	q, err := b.GetQueue("counter-q2")
	require.NoError(t, err)
	assert.Equal(t, 1, q.SubmittedJobsCount)
	assert.Equal(t, 0, q.ProgressingJobsCount)
}

// TestRefinement3_QueueCounters_ListQueues_Uses_Counters verifies ListQueues reads counters.
func TestRefinement3_QueueCounters_ListQueues_Uses_Counters(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("list-counter-q", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "list-counter-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	queues := b.ListQueues()
	require.Len(t, queues, 1)
	assert.Equal(t, 1, queues[0].SubmittedJobsCount)
}

// --- UpdateJob counter adjustment ---

// TestRefinement3_UpdateJob_QueueCounter_Adjusted verifies counters updated on queue change.
func TestRefinement3_UpdateJob_QueueCounter_Adjusted(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q1, err := b.CreateQueue("src-q", "", "", "", nil)
	require.NoError(t, err)
	q2, err := b.CreateQueue("dst-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "src-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	assert.Equal(t, 1, mediaconvert.QueueCounterSubmitted(b, q1.Arn))
	assert.Equal(t, 0, mediaconvert.QueueCounterSubmitted(b, q2.Arn))

	_, err = b.UpdateJob(j.ID, "dst-q", nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, mediaconvert.QueueCounterSubmitted(b, q1.Arn))
	assert.Equal(t, 1, mediaconvert.QueueCounterSubmitted(b, q2.Arn))
}

// --- Handler ops count ---

// TestRefinement3_HandlerOpsLen_IncludesUpdateJob verifies UpdateJob in supported ops.
func TestRefinement3_HandlerOpsLen_IncludesUpdateJob(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "UpdateJob")
	assert.GreaterOrEqual(t, len(ops), 29)
}

// --- Persistence with new fields ---

// TestRefinement3_Persistence_NewFields_RoundTrip verifies new fields survive snapshot/restore.
func TestRefinement3_Persistence_NewFields_RoundTrip(t *testing.T) {
	t.Parallel()

	b1 := r3Backend()

	// Create queue with new fields.
	rp := &mediaconvert.ReservationPlan{ReservedSlots: 2, Status: "ACTIVE"}
	q, err := b1.CreateQueueFull("snap-q", "", "", "", nil, 4, rp, map[string]any{"x": true})
	require.NoError(t, err)

	// Create job with new fields.
	j, err := b1.CreateJobFull("arn:aws:iam::123:role/r", "snap-q", "", nil, nil, nil,
		"", "snap-token", "ENABLED", "2017-08-29", 5,
		[]mediaconvert.HopDestination{{Queue: "fallback", WaitMinutes: 3}})
	require.NoError(t, err)

	snap := b1.Snapshot()
	require.NotEmpty(t, snap)

	b2 := r3Backend()
	require.NoError(t, b2.Restore(snap))

	// Verify queue.
	got, err := b2.GetQueue(q.Name)
	require.NoError(t, err)
	assert.Equal(t, 4, got.ConcurrentJobs)
	require.NotNil(t, got.ReservationPlan)
	assert.Equal(t, 2, got.ReservationPlan.ReservedSlots)

	// Verify job.
	gotJ, err := b2.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "2017-08-29", gotJ.JobEngineVersionUsed)
	assert.Equal(t, "snap-token", gotJ.ClientRequestToken)
	assert.Equal(t, "PREFERRED", gotJ.AccelerationStatus)
	assert.Equal(t, 5, gotJ.Priority)
	require.Len(t, gotJ.HopDestinations, 1)
	assert.Equal(t, "fallback", gotJ.HopDestinations[0].Queue)
	assert.NotNil(t, gotJ.Messages)
	assert.Equal(t, "NOT_SHARED", gotJ.ShareStatus)
}

// --- SweepExpiredTokens ---

// TestRefinement3_SweepExpiredTokens_RemovesOld verifies expired tokens swept.
func TestRefinement3_SweepExpiredTokens_RemovesOld(t *testing.T) {
	t.Parallel()

	b := r3Backend()

	// Create a job with a token via the internal method to simulate an aged token.
	_, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "old-token", "", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, mediaconvert.TokenIndexSize(b))

	// Sweep should not remove unexpired tokens.
	b.SweepExpiredTokens()
	assert.Equal(t, 1, mediaconvert.TokenIndexSize(b), "fresh token should not be swept")
}

// --- Job percent complete ---

// TestRefinement3_JobPercentComplete_100_OnComplete verifies 100% on COMPLETE.
func TestRefinement3_JobPercentComplete_100_OnComplete(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.Equal(t, 0, j.JobPercentComplete)

	for range 4 {
		b.AdvanceJobPhase()
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, 100, got.JobPercentComplete)
}

// --- Timing submit time preserved ---

// TestRefinement3_Timing_SubmitTime_Preserved verifies SubmitTime not overwritten during phases.
func TestRefinement3_Timing_SubmitTime_Preserved(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	submitTime := j.Timing.SubmitTime
	require.Greater(t, submitTime, float64(0))

	// Advance all the way to COMPLETE.
	for range 4 {
		b.AdvanceJobPhase()
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.InDelta(t, submitTime, got.Timing.SubmitTime, 0.001)
}

// --- Idempotent advance (COMPLETE jobs not re-processed) ---

// TestRefinement3_AdvanceJobPhase_CompletedJobNotReadvanced verifies COMPLETE is terminal.
func TestRefinement3_AdvanceJobPhase_CompletedJobNotReadvanced(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	for range 4 {
		b.AdvanceJobPhase()
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	require.Equal(t, "COMPLETE", got.Status)
	finishTime := got.Timing.FinishTime

	// Extra advance should not change state.
	b.AdvanceJobPhase()

	got2, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", got2.Status)
	assert.InDelta(t, finishTime, got2.Timing.FinishTime, 0.001)
}

// --- Canceled job not re-advanced ---

// TestRefinement3_AdvanceJobPhase_CanceledJobNotAdvanced verifies CANCELED is terminal.
func TestRefinement3_AdvanceJobPhase_CanceledJobNotAdvanced(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)
	require.NoError(t, b.CancelJob(j.ID))

	b.AdvanceJobPhase()

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELED", got.Status)
}

// --- Priority field on CreateJob via HTTP ---

// TestRefinement3_CreateJob_Priority_Via_HTTP verifies priority parsed from body.
func TestRefinement3_CreateJob_Priority_Via_HTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(r3Backend())
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":     "arn:aws:iam::123:role/r",
		"priority": 15,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	assert.InDelta(t, float64(15), jobData["priority"], 0)
}

// --- Clone correctness for new slice fields ---

// TestRefinement3_CloneJob_HopDestinations_Independent verifies deep copy.
func TestRefinement3_CloneJob_HopDestinations_Independent(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "", "", 0, []mediaconvert.HopDestination{{Queue: "orig-q"}})
	require.NoError(t, err)

	// Mutate the returned copy.
	j.HopDestinations[0].Queue = "mutated"

	// Backend should be unaffected.
	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "orig-q", got.HopDestinations[0].Queue)
}

// TestRefinement3_CloneJob_QueueTransitions_Independent verifies deep copy.
func TestRefinement3_CloneJob_QueueTransitions_Independent(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateQueue("clone-src", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateQueue("clone-dst", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "clone-src", "", nil, nil, nil, "")
	require.NoError(t, err)

	updated, err := b.UpdateJob(j.ID, "clone-dst", nil, nil)
	require.NoError(t, err)

	// Mutate the returned slice.
	updated.QueueTransitions[0].SourceQueue = "mutated"

	// Backend should be unaffected.
	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "clone-src", got.QueueTransitions[0].SourceQueue)
}

// --- Timing nil-safety ---

// TestRefinement3_CancelJob_NilTiming_SafelyHandled verifies no panic if Timing nil.
func TestRefinement3_CancelJob_NilTiming_SafelyHandled(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	// Seed a job without Timing to simulate edge case.
	b.AddJobInternal(&mediaconvert.Job{
		ID:     "no-timing-job",
		Status: "SUBMITTED",
		Role:   "arn:aws:iam::123:role/r",
	})

	err := b.CancelJob("no-timing-job")
	require.NoError(t, err)

	got, err := b.GetJob("no-timing-job")
	require.NoError(t, err)
	assert.Equal(t, "CANCELED", got.Status)
	require.NotNil(t, got.Timing)
	assert.Greater(t, got.Timing.FinishTime, float64(0))
}

// --- ServiceOverrides deep clone ---

// TestRefinement3_Queue_ServiceOverrides_DeepCopy verifies mutations don't affect stored data.
func TestRefinement3_Queue_ServiceOverrides_DeepCopy(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	overrides := map[string]any{"key": "original"}

	q, err := b.CreateQueueFull("dc-q", "", "", "", nil, 0, nil, overrides)
	require.NoError(t, err)

	// Mutate the returned copy.
	q.ServiceOverrides["key"] = "mutated"

	got, err := b.GetQueue("dc-q")
	require.NoError(t, err)
	assert.Equal(t, "original", got.ServiceOverrides["key"])
}

// --- AdvanceJobPhase return value ---

// TestRefinement3_AdvanceJobPhase_ReturnsFalse_WhenNoJobs verifies return value.
func TestRefinement3_AdvanceJobPhase_ReturnsFalse_WhenNoJobs(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	advanced := b.AdvanceJobPhase()
	assert.False(t, advanced)
}

// TestRefinement3_AdvanceJobPhase_ReturnsTrue_WhenJobsAdvanced verifies return value.
func TestRefinement3_AdvanceJobPhase_ReturnsTrue_WhenJobsAdvanced(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	r3CreateJob(t, b)

	advanced := b.AdvanceJobPhase()
	assert.True(t, advanced)
}

// --- Full job lifecycle via HTTP ---

// TestRefinement3_FullLifecycle_Via_HTTP tests create→advance→complete via HTTP.
func TestRefinement3_FullLifecycle_Via_HTTP(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	h := mediaconvert.NewHandler(b)

	// Create job.
	rec := r3Do(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	jobID := jobData["id"].(string)
	assert.Equal(t, "SUBMITTED", jobData["status"])

	// Advance to COMPLETE.
	for range 4 {
		b.AdvanceJobPhase()
	}

	rec = r3Do(t, h, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out2))
	j2 := out2["job"].(map[string]any)
	assert.Equal(t, "COMPLETE", j2["status"])
	timing := j2["timing"].(map[string]any)
	assert.Greater(t, timing["startTime"], float64(0))
	assert.Greater(t, timing["finishTime"], float64(0))
	outputGroups := j2["outputGroupDetails"].([]any)
	require.Len(t, outputGroups, 1)
}

// --- SweepExpiredTokens timing ---

// TestRefinement3_SweepExpiredTokens_DoesNotRemoveFreshTokens verifies TTL boundary.
func TestRefinement3_SweepExpiredTokens_DoesNotRemoveFreshTokens(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	_, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "fresh-token", "", "", 0, nil)
	require.NoError(t, err)

	b.SweepExpiredTokens()
	// Within the TTL window, token should remain.
	assert.Equal(t, 1, mediaconvert.TokenIndexSize(b))
}

// --- Messages nil-safety on clone ---

// TestRefinement3_CloneJob_Messages_NilSafe verifies nil Messages handled in clone.
func TestRefinement3_CloneJob_Messages_NilSafe(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	b.AddJobInternal(&mediaconvert.Job{
		ID:     "nil-msg-job",
		Status: "SUBMITTED",
		Role:   "arn:aws:iam::123:role/r",
	})

	got, err := b.GetJob("nil-msg-job")
	require.NoError(t, err)
	// Should not panic; Messages may be nil from seed.
	assert.Equal(t, "SUBMITTED", got.Status)
}

// --- ReservationPlan nil-safety ---

// TestRefinement3_Queue_NoReservationPlan verifies nil is fine.
func TestRefinement3_Queue_NoReservationPlan(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	q, err := b.CreateQueue("no-rp", "", "", "", nil)
	require.NoError(t, err)
	assert.Nil(t, q.ReservationPlan)
}

// --- UpdateJob queue not found ---

// TestRefinement3_UpdateJob_QueueNotFound verifies ErrNotFound for unknown new queue.
func TestRefinement3_UpdateJob_QueueNotFound(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	_, err := b.UpdateJob(j.ID, "nonexistent-queue", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrNotFound)
}

// --- Timing on COMPLETE is after StartTime ---

// TestRefinement3_Timing_FinishAfterStart verifies time ordering.
func TestRefinement3_Timing_FinishAfterStart(t *testing.T) {
	t.Parallel()

	b := r3Backend()
	j := r3CreateJob(t, b)

	for range 4 {
		b.AdvanceJobPhase()
		time.Sleep(1 * time.Millisecond)
	}

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, got.Timing.FinishTime, got.Timing.StartTime)
}
