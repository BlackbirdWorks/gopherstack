package mediaconvert_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// createTestJob creates a job via HTTP (role only, no queue) and returns its ID.
func createTestJob(t *testing.T, h *mediaconvert.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":     "arn:aws:iam::" + testAccountID + ":role/MediaConvert_Role",
		"settings": map[string]any{"inputs": []any{}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	job, ok := resp["job"].(map[string]any)
	require.True(t, ok)
	id, _ := job["id"].(string)
	require.NotEmpty(t, id)

	return id
}

// createTestJobDirect creates a job directly via the backend (bypassing HTTP) and returns it.
func createTestJobDirect(t *testing.T, b *mediaconvert.InMemoryBackend) *mediaconvert.Job {
	t.Helper()

	j, err := b.CreateJob("arn:aws:iam::123:role/role", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	return j
}

func TestMediaConvert_ListJobsWithFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListJobs with query params (exercises StartJobsQuery/jobMatchesFilters)
	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs?status=SUBMITTED&maxResults=10", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMediaConvert_Job_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	role := "arn:aws:iam::123456789012:role/MediaConvert_Default_Role"

	// Create job
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": role,
		"settings": map[string]any{
			"outputGroups": []any{},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	jobData, ok := createResp["job"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, role, jobData["role"])
	assert.Equal(t, "SUBMITTED", jobData["status"])
	assert.NotEmpty(t, jobData["id"])
	assert.NotEmpty(t, jobData["arn"])

	jobID, _ := jobData["id"].(string)

	// Get job
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID)

	// List jobs
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID)

	// Cancel job
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+jobID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify job is canceled (still exists but status changed)
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CANCELED")
}

func TestMediaConvert_Job_MissingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"queue": "arn:aws:mediaconvert:us-east-1:123456789012:queues/Default",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaConvert_Job_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCreateJob_WithTags verifies tags are stored at creation time.
func TestCreateJob_WithTags(t *testing.T) {
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

// TestCancelJob_InvalidStatus verifies only SUBMITTED/PROGRESSING jobs can be canceled.
func TestCancelJob_InvalidStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/role", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	require.NoError(t, b.CancelJob(j.ID))
	require.ErrorIs(t, b.CancelJob(j.ID), mediaconvert.ErrValidation)
}

// TestListJobs_SortedNewestFirst verifies jobs are sorted by creation time.
func TestListJobs_SortedNewestFirst(t *testing.T) {
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

// TestCreateJob_InitialStatusSubmitted verifies initial job status is SUBMITTED.
func TestCreateJob_InitialStatusSubmitted(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "SUBMITTED", j.Status)
}

// TestCancelJob_StatusBecomesCanceled verifies job status after cancel.
func TestCancelJob_StatusBecomesCanceled(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, b.CancelJob(j.ID))

	j2, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELED", j2.Status)
}

// TestCreateJob_MissingRole verifies role validation at backend level.
func TestCreateJob_MissingRole(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("", "", "", nil, nil, nil, "")
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestCreateJob_WithUserMetadata verifies userMetadata is stored.
func TestCreateJob_WithUserMetadata(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil,
		map[string]string{"key1": "val1"}, "")
	require.NoError(t, err)
	assert.Equal(t, "val1", j.UserMetadata["key1"])

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "val1", got.UserMetadata["key1"])
}

// TestCreateJob_TimingSubmitTimeSet verifies SubmitTime is set.
func TestCreateJob_TimingSubmitTimeSet(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	require.NotNil(t, j.Timing)
	assert.Greater(t, j.Timing.SubmitTime, float64(0))
}

// TestCreateJob_BillingTagsSourceStored verifies field is stored.
func TestCreateJob_BillingTagsSourceStored(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "JOB")
	require.NoError(t, err)
	assert.Equal(t, "JOB", j.BillingTagsSource)
}

// TestCreateJob_QueueArnSet verifies QueueArn is set when queue exists.
func TestCreateJob_QueueArnSet(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	q, err := b.CreateQueue("my-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "my-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	assert.Equal(t, q.Arn, j.QueueArn)
	assert.Equal(t, "my-q", j.Queue)
}

// TestCreateJob_UnknownQueueBackend returns ErrNotFound.
func TestCreateJob_UnknownQueueBackend(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "no-such-queue", "", nil, nil, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestListJobs_TotalCountInResponse verifies totalCount in list response.
func TestListJobs_TotalCountInResponse(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)

	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	_, err = b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(2), out["totalCount"], 0)
}

// TestCloneJob_UserMetadataIndependent ensures cloneJob deep-copies userMetadata.
func TestCloneJob_UserMetadataIndependent(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil,
		map[string]string{"a": "1"}, "")
	require.NoError(t, err)

	j.UserMetadata["a"] = "mutated"

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, "1", got.UserMetadata["a"])
}

// TestCloneJob_TimingIndependent ensures Timing is deep-copied.
func TestCloneJob_TimingIndependent(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)

	j.Timing.SubmitTime = 0

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Timing)
	assert.Greater(t, got.Timing.SubmitTime, float64(0))
}

// TestCreateJob_DefaultAccelerationStatus verifies default AccelerationStatus is set.
func TestCreateJob_DefaultAccelerationStatus(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "NOT_APPLICABLE", j.AccelerationStatus)
}

// TestCreateJob_TimingViaHTTP verifies timing in HTTP response.
func TestCreateJob_TimingViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	job := out["job"].(map[string]any)
	timing := job["timing"].(map[string]any)
	assert.Greater(t, timing["submitTime"], float64(0))
}

// TestCreateJob_UnknownQueueViaHTTP returns 404 when queue not found.
func TestCreateJob_UnknownQueueViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r", "queue": "missing"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestJob_ErrorStatusFieldPresence verifies ERROR is a valid job status value.
func TestJob_ErrorStatusFieldPresence(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestUpdateJob_Priority verifies priority change via UpdateJob.
func TestUpdateJob_Priority(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)
	require.Equal(t, 0, j.Priority)

	newPri := 10
	updated, err := b.UpdateJob(j.ID, "", &newPri, nil)
	require.NoError(t, err)
	assert.Equal(t, 10, updated.Priority)

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Equal(t, 10, got.Priority)
}

// TestUpdateJob_QueueChangeRecordsTransition verifies queue transition is recorded.
func TestUpdateJob_QueueChangeRecordsTransition(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestUpdateJob_HopDestinations verifies hop destinations are updated.
func TestUpdateJob_HopDestinations(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)

	hops := []mediaconvert.HopDestination{
		{WaitMinutes: 5, Priority: 2, Queue: "fallback-q"},
	}

	updated, err := b.UpdateJob(j.ID, "", nil, hops)
	require.NoError(t, err)
	require.Len(t, updated.HopDestinations, 1)
	assert.Equal(t, 5, updated.HopDestinations[0].WaitMinutes)
	assert.Equal(t, "fallback-q", updated.HopDestinations[0].Queue)
}

// TestUpdateJob_NotFound returns ErrNotFound for unknown job.
func TestUpdateJob_NotFound(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.UpdateJob("nonexistent-id", "", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrNotFound)
}

// TestUpdateJob_InvalidPriority returns ErrValidation for out-of-range priority.
func TestUpdateJob_InvalidPriority(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)

	badPri := 999
	_, err := b.UpdateJob(j.ID, "", &badPri, nil)
	require.ErrorIs(t, err, mediaconvert.ErrValidation)
}

// TestUpdateJob_ViaHTTP verifies PUT /jobs/{id} returns 200.
func TestUpdateJob_ViaHTTP(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)
	j := createTestJobDirect(t, b)

	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/jobs/"+j.ID,
		map[string]any{"priority": 5})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData, ok := out["job"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(5), jobData["priority"], 0)
}

// TestUpdateJob_NotFoundViaHTTP returns 404 for unknown job.
func TestUpdateJob_NotFoundViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/jobs/no-such-job",
		map[string]any{"priority": 1})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCreateJob_HopDestinations verifies hop destinations stored at creation.
func TestCreateJob_HopDestinations(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCreateJob_HopDestinationsViaHTTP verifies JSON parsing.
func TestCreateJob_HopDestinationsViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
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

// TestCreateJob_AccelerationEnabled verifies mode=ENABLED sets status=PREFERRED.
func TestCreateJob_AccelerationEnabled(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "ENABLED", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "PREFERRED", j.AccelerationStatus)
	require.NotNil(t, j.AccelerationSettings)
	assert.Equal(t, "ENABLED", j.AccelerationSettings.Mode)
}

// TestCreateJob_AccelerationDisabled verifies mode=DISABLED keeps NOT_APPLICABLE.
func TestCreateJob_AccelerationDisabled(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "DISABLED", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "NOT_APPLICABLE", j.AccelerationStatus)
}

// TestCreateJob_AccelerationViaHTTP verifies JSON input parsing.
func TestCreateJob_AccelerationViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
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

// TestUpdateJob_QueueTransitionsAccumulate verifies multiple transitions accumulate.
func TestUpdateJob_QueueTransitionsAccumulate(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestUpdateJob_SameQueueNoTransition verifies no new transition when queue unchanged.
func TestUpdateJob_SameQueueNoTransition(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateQueue("unchanged-q", "", "", "", nil)
	require.NoError(t, err)

	j, err := b.CreateJob("arn:aws:iam::123:role/r", "unchanged-q", "", nil, nil, nil, "")
	require.NoError(t, err)

	updated, err := b.UpdateJob(j.ID, "unchanged-q", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, updated.QueueTransitions)
}

// TestCreateJob_MessagesInitialized verifies Messages non-nil at creation.
func TestCreateJob_MessagesInitialized(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)
	require.NotNil(t, j.Messages)
	assert.NotNil(t, j.Warnings)
}

// TestCreateJob_WarningsInitialized verifies Warnings non-nil at creation.
func TestCreateJob_WarningsInitialized(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)
	assert.NotNil(t, j.Warnings)
}

// TestCreateJob_MessagesFieldViaHTTP verifies messages field appears in response.
func TestCreateJob_MessagesFieldViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs",
		map[string]any{"role": "arn:aws:iam::123:role/r"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	_, hasMessages := jobData["messages"]
	assert.True(t, hasMessages, "messages field should be present")
}

// TestCreateJobFull_ClientRequestTokenDedup verifies same token returns same job.
func TestCreateJobFull_ClientRequestTokenDedup(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCreateJobFull_DifferentTokensCreateDistinctJobs verifies different tokens create distinct jobs.
func TestCreateJobFull_DifferentTokensCreateDistinctJobs(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	j1, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "token-one", "", "", 0, nil)
	require.NoError(t, err)

	j2, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "token-two", "", "", 0, nil)
	require.NoError(t, err)

	assert.NotEqual(t, j1.ID, j2.ID)
	assert.Equal(t, 2, mediaconvert.JobCount(b))
}

// TestCreateJobFull_ClientRequestTokenStored verifies token field on job.
func TestCreateJobFull_ClientRequestTokenStored(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	token := "my-request-token"

	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", token, "", "", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, token, j.ClientRequestToken)
}

// TestCreateJobFull_TokenIndexGrows verifies token is indexed.
func TestCreateJobFull_TokenIndexGrows(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCreateJob_EmptyTokenNotIndexed verifies no token entry for empty token.
func TestCreateJob_EmptyTokenNotIndexed(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, 0, mediaconvert.TokenIndexSize(b))
}

// TestCreateJob_ClientRequestTokenDedupViaHTTP verifies token dedup over HTTP.
func TestCreateJob_ClientRequestTokenDedupViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	body := map[string]any{
		"role":               "arn:aws:iam::123:role/r",
		"clientRequestToken": "http-token-xyz",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&out1))
	id1 := out1["job"].(map[string]any)["id"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", body)
	require.Equal(t, http.StatusCreated, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out2))
	id2 := out2["job"].(map[string]any)["id"].(string)

	assert.Equal(t, id1, id2, "duplicate token should return same job ID")
}

// TestCreateJobFull_TokenDedupNoOverflow verifies the token index grows one
// entry per unique token and dedups repeats without adding new entries.
func TestCreateJobFull_TokenDedupNoOverflow(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateJobFull(
		"arn:aws:iam::"+testAccountID+":role/Role",
		"", "", map[string]any{}, nil, nil,
		"", "tok-a", "", "", 0, nil,
	)
	require.NoError(t, err)

	_, err = b.CreateJobFull(
		"arn:aws:iam::"+testAccountID+":role/Role",
		"", "", map[string]any{}, nil, nil,
		"", "tok-b", "", "", 0, nil,
	)
	require.NoError(t, err)

	assert.Equal(t, 2, mediaconvert.TokenIndexSize(b))

	// Token dedup: same token returns same job.
	j1, err := b.CreateJobFull(
		"arn:aws:iam::"+testAccountID+":role/Role",
		"", "", map[string]any{}, nil, nil,
		"", "tok-a", "", "", 0, nil,
	)
	require.NoError(t, err)

	// Token index should still be 2 (dedup, no new entry).
	assert.Equal(t, 2, mediaconvert.TokenIndexSize(b))
	assert.NotNil(t, j1)
}

// TestCreateJob_JobEngineVersionUsedAlwaysSet verifies JobEngineVersionUsed is always set.
func TestCreateJob_JobEngineVersionUsedAlwaysSet(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJob("arn:aws:iam::123:role/r", "", "", nil, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionUsed)
}

// TestCreateJobFull_JobEngineVersionRequestedStored verifies requested version stored.
func TestCreateJobFull_JobEngineVersionRequestedStored(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j, err := b.CreateJobFull("arn:aws:iam::123:role/r", "", "", nil, nil, nil,
		"", "", "", "2017-08-29", 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionRequested)
	assert.Equal(t, "2017-08-29", j.JobEngineVersionUsed)
}

// TestCreateJob_JobEngineVersionViaHTTP verifies field parsed from HTTP
// body. The real CreateJobInput wire field is "jobEngineVersion" (the
// response Job resource echoes it back as "jobEngineVersionRequested" --
// request and response field names differ).
func TestCreateJob_JobEngineVersionViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":             "arn:aws:iam::123:role/r",
		"jobEngineVersion": "2017-08-29",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	assert.Equal(t, "2017-08-29", jobData["jobEngineVersionUsed"])
	assert.Equal(t, "2017-08-29", jobData["jobEngineVersionRequested"])
}

// TestCreateJob_DefaultShareStatusNotShared verifies initial ShareStatus.
func TestCreateJob_DefaultShareStatusNotShared(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)
	assert.Equal(t, "NOT_SHARED", j.ShareStatus)
}

// TestCancelJob_SetsFinishTime verifies FinishTime is set on cancel.
func TestCancelJob_SetsFinishTime(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)
	require.Zero(t, j.Timing.FinishTime)

	require.NoError(t, b.CancelJob(j.ID))

	got, err := b.GetJob(j.ID)
	require.NoError(t, err)
	assert.Greater(t, got.Timing.FinishTime, float64(0))
}

// TestCancelJob_DecrementsQueueSubmittedCounter verifies counter decremented.
func TestCancelJob_DecrementsQueueSubmittedCounter(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCancelJob_NilTimingHandledSafely verifies no panic if Timing nil.
func TestCancelJob_NilTimingHandledSafely(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCreateJob_PriorityViaHTTP verifies priority parsed from body.
func TestCreateJob_PriorityViaHTTP(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role":     "arn:aws:iam::123:role/r",
		"priority": 15,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	jobData := out["job"].(map[string]any)
	assert.InDelta(t, float64(15), jobData["priority"], 0)
}

// TestCloneJob_HopDestinationsIndependent verifies deep copy.
func TestCloneJob_HopDestinationsIndependent(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCloneJob_QueueTransitionsIndependent verifies deep copy.
func TestCloneJob_QueueTransitionsIndependent(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestCloneJob_NilMessagesSafe verifies nil Messages handled in clone.
func TestCloneJob_NilMessagesSafe(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestUpdateJob_QueueNotFound verifies ErrNotFound for unknown new queue.
func TestUpdateJob_QueueNotFound(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	j := createTestJobDirect(t, b)

	_, err := b.UpdateJob(j.ID, "nonexistent-queue", nil, nil)
	require.ErrorIs(t, err, mediaconvert.ErrNotFound)
}

// TestUpdateJob_QueueCounterAdjustedOnQueueChange verifies counters updated on queue change.
func TestUpdateJob_QueueCounterAdjustedOnQueueChange(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
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

// TestListJobsFiltered_StatusFilter verifies status filtering at the backend level.
func TestListJobsFiltered_StatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
		wantCount    int
	}{
		{name: "all_jobs", statusFilter: "", wantCount: 2},
		{name: "submitted_only", statusFilter: "SUBMITTED", wantCount: 2},
		{name: "complete_only", statusFilter: "COMPLETE", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

			createTestJob(t, mediaconvert.NewHandler(b))
			createTestJob(t, mediaconvert.NewHandler(b))

			jobs := b.ListJobsFiltered(tt.statusFilter, "", "")
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

// TestListJobsFiltered_QueueFilter verifies queue filtering at the backend level.
func TestListJobsFiltered_QueueFilter(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)

	// Create two queues.
	q1, err := b.CreateQueue("queue-alpha", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateQueue("queue-beta", "", "", "", nil)
	require.NoError(t, err)

	// Create jobs in queue-alpha only.
	for range 3 {
		rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
			"role":     "arn:aws:iam::" + testAccountID + ":role/MediaConvert_Role",
			"queue":    q1.Arn,
			"settings": map[string]any{},
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name      string
		queue     string
		wantCount int
	}{
		// Jobs created with queue ARN: j.Queue == ARN, j.QueueArn == ARN
		{name: "by_arn", queue: q1.Arn, wantCount: 3},
		{name: "no_match_beta_arn", queue: "queue-beta", wantCount: 0},
		{name: "all", queue: "", wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			jobs := b.ListJobsFiltered("", tt.queue, "")
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

// TestListJobsFiltered_OrderAscending verifies ascending/descending ordering at the backend level.
func TestListJobsFiltered_OrderAscending(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	h := mediaconvert.NewHandler(b)

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
			"role":     fmt.Sprintf("arn:aws:iam::%s:role/MediaConvert_Role_%d", testAccountID, i),
			"settings": map[string]any{},
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	descJobs := b.ListJobsFiltered("", "", "DESCENDING")
	ascJobs := b.ListJobsFiltered("", "", "ASCENDING")

	require.Len(t, descJobs, 3)
	require.Len(t, ascJobs, 3)

	assert.GreaterOrEqual(t, descJobs[0].CreatedAt, descJobs[1].CreatedAt)
	assert.LessOrEqual(t, ascJobs[0].CreatedAt, ascJobs[1].CreatedAt)
}

// TestListJobs_Pagination verifies maxResults/nextToken pagination via HTTP.
func TestListJobs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxResults  string
		totalJobs   int
		wantFirst   int
		wantHasNext bool
	}{
		{name: "no_pagination", totalJobs: 3, maxResults: "", wantFirst: 3, wantHasNext: false},
		{name: "page_size_2", totalJobs: 5, maxResults: "2", wantFirst: 2, wantHasNext: true},
		{name: "exact_fit", totalJobs: 3, maxResults: "3", wantFirst: 3, wantHasNext: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.totalJobs {
				createTestJob(t, h)
			}

			path := "/2017-08-29/jobs"
			if tt.maxResults != "" {
				path += "?maxResults=" + tt.maxResults
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			jobs, ok := out["jobs"].([]any)
			require.True(t, ok)
			assert.Len(t, jobs, tt.wantFirst)

			_, hasNext := out["nextToken"]
			assert.Equal(t, tt.wantHasNext, hasNext)
		})
	}
}
