package emrserverless_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// --- Reset ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("111111111111", "us-west-2")
	_, err := b.CreateApplication("app1", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)
	require.Equal(t, 1, emrserverless.ApplicationCount(b))

	b.Reset()

	assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	assert.Equal(t, 0, emrserverless.JobRunCount(b))
	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 0, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("111111111111", "us-west-2")

	for range 3 {
		_, err := b.CreateApplication("app-cycle", "SPARK", "emr-6.6.0", nil)
		require.NoError(t, err)
		b.Reset()
		assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createApp(t, h, "handler-reset-app")

	h.Reset()

	rec := doRequest(t, h, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	apps := out["applications"].([]any)
	assert.Empty(t, apps)
}

// --- Provider ---

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &emrserverless.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emrserverless.ErrNilAppContext)
}

// --- GetSupportedOperations ---

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
		"StartApplication",
		"StopApplication",
		"StartJobRun",
		"GetJobRun",
		"ListJobRuns",
		"CancelJobRun",
		"GetDashboardForJobRun",
		"ListJobRunAttempts",
		"GetResourceDashboard",
		"StartSession",
		"GetSession",
		"ListSessions",
		"TerminateSession",
		"GetSessionEndpoint",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}

	assert.Len(t, ops, len(expected))

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

// --- Seed helpers + Export count helpers ---

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	now := time.Now().UTC()

	appID := "app-seed-1"
	app := &emrserverless.Application{
		ApplicationID: appID,
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
		Name:          "seed-app",
		Type:          "SPARK",
		ReleaseLabel:  "emr-6.6.0",
		State:         emrserverless.ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          map[string]string{"env": "test"},
	}
	b.AddApplicationInternal(app)

	jr := &emrserverless.JobRun{
		ApplicationID:    appID,
		JobRunID:         "jr-seed-1",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID + "/jobruns/jr-seed-1",
		Name:             "seed-run",
		State:            emrserverless.JobRunStateRunning,
		ExecutionRoleArn: "arn:aws:iam::000000000000:role/emr-role",
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	b.AddJobRunInternal(jr)

	assert.Equal(t, 1, emrserverless.ApplicationCount(b))
	assert.Equal(t, 1, emrserverless.JobRunCount(b))
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	assert.Equal(t, 0, emrserverless.JobRunCount(b))

	_, err := b.CreateApplication("count-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, emrserverless.ApplicationCount(b))

	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 1, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

// --- ErrValidation mapping ---

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       map[string]any{"type": "SPARK", "releaseLabel": "emr-6.6.0"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationException",
		},
		{
			name:       "missing_type",
			body:       map[string]any{"name": "myapp", "releaseLabel": "emr-6.6.0"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/applications", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]string
			mustUnmarshal(t, rec, &out)
			assert.Equal(t, tt.wantCode, out["code"])
		})
	}
}

// --- ErrInvalidState mapping ---

func TestRefinement1_ErrInvalidStateMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "state-check-app")

	// Start the application so it's in STARTED state.
	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/start", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Deleting a STARTED application should fail with 400.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+appID, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "RequestFailedException", out["code"])
}

// --- CreateApplication validation ---

func TestRefinement1_CreateApplication_RequiresName(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateApplication("", "SPARK", "emr-6.6.0", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emrserverless.ErrValidation)
}

func TestRefinement1_CreateApplication_RequiresType(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateApplication("my-app", "", "emr-6.6.0", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emrserverless.ErrValidation)
}

// --- StartJobRun validation ---

func TestRefinement1_StartJobRun_RequiresExecutionRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "exec-role-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"name": "no-role-run",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "ValidationException", out["code"])
}

// --- CancelJobRun state check ---

func TestRefinement1_CancelJobRun_RejectsTerminalState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
	}{
		{"already_cancelled", emrserverless.JobRunStateCancelled},
		{"already_succeeded", emrserverless.JobRunStateSuccess},
		{"already_failed", emrserverless.JobRunStateFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
			appID := "app-cancel-state"
			now := time.Now().UTC()

			app := &emrserverless.Application{
				ApplicationID: appID,
				Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
				Name:          "cancel-state-app",
				Type:          "SPARK",
				State:         emrserverless.ApplicationStateStarted,
				CreatedAt:     now,
				UpdatedAt:     now,
			}
			b.AddApplicationInternal(app)

			jr := &emrserverless.JobRun{
				ApplicationID:    appID,
				JobRunID:         "jr-terminal",
				Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID + "/jobruns/jr-terminal",
				Name:             "terminal-run",
				State:            tt.state,
				ExecutionRoleArn: "arn:aws:iam::000000000000:role/role",
				CreatedAt:        now,
				UpdatedAt:        now,
			}
			b.AddJobRunInternal(jr)

			_, err := b.CancelJobRun(appID, "jr-terminal")
			require.Error(t, err)
			assert.ErrorIs(t, err, emrserverless.ErrInvalidState)
		})
	}
}

// --- DeleteApplication state check ---

func TestRefinement1_DeleteApplication_RejectsStarted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "delete-started-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/start", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/applications/"+appID, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// App should still exist.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement1_DeleteApplication_AllowsStopped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "delete-stopped-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/start", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+appID+"/stop", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/applications/"+appID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Non-nil tags in returned resources ---

func TestRefinement1_NonNilTags_Application(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("no-tag-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)
	assert.NotNil(t, app.Tags)
}

func TestRefinement1_NonNilTags_JobRun(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateApplication("jr-notag-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)
	apps, _ := b.ListApplications("", 0)
	require.Len(t, apps, 1)
	appID := apps[0].ApplicationID

	jr, err := b.StartJobRun(appID, "arn:aws:iam::000000000000:role/r", "test-run", nil)
	require.NoError(t, err)
	assert.NotNil(t, jr.Tags)
}

// --- applicationToMap / jobRunToMap always include tags ---

func TestRefinement1_ApplicationToMap_AlwaysIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "map-tags-app")

	rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	app := out["application"].(map[string]any)
	_, hasTags := app["tags"]
	assert.True(t, hasTags, "application response should always include 'tags' key")
}

func TestRefinement1_JobRunToMap_AlwaysIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "jr-map-tags-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jr := out["jobRun"].(map[string]any)
	_, hasTags := jr["tags"]
	assert.True(t, hasTags, "jobRun response should always include 'tags' key")
}

// --- Sorted ListApplications ---

func TestRefinement1_SortedListApplications(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")

	// Insert via seed helper with deterministic IDs.
	now := time.Now().UTC()

	for _, id := range []string{"zzz", "aaa", "mmm"} {
		b.AddApplicationInternal(&emrserverless.Application{
			ApplicationID: id,
			Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + id,
			Name:          "app-" + id,
			Type:          "SPARK",
			State:         emrserverless.ApplicationStateCreated,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
	}

	apps, _ := b.ListApplications("", 0)
	require.Len(t, apps, 3)
	assert.Equal(t, "aaa", apps[0].ApplicationID)
	assert.Equal(t, "mmm", apps[1].ApplicationID)
	assert.Equal(t, "zzz", apps[2].ApplicationID)
}

// --- Persistence round-trip ---

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("persist-app", "SPARK", "emr-6.6.0", map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "persist-run", nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, emrserverless.ApplicationCount(b2))
	assert.Equal(t, 1, emrserverless.JobRunCount(b2))

	got, err := b2.GetApplication(app.ApplicationID)
	require.NoError(t, err)
	assert.Equal(t, app.Name, got.Name)
	assert.Equal(t, "v", got.Tags["k"])
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 0, emrserverless.ApplicationCount(b2))
	assert.Equal(t, 0, emrserverless.JobRunCount(b2))
}

// --- ARN indexes stay consistent after Reset ---

func TestRefinement1_ARNIndexesConsistentAfterReset(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("arn-idx-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)

	_, err = b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run1", nil)
	require.NoError(t, err)

	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 1, appARNs)
	assert.Equal(t, 1, jobRunARNs)

	b.Reset()

	appARNs, jobRunARNs = emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 0, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

// --- ErrNilAppContext sentinel ---

func TestRefinement1_ErrNilAppContextValue(t *testing.T) {
	t.Parallel()

	// Confirm the sentinel is a non-nil error value.
	require.Error(t, emrserverless.ErrNilAppContext)
}

// --- CancelJobRun allows running state ---

func TestRefinement1_CancelJobRun_AllowsRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "cancel-running-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, appID, out["applicationId"])
	assert.Equal(t, jobRunID, out["jobRunId"])
}

// --- ListJobRuns is sorted ---

func TestRefinement1_SortedListJobRuns(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("sorted-runs-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)

	now := time.Now().UTC()

	for _, runID := range []string{"zzz-run", "aaa-run", "mmm-run"} {
		arnVal := "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" +
			app.ApplicationID + "/jobruns/" + runID
		b.AddJobRunInternal(&emrserverless.JobRun{
			ApplicationID:    app.ApplicationID,
			JobRunID:         runID,
			Arn:              arnVal,
			Name:             "run-" + runID,
			State:            emrserverless.JobRunStateRunning,
			ExecutionRoleArn: "arn:aws:iam::000000000000:role/r",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
	}

	runs, _, err := b.ListJobRuns(app.ApplicationID, "", 0)
	require.NoError(t, err)
	require.Len(t, runs, 3)
	assert.Equal(t, "aaa-run", runs[0].JobRunID)
	assert.Equal(t, "mmm-run", runs[1].JobRunID)
	assert.Equal(t, "zzz-run", runs[2].JobRunID)
}

// --- ListTagsForResource returns stable result ---

func TestRefinement1_ListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("tags-empty-app", "SPARK", "emr-6.6.0", nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}

// --- Seed helper ensures non-nil tags in stored resource ---

func TestRefinement1_SeedHelper_NonNilTags(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	appID := "seed-nil-tags"
	now := time.Now().UTC()

	app := &emrserverless.Application{
		ApplicationID: appID,
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
		Name:          "nil-tags-app",
		Type:          "SPARK",
		State:         emrserverless.ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          nil, // intentionally nil
	}
	b.AddApplicationInternal(app)

	// TagResource must not panic even when seeded with nil Tags.
	err := b.TagResource(app.Arn, map[string]string{"k": "v"})
	require.NoError(t, err)

	got, err := b.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v", got["k"])
}

// --- Handler Snapshot / Restore delegation ---

func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createApp(t, h, "snap-app")

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := emrserverless.NewHandler(emrserverless.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(snap))

	rec := doRequest(t, h2, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	apps := out["applications"].([]any)
	assert.Len(t, apps, 1)
}
