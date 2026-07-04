package emrserverless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// createAppWithArch creates an application via the handler with optional architecture.
func createAppWithArch(t *testing.T, h *emrserverless.Handler, name, releaseLabel, architecture string) string {
	t.Helper()

	body := map[string]any{
		"name":         name,
		"type":         "SPARK",
		"releaseLabel": releaseLabel,
	}

	if architecture != "" {
		body["architecture"] = architecture
	}

	rec := doRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	id := out["applicationId"]
	require.NotEmpty(t, id)

	return id
}

// startJobRunWithMode starts a job run via the handler with optional mode.
func startJobRunWithMode(t *testing.T, h *emrserverless.Handler, appID, mode string) string {
	t.Helper()

	body := map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
	}

	if mode != "" {
		body["mode"] = mode
	}

	rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/applications/%s/jobruns", appID), body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	id := out["jobRunId"]
	require.NotEmpty(t, id)

	return id
}

// TestParity_StartJobRun_DefaultsModeToBATCH verifies omitting mode yields mode="BATCH" in GetJobRun,
// matching real AWS EMR Serverless behavior.
func TestParity_StartJobRun_DefaultsModeToBATCH(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mode         string
		expectedMode string
	}{
		{name: "no_mode_field", mode: "", expectedMode: "BATCH"},
		{name: "explicit_streaming", mode: "STREAMING", expectedMode: "STREAMING"},
		{name: "explicit_batch", mode: "BATCH", expectedMode: "BATCH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createAppWithArch(t, h, "mode-test-app", "emr-6.9.0", "")
			jrID := startJobRunWithMode(t, h, appID, tt.mode)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/applications/%s/jobruns/%s", appID, jrID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jr, _ := out["jobRun"].(map[string]any)
			assert.Equal(t, tt.expectedMode, jr["mode"])
		})
	}
}

// TestParity_CreateApplication_Architecture verifies architecture is stored and returned by GetApplication.
func TestParity_CreateApplication_Architecture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		architecture string
		wantInOutput bool
	}{
		{name: "arm64", architecture: "ARM64", wantInOutput: true},
		{name: "x86_64", architecture: "X86_64", wantInOutput: true},
		{name: "omitted", architecture: "", wantInOutput: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createAppWithArch(t, h, "arch-app", "emr-6.9.0", tt.architecture)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/applications/%s", appID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			app, _ := out["application"].(map[string]any)

			if tt.wantInOutput {
				assert.Equal(t, tt.architecture, app["architecture"])
			} else {
				_, hasArch := app["architecture"]
				assert.False(t, hasArch, "architecture should be absent when not set")
			}
		})
	}
}

// TestParity_StartApplication_RejectsInvalidStates verifies StartApplication returns error
// for states that cannot transition to STARTED.
func TestParity_StartApplication_RejectsInvalidStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fromState string
		wantErr   bool
	}{
		{name: "from_stopped", fromState: emrserverless.ApplicationStateStopped, wantErr: false},
		{name: "from_terminated", fromState: emrserverless.ApplicationStateTerminated, wantErr: true},
		{
			name:      "from_terminated_with_error",
			fromState: emrserverless.ApplicationStateTerminatedWithError,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emrserverless.NewInMemoryBackend("111111111111", "us-east-1")
			app, err := b.CreateApplication("state-test", "SPARK", "emr-6.9.0", "", nil)
			require.NoError(t, err)

			emrserverless.SetApplicationState(b, app.ApplicationID, tt.fromState)

			startErr := b.StartApplication(app.ApplicationID)
			if tt.wantErr {
				assert.Error(t, startErr)
			} else {
				assert.NoError(t, startErr)
			}
		})
	}
}

// TestParity_DeleteApplication_RejectsActiveStates verifies DeleteApplication rejects
// applications that have not been stopped.
func TestParity_DeleteApplication_RejectsActiveStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fromState string
		wantErr   bool
	}{
		{name: "from_stopped", fromState: emrserverless.ApplicationStateStopped, wantErr: false},
		{name: "from_created", fromState: emrserverless.ApplicationStateCreated, wantErr: false},
		{name: "from_started", fromState: emrserverless.ApplicationStateStarted, wantErr: true},
		{name: "from_starting", fromState: emrserverless.ApplicationStateStarting, wantErr: true},
		{name: "from_stopping", fromState: emrserverless.ApplicationStateStopping, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emrserverless.NewInMemoryBackend("111111111111", "us-east-1")
			app, err := b.CreateApplication("del-state-test", "SPARK", "emr-6.9.0", "", nil)
			require.NoError(t, err)

			emrserverless.SetApplicationState(b, app.ApplicationID, tt.fromState)

			delErr := b.DeleteApplication(app.ApplicationID)
			if tt.wantErr {
				assert.Error(t, delErr)
			} else {
				assert.NoError(t, delErr)
			}
		})
	}
}

// TestParity_GetJobRun_HasAttemptAndReleaseLabel verifies GetJobRun response includes
// attempt=0 and releaseLabel inherited from the application.
func TestParity_GetJobRun_HasAttemptAndReleaseLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		releaseLabel string
	}{
		{name: "spark_6_9", releaseLabel: "emr-6.9.0"},
		{name: "spark_7_0", releaseLabel: "emr-7.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID := createAppWithArch(t, h, "jr-rl-app", tt.releaseLabel, "")
			jrID := startJobRunWithMode(t, h, appID, "")

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/applications/%s/jobruns/%s", appID, jrID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jr, _ := out["jobRun"].(map[string]any)

			attempt, hasAttempt := jr["attempt"]
			assert.True(t, hasAttempt, "attempt field must be present")
			assert.EqualValues(t, 0, attempt)
			assert.Equal(t, tt.releaseLabel, jr["releaseLabel"])
		})
	}
}
