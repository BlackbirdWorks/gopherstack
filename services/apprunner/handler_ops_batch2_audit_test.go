package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestBatch2_PauseService_StateGuard verifies that PauseService rejects
// services not in RUNNING state. AWS returns InvalidStateException for
// pause attempts on already-PAUSED services.
func TestBatch2_PauseService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		wantCode int
		wantType string
	}{
		{
			name:     "pause RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "pause PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// TestBatch2_ResumeService_StateGuard verifies that ResumeService rejects
// services not in PAUSED state. AWS returns InvalidStateException for
// resume attempts on RUNNING services.
func TestBatch2_ResumeService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		wantCode int
		wantType string
	}{
		{
			name: "resume PAUSED service succeeds",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "resume RUNNING service returns InvalidStateException",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// TestBatch2_UpdateService_StateGuard verifies that UpdateService rejects
// services not in RUNNING state. AWS returns InvalidStateException for
// update attempts on PAUSED services.
func TestBatch2_UpdateService_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		wantCode int
		wantType string
	}{
		{
			name:     "update RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "update PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "UpdateService", map[string]any{
				"ServiceArn":            arn,
				"InstanceConfiguration": map[string]any{"Cpu": "2 vCPU", "Memory": "4 GB"},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// TestBatch2_StartDeployment_StateGuard verifies that StartDeployment rejects
// services not in RUNNING state. AWS returns InvalidStateException for
// deployment attempts on PAUSED services.
func TestBatch2_StartDeployment_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		wantCode int
		wantType string
	}{
		{
			name:     "deploy RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "deploy PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "StartDeployment", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}
