package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestPolicyGenerationLifecycle verifies Start/Get/Cancel/List policy generation.
func TestPolicyGenerationLifecycle(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)

	tests := []struct {
		runFn   func() *httptest.ResponseRecorder
		checkFn func(t *testing.T, rec *httptest.ResponseRecorder)
		name    string
	}{
		{
			name: "start_policy_generation",
			runFn: func() *httptest.ResponseRecorder {
				return doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/MyRole"},
				})
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["jobId"])
			},
		},
		{
			name: "list_policy_generations",
			runFn: func() *httptest.ResponseRecorder {
				// start one first
				doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})

				return doRequest(t, h, http.MethodGet, "/policy/generation", nil)
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["policyGenerations"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := tt.runFn()
			tt.checkFn(t, rec)
		})
	}
}

// TestGetGeneratedPolicy verifies getting a generated policy by jobId.
func TestGetGeneratedPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		missingJob bool
	}{
		{name: "existing_job", wantStatus: http.StatusOK},
		{name: "missing_job", wantStatus: http.StatusNotFound, missingJob: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)

			jobID := "missing-job-id"

			if !tt.missingJob {
				rec := doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				jobID = resp["jobId"]
			}

			rec := doRequest(t, h, http.MethodGet, "/policy/generation/"+jobID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			// types.JobDetails (GetGeneratedPolicyOutput.jobDetails) has no
			// principalArn member -- that value only appears under
			// generatedPolicyResult.properties.principalArn for this operation.
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobDetails, ok := resp["jobDetails"].(map[string]any)
			require.True(t, ok)
			_, hasPrincipalArn := jobDetails["principalArn"]
			assert.False(t, hasPrincipalArn, "jobDetails must not carry principalArn")

			result, ok := resp["generatedPolicyResult"].(map[string]any)
			require.True(t, ok)
			properties, ok := result["properties"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:iam::000000000000:role/R", properties["principalArn"])
		})
	}
}

// TestCancelPolicyGeneration verifies PUT /policy/generation/{jobId} cancels job.
func TestCancelPolicyGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		missingJob bool
	}{
		{name: "cancel_existing", wantStatus: http.StatusOK},
		{name: "cancel_missing", wantStatus: http.StatusNotFound, missingJob: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)

			jobID := "no-such-job"

			if !tt.missingJob {
				rec := doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				jobID = resp["jobId"]
			}

			rec := doRequest(t, h, http.MethodPut, "/policy/generation/"+jobID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
