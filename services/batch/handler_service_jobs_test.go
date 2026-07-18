package batch_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SubmitServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       map[string]any
		name        string
		wantJobName string
		wantStatus  int
	}{
		{
			name:        "valid_submit",
			input:       map[string]any{"serviceJobName": "my-sj", "serviceEnvironment": "se-1"},
			wantStatus:  http.StatusOK,
			wantJobName: "my-sj",
		},
		{
			name:       "missing_name",
			input:      map[string]any{"serviceEnvironment": "se-1"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := post(t, h, "/v1/submitservicejob", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantJobName != "" {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, tt.wantJobName, out["serviceJobName"])
				assert.NotEmpty(t, out["serviceJobArn"])
			}
		})
	}
}

func TestHandler_DescribeServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobName    string
		wantStatus int
		wantFound  bool
	}{
		{name: "found", jobName: "my-sj", wantStatus: http.StatusOK, wantFound: true},
		{name: "missing_id", jobName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var jobID string
			if tt.wantFound {
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"serviceJobName":     tt.jobName,
					"serviceEnvironment": "se-test",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				mustUnmarshal(t, rec, &out)
				arnStr := out["serviceJobArn"].(string)
				parts := strings.Split(arnStr, "/")
				jobID = parts[len(parts)-1]
			}

			rec2 := post(t, h, "/v1/describeservicejob", map[string]any{"serviceJob": jobID})
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantFound {
				var desc map[string]any
				mustUnmarshal(t, rec2, &desc)
				assert.Equal(t, tt.jobName, desc["serviceJobName"])
			}
		})
	}
}

func TestHandler_ListServiceJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		serviceEnv string
		submitEnvs []string
		wantCount  int
	}{
		{
			name:       "list_all",
			submitEnvs: []string{"env-a", "env-b"},
			wantCount:  2,
		},
		{
			name:       "filter_by_env",
			serviceEnv: "env-a",
			submitEnvs: []string{"env-a", "env-b"},
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i, env := range tt.submitEnvs {
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"serviceJobName":     fmt.Sprintf("sj-%d", i),
					"serviceEnvironment": env,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/listservicejobs", map[string]any{
				"serviceEnvironment": tt.serviceEnv,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			items := out["serviceJobs"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

func TestHandler_TerminateServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobID       string
		wantStatus  int
		submitFirst bool
	}{
		{
			name:        "terminate_existing",
			wantStatus:  http.StatusOK,
			submitFirst: true,
		},
		{
			name:       "missing_id",
			jobID:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			jobID:      "nonexistent-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			jobID := tt.jobID

			if tt.submitFirst {
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"serviceJobName":     "sj-term",
					"serviceEnvironment": "se-test",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				mustUnmarshal(t, rec, &out)
				arnStr := out["serviceJobArn"].(string)
				parts := strings.Split(arnStr, "/")
				jobID = parts[len(parts)-1]
			}

			rec := post(t, h, "/v1/terminateservicejob", map[string]any{
				"serviceJob": jobID,
				"reason":     "test termination",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
