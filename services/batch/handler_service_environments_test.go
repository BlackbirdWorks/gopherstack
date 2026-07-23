package batch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_ServiceEnvironment_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler)
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_success",
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name: "create_duplicate",
			setup: func(t *testing.T, h *batch.Handler) {
				t.Helper()
				rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
					"serviceEnvironmentName": "my-senv",
					"serviceEnvironmentType": "SAGEMAKER_TRAINING",
					"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
				"serviceEnvironmentName": "my-senv",
				"serviceEnvironmentType": "SAGEMAKER_TRAINING",
				"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
				"state":                  "ENABLED",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["serviceEnvironmentArn"], "my-senv")
				assert.Equal(t, "my-senv", out["serviceEnvironmentName"])
			}
		})
	}
}

func TestHandler_ServiceEnvironment_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createEnv  string
		deleteEnv  string
		wantStatus int
	}{
		{
			name:       "delete_success",
			wantStatus: http.StatusOK,
			createEnv:  "senv-to-delete",
			deleteEnv:  "senv-to-delete",
		},
		{
			name:       "delete_not_found",
			wantStatus: http.StatusBadRequest,
			createEnv:  "another-senv",
			deleteEnv:  "missing-senv",
		},
		{
			name:       "delete_by_arn",
			wantStatus: http.StatusOK,
			createEnv:  "senv-arn-del",
			deleteEnv:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
				"serviceEnvironmentName": tt.createEnv,
				"serviceEnvironmentType": "SAGEMAKER_TRAINING",
				"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			deleteTarget := tt.deleteEnv
			if tt.name == "delete_by_arn" {
				var createOut map[string]string
				mustUnmarshal(t, rec, &createOut)
				deleteTarget = createOut["serviceEnvironmentArn"]
			}

			delRec := post(t, h, "/v1/deleteserviceenvironment", map[string]any{
				"serviceEnvironment": deleteTarget,
			})
			assert.Equal(t, tt.wantStatus, delRec.Code)
		})
	}
}

func TestHandler_ServiceEnvironment_DefaultState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
		"serviceEnvironmentName": "default-state-senv",
		"serviceEnvironmentType": "SAGEMAKER_TRAINING",
		"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Contains(t, out["serviceEnvironmentArn"], "default-state-senv")
}

// --- Reset tests ---

func TestBatch_DescribeServiceEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		filterNames []string
		wantCount   int
	}{
		{
			name:        "all_environments",
			filterNames: nil,
			wantCount:   2,
		},
		{
			name:        "filtered_by_name",
			filterNames: []string{"senv-first"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, n := range []string{"senv-first", "senv-second"} {
				rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
					"serviceEnvironmentName": n,
					"serviceEnvironmentType": "SAGEMAKER_TRAINING",
					"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filterNames != nil {
				body["serviceEnvironments"] = tt.filterNames
			}

			rec := post(t, h, "/v1/describeserviceenvironments", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			items, _ := out["serviceEnvironments"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- UpdateServiceEnvironment tests ---

func TestBatch_UpdateServiceEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envName    string
		state      string
		createEnv  bool
		wantStatus int
	}{
		{
			name:       "success",
			envName:    "senv-update",
			state:      "DISABLED",
			createEnv:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			envName:    "nonexistent-senv",
			createEnv:  false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_field",
			envName:    "",
			createEnv:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.createEnv {
				rec := post(t, h, "/v1/createserviceenvironment", map[string]any{
					"serviceEnvironmentName": tt.envName,
					"serviceEnvironmentType": "SAGEMAKER_TRAINING",
					"capacityLimits":         []map[string]any{{"capacityUnit": "NUM_INSTANCES", "maxCapacity": 10}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/updateserviceenvironment", map[string]any{
				"serviceEnvironment": tt.envName,
				"state":              tt.state,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- TagsOnNewResourceTypes tests ---
