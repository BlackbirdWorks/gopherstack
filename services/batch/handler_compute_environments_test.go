package batch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_ComputeEnvironment_CRUD(t *testing.T) {
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
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "test-ce",
					"type":                   "MANAGED",
					"state":                  "ENABLED",
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

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
				"state":                  "ENABLED",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["computeEnvironmentArn"], "test-ce")
				assert.Equal(t, "test-ce", out["computeEnvironmentName"])
			}
		})
	}
}

func TestHandler_DescribeComputeEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantCount  int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantCount: 2, wantStatus: http.StatusOK},
		{name: "describe_one", filter: []string{"ce-1"}, wantCount: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"nonexistent"}, wantCount: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"ce-1", "ce-2"} {
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": name,
					"type":                   "MANAGED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["computeEnvironments"] = tt.filter
			}

			rec := post(t, h, "/v1/describecomputeenvironments", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["computeEnvironments"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_UpdateComputeEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ce         string
		state      string
		wantStatus int
	}{
		{name: "update_success", ce: "test-ce", state: "DISABLED", wantStatus: http.StatusOK},
		{name: "update_not_found", ce: "missing-ce", state: "DISABLED", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
				"state":                  "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
				"computeEnvironment": tt.ce,
				"state":              tt.state,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteComputeEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ce         string
		wantStatus int
	}{
		{name: "delete_success", ce: "test-ce", wantStatus: http.StatusOK},
		{name: "delete_not_found", ce: "missing", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.ce == "test-ce" {
				rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
					"computeEnvironment": "test-ce",
					"state":              "DISABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = post(t, h, "/v1/deletecomputeenvironment", map[string]any{
				"computeEnvironment": tt.ce,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Job Queue tests ---

func TestHandler_ComputeEnvironmentByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "arn-lookup-ce",
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	ceARN := out["computeEnvironmentArn"]

	// Update using ARN instead of name.
	rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
		"computeEnvironment": ceARN,
		"state":              "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by ARN.
	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{ceARN},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut map[string]any
	mustUnmarshal(t, rec, &descOut)
	ces := descOut["computeEnvironments"].([]any)
	require.Len(t, ces, 1)
	assert.Equal(t, "DISABLED", ces[0].(map[string]any)["state"])
}

func TestHandler_DeleteComputeEnvironment_RequiresDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      string
		wantStatus int
	}{
		{name: "delete_enabled_fails", state: "ENABLED", wantStatus: http.StatusBadRequest},
		{name: "delete_disabled_succeeds", state: "DISABLED", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "ce-test",
				"type":                   "MANAGED",
				"state":                  "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.state == "DISABLED" {
				rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
					"computeEnvironment": "ce-test",
					"state":              "DISABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = post(t, h, "/v1/deletecomputeenvironment", map[string]any{
				"computeEnvironment": "ce-test",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeComputeEnvironments_TagsPresentNoTags verifies that
// DescribeComputeEnvironments always includes "tags": {} when a CE has no tags.
// AWS always returns tags:{} in this response.
func TestDescribeComputeEnvironments_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-notags",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{"ce-notags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["computeEnvironments"].([]any)
	require.Len(t, items, 1)

	itemBytes, err := json.Marshal(items[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestDescribeComputeEnvironments_EmptyList verifies that
// DescribeComputeEnvironments returns "computeEnvironments": [] not null.
func TestDescribeComputeEnvironments_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describecomputeenvironments", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["computeEnvironments"]
	require.True(t, ok, "computeEnvironments key must be present")
	assert.Equal(t, "[]", string(raw), "computeEnvironments must be [] not null when empty")
}

// TestDescribeComputeEnvironments_TagsRoundTrip verifies that tags
// set on a compute environment are returned in DescribeComputeEnvironments.
func TestDescribeComputeEnvironments_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-withtags",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
		"tags":                   map[string]string{"owner": "ops"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{"ce-withtags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["computeEnvironments"].([]any)
	require.Len(t, items, 1)
	tags := items[0].(map[string]any)["tags"].(map[string]any)
	assert.Equal(t, "ops", tags["owner"])
}
