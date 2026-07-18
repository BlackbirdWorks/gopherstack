package inspector2_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestFilterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "create_filter",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "my-filter")
				require.NotEmpty(t, filterARN)
				assert.Contains(t, filterARN, "arn:aws:inspector2:")
				assert.Contains(t, filterARN, "filter/")
			},
		},
		{
			name: "create_duplicate_name_conflicts",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditCreateFilter(t, h, "dup-filter")

				rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
					"name":   "dup-filter",
					"action": "NONE",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "create_missing_name_returns_400",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
					"action": "NONE",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_filters_returns_created",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditCreateFilter(t, h, "list-me")

				rec := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				filters, ok := resp["filters"].([]any)
				require.True(t, ok)
				require.Len(t, filters, 1)

				f := filters[0].(map[string]any)
				assert.Equal(t, "list-me", f["name"])
			},
		},
		{
			name: "list_filters_by_arn",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				arn1 := auditCreateFilter(t, h, "filter-a")
				auditCreateFilter(t, h, "filter-b")

				rec := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{
					"arns": []string{arn1},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				filters := resp["filters"].([]any)
				require.Len(t, filters, 1)

				f := filters[0].(map[string]any)
				assert.Equal(t, "filter-a", f["name"])
			},
		},
		{
			name: "list_filters_by_action",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditCreateFilter(t, h, "none-filter")

				rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
					"name":   "suppress-filter",
					"action": "SUPPRESS",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				rec = auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{
					"action": "SUPPRESS",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				filters := resp["filters"].([]any)
				require.Len(t, filters, 1)

				f := filters[0].(map[string]any)
				assert.Equal(t, "suppress-filter", f["name"])
			},
		},
		{
			name: "update_filter",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "update-me")

				rec := auditDo(t, h, http.MethodPost, "/filters/update", map[string]any{
					"filterArn":   filterARN,
					"action":      "SUPPRESS",
					"description": "updated description",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, filterARN, resp["arn"])

				// Verify via list
				listRec := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				filters := listResp["filters"].([]any)
				require.Len(t, filters, 1)

				f := filters[0].(map[string]any)
				assert.Equal(t, "SUPPRESS", f["action"])
				assert.Equal(t, "updated description", f["description"])
			},
		},
		{
			name: "update_filter_not_found",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/filters/update", map[string]any{
					"filterArn": "arn:aws:inspector2:us-east-1:123456789012:filter/nonexistent",
					"action":    "SUPPRESS",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_filter",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "delete-me")

				// Use a list check to verify filter was created
				listRec := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.Len(t, listResp["filters"].([]any), 1)

				rec := auditDo(t, h, http.MethodPost, "/filters/delete", map[string]any{
					"arn": filterARN,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				listRec2 := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{})
				var listResp2 map[string]any
				require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
				assert.Empty(t, listResp2["filters"].([]any))
			},
		},
		{
			name: "delete_filter_not_found",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/filters/delete", map[string]any{
					"arn": "arn:aws:inspector2:us-east-1:123456789012:filter/does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_filter_missing_arn",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/filters/delete", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "filter_with_tags",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
					"name":   "tagged-filter",
					"action": "NONE",
					"tags": map[string]string{
						"env": "prod",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				listRec := auditDo(t, h, http.MethodPost, "/filters/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				filters := listResp["filters"].([]any)
				require.Len(t, filters, 1)

				f := filters[0].(map[string]any)
				tags, ok := f["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

func TestCreateFilterActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "NONE accepted",
			action:   "NONE",
			wantCode: http.StatusOK,
		},
		{
			name:     "SUPPRESS accepted",
			action:   "SUPPRESS",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty action accepted (defaults to NONE)",
			action:   "",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid action rejected",
			action:   "ALLOW",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lowercase action rejected",
			action:   "none",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)

			body := map[string]any{"name": "filter-" + tc.name}
			if tc.action != "" {
				body["action"] = tc.action
			}

			rec := auditDo(t, h, http.MethodPost, "/filters/create", body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestUpdateFilterActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "SUPPRESS accepted",
			action:   "SUPPRESS",
			wantCode: http.StatusOK,
		},
		{
			name:     "NONE accepted",
			action:   "NONE",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid action rejected",
			action:   "DENY",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			filterARN := auditCreateFilter(t, h, "update-action-"+tc.name)

			rec := auditDo(t, h, http.MethodPost, "/filters/update", map[string]any{
				"filterArn": filterARN,
				"action":    tc.action,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestCreateFilterTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags in create accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty tag key in create rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key over limit in create rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value over limit in create rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)

			rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
				"name":   "create-tag-" + tc.name,
				"action": "NONE",
				"tags":   tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestCreateFilterInvalidActionErrorType(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
		"name":   "bad-action-filter",
		"action": "INVALID",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, inspector2UnmarshalError(t, rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}
