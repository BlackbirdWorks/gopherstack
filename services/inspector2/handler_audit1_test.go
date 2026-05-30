package inspector2_test

// Audit batch-1: AWS-accuracy coverage for Inspector2 (go-ak0i).
//
// Covers:
//   - Enable / Disable lifecycle
//   - BatchGetAccountStatus reflects enabled state
//   - Filter CRUD: CreateFilter, UpdateFilter, DeleteFilter, ListFilters
//   - ListFindings returns empty list
//   - GetConfiguration / UpdateConfiguration round-trip
//   - Tags: TagResource, ListTagsForResource, UntagResource
//   - Error paths: duplicate filter name, filter not found

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// --- helpers ---

func newAuditBackend() *inspector2.InMemoryBackend {
	return inspector2.NewInMemoryBackend("123456789012", "us-east-1")
}

func newAuditHandler(t *testing.T) *inspector2.Handler {
	t.Helper()

	return inspector2.NewHandler(newAuditBackend())
}

func auditDo(t *testing.T, h *inspector2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte

	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditCreateFilter(t *testing.T, h *inspector2.Handler, name string) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
		"name":   name,
		"action": "NONE",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, ok := resp["arn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// --- enable/disable tests ---

func TestAudit1_Enable_Disable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "enable_returns_account",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/enable", map[string]any{
					"resourceTypes": []string{"EC2", "ECR"},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				assert.Equal(t, "ENABLED", acc["status"])
			},
		},
		{
			name: "disable_returns_account",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				// Enable first
				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})

				rec := auditDo(t, h, http.MethodPost, "/disable", map[string]any{
					"resourceTypes": []string{"EC2"},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				assert.Equal(t, "DISABLED", acc["status"])
			},
		},
		{
			name: "enable_empty_body",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/enable", nil)
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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

// --- BatchGetAccountStatus tests ---

func TestAudit1_BatchGetAccountStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "disabled_by_default",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts, ok := resp["accounts"].([]any)
				require.True(t, ok)
				require.Len(t, accounts, 1)

				acc := accounts[0].(map[string]any)
				state := acc["state"].(map[string]any)
				status := state["status"].(map[string]any)
				assert.Equal(t, "DISABLED", status["status"])
			},
		},
		{
			name: "enabled_after_enable",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditDo(t, h, http.MethodPost, "/enable", map[string]any{})

				rec := auditDo(t, h, http.MethodPost, "/status/batch/get", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				accounts := resp["accounts"].([]any)
				acc := accounts[0].(map[string]any)
				state := acc["state"].(map[string]any)
				status := state["status"].(map[string]any)
				assert.Equal(t, "ENABLED", status["status"])
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

// --- filter tests ---

func TestAudit1_Filter_Lifecycle(t *testing.T) {
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
				assert.Len(t, listResp2["filters"].([]any), 0)
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

// --- findings tests ---

func TestAudit1_ListFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "empty_list",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/findings/list", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				findings, ok := resp["findings"]
				require.True(t, ok)
				require.NotNil(t, findings)
			},
		},
		{
			name: "with_max_results",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/findings/list", map[string]any{
					"maxResults": 10,
				})
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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

// --- configuration tests ---

func TestAudit1_Configuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "get_defaults",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/get", nil)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				_, hasEC2 := resp["ec2Configuration"]
				assert.True(t, hasEC2)
				_, hasECR := resp["ecrConfiguration"]
				assert.True(t, hasECR)
			},
		},
		{
			name: "update_configuration",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/update", map[string]any{
					"ec2Configuration": map[string]any{
						"scanMode": "EC2_HYBRID",
					},
					"ecrConfiguration": map[string]any{
						"rescanDuration": "DAYS_30",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				// Verify via get
				getRec := auditDo(t, h, http.MethodPost, "/configuration/get", nil)
				var getResp map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

				ec2Cfg := getResp["ec2Configuration"].(map[string]any)
				scanModeState := ec2Cfg["scanModeState"].(map[string]any)
				assert.Equal(t, "EC2_HYBRID", scanModeState["scanMode"])

				ecrCfg := getResp["ecrConfiguration"].(map[string]any)
				rescanState := ecrCfg["rescanDurationState"].(map[string]any)
				assert.Equal(t, "DAYS_30", rescanState["rescanDuration"])
			},
		},
		{
			name: "update_empty_body",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/configuration/update", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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

// --- tags tests ---

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "tag_filter_and_list",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "tag-target")

				tagRec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
					"tags": map[string]string{
						"team": "security",
						"env":  "staging",
					},
				})
				require.Equal(t, http.StatusOK, tagRec.Code, tagRec.Body.String())

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "security", tags["team"])
				assert.Equal(t, "staging", tags["env"])
			},
		},
		{
			name: "untag_removes_specific_keys",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "untag-target")

				auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
					"tags": map[string]string{
						"keep":   "yes",
						"remove": "no",
					},
				})

				// DELETE with query params
				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, "/tags/"+filterARN+"?tagKeys=remove", nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				c.SetRequest(req)
				require.NoError(t, h.Handler()(c))
				require.Equal(t, http.StatusOK, rec.Code)

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.Equal(t, "yes", tags["keep"])
				_, hasRemove := tags["remove"]
				assert.False(t, hasRemove)
			},
		},
		{
			name: "tag_nonexistent_resource_returns_404",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(
					t,
					h,
					http.MethodPost,
					"/tags/arn:aws:inspector2:us-east-1:123456789012:filter/nonexistent",
					map[string]any{
						"tags": map[string]string{"k": "v"},
					},
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_tags_nonexistent_resource_returns_404",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(
					t,
					h,
					http.MethodGet,
					"/tags/arn:aws:inspector2:us-east-1:123456789012:filter/ghost",
					nil,
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_tags_empty_on_new_filter",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "no-tags")

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.Empty(t, tags)
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

// --- handler ops coverage ---

func TestAudit1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	assert.Equal(t, 13, inspector2.HandlerOpsLen(h))
}
