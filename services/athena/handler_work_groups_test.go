package athena_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWorkGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			body:       `{"Name":"test-wg","Description":"desc","State":"ENABLED"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "duplicate",
			body:       `{"Name":"test-wg","Description":"desc"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "bytes_scanned_cutoff_below_minimum_rejected",
			body:       `{"Name":"too-small-cutoff","Configuration":{"BytesScannedCutoffPerQuery":1024}}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "bytes_scanned_cutoff_above_minimum_accepted",
			body:       `{"Name":"good-cutoff","Configuration":{"BytesScannedCutoffPerQuery":20971520}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "bytes_scanned_cutoff_zero_means_unlimited",
			body:       `{"Name":"unlimited-cutoff","Configuration":{"BytesScannedCutoffPerQuery":0}}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := newTestHandler(t)

			if tt.name == "duplicate" {
				_ = doRequest(t, handler, "CreateWorkGroup", `{"Name":"test-wg"}`)
			}

			rec := doRequest(t, handler, "CreateWorkGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr {
				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.NotEmpty(t, errResp["__type"])
			}
		})
	}
}

func TestHandler_GetWorkGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		workGroup  string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success_primary",
			workGroup:  "primary",
			wantStatus: http.StatusOK,
			wantName:   "primary",
		},
		{
			name:       "not_found",
			workGroup:  "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := `{"WorkGroup":"` + tt.workGroup + `"}`
			rec := doRequest(t, h, "GetWorkGroup", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["WorkGroup"]["Name"])
			}
		})
	}
}

func TestHandler_ListWorkGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListWorkGroups", `{}`)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string][]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.GreaterOrEqual(t, len(resp["WorkGroups"]), 1)

	found := false
	for _, wg := range resp["WorkGroups"] {
		if wg["Name"] == "primary" {
			found = true

			break
		}
	}

	assert.True(t, found, "primary workgroup should be in list")
}

func TestHandler_DeleteWorkGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		workGroup  string
		wantStatus int
	}{
		{
			name:       "success",
			workGroup:  "deletable",
			wantStatus: http.StatusOK,
		},
		{
			name:       "protected_primary",
			workGroup:  "primary",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			workGroup:  "does-not-exist",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "success" {
				createRec := doRequest(t, h, "CreateWorkGroup", `{"Name":"deletable"}`)
				assert.Equal(t, http.StatusOK, createRec.Code)
			}

			rec := doRequest(t, h, "DeleteWorkGroup", `{"WorkGroup":"`+tt.workGroup+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- NamedQuery tests ---

func TestHandler_UpdateWorkGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "success" {
				_ = doRequest(t, h, "CreateWorkGroup", `{"Name":"upd-wg"}`)
				rec := doRequest(t, h, "UpdateWorkGroup",
					`{"WorkGroup":"upd-wg","Description":"updated","State":"DISABLED"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "UpdateWorkGroup",
					`{"WorkGroup":"no-such-wg","Description":"x"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// --- Additional NamedQuery tests ---

func TestHandler_CreateWorkGroup_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_tags",
			body:       `{"Name":"tagged-wg","Tags":[{"Key":"env","Value":"test"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateWorkGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateWorkGroup_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"Description":"no name"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateWorkGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_WorkGroupConfiguration_EnforceAndExtras(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := `{
		"Name":"wg-enforce",
		"Configuration":{
			"EnforceWorkGroupConfiguration":true,
			"EnableMinimumEncryptionConfiguration":true,
			"ExecutionRole":"arn:aws:iam::000000000000:role/AthenaRole",
			"AdditionalConfiguration":"{\"spark.conf\":\"value\"}",
			"CustomerContentEncryptionConfiguration":{"KmsKey":"arn:aws:kms:us-east-1:000000000000:key/test"}
		}
	}`
	rec := doRequest(t, h, "CreateWorkGroup", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetWorkGroup", `{"WorkGroup":"wg-enforce"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wg := resp["WorkGroup"].(map[string]any)
	cfg := wg["Configuration"].(map[string]any)

	assert.Equal(t, true, cfg["EnforceWorkGroupConfiguration"])
	assert.Equal(t, true, cfg["EnableMinimumEncryptionConfiguration"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/AthenaRole", cfg["ExecutionRole"])
	assert.NotEmpty(t, cfg["AdditionalConfiguration"])

	cce := cfg["CustomerContentEncryptionConfiguration"].(map[string]any)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test", cce["KmsKey"])

	assert.NotZero(t, wg["CreationTime"])
}

func TestHandler_WorkGroupSummary_IncludesFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateWorkGroup", `{
		"Name":"wg-summary",
		"Description":"my workgroup",
		"Configuration":{"EngineVersion":{"SelectedEngineVersion":"Athena engine version 3"}}
	}`)

	rec := doRequest(t, h, "ListWorkGroups", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string][]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	var found map[string]any
	for _, wg := range resp["WorkGroups"] {
		if wg["Name"] == "wg-summary" {
			found = wg

			break
		}
	}
	require.NotNil(t, found, "wg-summary not in list")

	assert.Equal(t, "my workgroup", found["Description"])
	assert.NotZero(t, found["CreationTime"])
	ev := found["EngineVersion"].(map[string]any)
	assert.Equal(t, "Athena engine version 3", ev["SelectedEngineVersion"])
}

func TestWorkGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "create_response_is_empty_object",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "CreateWorkGroup", `{"Name":"audit-wg","Description":"d","State":"ENABLED"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				// AWS CreateWorkGroup returns an empty JSON object {}.
				m := a1Unmarshal(t, rec)
				assert.Empty(t, m, "CreateWorkGroup must return empty object")
			},
		},
		{
			name: "get_response_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateWorkGroup", `{"Name":"audit-wg","Description":"desc","State":"ENABLED"}`)
				rec := a1Do(t, h, "GetWorkGroup", `{"WorkGroup":"audit-wg"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				m := a1Unmarshal(t, rec)
				wg, ok := m["WorkGroup"].(map[string]any)
				require.True(t, ok, "WorkGroup key must be present")
				assert.Equal(t, "audit-wg", wg["Name"])
				assert.Equal(t, "ENABLED", wg["State"])
				assert.Equal(t, "desc", wg["Description"])
				assert.NotZero(t, wg["CreationTime"], "CreationTime must be set")
			},
		},
		{
			name: "list_response_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateWorkGroup", `{"Name":"audit-wg","Description":"desc"}`)
				rec := a1Do(t, h, "ListWorkGroups", `{}`)
				require.Equal(t, http.StatusOK, rec.Code)

				m := a1Unmarshal(t, rec)
				wgs, ok := m["WorkGroups"].([]any)
				require.True(t, ok, "WorkGroups key must be present")
				require.GreaterOrEqual(t, len(wgs), 1)

				var found map[string]any
				for _, item := range wgs {
					s, _ := item.(map[string]any)
					if s["Name"] == "audit-wg" {
						found = s

						break
					}
				}
				require.NotNil(t, found, "created workgroup must appear in list")
				assert.NotZero(t, found["CreationTime"])
				assert.Equal(t, "ENABLED", found["State"])
			},
		},
		{
			name: "update_state_to_disabled",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateWorkGroup", `{"Name":"audit-wg","State":"ENABLED"}`)
				rec := a1Do(t, h, "UpdateWorkGroup", `{"WorkGroup":"audit-wg","State":"DISABLED"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				assert.Empty(t, m, "UpdateWorkGroup must return empty object")

				rec = a1Do(t, h, "GetWorkGroup", `{"WorkGroup":"audit-wg"}`)
				wg := a1Unmarshal(t, rec)["WorkGroup"].(map[string]any)
				assert.Equal(t, "DISABLED", wg["State"])
			},
		},
		{
			name: "configuration_present_when_set",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(
					t,
					h,
					"CreateWorkGroup",
					`{"Name":"cfg-wg","Configuration":{"EnforceWorkGroupConfiguration":true,`+
						`"ExecutionRole":"arn:aws:iam::000000000000:role/AthenaRole"}}`,
				)
				rec := a1Do(t, h, "GetWorkGroup", `{"WorkGroup":"cfg-wg"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				wg := a1Unmarshal(t, rec)["WorkGroup"].(map[string]any)
				cfg, ok := wg["Configuration"].(map[string]any)
				require.True(t, ok, "Configuration must be present when set")
				assert.Equal(t, true, cfg["EnforceWorkGroupConfiguration"])
				assert.Equal(t, "arn:aws:iam::000000000000:role/AthenaRole", cfg["ExecutionRole"])
			},
		},
		{
			name: "delete_then_not_found",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateWorkGroup", `{"Name":"del-wg"}`)
				rec := a1Do(t, h, "DeleteWorkGroup", `{"WorkGroup":"del-wg"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetWorkGroup", `{"WorkGroup":"del-wg"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := a1Unmarshal(t, rec)
				assert.NotEmpty(t, m["__type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := a1Handler(t)
			tt.fn(t, h)
		})
	}
}

func TestWorkGroup_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       string
		wantStatus int
	}{
		{
			name:       "create_enabled_state_accepted",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-a","State":"ENABLED"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_disabled_state_accepted",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-b","State":"DISABLED"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_empty_state_defaults_to_enabled",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-c","State":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_invalid_state_returns_400",
			action:     "CreateWorkGroup",
			body:       `{"Name":"wg-d","State":"ACTIVE"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update_invalid_state_returns_400",
			action:     "UpdateWorkGroup",
			body:       `{"WorkGroup":"primary","State":"UNKNOWN"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update_valid_state_accepted",
			action:     "UpdateWorkGroup",
			body:       `{"WorkGroup":"primary","State":"DISABLED"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"invalid State must return InvalidRequestException")
			}
		})
	}
}

// TestListWorkGroups_Pagination verifies ListWorkGroups MaxResults/NextToken.
func TestListWorkGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)

	// Create 3 extra workgroups (primary exists by default → 4 total).
	for _, wg := range []string{"wg1", "wg2", "wg3"} {
		rec := athenaDoPass5(t, h, "CreateWorkGroup", fmt.Sprintf(`{"Name":%q}`, wg))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name          string
		body          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "page1_two_results",
			body:          `{"MaxResults":2}`,
			wantLen:       2,
			wantNextToken: true,
		},
		{
			name:          "no_limit_returns_all",
			body:          `{}`,
			wantLen:       4,
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := athenaDoPass5(t, h, "ListWorkGroups", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			m := athenaUnmarshalPass5(t, rec)
			wgs, _ := m["WorkGroups"].([]any)
			assert.Len(t, wgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, m["NextToken"])
			} else {
				assert.Empty(t, m["NextToken"])
			}
		})
	}
}
