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

func TestHandler_CreateDataCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"my-catalog","Type":"GLUE","Description":"test catalog"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "duplicate",
			body:       `{"Name":"my-catalog","Type":"GLUE"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate" {
				_ = doRequest(t, h, "CreateDataCatalog", `{"Name":"my-catalog","Type":"GLUE"}`)
			}

			rec := doRequest(t, h, "CreateDataCatalog", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetDataCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		catalog    string
		wantType   string
		wantStatus int
	}{
		{
			name:       "success",
			catalog:    "my-catalog",
			wantStatus: http.StatusOK,
			wantType:   "GLUE",
		},
		{
			name:       "not_found",
			catalog:    "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantType != "" {
				rec := doRequest(t, h, "CreateDataCatalog", `{"Name":"my-catalog","Type":"GLUE"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetDataCatalog", `{"Name":"`+tt.catalog+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dc, _ := resp["DataCatalog"].(map[string]any)
				require.NotNil(t, dc)
				assert.Equal(t, tt.wantType, dc["Type"])
			}
		})
	}
}

// TestHandler_CreateDataCatalog_ReturnsDataCatalog locks in that
// CreateDataCatalogOutput carries the optional DataCatalog field the real
// AWS API returns (types.DataCatalog for the just-created catalog) --
// previously gopherstack returned an empty struct{}{} body, leaving a real
// SDK client's CreateDataCatalogOutput.DataCatalog permanently nil.
func TestHandler_CreateDataCatalog_ReturnsDataCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDataCatalog",
		`{"Name":"created-cat","Type":"GLUE","Description":"d"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dc, ok := resp["DataCatalog"].(map[string]any)
	require.True(t, ok, "CreateDataCatalogOutput must carry a DataCatalog object")
	assert.Equal(t, "created-cat", dc["Name"])
	assert.Equal(t, "GLUE", dc["Type"])
	_, hasTags := dc["Tags"]
	assert.False(t, hasTags, "DataCatalog object must not carry an invented Tags field")
}

// --- Tag tests ---

func TestHandler_ListDataCatalogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "returns_list",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = doRequest(t, h, "CreateDataCatalog", `{"Name":"cat1","Type":"GLUE"}`)

			rec := doRequest(t, h, "ListDataCatalogs", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list, _ := resp["DataCatalogsSummary"].([]any)
			assert.GreaterOrEqual(t, len(list), 1)
		})
	}
}

func TestHandler_UpdateDataCatalog(t *testing.T) {
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
				_ = doRequest(t, h, "CreateDataCatalog", `{"Name":"upd-cat","Type":"GLUE"}`)
				rec := doRequest(t, h, "UpdateDataCatalog",
					`{"Name":"upd-cat","Type":"GLUE","Description":"updated"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "UpdateDataCatalog",
					`{"Name":"missing-cat","Type":"GLUE","Description":"x"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_DeleteDataCatalog(t *testing.T) {
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
				_ = doRequest(t, h, "CreateDataCatalog", `{"Name":"del-cat","Type":"GLUE"}`)
				rec := doRequest(t, h, "DeleteDataCatalog", `{"Name":"del-cat"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "DeleteDataCatalog", `{"Name":"nonexistent"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_DeleteDataCatalog_ReturnsDataCatalog locks in that
// DeleteDataCatalogOutput carries the optional DataCatalog field (the record
// as it existed immediately before deletion) the real AWS API returns.
func TestHandler_DeleteDataCatalog_ReturnsDataCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDataCatalog", `{"Name":"del-cat2","Type":"GLUE"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DeleteDataCatalog", `{"Name":"del-cat2"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dc, ok := resp["DataCatalog"].(map[string]any)
	require.True(t, ok, "DeleteDataCatalogOutput must carry the deleted DataCatalog object")
	assert.Equal(t, "del-cat2", dc["Name"])
}

// TestHandler_DeleteDataCatalog_DeleteCatalogOnly locks in that
// DeleteCatalogOnly is rejected for non-FEDERATED catalogs (real AWS: "You
// can only use this with the FEDERATED catalogs" --
// aws-sdk-go-v2/service/athena@v1.60.4 api_op_DeleteDataCatalog.go:34-37) and
// accepted for FEDERATED ones.
func TestHandler_DeleteDataCatalog_DeleteCatalogOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		catalogType string
		wantStatus  int
	}{
		{name: "rejected_for_glue", catalogType: "GLUE", wantStatus: http.StatusBadRequest},
		{name: "rejected_for_hive", catalogType: "HIVE", wantStatus: http.StatusBadRequest},
		{name: "rejected_for_lambda", catalogType: "LAMBDA", wantStatus: http.StatusBadRequest},
		{name: "allowed_for_federated", catalogType: "FEDERATED", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			catalogName := "dco-" + tt.name

			rec := doRequest(t, h, "CreateDataCatalog",
				fmt.Sprintf(`{"Name":%q,"Type":%q}`, catalogName, tt.catalogType))
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "DeleteDataCatalog",
				fmt.Sprintf(`{"Name":%q,"DeleteCatalogOnly":true}`, catalogName))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteDataCatalog_FederatedReportsDeleteComplete locks in that
// deleting a FEDERATED catalog reports DELETE_COMPLETE, one of the
// documented deletion statuses for FEDERATED catalogs (real AWS:
// aws-sdk-go-v2/service/athena@v1.60.4 types/types.go DataCatalog.Status doc
// comment) -- rather than echoing back the stale pre-deletion creation
// status.
func TestHandler_DeleteDataCatalog_FederatedReportsDeleteComplete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDataCatalog", `{"Name":"fed-del","Type":"FEDERATED"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DeleteDataCatalog", `{"Name":"fed-del"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dc, ok := resp["DataCatalog"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "DELETE_COMPLETE", dc["Status"])
}

// --- Additional QueryExecution tests ---

func TestHandler_CreateDataCatalog_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"Type":"GLUE"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_type",
			body:       `{"Name":"cat1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_type",
			body:       `{"Name":"cat1","Type":"INVALID"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_federated_type",
			body:       `{"Name":"fed-cat","Type":"FEDERATED"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDataCatalog", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DataCatalog_FederatedStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		catalogType    string
		connectionType string
		wantStatus     string
	}{
		{name: "glue_complete", catalogType: "GLUE", wantStatus: "CREATE_COMPLETE"},
		{name: "lambda_complete", catalogType: "LAMBDA", wantStatus: "CREATE_COMPLETE"},
		{name: "hive_complete", catalogType: "HIVE", wantStatus: "CREATE_COMPLETE"},
		{
			name:           "federated_in_progress",
			catalogType:    "FEDERATED",
			connectionType: "REDSHIFT",
			wantStatus:     "CREATE_IN_PROGRESS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connField := ""
			if tt.connectionType != "" {
				connField = `,"ConnectionType":"` + tt.connectionType + `"`
			}
			body := `{"Name":"cat-` + tt.name + `","Type":"` + tt.catalogType + `"` + connField + `}`
			rec := doRequest(t, h, "CreateDataCatalog", body)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "GetDataCatalog", `{"Name":"cat-`+tt.name+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			dc := resp["DataCatalog"].(map[string]any)

			assert.Equal(t, tt.wantStatus, dc["Status"])
			if tt.connectionType != "" {
				assert.Equal(t, tt.connectionType, dc["ConnectionType"])
			}
		})
	}
}

func TestHandler_DataCatalog_ListIncludesStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateDataCatalog", `{"Name":"fed-cat","Type":"FEDERATED","ConnectionType":"MYSQL"}`)

	rec := doRequest(t, h, "ListDataCatalogs", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["DataCatalogsSummary"].([]any)
	require.True(t, ok)

	var found map[string]any
	for _, item := range summaries {
		s, sOK := item.(map[string]any)
		require.True(t, sOK)
		if s["CatalogName"] == "fed-cat" {
			found = s

			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, "CREATE_IN_PROGRESS", found["Status"])
	assert.Equal(t, "MYSQL", found["ConnectionType"])
}

func TestDataCatalog_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "glue_catalog_status_create_complete",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateDataCatalog", `{"Name":"glue-cat","Type":"GLUE"}`)
				rec := a1Do(t, h, "GetDataCatalog", `{"Name":"glue-cat"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				dc := a1Unmarshal(t, rec)["DataCatalog"].(map[string]any)
				assert.Equal(t, "GLUE", dc["Type"])
				assert.Equal(t, "CREATE_COMPLETE", dc["Status"],
					"non-FEDERATED catalog must have Status=CREATE_COMPLETE")
			},
		},
		{
			name: "federated_catalog_status_in_progress",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateDataCatalog",
					`{"Name":"fed-cat","Type":"FEDERATED","ConnectionType":"REDSHIFT"}`)
				rec := a1Do(t, h, "GetDataCatalog", `{"Name":"fed-cat"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				dc := a1Unmarshal(t, rec)["DataCatalog"].(map[string]any)
				assert.Equal(t, "CREATE_IN_PROGRESS", dc["Status"],
					"FEDERATED catalog must have Status=CREATE_IN_PROGRESS")
				assert.Equal(t, "REDSHIFT", dc["ConnectionType"])
			},
		},
		{
			name: "list_summary_includes_status",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateDataCatalog", `{"Name":"list-cat","Type":"GLUE"}`)
				rec := a1Do(t, h, "ListDataCatalogs", `{}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				summaries, ok := m["DataCatalogsSummary"].([]any)
				require.True(t, ok)
				var found map[string]any
				for _, item := range summaries {
					s := item.(map[string]any)
					if s["CatalogName"] == "list-cat" {
						found = s

						break
					}
				}
				require.NotNil(t, found)
				assert.Equal(t, "CREATE_COMPLETE", found["Status"])
				assert.Equal(t, "GLUE", found["Type"])
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

func TestListDataCatalogs_NextTokenOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
	rec := doRequest(t, h, "ListDataCatalogs", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasNextToken := resp["NextToken"]
	assert.False(t, hasNextToken,
		"ListDataCatalogs must not include NextToken on last page; got %q", resp["NextToken"])
}

func TestListDataCatalogs_Pagination(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)

	// Create 3 extra catalogs (AwsDataCatalog exists by default → 4 total).
	for _, cat := range []string{"cat1", "cat2", "cat3"} {
		rec := athenaDoPass5(t, h, "CreateDataCatalog",
			fmt.Sprintf(`{"Name":%q,"Type":"GLUE"}`, cat))
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
			rec := athenaDoPass5(t, h, "ListDataCatalogs", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			m := athenaUnmarshalPass5(t, rec)
			cats, _ := m["DataCatalogsSummary"].([]any)
			assert.Len(t, cats, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, m["NextToken"])
			} else {
				assert.Empty(t, m["NextToken"])
			}
		})
	}
}

func TestAwsDataCatalog_BuiltIn(t *testing.T) {
	t.Parallel()

	type catalogCase struct {
		check      func(t *testing.T, resp map[string]any)
		name       string
		action     string
		body       string
		wantStatus int
	}

	tests := []catalogCase{
		{
			name:       "get_AwsDataCatalog_returns_GLUE_catalog",
			action:     "GetDataCatalog",
			body:       `{"Name":"AwsDataCatalog"}`,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				dc, isDC := resp["DataCatalog"].(map[string]any)
				require.True(t, isDC, "DataCatalog field required")
				assert.Equal(t, "AwsDataCatalog", dc["Name"])
				assert.Equal(t, "GLUE", dc["Type"])
			},
		},
		{
			name:       "AwsDataCatalog_appears_in_list",
			action:     "ListDataCatalogs",
			body:       `{}`,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				items, isList := resp["DataCatalogsSummary"].([]any)
				require.True(t, isList, "DataCatalogsSummary field required")
				found := false
				for _, item := range items {
					m, isMap := item.(map[string]any)
					if isMap && m["CatalogName"] == "AwsDataCatalog" {
						found = true
						assert.Equal(t, "GLUE", m["Type"])
					}
				}
				assert.True(t, found, "AwsDataCatalog must appear in ListDataCatalogs")
			},
		},
		{
			name:       "delete_AwsDataCatalog_returns_400",
			action:     "DeleteDataCatalog",
			body:       `{"Name":"AwsDataCatalog"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}
