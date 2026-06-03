package athena_test

// Audit batch-2 ops: AWS-accuracy fixes for Athena (go-vr1y).
//
// Covers:
//   - BatchGetNamedQuery: unprocessed IDs return objects (NamedQueryId+ErrorCode+ErrorMessage)
//   - BatchGetQueryExecution: unprocessed IDs return objects (QueryExecutionId+ErrorCode+ErrorMessage)
//   - StartQueryExecution: unknown workgroup returns InvalidRequestException
//   - StartQueryExecution: empty workgroup defaults to "primary"
//   - CreateNamedQuery: unknown workgroup returns InvalidRequestException
//   - CreateNamedQuery: empty workgroup defaults to "primary"
//   - GetDataCatalog: built-in AwsDataCatalog returns GLUE catalog
//   - DeleteDataCatalog: built-in AwsDataCatalog cannot be deleted

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudit2_BatchGetNamedQuery_UnprocessedShape(t *testing.T) {
	t.Parallel()

	type batchNQCase struct {
		checkUnproc func(t *testing.T, items []any)
		name        string
		ids         []string
		wantFound   int
		wantUnproc  int
	}

	tests := []batchNQCase{
		{
			name:       "all_found_empty_unprocessed",
			ids:        nil,
			wantFound:  0,
			wantUnproc: 0,
		},
		{
			name:       "unknown_id_returns_object_not_string",
			ids:        []string{"no-such-id"},
			wantFound:  0,
			wantUnproc: 1,
			checkUnproc: func(t *testing.T, items []any) {
				t.Helper()

				obj, isObj := items[0].(map[string]any)
				require.True(t, isObj, "unprocessed item must be an object, not a string")
				assert.Equal(t, "no-such-id", obj["NamedQueryId"], "NamedQueryId field required")
				assert.NotEmpty(t, obj["ErrorCode"], "ErrorCode field required")
				assert.NotEmpty(t, obj["ErrorMessage"], "ErrorMessage field required")
			},
		},
		{
			name:       "mixed_found_and_unprocessed",
			ids:        []string{"existing", "missing-1", "missing-2"},
			wantFound:  1,
			wantUnproc: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if len(tt.ids) > 0 {
				rec := doRequest(t, h, "CreateNamedQuery",
					`{"Name":"q1","Database":"db","QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				existingID := jsonField(t, rec.Body.Bytes(), "NamedQueryId")

				for i, id := range tt.ids {
					if id == "existing" {
						tt.ids[i] = existingID
					}
				}
			}

			body, err := json.Marshal(map[string]any{"NamedQueryIds": tt.ids})
			require.NoError(t, err)

			rec := doRequest(t, h, "BatchGetNamedQuery", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			found, _ := resp["NamedQueries"].([]any)
			assert.Len(t, found, tt.wantFound)

			unproc, _ := resp["UnprocessedNamedQueryIds"].([]any)
			assert.Len(t, unproc, tt.wantUnproc)

			if tt.checkUnproc != nil && len(unproc) > 0 {
				tt.checkUnproc(t, unproc)
			}
		})
	}
}

func TestAudit2_BatchGetQueryExecution_UnprocessedShape(t *testing.T) {
	t.Parallel()

	type batchQECase struct {
		checkUnproc func(t *testing.T, items []any)
		name        string
		ids         []string
		wantFound   int
		wantUnproc  int
	}

	tests := []batchQECase{
		{
			name:       "all_found_empty_unprocessed",
			ids:        nil,
			wantFound:  0,
			wantUnproc: 0,
		},
		{
			name:       "unknown_id_returns_object_not_string",
			ids:        []string{"ghost-exec-id"},
			wantFound:  0,
			wantUnproc: 1,
			checkUnproc: func(t *testing.T, items []any) {
				t.Helper()

				obj, isObj := items[0].(map[string]any)
				require.True(t, isObj, "unprocessed item must be an object, not a string")
				assert.Equal(t, "ghost-exec-id", obj["QueryExecutionId"], "QueryExecutionId field required")
				assert.NotEmpty(t, obj["ErrorCode"], "ErrorCode field required")
				assert.NotEmpty(t, obj["ErrorMessage"], "ErrorMessage field required")
			},
		},
		{
			name:       "mixed_found_and_unprocessed",
			ids:        []string{"existing", "ghost-1"},
			wantFound:  1,
			wantUnproc: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if len(tt.ids) > 0 {
				rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				existingID := jsonField(t, rec.Body.Bytes(), "QueryExecutionId")

				for i, id := range tt.ids {
					if id == "existing" {
						tt.ids[i] = existingID
					}
				}
			}

			body, err := json.Marshal(map[string]any{"QueryExecutionIds": tt.ids})
			require.NoError(t, err)

			rec := doRequest(t, h, "BatchGetQueryExecution", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			found, _ := resp["QueryExecutions"].([]any)
			assert.Len(t, found, tt.wantFound)

			unproc, _ := resp["UnprocessedQueryExecutionIds"].([]any)
			assert.Len(t, unproc, tt.wantUnproc)

			if tt.checkUnproc != nil && len(unproc) > 0 {
				tt.checkUnproc(t, unproc)
			}
		})
	}
}

func TestAudit2_StartQueryExecution_WorkgroupValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "unknown_workgroup_returns_400",
			body:       `{"QueryString":"SELECT 1","WorkGroup":"no-such-wg"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_workgroup_defaults_to_primary_succeeds",
			body:       `{"QueryString":"SELECT 1","WorkGroup":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "omitted_workgroup_defaults_to_primary_succeeds",
			body:       `{"QueryString":"SELECT 1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "explicit_primary_workgroup_succeeds",
			body:       `{"QueryString":"SELECT 1","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartQueryExecution", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException")
			}
		})
	}
}

func TestAudit2_CreateNamedQuery_WorkgroupValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "unknown_workgroup_returns_400",
			body:       `{"Name":"q","Database":"db","QueryString":"SELECT 1","WorkGroup":"ghost-wg"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_workgroup_defaults_to_primary_succeeds",
			body:       `{"Name":"q","Database":"db","QueryString":"SELECT 1","WorkGroup":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "omitted_workgroup_defaults_to_primary_succeeds",
			body:       `{"Name":"q2","Database":"db","QueryString":"SELECT 1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "explicit_primary_workgroup_succeeds",
			body:       `{"Name":"q3","Database":"db","QueryString":"SELECT 1","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNamedQuery", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException")
			}
		})
	}
}

func TestAudit2_AwsDataCatalog_BuiltIn(t *testing.T) {
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
