package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestHandler_CreateNamedQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "success",
			body:       `{"Name":"my-query","Database":"mydb","QueryString":"SELECT 1","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNamedQuery", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["NamedQueryId"])
			}
		})
	}
}

func TestHandler_GetNamedQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryID    string
		wantStatus int
		wantQuery  bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantQuery:  true,
		},
		{
			name:       "not_found",
			queryID:    "nonexistent-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			queryID := tt.queryID
			if tt.name == "success" {
				rec := doRequest(t, h, "CreateNamedQuery",
					`{"Name":"q","Database":"db","QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				queryID = created["NamedQueryId"]
			}

			rec := doRequest(t, h, "GetNamedQuery", `{"NamedQueryId":"`+queryID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantQuery {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				nq, _ := resp["NamedQuery"].(map[string]any)
				require.NotNil(t, nq)
				assert.Equal(t, "q", nq["Name"])
			}
		})
	}
}

// --- QueryExecution tests ---

func TestHandler_ListNamedQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_ids_for_workgroup",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = doRequest(t, h, "CreateNamedQuery",
				`{"Name":"q1","Database":"db","QueryString":"SELECT 1","WorkGroup":"primary"}`)

			rec := doRequest(t, h, "ListNamedQueries", `{"WorkGroup":"primary"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string][]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["NamedQueryIds"], tt.wantCount)
		})
	}
}

func TestHandler_BatchGetNamedQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "returns_found_and_unprocessed",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateNamedQuery",
				`{"Name":"bq1","Database":"db","QueryString":"SELECT 1"}`)
			require.Equal(t, http.StatusOK, createRec.Code)

			var created map[string]string
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			id := created["NamedQueryId"]

			rec := doRequest(t, h, "BatchGetNamedQuery",
				`{"NamedQueryIds":["`+id+`","missing-id"]}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			found, _ := resp["NamedQueries"].([]any)
			assert.Len(t, found, 1)
			unprocessed, _ := resp["UnprocessedNamedQueryIds"].([]any)
			assert.Len(t, unprocessed, 1)
		})
	}
}

func TestHandler_DeleteNamedQuery(t *testing.T) {
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
				createRec := doRequest(t, h, "CreateNamedQuery",
					`{"Name":"del-q","Database":"db","QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, createRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

				rec := doRequest(t, h, "DeleteNamedQuery", `{"NamedQueryId":"`+cr["NamedQueryId"]+`"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "DeleteNamedQuery", `{"NamedQueryId":"nonexistent"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// --- Additional DataCatalog tests ---

func TestHandler_CreateNamedQuery_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_query_name",
			body:       `{"Database":"db","QueryString":"SELECT 1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_database",
			body:       `{"Name":"q1","QueryString":"SELECT 1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_query_string",
			body:       `{"Name":"q1","Database":"db"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNamedQuery", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateNamedQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryID    string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_id",
			queryID:    "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			queryID:    "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queryID := tt.queryID

			if tt.name == "success" {
				rec := doRequest(t, h, "CreateNamedQuery",
					`{"Name":"q","Database":"db","QueryString":"SELECT 1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				queryID = jsonField(t, rec.Body.Bytes(), "NamedQueryId")
			}

			body := `{"NamedQueryId":"` + queryID + `","Name":"q2","QueryString":"SELECT 2","Description":"x"}`
			rec := doRequest(t, h, "UpdateNamedQuery", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestNamedQuery_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "create_returns_id",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "CreateNamedQuery",
					`{"Name":"q1","Database":"db","QueryString":"SELECT 1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				id, ok := a1Unmarshal(t, rec)["NamedQueryId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name: "get_response_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				cRec := a1Do(
					t,
					h,
					"CreateNamedQuery",
					`{"Name":"q2","Database":"mydb","QueryString":"SELECT 2","WorkGroup":"primary","Description":"test"}`,
				)
				id := a1Unmarshal(t, cRec)["NamedQueryId"].(string)

				rec := a1Do(t, h, "GetNamedQuery", `{"NamedQueryId":"`+id+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				nq, ok := a1Unmarshal(t, rec)["NamedQuery"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "q2", nq["Name"])
				assert.Equal(t, "mydb", nq["Database"])
				assert.Equal(t, "SELECT 2", nq["QueryString"])
				assert.Equal(t, "primary", nq["WorkGroup"])
				assert.Equal(t, "test", nq["Description"])
				assert.Equal(t, id, nq["NamedQueryId"])
			},
		},
		{
			name: "batch_get_returns_found_and_unprocessed",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				cRec := a1Do(t, h, "CreateNamedQuery",
					`{"Name":"bq","Database":"db","QueryString":"SELECT 1"}`)
				id := a1Unmarshal(t, cRec)["NamedQueryId"].(string)

				rec := a1Do(t, h, "BatchGetNamedQuery",
					`{"NamedQueryIds":["`+id+`","missing-id"]}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				found, _ := m["NamedQueries"].([]any)
				assert.Len(t, found, 1)
				unproc, _ := m["UnprocessedNamedQueryIds"].([]any)
				assert.Len(t, unproc, 1)
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

func TestBatchGetNamedQuery_UnprocessedShape(t *testing.T) {
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

func TestCreateNamedQuery_WorkgroupValidation(t *testing.T) {
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
