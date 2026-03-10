package athena_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func newTestHandler(t *testing.T) *athena.Handler {
	t.Helper()

	return athena.NewHandler(athena.NewInMemoryBackend())
}

func doRequest(t *testing.T, h *athena.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonAthena."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- WorkGroup tests ---

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
				var resp map[string]map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "q", resp["NamedQuery"]["Name"])
			}
		})
	}
}

// --- QueryExecution tests ---

func TestHandler_StartQueryExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "success",
			body:       `{"QueryString":"SELECT 1","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartQueryExecution", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["QueryExecutionId"])
			}
		})
	}
}

func TestHandler_GetQueryExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		execID     string
		wantState  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantState:  "SUCCEEDED",
		},
		{
			name:       "not_found",
			execID:     "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			execID := tt.execID
			if tt.name == "success" {
				rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				execID = created["QueryExecutionId"]
			}

			rec := doRequest(t, h, "GetQueryExecution", `{"QueryExecutionId":"`+execID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				qe, ok := resp["QueryExecution"].(map[string]any)
				require.True(t, ok)
				status, ok := qe["Status"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantState, status["State"])
			}
		})
	}
}

// --- DataCatalog tests ---

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
				var resp map[string]map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["DataCatalog"]["Type"])
			}
		})
	}
}

// --- Tag tests ---

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	const primaryWGARN = "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"ResourceARN":"` + primaryWGARN + `","Tags":[{"Key":"env","Value":"prod"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

			_ = doRequest(t, h, "TagResource",
				`{"ResourceARN":"`+arn+`","Tags":[{"Key":"env","Value":"prod"}]}`)

			rec := doRequest(t, h, "UntagResource",
				`{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_tags_after_tagging",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

			_ = doRequest(t, h, "TagResource",
				`{"ResourceARN":"`+arn+`","Tags":[{"Key":"team","Value":"platform"}]}`)

			rec := doRequest(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string][]map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["Tags"], tt.wantCount)
			assert.Equal(t, "team", resp["Tags"][0]["Key"])
			assert.Equal(t, "platform", resp["Tags"][0]["Value"])
		})
	}
}

// --- Unknown operation ---

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "unknown_op_returns_400",
			action:     "SomeUnknownOp",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, `{}`)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.NotEmpty(t, errResp["__type"])
		})
	}
}

// --- Additional WorkGroup tests ---

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

// --- Additional QueryExecution tests ---

func TestHandler_StopQueryExecution(t *testing.T) {
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
				startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, startRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &cr))

				rec := doRequest(t, h, "StopQueryExecution",
					`{"QueryExecutionId":"`+cr["QueryExecutionId"]+`"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "StopQueryExecution", `{"QueryExecutionId":"missing"}`)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_ListQueryExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_ids",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = doRequest(t, h, "StartQueryExecution",
				`{"QueryString":"SELECT 1","WorkGroup":"primary"}`)

			rec := doRequest(t, h, "ListQueryExecutions", `{"WorkGroup":"primary"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ids, _ := resp["QueryExecutionIds"].([]any)
			assert.Len(t, ids, tt.wantCount)
		})
	}
}

func TestHandler_BatchGetQueryExecution(t *testing.T) {
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

			startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
			require.Equal(t, http.StatusOK, startRec.Code)

			var cr map[string]string
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &cr))
			id := cr["QueryExecutionId"]

			rec := doRequest(t, h, "BatchGetQueryExecution",
				`{"QueryExecutionIds":["`+id+`","missing-id"]}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			found, _ := resp["QueryExecutions"].([]any)
			assert.Len(t, found, 1)
			unprocessed, _ := resp["UnprocessedQueryExecutionIds"].([]any)
			assert.Len(t, unprocessed, 1)
		})
	}
}

func TestHandler_GetQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "returns_empty_result_set",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetQueryResults", `{"QueryExecutionId":"any-id"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rs, _ := resp["ResultSet"].(map[string]any)
			require.NotNil(t, rs, "ResultSet should be present")
			rows, _ := rs["Rows"].([]any)
			assert.Empty(t, rows, "rows should be empty for mock")
		})
	}
}
