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

	return athena.NewHandler(athena.NewInMemoryBackend("", ""))
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
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				dc, _ := resp["DataCatalog"].(map[string]any)
				require.NotNil(t, dc)
				assert.Equal(t, tt.wantType, dc["Type"])
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
		setup      func(*athena.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) string {
				startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, startRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &cr))

				execID := cr["QueryExecutionId"]
				// Override state to RUNNING so it can be stopped.
				h.Backend.(*athena.InMemoryBackend).SetQueryExecutionState(execID, "RUNNING", 0)

				return execID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *athena.Handler) string {
				return "missing"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "already_terminal",
			setup: func(h *athena.Handler) string {
				startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, startRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &cr))

				return cr["QueryExecutionId"] // default state is SUCCEEDED
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			execID := tt.setup(h)

			rec := doRequest(t, h, "StopQueryExecution",
				`{"QueryExecutionId":"`+execID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
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

// TestHandler_GetQueryResults validates the per-AWS contract for GetQueryResults:
// missing/unknown QueryExecutionId yields InvalidRequestException, MaxResults out
// of [1, 1000] yields InvalidRequestException, and a successful call against an
// existing query returns an empty result set.
func TestHandler_GetQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       string
		name       string
		setupID    bool
		wantStatus int
	}{
		{
			name:       "valid_known_id_returns_empty_result_set",
			setupID:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_query_execution_id",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_query_execution_id",
			body:       `{"QueryExecutionId":"does-not-exist"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "max_results_too_large",
			setupID:    true,
			body:       `{"MaxResults":2000}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := tt.body
			if tt.setupID {
				id, err := h.Backend.StartQueryExecution(
					"SELECT 1", "primary",
					athena.QueryExecutionContext{},
					athena.ResultConfiguration{}, nil, nil,
				)
				require.NoError(t, err)

				if body == "" {
					body = `{"QueryExecutionId":"` + id + `"}`
				} else {
					// inject the real id into the supplied body
					body = `{"QueryExecutionId":"` + id + `","MaxResults":2000}`
				}
			}

			rec := doRequest(t, h, "GetQueryResults", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rs, _ := resp["ResultSet"].(map[string]any)
			require.NotNil(t, rs, "ResultSet should be present")
			rows, _ := rs["Rows"].([]any)
			assert.Empty(t, rows, "rows should be empty for mock")
		})
	}
}

// --- PreparedStatement tests ---

func TestHandler_CreatePreparedStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			body:       `{"StatementName":"stmt1","WorkGroup":"primary","QueryStatement":"SELECT ?"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"stmt1","WorkGroup":"primary","QueryStatement":"SELECT ?"}`)
			},
			body:       `{"StatementName":"stmt1","WorkGroup":"primary","QueryStatement":"SELECT ?"}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreatePreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr {
				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.NotEmpty(t, errResp["__type"])
			}
		})
	}
}

func TestHandler_BatchGetPreparedStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(*athena.Handler)
		name            string
		body            string
		wantStatus      int
		wantFound       int
		wantUnprocessed int
	}{
		{
			name: "found_and_unprocessed",
			setup: func(h *athena.Handler) {
				createRec := doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"s1","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)
				require.Equal(t, http.StatusOK, createRec.Code)
			},
			body:            `{"WorkGroup":"primary","StatementNames":["s1","missing"]}`,
			wantStatus:      http.StatusOK,
			wantFound:       1,
			wantUnprocessed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "BatchGetPreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			found, _ := resp["PreparedStatements"].([]any)
			assert.Len(t, found, tt.wantFound)
			unprocessed, _ := resp["UnprocessedStatementNames"].([]any)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestHandler_DeletePreparedStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"del-stmt","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)
			},
			body:       `{"StatementName":"del-stmt","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"StatementName":"no-such-stmt","WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeletePreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- CapacityReservation tests ---

func TestHandler_CreateCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			body:       `{"Name":"res1","TargetDpus":24}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"res1","TargetDpus":24}`)
			},
			body:       `{"Name":"res1","TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr {
				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.NotEmpty(t, errResp["__type"])
			}
		})
	}
}

func TestHandler_CancelCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cancel-res","TargetDpus":24}`)
			},
			body:       `{"Name":"cancel-res"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"no-such-res"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CancelCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"del-res","TargetDpus":24}`)
				_ = doRequest(t, h, "CancelCapacityReservation", `{"Name":"del-res"}`)
			},
			body:       `{"Name":"del-res"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"no-such-res"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Notebook tests ---

func TestHandler_CreateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Name":"my-notebook"}`,
			wantStatus: http.StatusOK,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["NotebookId"])
			}
		})
	}
}

func TestHandler_CreatePresignedNotebookUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		sessionID  string
		wantStatus int
		wantURL    bool
	}{
		{
			name:       "success",
			sessionID:  "sess-abc123",
			body:       `{"SessionId":"sess-abc123"}`,
			wantStatus: http.StatusOK,
			wantURL:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreatePresignedNotebookUrl", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantURL {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["NotebookSessionUrl"], tt.sessionID)
			}
		})
	}
}

func TestHandler_DeleteNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) string {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"del-nb"}`)
				require.Equal(t, http.StatusOK, createRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

				return cr["NotebookId"]
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *athena.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.setup(h)

			rec := doRequest(t, h, "DeleteNotebook", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ExportNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler) string
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) string {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"exp-nb"}`)
				require.Equal(t, http.StatusOK, createRec.Code)

				var cr map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))

				return cr["NotebookId"]
			},
			wantStatus: http.StatusOK,
			wantName:   "exp-nb",
		},
		{
			name: "not_found",
			setup: func(_ *athena.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.setup(h)

			rec := doRequest(t, h, "ExportNotebook", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				meta, _ := resp["NotebookMetadata"].(map[string]any)
				require.NotNil(t, meta, "NotebookMetadata should be present")
				assert.Equal(t, tt.wantName, meta["Name"])
			}
		})
	}
}

// --- Tags via CreateWorkGroup and CreateCapacityReservation ---

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

func TestHandler_CreateCapacityReservation_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_tags",
			body:       `{"Name":"tagged-res","TargetDpus":24,"Tags":[{"Key":"owner","Value":"platform"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateNotebook_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_tags",
			body:       `{"WorkGroup":"primary","Name":"tagged-nb","Tags":[{"Key":"env","Value":"dev"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Validation tests ---

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

func TestHandler_CreatePreparedStatement_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_statement_name",
			body:       `{"WorkGroup":"primary","QueryStatement":"SELECT ?"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_workgroup",
			body:       `{"StatementName":"s1","QueryStatement":"SELECT ?"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_query_statement",
			body:       `{"StatementName":"s1","WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreatePreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateCapacityReservation_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero_target_dpus",
			body:       `{"Name":"res1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "negative_target_dpus",
			body:       `{"Name":"res1","TargetDpus":-1}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateNotebook_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_workgroup",
			body:       `{"Name":"nb1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_name_in_workgroup",
			body:       `{"WorkGroup":"primary","Name":"dup-nb"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_name_in_workgroup" {
				rec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"dup-nb"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteCapacityReservation_ActiveBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "active_reservation_cannot_be_deleted",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"active-res","TargetDpus":24}`)

			rec := doRequest(t, h, "DeleteCapacityReservation", `{"Name":"active-res"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- GetPreparedStatement and ListPreparedStatements tests ---

func TestHandler_GetPreparedStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"get-stmt","WorkGroup":"primary","QueryStatement":"SELECT ?"}`)
			},
			body:       `{"StatementName":"get-stmt","WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
			wantName:   "get-stmt",
		},
		{
			name:       "not_found",
			body:       `{"StatementName":"missing","WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetPreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ps, _ := resp["PreparedStatement"].(map[string]any)
				require.NotNil(t, ps, "PreparedStatement must be present")
				assert.Equal(t, tt.wantName, ps["StatementName"])
			}
		})
	}
}

func TestHandler_ListPreparedStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "returns_statements_in_workgroup",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"s1","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)
				_ = doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"s2","WorkGroup":"primary","QueryStatement":"SELECT 2"}`)
			},
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty_workgroup_returns_empty_list",
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListPreparedStatements", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string][]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["PreparedStatements"], tt.wantCount)
		})
	}
}

func TestHandler_ListPreparedStatements_ReturnsSummary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreatePreparedStatement",
		`{"StatementName":"stmt1","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)

	rec := doRequest(t, h, "ListPreparedStatements", `{"WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string][]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp["PreparedStatements"], 1)

	sum := resp["PreparedStatements"][0]
	assert.Equal(t, "stmt1", sum["StatementName"])
	assert.NotZero(t, sum["LastModifiedTime"])
	assert.Empty(t, sum["QueryStatement"], "summary must not contain QueryStatement")
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

func TestHandler_QueryExecution_EngineVersionAndStatementType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		query             string
		wantStatementType string
	}{
		{name: "dml_select", query: "SELECT 1", wantStatementType: "DML"},
		{name: "ddl_create", query: "CREATE TABLE foo (id int)", wantStatementType: "DDL"},
		{name: "ddl_drop", query: "DROP TABLE foo", wantStatementType: "DDL"},
		{name: "utility_show", query: "SHOW TABLES", wantStatementType: "UTILITY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			startRec := doRequest(t, h, "StartQueryExecution",
				`{"QueryString":"`+tt.query+`","WorkGroup":"primary"}`)
			require.Equal(t, http.StatusOK, startRec.Code)

			id := jsonField(t, startRec.Body.Bytes(), "QueryExecutionId")

			rec := doRequest(t, h, "GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			qe := resp["QueryExecution"].(map[string]any)

			assert.Equal(t, tt.wantStatementType, qe["StatementType"])

			ev := qe["EngineVersion"].(map[string]any)
			assert.Equal(t, "AUTO", ev["SelectedEngineVersion"])
			assert.NotEmpty(t, ev["EffectiveEngineVersion"])

			stats := qe["Statistics"].(map[string]any)
			assert.NotZero(t, stats["EngineExecutionTimeInMillis"])
			assert.NotZero(t, stats["TotalExecutionTimeInMillis"])
		})
	}
}

func TestHandler_QueryExecution_ExecutionParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartQueryExecution",
		`{"QueryString":"SELECT ? FROM t WHERE id=?","WorkGroup":"primary","ExecutionParameters":["col1","42"]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	id := jsonField(t, rec.Body.Bytes(), "QueryExecutionId")

	rec = doRequest(t, h, "GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	qe := resp["QueryExecution"].(map[string]any)

	params, ok := qe["ExecutionParameters"].([]any)
	require.True(t, ok)
	require.Len(t, params, 2)
	assert.Equal(t, "col1", params[0])
	assert.Equal(t, "42", params[1])
}

func TestHandler_ResultConfiguration_AclAndOwner(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := `{
		"QueryString":"SELECT 1",
		"WorkGroup":"primary",
		"ResultConfiguration":{
			"OutputLocation":"s3://my-bucket/results/",
			"ExpectedBucketOwner":"111111111111",
			"ACLConfiguration":{"S3AclOption":"BUCKET_OWNER_FULL_CONTROL"},
			"EncryptionConfiguration":{"EncryptionOption":"SSE_KMS","KmsKey":"arn:aws:kms:us-east-1:000000000000:key/abc"}
		}
	}`
	rec := doRequest(t, h, "StartQueryExecution", body)
	require.Equal(t, http.StatusOK, rec.Code)

	id := jsonField(t, rec.Body.Bytes(), "QueryExecutionId")
	rec = doRequest(t, h, "GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	qe := resp["QueryExecution"].(map[string]any)
	rc := qe["ResultConfiguration"].(map[string]any)

	assert.Equal(t, "s3://my-bucket/results/", rc["OutputLocation"])
	assert.Equal(t, "111111111111", rc["ExpectedBucketOwner"])
	acl := rc["ACLConfiguration"].(map[string]any)
	assert.Equal(t, "BUCKET_OWNER_FULL_CONTROL", acl["S3AclOption"])
	enc := rc["EncryptionConfiguration"].(map[string]any)
	assert.Equal(t, "SSE_KMS", enc["EncryptionOption"])
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

func TestHandler_CapacityReservation_LastAllocationStruct(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap-test","TargetDpus":24}`)

	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"cap-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cr := resp["CapacityReservation"].(map[string]any)

	assert.Equal(t, "ACTIVE", cr["Status"])
	assert.NotZero(t, cr["CreationTime"])
	assert.NotZero(t, cr["LastSuccessfulAllocationTime"])

	lastAlloc := cr["LastAllocation"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
	assert.NotZero(t, lastAlloc["RequestTime"])
	assert.NotZero(t, lastAlloc["RequestCompletionTime"])
}

func TestHandler_CapacityReservation_UpdateLastAllocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap-upd","TargetDpus":24}`)
	_ = doRequest(t, h, "UpdateCapacityReservation", `{"Name":"cap-upd","TargetDpus":30}`)

	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"cap-upd"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cr := resp["CapacityReservation"].(map[string]any)

	assert.InDelta(t, float64(30), cr["TargetDpus"], 0.001)
	lastAlloc := cr["LastAllocation"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
}

func TestBackend_ARNsUseRegionAndAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		accountID string
	}{
		{
			name:      "default us-east-1",
			region:    "us-east-1",
			accountID: "000000000000",
		},
		{
			name:      "eu-west-1 cross-region",
			region:    "eu-west-1",
			accountID: "111122223333",
		},
		{
			name:      "ap-southeast-2 cross-region",
			region:    "ap-southeast-2",
			accountID: "999988887777",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend(tt.region, tt.accountID)

			err := b.CreateWorkGroup(
				"my-wg", "test", "ENABLED", athena.WorkGroupConfiguration{}, map[string]string{"env": "test"},
			)
			require.NoError(t, err)

			wgARN := "arn:aws:athena:" + tt.region + ":" + tt.accountID + ":workgroup/my-wg"
			gotTags, err := b.ListTagsForResource(wgARN)
			require.NoError(t, err)
			require.Len(t, gotTags, 1)
			assert.Equal(t, "env", gotTags[0].Key)

			presigned, err := b.CreatePresignedNotebookURL("sess-1")
			require.NoError(t, err)
			assert.Contains(t, presigned, tt.region)
			assert.Contains(t, presigned, "sess-1")
		})
	}
}
