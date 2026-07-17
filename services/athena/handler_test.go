package athena_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- shared test helpers ---

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

func jsonField(t *testing.T, body []byte, key string) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	v, ok := m[key]
	require.True(t, ok, "key %q missing", key)

	s, ok := v.(string)
	require.True(t, ok, "key %q is not string", key)

	return s
}

func athenaDoPass5(t *testing.T, h *athena.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonAthena."+action)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func athenaUnmarshalPass5(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func a1Handler(t *testing.T) *athena.Handler {
	t.Helper()

	return athena.NewHandler(athena.NewInMemoryBackend("", ""))
}

func a1Do(t *testing.T, h *athena.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonAthena."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1Unmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// --- core dispatch / protocol tests ---

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

			presigned, authToken, authTokenExpiration, err := b.CreatePresignedNotebookURL("sess-1")
			require.NoError(t, err)
			assert.Contains(t, presigned, tt.region)
			assert.Contains(t, presigned, "sess-1")
			assert.NotEmpty(t, authToken)
			assert.Positive(t, authTokenExpiration)
		})
	}
}

func TestHandler_BadJSON_Extra(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "start_session", action: "StartSession"},
		{name: "get_session", action: "GetSession"},
		{name: "get_session_status", action: "GetSessionStatus"},
		{name: "get_session_endpoint", action: "GetSessionEndpoint"},
		{name: "terminate_session", action: "TerminateSession"},
		{name: "list_sessions", action: "ListSessions"},
		{name: "list_notebook_sessions", action: "ListNotebookSessions"},
		{name: "start_calculation", action: "StartCalculationExecution"},
		{name: "get_calculation", action: "GetCalculationExecution"},
		{name: "get_calculation_status", action: "GetCalculationExecutionStatus"},
		{name: "get_calculation_code", action: "GetCalculationExecutionCode"},
		{name: "stop_calculation", action: "StopCalculationExecution"},
		{name: "list_calculations", action: "ListCalculationExecutions"},
		{name: "get_capacity_reservation", action: "GetCapacityReservation"},
		{name: "update_capacity_reservation", action: "UpdateCapacityReservation"},
		{name: "put_capacity_assignment", action: "PutCapacityAssignmentConfiguration"},
		{name: "get_capacity_assignment", action: "GetCapacityAssignmentConfiguration"},
		{name: "get_database", action: "GetDatabase"},
		{name: "list_databases", action: "ListDatabases"},
		{name: "get_table_metadata", action: "GetTableMetadata"},
		{name: "list_table_metadata", action: "ListTableMetadata"},
		{name: "get_notebook_metadata", action: "GetNotebookMetadata"},
		{name: "list_notebook_metadata", action: "ListNotebookMetadata"},
		{name: "import_notebook", action: "ImportNotebook"},
		{name: "update_notebook", action: "UpdateNotebook"},
		{name: "update_notebook_metadata", action: "UpdateNotebookMetadata"},
		{name: "update_named_query", action: "UpdateNamedQuery"},
		{name: "update_prepared_statement", action: "UpdatePreparedStatement"},
		{name: "list_executors", action: "ListExecutors"},
		{name: "get_query_runtime_statistics", action: "GetQueryRuntimeStatistics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, `{not json`)
			assert.NotEqual(t, http.StatusOK, rec.Code)
		})
	}
}

func TestProtocol_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       string
		wantCT     string
		wantStatus int
	}{
		{
			name:       "success_response_has_json11_content_type",
			action:     "ListWorkGroups",
			body:       `{}`,
			wantStatus: http.StatusOK,
			wantCT:     "application/x-amz-json-1.1",
		},
		{
			name:       "error_response_has_json11_content_type",
			action:     "GetWorkGroup",
			body:       `{"WorkGroup":"nope"}`,
			wantStatus: http.StatusBadRequest,
			wantCT:     "application/x-amz-json-1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			rec := a1Do(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestProtocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{name: "not_found", action: "GetWorkGroup", body: `{"WorkGroup":"missing"}`},
		{name: "already_exists", action: "CreateWorkGroup", body: `{"Name":"primary"}`},
		{name: "unknown_operation", action: "NoSuchOp", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			rec := a1Do(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			m := a1Unmarshal(t, rec)
			assert.NotEmpty(t, m["__type"], "__type field must be present in error response")
			assert.NotEmpty(t, m["message"], "message field must be present in error response")
		})
	}
}

func TestProtocol_HTTPMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GET_slash_returns_op_list",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "PUT_returns_405",
			method:     http.MethodPut,
			path:       "/",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "DELETE_returns_405",
			method:     http.MethodDelete,
			path:       "/",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestProtocol_MissingTarget(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
