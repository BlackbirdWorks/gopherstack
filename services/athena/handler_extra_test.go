package athena_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// --- Helpers ---

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

func jsonNested(t *testing.T, body []byte, keys ...string) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	cur := m

	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		require.True(t, ok, "missing nested key %q", k)
		cur = next
	}

	return cur
}

func startSession(t *testing.T, h *athena.Handler) string {
	t.Helper()

	rec := doRequest(t, h, "StartSession", `{"WorkGroup":"primary","Description":"test","NotebookVersion":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "SessionId")
}

func startCalc(t *testing.T, h *athena.Handler, sessionID string) string {
	t.Helper()

	rec := doRequest(t, h, "StartCalculationExecution",
		`{"SessionId":"`+sessionID+`","CodeBlock":"print(1)","Description":"hi"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "CalculationExecutionId")
}

func capacityHandler(t *testing.T) *athena.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap1","TargetDpus":24}`).Code)

	return h
}

func importNotebook(t *testing.T, h *athena.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "ImportNotebook",
		`{"WorkGroup":"primary","Name":"`+name+`","Payload":"{}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "NotebookId")
}

// --- Session tests ---

func TestHandler_StartSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Description":"test","NotebookVersion":"v1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_workgroup",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_workgroup",
			body:       `{"WorkGroup":"nope"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartSession", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantField  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantField:  "SessionId",
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSession", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.NotEmpty(t, jsonField(t, rec.Body.Bytes(), tt.wantField))
			}
		})
	}
}

func TestHandler_GetSessionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantState  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantState:  "IDLE",
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSessionStatus", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				st := jsonNested(t, rec.Body.Bytes(), "Status")
				assert.Equal(t, tt.wantState, st["State"])
			}
		})
	}
}

func TestHandler_GetSessionEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sessionID    string
		wantContains string
		wantStatus   int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			rec := doRequest(t, h, "GetSessionEndpoint", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				assert.Contains(t, jsonField(t, rec.Body.Bytes(), "SessionEndpoint"), id)
			}
		})
	}
}

func TestHandler_ListSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantNonEmpty bool
	}{
		{
			name:         "success",
			body:         `{"WorkGroup":"primary"}`,
			wantStatus:   http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name:       "filter_excludes",
			body:       `{"WorkGroup":"other","StateFilter":"BUSY"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			startSession(t, h)

			rec := doRequest(t, h, "ListSessions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantNonEmpty {
				var m map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
				assert.NotEmpty(t, m["Sessions"])
			}
		})
	}
}

func TestHandler_TerminateSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sessionID  string
		wantState  string
		wantStatus int
		double     bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantState:  "TERMINATED",
		},
		{
			name:       "double_terminate",
			double:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.sessionID

			if id == "" {
				id = startSession(t, h)
			}

			if tt.double {
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
			}

			rec := doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantState != "" {
				assert.Equal(t, tt.wantState, jsonField(t, rec.Body.Bytes(), "State"))
			}
		})
	}
}

func TestHandler_ListNotebookSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_empty_id",
			notebookID: "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			if tt.name == "success" {
				createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"nb1"}`)
				require.Equal(t, http.StatusOK, createRec.Code)
				notebookID = jsonField(t, createRec.Body.Bytes(), "NotebookId")

				startRec := doRequest(t, h, "StartSession",
					`{"WorkGroup":"primary","NotebookVersion":"v1","NotebookId":"`+notebookID+`"}`)
				require.Equal(t, http.StatusOK, startRec.Code)
			}

			rec := doRequest(t, h, "ListNotebookSessions", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Calculation tests ---

func TestHandler_StartCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		terminate  bool
		wantStatus int
	}{
		{
			name:       "no_session",
			body:       `{"CodeBlock":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			body:       `{"SessionId":"missing","CodeBlock":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminated_session",
			terminate:  true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body

			if tt.terminate {
				id := startSession(t, h)
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
				body = `{"SessionId":"` + id + `","CodeBlock":"x"}`
			}

			rec := doRequest(t, h, "StartCalculationExecution", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantField  string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantField:  "SessionId",
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecution", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.NotEmpty(t, jsonField(t, rec.Body.Bytes(), tt.wantField))
			}
		})
	}
}

func TestHandler_GetCalculationExecutionStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecutionStatus", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCalculationExecutionCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			wantCode:   "print(1)",
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "GetCalculationExecutionCode", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, jsonField(t, rec.Body.Bytes(), "CodeBlock"))
			}
		})
	}
}

func TestHandler_StopCalculationExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		calcID     string
		wantStatus int
	}{
		{
			name:       "terminal_fails",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			calcID:     "x",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			calcID := tt.calcID

			if calcID == "" {
				sid := startSession(t, h)
				calcID = startCalc(t, h, sid)
			}

			rec := doRequest(t, h, "StopCalculationExecution", `{"CalculationExecutionId":"`+calcID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBackend_StopCalculation_Cancellable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		wantState string
		wantErr   bool
	}{
		{
			name:      "running_can_be_canceled",
			state:     "RUNNING",
			wantState: "CANCELED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("", "")
			sid, _, err := backend.StartSession("primary", "", "",
				athena.EngineConfiguration{}, athena.SessionConfiguration{}, "")
			require.NoError(t, err)

			cid, _, err := backend.StartCalculationExecution(sid, "", "x")
			require.NoError(t, err)

			backend.SetCalculationState(cid, tt.state)
			got, err := backend.StopCalculationExecution(cid)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got)
		})
	}
}

func TestHandler_ListCalculationExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		useSession bool
		wantStatus int
	}{
		{
			name:       "success",
			useSession: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "filter",
			useSession: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_session",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			body:       `{"SessionId":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body

			if tt.useSession {
				sid := startSession(t, h)
				startCalc(t, h, sid)

				if tt.name == "filter" {
					body = `{"SessionId":"` + sid + `","StateFilter":"NEVER"}`
				} else {
					body = `{"SessionId":"` + sid + `"}`
				}
			}

			rec := doRequest(t, h, "ListCalculationExecutions", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Capacity tests ---

func TestHandler_GetCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resName    string
		wantStatus int
	}{
		{
			name:       "success",
			resName:    "cap1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			resName:    "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"`+tt.resName+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListCapacityReservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		wantNonEmpty bool
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "ListCapacityReservations", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"cap1","TargetDpus":30}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_dpus",
			body:       `{"Name":"cap1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"Name":"missing","TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "UpdateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutCapacityAssignmentConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantStatus   int
	}{
		{
			name:       "success",
			body:       `{"CapacityReservationName":"cap1","CapacityAssignments":[{"WorkGroupNames":["primary"]}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_name",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CapacityReservationName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "PutCapacityAssignmentConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCapacityAssignmentConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resName      string
		wantContains string
		wantStatus   int
		setup        bool
	}{
		{
			name:         "success",
			resName:      "cap1",
			setup:        true,
			wantStatus:   http.StatusOK,
			wantContains: "primary",
		},
		{
			name:       "not_found",
			resName:    "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)

			if tt.setup {
				doRequest(t, h, "PutCapacityAssignmentConfiguration",
					`{"CapacityReservationName":"cap1","CapacityAssignments":[{"WorkGroupNames":["primary"]}]}`)
			}

			rec := doRequest(t, h, "GetCapacityAssignmentConfiguration",
				`{"CapacityReservationName":"`+tt.resName+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// --- Metadata tests ---

func TestHandler_GetDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_catalog",
			body:       `{"CatalogName":""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_database",
			body:       `{"CatalogName":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDatabase", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_catalog",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListDatabases", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetTableMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"sample_table"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_missing_fields",
			body:       `{"CatalogName":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetTableMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTableMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantExclude  string
		wantStatus   int
	}{
		{
			name:       "success",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "filtered_include",
			body:       `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"sample"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:        "filtered_exclude",
			body:        `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"nope"}`,
			wantStatus:  http.StatusOK,
			wantExclude: "sample_table",
		},
		{
			name:       "validation_no_catalog",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListTableMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}

			if tt.wantExclude != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantExclude)
			}
		})
	}
}

// --- Notebook tests ---

func TestHandler_ImportNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setup      bool
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"WorkGroup":"primary","Name":"imported","Payload":"{}"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_workgroup",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_name",
			body:       `{"WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_payload",
			body:       `{"WorkGroup":"primary","Name":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate",
			body:       `{"WorkGroup":"primary","Name":"imported","Payload":"{}"}`,
			setup:      true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup {
				importNotebook(t, h, "imported")
			}

			rec := doRequest(t, h, "ImportNotebook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			if notebookID == "" {
				notebookID = importNotebook(t, h, "imp1")
			}

			rec := doRequest(t, h, "GetNotebookMetadata", `{"NotebookId":"`+notebookID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "filtered_match",
			body:         `{"WorkGroup":"primary","Filters":{"Name":"imp"}}`,
			wantStatus:   http.StatusOK,
			wantContains: "imported",
		},
		{
			name:       "no_match",
			body:       `{"WorkGroup":"other"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			importNotebook(t, h, "imported")

			rec := doRequest(t, h, "ListNotebookMetadata", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestHandler_UpdateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		sessionID  string
		payload    string
		wantStatus int
	}{
		{
			name:       "success",
			payload:    "new",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_notebook_id",
			notebookID: "",
			payload:    "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_payload",
			notebookID: "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			payload:    "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_session",
			payload:    "x",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "with_valid_session",
			payload:    "x",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			notebookID := tt.notebookID
			sessionID := tt.sessionID

			if tt.name == "success" || tt.name == "unknown_session" || tt.name == "with_valid_session" {
				notebookID = importNotebook(t, h, "nb-"+tt.name)
			}

			if tt.name == "with_valid_session" {
				sessionID = startSession(t, h)
			}

			body := `{"NotebookId":"` + notebookID + `","Payload":"` + tt.payload + `"`
			if sessionID != "" {
				body += `,"SessionId":"` + sessionID + `"`
			}

			body += `}`

			rec := doRequest(t, h, "UpdateNotebook", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateNotebookMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notebookID string
		newName    string
		conflict   bool
		wantStatus int
	}{
		{
			name:       "success",
			newName:    "renamed",
			wantStatus: http.StatusOK,
		},
		{
			name:       "idempotent",
			newName:    "renamed",
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_notebook_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_name",
			notebookID: "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			notebookID: "missing",
			newName:    "x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "conflict",
			newName:    "renamed-target",
			conflict:   true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			notebookID := tt.notebookID

			switch tt.name {
			case "success", "idempotent":
				notebookID = importNotebook(t, h, "rename-me-"+tt.name)
				if tt.name == "idempotent" {
					doRequest(t, h, "UpdateNotebookMetadata",
						`{"NotebookId":"`+notebookID+`","Name":"renamed"}`)
				}
			case "conflict":
				importNotebook(t, h, "renamed-target")
				other := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"other"}`)
				require.Equal(t, http.StatusOK, other.Code)
				notebookID = jsonField(t, other.Body.Bytes(), "NotebookId")
			}

			body := `{"NotebookId":"` + notebookID + `","Name":"` + tt.newName + `"}`
			rec := doRequest(t, h, "UpdateNotebookMetadata", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Named query / prepared statement tests ---

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

func TestHandler_UpdatePreparedStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setup      bool
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"StatementName":"ps","WorkGroup":"primary","QueryStatement":"SELECT 9","Description":"d"}`,
			setup:      true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_name",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_workgroup",
			body:       `{"StatementName":"ps"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "validation_no_query",
			body:       `{"StatementName":"ps","WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"StatementName":"missing","WorkGroup":"primary","QueryStatement":"x"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup {
				doRequest(t, h, "CreatePreparedStatement",
					`{"StatementName":"ps","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)
			}

			rec := doRequest(t, h, "UpdatePreparedStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Misc / engine tests ---

func TestHandler_ListEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantContains: "engine version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListEngineVersions", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ListApplicationDPUSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantContains: "dpu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListApplicationDPUSizes", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, strings.ToLower(rec.Body.String()), tt.wantContains)
		})
	}
}

func TestHandler_ListExecutors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionID   string
		stateFilter string
		terminate   bool
		wantStatus  int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:        "filtered_out",
			stateFilter: "NEVER",
			wantStatus:  http.StatusOK,
		},
		{
			name:       "validation_no_session",
			sessionID:  "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			sessionID:  "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminated_session",
			terminate:  true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			sid := tt.sessionID

			needsSession := sid == "" && tt.name != "validation_no_session"
			if needsSession {
				sid = startSession(t, h)
			}

			if tt.terminate {
				doRequest(t, h, "TerminateSession", `{"SessionId":"`+sid+`"}`)
			}

			body := `{"SessionId":"` + sid + `"`
			if tt.stateFilter != "" {
				body += `,"ExecutorStateFilter":"` + tt.stateFilter + `"`
			}

			body += `}`

			rec := doRequest(t, h, "ListExecutors", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetQueryRuntimeStatistics(t *testing.T) {
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

			if queryID == "" {
				startQE := doRequest(t, h, "StartQueryExecution",
					`{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, startQE.Code)
				queryID = jsonField(t, startQE.Body.Bytes(), "QueryExecutionId")
			}

			rec := doRequest(t, h, "GetQueryRuntimeStatistics", `{"QueryExecutionId":"`+queryID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
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
