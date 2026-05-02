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

// jsonField extracts a string field from a JSON response body.
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

// jsonNested gets a nested map from a JSON response.
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

func TestHandler_StartSession_MissingWorkgroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartSession", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartSession_UnknownWorkgroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartSession", `{"WorkGroup":"nope"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetSession_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := startSession(t, h)

	rec := doRequest(t, h, "GetSession", `{"SessionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, id, jsonField(t, rec.Body.Bytes(), "SessionId"))
}

func TestHandler_GetSession_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetSession", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetSessionStatus_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := startSession(t, h)

	rec := doRequest(t, h, "GetSessionStatus", `{"SessionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	st := jsonNested(t, rec.Body.Bytes(), "Status")
	assert.Equal(t, "IDLE", st["State"])
}

func TestHandler_GetSessionStatus_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetSessionStatus", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetSessionEndpoint_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := startSession(t, h)

	rec := doRequest(t, h, "GetSessionEndpoint", `{"SessionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, jsonField(t, rec.Body.Bytes(), "SessionEndpoint"), id)
}

func TestHandler_GetSessionEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetSessionEndpoint", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListSessions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startSession(t, h)

	rec := doRequest(t, h, "ListSessions", `{"WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var m map[string]any

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	assert.NotEmpty(t, m["Sessions"])
}

func TestHandler_ListSessions_FilterExcludes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListSessions", `{"WorkGroup":"other","StateFilter":"BUSY"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_TerminateSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := startSession(t, h)

	rec := doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "TERMINATED", jsonField(t, rec.Body.Bytes(), "State"))

	rec2 := doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_TerminateSession_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TerminateSession", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListNotebookSessions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"nb1"}`)
	require.Equal(t, http.StatusOK, createRec.Code)
	notebookID := jsonField(t, createRec.Body.Bytes(), "NotebookId")

	startRec := doRequest(t, h, "StartSession",
		`{"WorkGroup":"primary","NotebookVersion":"v1","NotebookId":"`+notebookID+`"}`)
	require.Equal(t, http.StatusOK, startRec.Code)

	rec := doRequest(t, h, "ListNotebookSessions", `{"NotebookId":"`+notebookID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListNotebookSessions_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListNotebookSessions", `{"NotebookId":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListNotebookSessions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListNotebookSessions", `{"NotebookId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func startCalc(t *testing.T, h *athena.Handler, sessionID string) string {
	t.Helper()

	rec := doRequest(t, h, "StartCalculationExecution",
		`{"SessionId":"`+sessionID+`","CodeBlock":"print(1)","Description":"hi"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "CalculationExecutionId")
}

func TestHandler_StartCalc_NoSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartCalculationExecution", `{"CodeBlock":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartCalc_UnknownSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartCalculationExecution", `{"SessionId":"missing","CodeBlock":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartCalc_TerminatedSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := startSession(t, h)
	_ = doRequest(t, h, "TerminateSession", `{"SessionId":"`+id+`"}`)

	rec := doRequest(t, h, "StartCalculationExecution", `{"SessionId":"`+id+`","CodeBlock":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetCalc(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	cid := startCalc(t, h, sid)

	rec := doRequest(t, h, "GetCalculationExecution", `{"CalculationExecutionId":"`+cid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, sid, jsonField(t, rec.Body.Bytes(), "SessionId"))
}

func TestHandler_GetCalc_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetCalculationExecution", `{"CalculationExecutionId":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetCalcStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	cid := startCalc(t, h, sid)

	rec := doRequest(t, h, "GetCalculationExecutionStatus", `{"CalculationExecutionId":"`+cid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetCalcStatus_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetCalculationExecutionStatus", `{"CalculationExecutionId":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetCalcCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	cid := startCalc(t, h, sid)

	rec := doRequest(t, h, "GetCalculationExecutionCode", `{"CalculationExecutionId":"`+cid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "print(1)", jsonField(t, rec.Body.Bytes(), "CodeBlock"))
}

func TestHandler_GetCalcCode_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetCalculationExecutionCode", `{"CalculationExecutionId":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StopCalc_TerminalFails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	cid := startCalc(t, h, sid)

	rec := doRequest(t, h, "StopCalculationExecution", `{"CalculationExecutionId":"`+cid+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StopCalc_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopCalculationExecution", `{"CalculationExecutionId":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCalcs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	startCalc(t, h, sid)

	rec := doRequest(t, h, "ListCalculationExecutions", `{"SessionId":"`+sid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListCalcs_Filter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	startCalc(t, h, sid)

	rec := doRequest(t, h, "ListCalculationExecutions", `{"SessionId":"`+sid+`","StateFilter":"NEVER"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListCalcs_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListCalculationExecutions", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCalcs_UnknownSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListCalculationExecutions", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBackend_StopCalculation_Cancellable(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend()
	sid, _, err := backend.StartSession("primary", "", "",
		athena.EngineConfiguration{}, athena.SessionConfiguration{}, "")
	require.NoError(t, err)
	cid, _, err := backend.StartCalculationExecution(sid, "", "x")
	require.NoError(t, err)

	backend.SetCalculationState(cid, "RUNNING")
	state, err := backend.StopCalculationExecution(cid)
	require.NoError(t, err)
	assert.Equal(t, "CANCELED", state)
}

func capacityHandler(t *testing.T) *athena.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap1","TargetDpus":4}`).Code)

	return h
}

func TestHandler_GetCapacityReservation(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"cap1"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetCapacityReservation_NotFound(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCapacityReservations(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "ListCapacityReservations", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateCapacityReservation(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "UpdateCapacityReservation", `{"Name":"cap1","TargetDpus":8}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateCapacityReservation_Invalid(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "UpdateCapacityReservation", `{"Name":"cap1","TargetDpus":0}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateCapacityReservation_NotFound(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "UpdateCapacityReservation", `{"Name":"missing","TargetDpus":2}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PutAndGetCapacityAssignment(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	body := `{"CapacityReservationName":"cap1","CapacityAssignments":[{"WorkGroupNames":["primary"]}]}`
	rec := doRequest(t, h, "PutCapacityAssignmentConfiguration", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "GetCapacityAssignmentConfiguration", `{"CapacityReservationName":"cap1"}`)
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "primary")
}

func TestHandler_PutCapacityAssignment_Validation(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "PutCapacityAssignmentConfiguration", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PutCapacityAssignment_NotFound(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "PutCapacityAssignmentConfiguration", `{"CapacityReservationName":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetCapacityAssignment_NotFound(t *testing.T) {
	t.Parallel()

	h := capacityHandler(t)
	rec := doRequest(t, h, "GetCapacityAssignmentConfiguration", `{"CapacityReservationName":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDatabase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetDatabase", `{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetDatabase_ValidationCatalog(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetDatabase", `{"CatalogName":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDatabase_ValidationDB(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetDatabase", `{"CatalogName":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDatabase_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetDatabase", `{"CatalogName":"AwsDataCatalog","DatabaseName":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListDatabases", `{"CatalogName":"AwsDataCatalog"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListDatabases_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListDatabases", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetTableMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetTableMetadata",
		`{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"sample_table"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetTableMetadata_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetTableMetadata", `{"CatalogName":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetTableMetadata_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetTableMetadata",
		`{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTableMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTableMetadata",
		`{"CatalogName":"AwsDataCatalog","DatabaseName":"default"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTableMetadata_Filtered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTableMetadata",
		`{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"sample"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTableMetadata_FilterExcludes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTableMetadata",
		`{"CatalogName":"AwsDataCatalog","DatabaseName":"default","Expression":"nope"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "sample_table")
}

func TestHandler_ListTableMetadata_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTableMetadata", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ImportNotebook_Validations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, body := range []string{`{}`, `{"WorkGroup":"primary"}`, `{"WorkGroup":"primary","Name":"x"}`} {
		rec := doRequest(t, h, "ImportNotebook", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func importNotebook(t *testing.T, h *athena.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "ImportNotebook",
		`{"WorkGroup":"primary","Name":"`+name+`","Payload":"{}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	return jsonField(t, rec.Body.Bytes(), "NotebookId")
}

func TestHandler_ImportNotebook_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	importNotebook(t, h, "imported")

	dup := doRequest(t, h, "ImportNotebook", `{"WorkGroup":"primary","Name":"imported","Payload":"{}"}`)
	assert.Equal(t, http.StatusBadRequest, dup.Code)
}

func TestHandler_GetNotebookMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := importNotebook(t, h, "imp1")

	rec := doRequest(t, h, "GetNotebookMetadata", `{"NotebookId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetNotebookMetadata_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetNotebookMetadata", `{"NotebookId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListNotebookMetadata_Filtered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	importNotebook(t, h, "imported")

	rec := doRequest(t, h, "ListNotebookMetadata", `{"WorkGroup":"primary","Filters":{"Name":"imp"}}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "imported")
}

func TestHandler_ListNotebookMetadata_Excludes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListNotebookMetadata", `{"WorkGroup":"other"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateNotebook(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := importNotebook(t, h, "u1")

	rec := doRequest(t, h, "UpdateNotebook", `{"NotebookId":"`+id+`","Payload":"new","Type":"IPYNB"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateNotebook_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, body := range []string{`{}`, `{"NotebookId":"x"}`} {
		rec := doRequest(t, h, "UpdateNotebook", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func TestHandler_UpdateNotebook_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateNotebook", `{"NotebookId":"missing","Payload":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateNotebook_UnknownSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := importNotebook(t, h, "us1")

	rec := doRequest(t, h, "UpdateNotebook",
		`{"NotebookId":"`+id+`","Payload":"x","SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateNotebook_WithSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := importNotebook(t, h, "us2")
	sid := startSession(t, h)

	rec := doRequest(t, h, "UpdateNotebook",
		`{"NotebookId":"`+id+`","Payload":"x","SessionId":"`+sid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateNotebookMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := importNotebook(t, h, "rename-me")

	rec := doRequest(t, h, "UpdateNotebookMetadata", `{"NotebookId":"`+id+`","Name":"renamed"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// idempotent
	rec2 := doRequest(t, h, "UpdateNotebookMetadata", `{"NotebookId":"`+id+`","Name":"renamed"}`)
	require.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_UpdateNotebookMetadata_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, body := range []string{`{}`, `{"NotebookId":"x"}`} {
		rec := doRequest(t, h, "UpdateNotebookMetadata", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func TestHandler_UpdateNotebookMetadata_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateNotebookMetadata", `{"NotebookId":"missing","Name":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateNotebookMetadata_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	importNotebook(t, h, "renamed-target")

	otherRec := doRequest(t, h, "CreateNotebook", `{"WorkGroup":"primary","Name":"other"}`)
	require.Equal(t, http.StatusOK, otherRec.Code)
	otherID := jsonField(t, otherRec.Body.Bytes(), "NotebookId")

	rec := doRequest(t, h, "UpdateNotebookMetadata",
		`{"NotebookId":"`+otherID+`","Name":"renamed-target"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateNamedQuery(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateNamedQuery",
		`{"Name":"q","Database":"db","QueryString":"SELECT 1","WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	qid := jsonField(t, rec.Body.Bytes(), "NamedQueryId")

	upd := doRequest(t, h, "UpdateNamedQuery",
		`{"NamedQueryId":"`+qid+`","Name":"q2","QueryString":"SELECT 2","Description":"x"}`)
	require.Equal(t, http.StatusOK, upd.Code)
}

func TestHandler_UpdateNamedQuery_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateNamedQuery", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateNamedQuery_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateNamedQuery", `{"NamedQueryId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdatePreparedStatement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreatePreparedStatement",
			`{"StatementName":"ps","WorkGroup":"primary","QueryStatement":"SELECT 1"}`).Code)

	rec := doRequest(t, h, "UpdatePreparedStatement",
		`{"StatementName":"ps","WorkGroup":"primary","QueryStatement":"SELECT 9","Description":"d"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdatePreparedStatement_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, body := range []string{`{}`, `{"StatementName":"ps"}`, `{"StatementName":"ps","WorkGroup":"primary"}`} {
		rec := doRequest(t, h, "UpdatePreparedStatement", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func TestHandler_UpdatePreparedStatement_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdatePreparedStatement",
		`{"StatementName":"missing","WorkGroup":"primary","QueryStatement":"x"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListEngineVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListEngineVersions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "engine version")
}

func TestHandler_ListApplicationDPUSizes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListApplicationDPUSizes", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, strings.ToLower(rec.Body.String()), "dpu")
}

func TestHandler_ListExecutors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)

	rec := doRequest(t, h, "ListExecutors", `{"SessionId":"`+sid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListExecutors_FilteredOut(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)

	rec := doRequest(t, h, "ListExecutors", `{"SessionId":"`+sid+`","ExecutorStateFilter":"NEVER"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListExecutors_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListExecutors", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListExecutors_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListExecutors", `{"SessionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListExecutors_Terminated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sid := startSession(t, h)
	_ = doRequest(t, h, "TerminateSession", `{"SessionId":"`+sid+`"}`)

	rec := doRequest(t, h, "ListExecutors", `{"SessionId":"`+sid+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetQueryRuntimeStatistics(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startQE := doRequest(t, h, "StartQueryExecution",
		`{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, startQE.Code)
	queryID := jsonField(t, startQE.Body.Bytes(), "QueryExecutionId")

	rec := doRequest(t, h, "GetQueryRuntimeStatistics", `{"QueryExecutionId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetQueryRuntimeStatistics_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetQueryRuntimeStatistics", `{"QueryExecutionId":"missing"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_BadJSON_Extra(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, action := range []string{
		"StartSession", "GetSession", "GetSessionStatus", "GetSessionEndpoint", "TerminateSession",
		"ListSessions", "ListNotebookSessions",
		"StartCalculationExecution", "GetCalculationExecution", "GetCalculationExecutionStatus",
		"GetCalculationExecutionCode", "StopCalculationExecution", "ListCalculationExecutions",
		"GetCapacityReservation", "UpdateCapacityReservation", "PutCapacityAssignmentConfiguration",
		"GetCapacityAssignmentConfiguration",
		"GetDatabase", "ListDatabases", "GetTableMetadata", "ListTableMetadata",
		"GetNotebookMetadata", "ListNotebookMetadata", "ImportNotebook", "UpdateNotebook", "UpdateNotebookMetadata",
		"UpdateNamedQuery", "UpdatePreparedStatement", "ListExecutors", "GetQueryRuntimeStatistics",
	} {
		rec := doRequest(t, h, action, `{not json`)
		assert.NotEqual(t, http.StatusOK, rec.Code, action)
	}
}
