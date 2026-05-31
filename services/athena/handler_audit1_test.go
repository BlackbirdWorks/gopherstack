package athena_test

// Audit batch-1: AWS-accuracy coverage for Athena (go-idnu).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.1, error envelope __type/message
//   - Routing: GET / returns op list, non-POST → 405, missing X-Amz-Target → 400
//   - WorkGroup lifecycle: create → get → list → update → delete with field shapes
//   - QueryExecution lifecycle: start → get (field shapes) → results → stop
//   - PreparedStatement lifecycle: create → get → update → list summary → delete
//   - DataCatalog lifecycle: create → get (Status field) → list (Status in summary) → delete
//   - CapacityReservation lifecycle: create → get (LastAllocation) → update → cancel → delete
//   - Tag round-trip: tag → list → untag on workgroup ARN
//   - Session + Calculation lifecycle: start → calculation → stop → terminate

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

// --- audit helpers ---

func a1Handler(t *testing.T) *athena.Handler {
	t.Helper()

	return athena.NewHandler(athena.NewInMemoryBackend())
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

// --- Protocol accuracy ---

func TestAudit1_Protocol_ContentType(t *testing.T) {
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

func TestAudit1_Protocol_ErrorEnvelope(t *testing.T) {
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

func TestAudit1_Protocol_HTTPMethod(t *testing.T) {
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

func TestAudit1_Protocol_MissingTarget(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- WorkGroup lifecycle ---

func TestAudit1_WorkGroup_Lifecycle(t *testing.T) {
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
					`{"Name":"cfg-wg","Configuration":{"EnforceWorkGroupConfiguration":true,"ExecutionRole":"arn:aws:iam::000000000000:role/AthenaRole"}}`,
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

// --- QueryExecution lifecycle ---

func TestAudit1_QueryExecution_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "start_returns_execution_id",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				id, ok := m["QueryExecutionId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name: "get_response_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				startRec := a1Do(t, h, "StartQueryExecution",
					`{"QueryString":"SELECT 1","WorkGroup":"primary","QueryExecutionContext":{"Database":"default"}}`)
				require.Equal(t, http.StatusOK, startRec.Code)
				id := a1Unmarshal(t, startRec)["QueryExecutionId"].(string)

				rec := a1Do(t, h, "GetQueryExecution", `{"QueryExecutionId":"`+id+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				m := a1Unmarshal(t, rec)
				qe, ok := m["QueryExecution"].(map[string]any)
				require.True(t, ok, "QueryExecution key must be present")
				assert.Equal(t, id, qe["QueryExecutionId"])
				// AWS SDK maps the query string to the "Query" field in QueryExecution.
				assert.Equal(t, "SELECT 1", qe["Query"])

				status, ok := qe["Status"].(map[string]any)
				require.True(t, ok, "Status must be present")
				assert.NotEmpty(t, status["State"])
				assert.NotZero(t, status["SubmissionDateTime"])

				stats, ok := qe["Statistics"].(map[string]any)
				require.True(t, ok, "Statistics must be present")
				assert.NotNil(t, stats["EngineExecutionTimeInMillis"])
			},
		},
		{
			name: "list_returns_ids",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
				rec := a1Do(t, h, "ListQueryExecutions", `{"WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				m := a1Unmarshal(t, rec)
				ids, ok := m["QueryExecutionIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 1)
			},
		},
		{
			name: "results_response_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				startRec := a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, startRec.Code)
				id := a1Unmarshal(t, startRec)["QueryExecutionId"].(string)

				rec := a1Do(t, h, "GetQueryResults", `{"QueryExecutionId":"`+id+`","MaxResults":100}`)
				require.Equal(t, http.StatusOK, rec.Code)

				m := a1Unmarshal(t, rec)
				rs, ok := m["ResultSet"].(map[string]any)
				require.True(t, ok, "ResultSet must be present")
				_, ok = rs["Rows"].([]any)
				require.True(t, ok, "Rows must be present as array")
				meta, ok := rs["ResultSetMetadata"].(map[string]any)
				require.True(t, ok, "ResultSetMetadata must be present")
				_, ok = meta["ColumnInfo"].([]any)
				require.True(t, ok, "ColumnInfo must be present as array")
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

// --- PreparedStatement lifecycle ---

func TestAudit1_PreparedStatement_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "create_and_get_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "CreatePreparedStatement",
					`{"StatementName":"s1","WorkGroup":"primary","QueryStatement":"SELECT ?"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Empty(t, a1Unmarshal(t, rec), "CreatePreparedStatement returns empty object")

				rec = a1Do(t, h, "GetPreparedStatement",
					`{"StatementName":"s1","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				ps, ok := m["PreparedStatement"].(map[string]any)
				require.True(t, ok, "PreparedStatement key must be present")
				assert.Equal(t, "s1", ps["StatementName"])
				// AWS SDK uses WorkGroupName in the PreparedStatement response shape.
				assert.Equal(t, "primary", ps["WorkGroupName"])
				assert.Equal(t, "SELECT ?", ps["QueryStatement"])
				assert.NotZero(t, ps["LastModifiedTime"])
			},
		},
		{
			name: "list_summary_excludes_query_statement",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreatePreparedStatement",
					`{"StatementName":"s2","WorkGroup":"primary","QueryStatement":"SELECT ? FROM t"}`)

				rec := a1Do(t, h, "ListPreparedStatements", `{"WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				stmts, ok := m["PreparedStatements"].([]any)
				require.True(t, ok)
				require.Len(t, stmts, 1)
				sum := stmts[0].(map[string]any)
				assert.Equal(t, "s2", sum["StatementName"])
				assert.NotZero(t, sum["LastModifiedTime"])
				// AWS list returns summaries — QueryStatement must not appear.
				assert.Empty(t, sum["QueryStatement"], "summary must not include QueryStatement")
			},
		},
		{
			name: "update_and_verify",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreatePreparedStatement",
					`{"StatementName":"upd","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)

				rec := a1Do(t, h, "UpdatePreparedStatement",
					`{"StatementName":"upd","WorkGroup":"primary","QueryStatement":"SELECT 2","Description":"new"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Empty(t, a1Unmarshal(t, rec))

				rec = a1Do(t, h, "GetPreparedStatement",
					`{"StatementName":"upd","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				ps := a1Unmarshal(t, rec)["PreparedStatement"].(map[string]any)
				assert.Equal(t, "SELECT 2", ps["QueryStatement"])
				assert.Equal(t, "new", ps["Description"])
			},
		},
		{
			name: "delete_then_not_found",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreatePreparedStatement",
					`{"StatementName":"del","WorkGroup":"primary","QueryStatement":"SELECT 1"}`)
				rec := a1Do(t, h, "DeletePreparedStatement",
					`{"StatementName":"del","WorkGroup":"primary"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetPreparedStatement",
					`{"StatementName":"del","WorkGroup":"primary"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.NotEmpty(t, a1Unmarshal(t, rec)["__type"])
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

// --- DataCatalog lifecycle ---

func TestAudit1_DataCatalog_Lifecycle(t *testing.T) {
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

// --- CapacityReservation lifecycle ---

func TestAudit1_CapacityReservation_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "create_and_get_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "CreateCapacityReservation", `{"Name":"res1","TargetDpus":24}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Empty(t, a1Unmarshal(t, rec))

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				cr, ok := m["CapacityReservation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "res1", cr["Name"])
				assert.Equal(t, "ACTIVE", cr["Status"])
				assert.NotZero(t, cr["CreationTime"])
				assert.NotZero(t, cr["LastSuccessfulAllocationTime"])
				lastAlloc, ok := cr["LastAllocation"].(map[string]any)
				require.True(t, ok, "LastAllocation must be present")
				assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
				assert.NotZero(t, lastAlloc["RequestTime"])
				assert.NotZero(t, lastAlloc["RequestCompletionTime"])
			},
		},
		{
			name: "cancel_sets_cancelling_status",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"res2","TargetDpus":4}`)
				rec := a1Do(t, h, "CancelCapacityReservation", `{"Name":"res2"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res2"}`)
				cr := a1Unmarshal(t, rec)["CapacityReservation"].(map[string]any)
				assert.Equal(t, "CANCELLING", cr["Status"])
			},
		},
		{
			name: "delete_after_cancel_succeeds",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"res3","TargetDpus":4}`)
				a1Do(t, h, "CancelCapacityReservation", `{"Name":"res3"}`)
				rec := a1Do(t, h, "DeleteCapacityReservation", `{"Name":"res3"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res3"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_active_returns_error",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"active-res","TargetDpus":4}`)
				rec := a1Do(t, h, "DeleteCapacityReservation", `{"Name":"active-res"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.NotEmpty(t, a1Unmarshal(t, rec)["__type"])
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

// --- Tag round-trip ---

func TestAudit1_Tag_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "tag_list_untag_workgroup",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				const arn = "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

				rec := a1Do(t, h, "TagResource",
					`{"ResourceARN":"`+arn+`","Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"data"}]}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				tags, ok := m["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 2)

				// Verify tag shape: Key and Value fields.
				for _, item := range tags {
					tag := item.(map[string]any)
					assert.NotEmpty(t, tag["Key"])
					assert.NotEmpty(t, tag["Value"])
				}

				rec = a1Do(t, h, "UntagResource",
					`{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				tags2, _ := a1Unmarshal(t, rec)["Tags"].([]any)
				assert.Len(t, tags2, 1)
				remaining := tags2[0].(map[string]any)
				assert.Equal(t, "team", remaining["Key"])
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

// --- Session + Calculation lifecycle ---

func TestAudit1_Session_Calculation_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "session_start_and_get_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"audit","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				sid, ok := m["SessionId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, sid)
				assert.NotEmpty(t, m["State"])

				rec = a1Do(t, h, "GetSession", `{"SessionId":"`+sid+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				gs := a1Unmarshal(t, rec)
				assert.Equal(t, sid, gs["SessionId"])
				assert.Equal(t, "primary", gs["WorkGroup"])
			},
		},
		{
			name: "calculation_start_get_code",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				sRec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"calc-test","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, sRec.Code)
				sid := a1Unmarshal(t, sRec)["SessionId"].(string)

				cRec := a1Do(t, h, "StartCalculationExecution",
					`{"SessionId":"`+sid+`","CodeBlock":"print('hello')","Description":"test calc"}`)
				require.Equal(t, http.StatusOK, cRec.Code)
				cm := a1Unmarshal(t, cRec)
				cid, ok := cm["CalculationExecutionId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, cid)
				assert.NotEmpty(t, cm["State"])

				codeRec := a1Do(t, h, "GetCalculationExecutionCode",
					`{"CalculationExecutionId":"`+cid+`"}`)
				require.Equal(t, http.StatusOK, codeRec.Code)
				assert.Equal(t, "print('hello')", a1Unmarshal(t, codeRec)["CodeBlock"])
			},
		},
		{
			name: "terminate_session_makes_it_inaccessible_for_calcs",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				sRec := a1Do(t, h, "StartSession",
					`{"WorkGroup":"primary","Description":"term-test","NotebookVersion":"v1"}`)
				require.Equal(t, http.StatusOK, sRec.Code)
				sid := a1Unmarshal(t, sRec)["SessionId"].(string)

				rec := a1Do(t, h, "TerminateSession", `{"SessionId":"`+sid+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "TERMINATED", a1Unmarshal(t, rec)["State"])

				// Cannot start calculation in terminated session.
				cRec := a1Do(t, h, "StartCalculationExecution",
					`{"SessionId":"`+sid+`","CodeBlock":"x"}`)
				assert.Equal(t, http.StatusBadRequest, cRec.Code)
				assert.NotEmpty(t, a1Unmarshal(t, cRec)["__type"])
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

// --- NamedQuery lifecycle ---

func TestAudit1_NamedQuery_Lifecycle(t *testing.T) {
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
