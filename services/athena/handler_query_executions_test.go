package athena_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			"AclConfiguration":{"S3AclOption":"BUCKET_OWNER_FULL_CONTROL"},
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
	// Wire key is "AclConfiguration" (not "ACLConfiguration") to match the real
	// Athena deserializer's exact-case switch on the JSON key.
	acl := rc["AclConfiguration"].(map[string]any)
	assert.Equal(t, "BUCKET_OWNER_FULL_CONTROL", acl["S3AclOption"])
	enc := rc["EncryptionConfiguration"].(map[string]any)
	assert.Equal(t, "SSE_KMS", enc["EncryptionOption"])
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

// TestHandler_GetResourceDashboard asserts the wire-accurate response shape:
// a single top-level "Url" string field (AWS's GetResourceDashboardOutput),
// not the fabricated empty "ResourceDashboard" object this handler used to
// return regardless of input.

func TestQueryExecution_Lifecycle(t *testing.T) {
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

func TestStartQueryExecution_EmptyQueryStringRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_string_query_returns_400",
			body:       `{"QueryString":""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "omitted_query_string_returns_400",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non_empty_query_string_succeeds",
			body:       `{"QueryString":"SELECT 1"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := doRequest(t, h, "StartQueryExecution", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"empty QueryString must return InvalidRequestException")
			}
		})
	}
}

func TestListQueryExecutions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const total = 5
	for range total {
		rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	type listResp struct {
		NextToken         string   `json:"NextToken"`
		QueryExecutionIDs []string `json:"QueryExecutionIds"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := `{"MaxResults":2}`
		if token != "" {
			body = `{"MaxResults":2,"NextToken":"` + token + `"}`
		}

		rec := doRequest(t, h, "ListQueryExecutions", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.QueryExecutionIDs), 2, "page exceeds MaxResults")

		for _, id := range resp.QueryExecutionIDs {
			assert.False(t, seen[id], "id %s returned twice", id)
			seen[id] = true
		}

		pages++
		require.Less(t, pages, 10, "pagination did not terminate")

		token = resp.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total, "all executions returned exactly once")
	assert.GreaterOrEqual(t, pages, 3, "MaxResults=2 over 5 items should span >=3 pages")
}

// TestListQueryExecutions_NextTokenOmittedOnLastPage verifies the final page
// carries no NextToken.
func TestListQueryExecutions_NextTokenOmittedOnLastPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListQueryExecutions", `{"MaxResults":50}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasToken := resp["NextToken"]
	assert.False(t, hasToken, "NextToken must be omitted on the last page")
}

// TestBatchGet_SizeLimits verifies AWS batch operation size caps.
func TestBatchGet_SizeLimits(t *testing.T) {
	t.Parallel()

	buildIDs := func(n int) string {
		ids := make([]string, n)
		for i := range n {
			ids[i] = fmt.Sprintf("%q", fmt.Sprintf("id-%d", i))
		}

		return "[" + strings.Join(ids, ",") + "]"
	}

	buildNames := func(n int) string {
		names := make([]string, n)
		for i := range n {
			names[i] = fmt.Sprintf("%q", fmt.Sprintf("stmt-%d", i))
		}

		return "[" + strings.Join(names, ",") + "]"
	}

	tests := []struct {
		name       string
		action     string
		body       string
		wantStatus int
	}{
		{
			name:       "BatchGetNamedQuery_over_50_rejected",
			action:     "BatchGetNamedQuery",
			body:       `{"NamedQueryIds":` + buildIDs(51) + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BatchGetQueryExecution_over_50_rejected",
			action:     "BatchGetQueryExecution",
			body:       `{"QueryExecutionIds":` + buildIDs(51) + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BatchGetPreparedStatement_over_25_rejected",
			action:     "BatchGetPreparedStatement",
			body:       `{"PreparedStatementNames":` + buildNames(26) + `,"WorkGroup":"primary"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := athenaDoPass5(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestStartQueryExecution_DisabledWorkgroup verifies disabled workgroups reject queries.
func TestStartQueryExecution_DisabledWorkgroup(t *testing.T) {
	t.Parallel()

	h := athena.NewHandler(athena.NewInMemoryBackend("", ""))

	rec := athenaDoPass5(t, h, "CreateWorkGroup", `{"Name":"disabled-wg"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = athenaDoPass5(t, h, "UpdateWorkGroup",
		`{"WorkGroup":"disabled-wg","State":"DISABLED"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		name       string
		workgroup  string
		wantStatus int
	}{
		{
			name:       "disabled_workgroup_rejected",
			workgroup:  "disabled-wg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "active_workgroup_accepted",
			workgroup:  "primary",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body := fmt.Sprintf(
				`{"QueryString":"SELECT 1","WorkGroup":%q,"QueryExecutionContext":{"Database":"testdb"}}`,
				tt.workgroup,
			)
			result := athenaDoPass5(t, h, "StartQueryExecution", body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestBatchGetQueryExecution_UnprocessedShape(t *testing.T) {
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

func TestStartQueryExecution_WorkgroupValidation(t *testing.T) {
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
