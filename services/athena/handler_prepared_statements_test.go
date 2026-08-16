package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

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
			body:            `{"WorkGroup":"primary","PreparedStatementNames":["s1","missing"]}`,
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
			unprocessed, _ := resp["UnprocessedPreparedStatementNames"].([]any)
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

func TestPreparedStatement_Lifecycle(t *testing.T) {
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
