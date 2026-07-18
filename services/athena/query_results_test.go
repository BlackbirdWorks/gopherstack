package athena_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestGetQueryResults_SQLExecution verifies that GetQueryResults returns real rows
// for SELECT queries against registered in-memory tables.
func TestGetQueryResults_SQLExecution(t *testing.T) {
	t.Parallel()

	const (
		catalog  = "AwsDataCatalog"
		database = "testdb"
		table    = "users"
	)

	rows := []map[string]any{
		{"id": "1", "name": "Alice", "age": "30"},
		{"id": "2", "name": "Bob", "age": "25"},
		{"id": "3", "name": "Charlie", "age": "35"},
	}

	tests := []struct {
		name       string
		query      string
		wantFirst  string   // first data cell of first result row (col 0)
		wantCols   []string // expected column names (in order)
		wantRowLen int      // expected data rows (excludes header)
	}{
		{
			name:       "select_star_returns_all_rows",
			query:      "SELECT * FROM " + database + "." + table,
			wantCols:   []string{"age", "id", "name"}, // sorted: no explicit metadata
			wantRowLen: 3,
			wantFirst:  "30",
		},
		{
			name:       "select_named_columns",
			query:      "SELECT name, id FROM " + database + "." + table,
			wantCols:   []string{"name", "id"},
			wantRowLen: 3,
			wantFirst:  "Alice",
		},
		{
			name:       "where_clause_filters_rows",
			query:      "SELECT name FROM " + database + "." + table + " WHERE name = 'Bob'",
			wantCols:   []string{"name"},
			wantRowLen: 1,
			wantFirst:  "Bob",
		},
		{
			name:       "limit_restricts_rows",
			query:      "SELECT * FROM " + database + "." + table + " LIMIT 2",
			wantCols:   []string{"age", "id", "name"},
			wantRowLen: 2,
			wantFirst:  "30",
		},
		{
			name:       "where_and_limit_combined",
			query:      "SELECT id, name FROM " + database + "." + table + " WHERE age = '25' LIMIT 1",
			wantCols:   []string{"id", "name"},
			wantRowLen: 1,
			wantFirst:  "2",
		},
		{
			name:       "unknown_table_returns_empty",
			query:      "SELECT * FROM " + database + ".no_such_table",
			wantCols:   nil,
			wantRowLen: 0,
			wantFirst:  "",
		},
		{
			name:       "no_from_clause_returns_empty",
			query:      "SELECT 1",
			wantCols:   nil,
			wantRowLen: 0,
			wantFirst:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")
			b.InsertRows(catalog, database, table, rows)

			h := athena.NewHandler(b)

			id, err := b.StartQueryExecution(
				tt.query, "primary",
				athena.QueryExecutionContext{Catalog: catalog, Database: database},
				athena.ResultConfiguration{}, nil, nil,
			)
			require.NoError(t, err)

			body := `{"QueryExecutionId":"` + id + `"}`
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", "AmazonAthena.GetQueryResults")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			rs, _ := resp["ResultSet"].(map[string]any)
			require.NotNil(t, rs)

			allRows, _ := rs["Rows"].([]any)

			// Rows includes a header row (col names) when columns exist.
			if tt.wantRowLen == 0 && len(tt.wantCols) == 0 {
				assert.Empty(t, allRows, "no rows expected for empty result set")

				return
			}

			// First row is the header.
			require.NotEmpty(t, allRows, "at least header row expected")
			headerRow := allRows[0].(map[string]any)
			headerData, _ := headerRow["Data"].([]any)
			require.Len(t, headerData, len(tt.wantCols), "header column count")

			for i, wantCol := range tt.wantCols {
				cell := headerData[i].(map[string]any)
				assert.Equal(t, wantCol, cell["VarCharValue"], "header col %d name", i)
			}

			// Data rows follow the header.
			dataRows := allRows[1:]
			assert.Len(t, dataRows, tt.wantRowLen, "data row count")

			if tt.wantFirst != "" && len(dataRows) > 0 {
				firstRow := dataRows[0].(map[string]any)
				firstData := firstRow["Data"].([]any)
				firstCell := firstData[0].(map[string]any)
				assert.Equal(t, tt.wantFirst, firstCell["VarCharValue"], "first cell of first data row")
			}
		})
	}
}

// TestGetQueryResults_Pagination verifies NextToken-based pagination.
func TestGetQueryResults_Pagination(t *testing.T) {
	t.Parallel()

	const (
		catalog  = "AwsDataCatalog"
		database = "testdb"
		table    = "items"
	)

	testRows := make([]map[string]any, 0, 5)
	for i := range 5 {
		testRows = append(testRows, map[string]any{"n": i})
	}

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows(catalog, database, table, testRows)

	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT * FROM "+database+"."+table, "primary",
		athena.QueryExecutionContext{Catalog: catalog, Database: database},
		athena.ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	// Page 1: MaxResults=2
	body1 := `{"QueryExecutionId":"` + id + `","MaxResults":2}`
	req1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body1))
	req1.Header.Set("X-Amz-Target", "AmazonAthena.GetQueryResults")
	rec1 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req1, rec1)))
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	nextToken, _ := resp1["NextToken"].(string)
	assert.NotEmpty(t, nextToken, "page 1 must have NextToken")

	rs1 := resp1["ResultSet"].(map[string]any)
	rows1 := rs1["Rows"].([]any)
	assert.Len(t, rows1, 3, "page 1: header + 2 data rows") // header + 2 data

	// Page 2: remaining rows
	body2 := `{"QueryExecutionId":"` + id + `","MaxResults":10,"NextToken":"` + nextToken + `"}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body2))
	req2.Header.Set("X-Amz-Target", "AmazonAthena.GetQueryResults")
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req2, rec2)))
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Empty(t, resp2["NextToken"], "last page must have no NextToken")

	rs2 := resp2["ResultSet"].(map[string]any)
	rows2 := rs2["Rows"].([]any)
	assert.Len(t, rows2, 3, "page 2: 3 remaining data rows (no header on continuation pages)")
}

// TestGetQueryResults_CatalogQualifiedTable verifies 3-part table names are resolved.
func TestGetQueryResults_CatalogQualifiedTable(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows("AwsDataCatalog", "mydb", "orders", []map[string]any{
		{"order_id": "ORD-001", "status": "shipped"},
		{"order_id": "ORD-002", "status": "pending"},
	})

	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT status FROM AwsDataCatalog.mydb.orders WHERE status = 'shipped'",
		"primary",
		athena.QueryExecutionContext{},
		athena.ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	body := `{"QueryExecutionId":"` + id + `"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonAthena.GetQueryResults")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rs := resp["ResultSet"].(map[string]any)
	allRows := rs["Rows"].([]any)
	// header + 1 data row (only "shipped" matches)
	require.Len(t, allRows, 2)

	dataRow := allRows[1].(map[string]any)
	data := dataRow["Data"].([]any)
	cell := data[0].(map[string]any)
	assert.Equal(t, "shipped", cell["VarCharValue"])
}

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

// TestGetQueryResults_HeaderOnlyOnFirstPage verifies AWS parity:
// column header row appears only on page 1, not subsequent pages.
func TestGetQueryResults_HeaderOnlyOnFirstPage(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows("AwsDataCatalog", "db", "t", []map[string]any{
		{"col": "a"}, {"col": "b"}, {"col": "c"},
	})
	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT col FROM db.t", "primary",
		athena.QueryExecutionContext{Catalog: "AwsDataCatalog", Database: "db"},
		athena.ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	t.Run("first_page_has_header", func(t *testing.T) {
		t.Parallel()
		body := fmt.Sprintf(`{"QueryExecutionId":%q,"MaxResults":2}`, id)
		rec := athenaDoPass5(t, h, "GetQueryResults", body)
		require.Equal(t, http.StatusOK, rec.Code)
		m := athenaUnmarshalPass5(t, rec)
		rs := m["ResultSet"].(map[string]any)
		rows := rs["Rows"].([]any)
		// header + 2 data rows
		assert.Len(t, rows, 3, "page1: header + 2 data rows")
		headerRow := rows[0].(map[string]any)["Data"].([]any)
		cell := headerRow[0].(map[string]any)
		assert.Equal(t, "col", cell["VarCharValue"])

		// Fetch page 2 — no header expected.
		tok := m["NextToken"].(string)
		body2 := fmt.Sprintf(`{"QueryExecutionId":%q,"MaxResults":10,"NextToken":%q}`, id, tok)
		rec2 := athenaDoPass5(t, h, "GetQueryResults", body2)
		require.Equal(t, http.StatusOK, rec2.Code)
		m2 := athenaUnmarshalPass5(t, rec2)
		rs2 := m2["ResultSet"].(map[string]any)
		rows2 := rs2["Rows"].([]any)
		assert.Len(t, rows2, 1, "page2: 1 data row, no header")
	})
}

// TestGetQueryResults_ColumnInfo verifies richer ColumnInfo fields on first page.
func TestGetQueryResults_ColumnInfo(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows("AwsDataCatalog", "db", "tab", []map[string]any{
		{"x": "1"},
	})
	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT x FROM db.tab", "primary",
		athena.QueryExecutionContext{Catalog: "AwsDataCatalog", Database: "db"},
		athena.ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	body := fmt.Sprintf(`{"QueryExecutionId":%q}`, id)
	rec := athenaDoPass5(t, h, "GetQueryResults", body)
	require.Equal(t, http.StatusOK, rec.Code)
	m := athenaUnmarshalPass5(t, rec)

	rs := m["ResultSet"].(map[string]any)
	meta, ok := rs["ResultSetMetadata"].(map[string]any)
	require.True(t, ok, "ResultSetMetadata present")
	cols, ok := meta["ColumnInfo"].([]any)
	require.True(t, ok, "ColumnInfo present")
	require.Len(t, cols, 1)

	col := cols[0].(map[string]any)
	assert.Equal(t, "x", col["Name"])
	assert.Equal(t, "x", col["Label"])
	assert.Equal(t, "string", col["Type"])
	assert.NotNil(t, col["Nullable"])
	assert.NotNil(t, col["CaseSensitive"])
}

func TestGetQueryResults_NonSucceededQueryRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setState   string
		wantStatus int
	}{
		{
			name:       "succeeded_query_returns_results",
			setState:   "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "cancelled_query_returns_400",
			setState:   "CANCELLED",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "failed_query_returns_400",
			setState:   "FAILED",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")
			h := athena.NewHandler(b)

			startRec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
			require.Equal(t, http.StatusOK, startRec.Code)
			execID := jsonField(t, startRec.Body.Bytes(), "QueryExecutionId")

			if tt.setState != "" {
				b.SetQueryExecutionState(execID, tt.setState, 0)
			}

			rec := doRequest(t, h, "GetQueryResults", `{"QueryExecutionId":"`+execID+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Contains(t, errResp["__type"], "InvalidRequestException",
					"GetQueryResults on a non-SUCCEEDED query must return InvalidRequestException")
				msg, _ := errResp["message"].(string)
				assert.Contains(t, msg, tt.setState,
					"error message must include the current state")
			}
		})
	}
}
