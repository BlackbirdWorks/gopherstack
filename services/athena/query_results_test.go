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
				athena.ResultConfiguration{}, nil,
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
		athena.ResultConfiguration{}, nil,
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
	assert.Len(t, rows2, 4, "page 2: header + 3 remaining data rows")
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
		athena.ResultConfiguration{}, nil,
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
