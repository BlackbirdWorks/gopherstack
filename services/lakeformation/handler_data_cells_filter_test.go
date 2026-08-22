package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_CreateDataCellsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1",` +
				`"TableName":"tbl1","Name":"filter1"}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_table_data",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/CreateDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateDataCellsFilter_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"dup"}}`

	rec := doLFRequest(t, h, "/CreateDataCellsFilter", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateDataCellsFilter", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteDataCellsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				err := b.CreateDataCellsFilter(&lakeformation.DataCellsFilter{
					TableCatalogID: "123456789012",
					DatabaseName:   "db1",
					TableName:      "tbl1",
					Name:           "filter1",
				})
				require.NoError(t, err)
			},
			body:       `{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"filter1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"nofilter"}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/DeleteDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateDataCellsFilter_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "cat",
			"DatabaseName":   "db",
			"TableName":      "tbl",
			"Name":           "filter1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.DataCellsFilterCount())
}

func TestCreateDataCellsFilter_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "f1",
	})

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "cat",
			"DatabaseName":   "db",
			"TableName":      "tbl",
			"Name":           "f1",
		},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestDeleteDataCellsFilter_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "missing",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListDataCellsFilter_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// ListDataCellsFilterInput marks no member required, Table included
	// (api_op_ListDataCellsFilter.go, lakeformation@v1.50.4) -- omitting it
	// lists every accessible filter, unscoped. A prior version of this test
	// asserted a 400 here, which was itself wrong: gopherstack-4ly2.
	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestListDataCellsFilter_WithTable(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{
		"Table": map[string]any{
			"DatabaseName": "mydb",
			"Name":         "mytable",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	filters, ok := out["DataCellsFilters"]
	if ok {
		assert.NotNil(t, filters)
	}
}

func TestListDataCellsFilter_AfterCreate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "123456789012",
		DatabaseName:   "mydb",
		TableName:      "mytable",
		Name:           "myfilter",
	})

	rec := postJSON(t, h, "/ListDataCellsFilter", map[string]any{
		"Table": map[string]any{"DatabaseName": "mydb", "Name": "mytable"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	filters := out["DataCellsFilters"].([]any)
	assert.Len(t, filters, 1)
}

// --- ListLFTagExpressions tests ---

func TestCreateDataCellsFilter_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		status int
	}{
		{
			name: "missing_table_catalog_id",
			body: map[string]any{
				"TableData": map[string]any{
					"DatabaseName": "db", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_database_name",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_table_name",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "DatabaseName": "db", "Name": "f",
				},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "valid_all_fields",
			body: map[string]any{
				"TableData": map[string]any{
					"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "f",
				},
			},
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			rec := postJSON(t, h, "/CreateDataCellsFilter", tt.body)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

// TestDeleteDataCellsFilter_MissingName verifies an empty Name is not
// rejected as a validation error. DeleteDataCellsFilterInput marks no
// member required (api_op_DeleteDataCellsFilter.go, lakeformation@v1.50.4)
// -- unlike Get/Create/Update, Name is optional here. A prior version of
// this test asserted 400, ratifying an over-validation: gopherstack-i8lo.
// The request is well-formed, so it falls through to the ordinary
// not-found path since no filter is keyed by an empty Name.
func TestDeleteDataCellsFilter_MissingName(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteDataCellsFilter", map[string]any{
		"TableCatalogId": "123",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpdateDataCellsFilter_RequiresAllFields(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "123", DatabaseName: "db", TableName: "tbl", Name: "f",
	})

	tests := []struct {
		body   map[string]any
		name   string
		status int
	}{
		{
			name:   "missing_catalog",
			body:   map[string]any{"TableData": map[string]any{"DatabaseName": "db", "TableName": "tbl", "Name": "f"}},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_database",
			body: map[string]any{
				"TableData": map[string]any{"TableCatalogId": "123", "TableName": "tbl", "Name": "f"},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing_table",
			body: map[string]any{
				"TableData": map[string]any{"TableCatalogId": "123", "DatabaseName": "db", "Name": "f"},
			},
			status: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: map[string]any{"TableData": map[string]any{
				"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "nonexistent",
			}},
			status: http.StatusNotFound,
		},
		{
			name: "success",
			body: map[string]any{"TableData": map[string]any{
				"TableCatalogId": "123", "DatabaseName": "db", "TableName": "tbl", "Name": "f",
			}},
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h2 := lakeformation.NewHandler(b)
			rec := postJSON(t, h2, "/UpdateDataCellsFilter", tt.body)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

// --- StartQueryPlanning validation ---

func TestGetDataCellsFilter_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "myfilter",
	})

	rec := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "myfilter",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["DataCellsFilter"])
}

// TestGetDataCellsFilter_RequiredFields proves the three members that
// were previously unvalidated -- TableCatalogId, DatabaseName, TableName
// -- are now rejected when missing. GetDataCellsFilterInput marks all four
// of TableCatalogId, DatabaseName, TableName and Name required
// (api_op_GetDataCellsFilter.go, lakeformation@v1.50.4): gopherstack-i8lo.
func TestGetDataCellsFilter_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		status int
	}{
		{
			name:   "missing_table_catalog_id",
			body:   map[string]any{"DatabaseName": "db", "TableName": "tbl", "Name": "f"},
			status: http.StatusBadRequest,
		},
		{
			name:   "missing_database_name",
			body:   map[string]any{"TableCatalogId": "123", "TableName": "tbl", "Name": "f"},
			status: http.StatusBadRequest,
		},
		{
			name:   "missing_table_name",
			body:   map[string]any{"TableCatalogId": "123", "DatabaseName": "db", "Name": "f"},
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
				TableCatalogID: "123", DatabaseName: "db", TableName: "tbl", Name: "f",
			})

			rec := postJSON(t, h, "/GetDataCellsFilter", tt.body)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

func TestGetDataCellsFilter_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "cat",
		"DatabaseName":   "db",
		"TableName":      "tbl",
		"Name":           "missing",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- GetLFTagExpression ---

func TestDataCellsFilter_RowFilterAndColumns(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	expr := "col1 > 100"

	rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{
		"TableData": map[string]any{
			"TableCatalogId": "123456789012",
			"DatabaseName":   "mydb",
			"TableName":      "mytable",
			"Name":           "filter1",
			"RowFilter": map[string]any{
				"FilterExpression": expr,
			},
			"ColumnNames": []any{"col1", "col2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
		"TableCatalogId": "123456789012",
		"DatabaseName":   "mydb",
		"TableName":      "mytable",
		"Name":           "filter1",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	filter := out["DataCellsFilter"].(map[string]any)

	rowFilter, ok := filter["RowFilter"].(map[string]any)
	require.True(t, ok, "RowFilter should be present")
	assert.Equal(t, expr, rowFilter["FilterExpression"])

	cols := filter["ColumnNames"].([]any)
	assert.Len(t, cols, 2)
}

// TestDataCellsFilter_ColumnWildcard verifies the real API's ColumnWildcard
// field (an exclusion-list alternative to ColumnNames) round-trips, and that
// specifying both ColumnNames and ColumnWildcard together is rejected --
// per types.DataCellsFilter.ColumnWildcard's doc comment: "You must specify
// either a ColumnNames list or the ColumnWildCard".
func TestDataCellsFilter_ColumnWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tableData  map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "ColumnWildcard alone is accepted",
			tableData: map[string]any{
				"TableCatalogId": "123456789012",
				"DatabaseName":   "mydb",
				"TableName":      "mytable",
				"Name":           "wildcard-filter",
				"ColumnWildcard": map[string]any{"ExcludedColumnNames": []any{"ssn"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "ColumnNames and ColumnWildcard together are rejected",
			tableData: map[string]any{
				"TableCatalogId": "123456789012",
				"DatabaseName":   "mydb",
				"TableName":      "mytable",
				"Name":           "both-filter",
				"ColumnNames":    []any{"col1"},
				"ColumnWildcard": map[string]any{"ExcludedColumnNames": []any{"ssn"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/CreateDataCellsFilter", map[string]any{"TableData": tt.tableData})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			rec2 := postJSON(t, h, "/GetDataCellsFilter", map[string]any{
				"TableCatalogId": tt.tableData["TableCatalogId"],
				"DatabaseName":   tt.tableData["DatabaseName"],
				"TableName":      tt.tableData["TableName"],
				"Name":           tt.tableData["Name"],
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec2.Body, &out))
			filter := out["DataCellsFilter"].(map[string]any)

			wildcard, ok := filter["ColumnWildcard"].(map[string]any)
			require.True(t, ok, "ColumnWildcard should round-trip")
			excluded := wildcard["ExcludedColumnNames"].([]any)
			assert.Equal(t, []any{"ssn"}, excluded)
			assert.NotEmpty(t, filter["VersionId"], "Create/UpdateDataCellsFilter must assign a VersionId")
		})
	}
}

// --- #15: ListDataCellsFilter requires Table ---

// TestListDataCellsFilter_TableOptional verifies Table and its DatabaseName
// member are both optional (ListDataCellsFilterInput marks no member
// required, api_op_ListDataCellsFilter.go, lakeformation@v1.50.4) -- each
// narrows the listing only when supplied. A prior version of this test
// (TestListDataCellsFilter_RequiresTable) asserted 400 for the first two
// cases, which was itself wrong: gopherstack-4ly2.
func TestListDataCellsFilter_TableOptional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "no Table field lists unscoped",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "Table with missing DatabaseName still lists, scoped by Name only",
			body: map[string]any{
				"Table": map[string]any{"Name": "mytable"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "valid Table returns 200",
			body: map[string]any{
				"Table": map[string]any{
					"DatabaseName": "mydb",
					"Name":         "mytable",
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/ListDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, "test: %s", tt.name)
		})
	}
}
