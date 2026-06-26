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

// TestParity_GetQueryResults_HeaderOnlyOnFirstPage verifies AWS parity:
// column header row appears only on page 1, not subsequent pages.
func TestParity_GetQueryResults_HeaderOnlyOnFirstPage(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows("AwsDataCatalog", "db", "t", []map[string]any{
		{"col": "a"}, {"col": "b"}, {"col": "c"},
	})
	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT col FROM db.t", "primary",
		athena.QueryExecutionContext{Catalog: "AwsDataCatalog", Database: "db"},
		athena.ResultConfiguration{}, nil,
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

// TestParity_CreateCapacityReservation_MinDPUs enforces the AWS minimum of 24 DPUs.
func TestParity_CreateCapacityReservation_MinDPUs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "zero_dpus_rejected",
			body:       `{"Name":"r1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "below_minimum_rejected",
			body:       `{"Name":"r2","TargetDpus":23}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exactly_24_accepted",
			body:       `{"Name":"r3","TargetDpus":24}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "above_minimum_accepted",
			body:       `{"Name":"r4","TargetDpus":48}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := athenaDoPass5(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_CancelCapacityReservation_SetsCancelling verifies cancel sets CANCELLING status.
func TestParity_CancelCapacityReservation_SetsCancelling(t *testing.T) {
	t.Parallel()

	h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
	rec := athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = athenaDoPass5(t, h, "CancelCapacityReservation", `{"Name":"r"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = athenaDoPass5(t, h, "GetCapacityReservation", `{"Name":"r"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	m := athenaUnmarshalPass5(t, rec)
	cr := m["CapacityReservation"].(map[string]any)
	assert.Equal(t, "CANCELLING", cr["Status"])
}

// TestParity_DeleteCapacityReservation_AfterCancel verifies delete works after cancel.
func TestParity_DeleteCapacityReservation_AfterCancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "delete_after_cancel_succeeds",
			setup: func(h *athena.Handler) {
				athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
				athenaDoPass5(t, h, "CancelCapacityReservation", `{"Name":"r"}`)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_active_rejected",
			setup: func(h *athena.Handler) {
				athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			tt.setup(h)
			rec := athenaDoPass5(t, h, "DeleteCapacityReservation", `{"Name":"r"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_BatchGet_SizeLimits verifies AWS batch operation size caps.
func TestParity_BatchGet_SizeLimits(t *testing.T) {
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
			body:       `{"StatementNames":` + buildNames(26) + `,"WorkGroup":"primary"}`,
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

// TestParity_ListWorkGroups_Pagination verifies ListWorkGroups MaxResults/NextToken.
func TestParity_ListWorkGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)

	// Create 3 extra workgroups (primary exists by default → 4 total).
	for _, wg := range []string{"wg1", "wg2", "wg3"} {
		rec := athenaDoPass5(t, h, "CreateWorkGroup", fmt.Sprintf(`{"Name":%q}`, wg))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name          string
		body          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "page1_two_results",
			body:          `{"MaxResults":2}`,
			wantLen:       2,
			wantNextToken: true,
		},
		{
			name:          "no_limit_returns_all",
			body:          `{}`,
			wantLen:       4,
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := athenaDoPass5(t, h, "ListWorkGroups", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			m := athenaUnmarshalPass5(t, rec)
			wgs, _ := m["WorkGroups"].([]any)
			assert.Len(t, wgs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, m["NextToken"])
			} else {
				assert.Empty(t, m["NextToken"])
			}
		})
	}
}

// TestParity_ListDataCatalogs_Pagination verifies ListDataCatalogs MaxResults/NextToken.
func TestParity_ListDataCatalogs_Pagination(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)

	// Create 3 extra catalogs (AwsDataCatalog exists by default → 4 total).
	for _, cat := range []string{"cat1", "cat2", "cat3"} {
		rec := athenaDoPass5(t, h, "CreateDataCatalog",
			fmt.Sprintf(`{"Name":%q,"Type":"GLUE"}`, cat))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name          string
		body          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "page1_two_results",
			body:          `{"MaxResults":2}`,
			wantLen:       2,
			wantNextToken: true,
		},
		{
			name:          "no_limit_returns_all",
			body:          `{}`,
			wantLen:       4,
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := athenaDoPass5(t, h, "ListDataCatalogs", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			m := athenaUnmarshalPass5(t, rec)
			cats, _ := m["DataCatalogsSummary"].([]any)
			assert.Len(t, cats, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, m["NextToken"])
			} else {
				assert.Empty(t, m["NextToken"])
			}
		})
	}
}

// TestParity_GetQueryResults_ColumnInfo verifies richer ColumnInfo fields on first page.
func TestParity_GetQueryResults_ColumnInfo(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	b.InsertRows("AwsDataCatalog", "db", "tab", []map[string]any{
		{"x": "1"},
	})
	h := athena.NewHandler(b)

	id, err := b.StartQueryExecution(
		"SELECT x FROM db.tab", "primary",
		athena.QueryExecutionContext{Catalog: "AwsDataCatalog", Database: "db"},
		athena.ResultConfiguration{}, nil,
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

// TestParity_StartQueryExecution_DisabledWorkgroup verifies disabled workgroups reject queries.
func TestParity_StartQueryExecution_DisabledWorkgroup(t *testing.T) {
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
