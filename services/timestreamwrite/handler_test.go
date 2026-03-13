package timestreamwrite_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func newTestHandler(t *testing.T) *timestreamwrite.Handler {
	t.Helper()

	return timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *timestreamwrite.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Timestream_20181101."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "TimestreamWrite", h.Name())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "matching target", target: "Timestream_20181101.CreateDatabase", want: true},
		{name: "non-matching target", target: "SageMaker.ListModels", want: false},
		{name: "empty target", target: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_CreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "my-db"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing name",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDatabase", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				db, ok := resp["Database"].(map[string]any)
				assert.True(t, ok)
				assert.Equal(t, "my-db", db["DatabaseName"])
			}
		})
	}
}

func TestHandler_CreateDatabase_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"DatabaseName": "dup-db"}

	rec := doRequest(t, h, "CreateDatabase", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateDatabase", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DescribeDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		queryDB    string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "my-db",
			queryDB:    "my-db",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			queryDB:    "missing-db",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setupDB != "" {
				rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.setupDB})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeDatabase", map[string]string{"DatabaseName": tt.queryDB})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"db-a", "db-b"} {
		rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dbs, ok := resp["Databases"].([]any)
	assert.True(t, ok)
	assert.Len(t, dbs, 2)
}

func TestHandler_DeleteDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		deleteDB   string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "del-db",
			deleteDB:   "del-db",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			deleteDB:   "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setupDB != "" {
				rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": tt.setupDB})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteDatabase", map[string]string{"DatabaseName": tt.deleteDB})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "db"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "db", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing table name",
			body:       map[string]string{"DatabaseName": "db"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "database not found",
			body:       map[string]string{"DatabaseName": "missing", "TableName": "tbl"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "CreateTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_DescribeTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "db"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "db", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "db", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "db", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "DescribeTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_ListTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "db"})
	require.Equal(t, http.StatusOK, rec.Code)

	for _, name := range []string{"t1", "t2"} {
		rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "db", "TableName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "db"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tbls, ok := resp["Tables"].([]any)
	assert.True(t, ok)
	assert.Len(t, tbls, 2)
}

func TestHandler_WriteRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "db"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "db", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"DatabaseName": "db",
				"TableName":    "tbl",
				"Records": []map[string]any{
					{
						"MeasureName":      "cpu_utilization",
						"MeasureValue":     "13.5",
						"MeasureValueType": "DOUBLE",
						"Time":             "1609459200000",
						"TimeUnit":         "MILLISECONDS",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing names",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "WriteRecords", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_TagResource_UntagResource_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:timestream:us-east-1:000000000000:database/my-db"

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	assert.True(t, ok)
	assert.Len(t, tags, 2)

	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok = listResp["Tags"].([]any)
	assert.True(t, ok)
	assert.Len(t, tags, 1)
}

func TestHandler_DescribeEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]string{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["Endpoints"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.Equal(t, "localhost", ep["Address"])
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOperation", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateDatabase")
	assert.Contains(t, ops, "DescribeDatabase")
	assert.Contains(t, ops, "WriteRecords")
	assert.Contains(t, ops, "DescribeEndpoints")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "valid target", target: "Timestream_20181101.CreateDatabase", want: "CreateDatabase"},
		{name: "empty target", target: "", want: "Unknown"},
		{name: "wrong prefix", target: "Something.Action", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}
