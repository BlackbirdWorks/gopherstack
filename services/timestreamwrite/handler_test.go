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
		{
			name:   "timestream query operation not matched",
			target: "Timestream_20181101.ListScheduledQueries",
			want:   false,
		},
		{name: "timestream write operation matched", target: "Timestream_20181101.WriteRecords", want: true},
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

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing table name",
			body:       map[string]string{"DatabaseName": "mydb"},
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

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
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

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	for _, name := range []string{"t1", "t2"} {
		rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "mydb"})
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

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"DatabaseName": "mydb",
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

	// Create the database first so that the ARN is known to the backend.
	dbRec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "my-db"})
	require.Equal(t, http.StatusOK, dbRec.Code)

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

func TestHandler_ChaosInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "timestreamwrite", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	assert.Empty(t, h.ExtractResource(c))
}

func TestHandler_UpdateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDB    string
		updateDB   string
		kmsKey     string
		wantStatus int
	}{
		{
			name:       "success",
			setupDB:    "my-db",
			updateDB:   "my-db",
			kmsKey:     "arn:aws:kms:us-east-1:000000000000:key/1234",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setupDB:    "",
			updateDB:   "missing",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing name",
			setupDB:    "",
			updateDB:   "",
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

			rec := doRequest(t, h, "UpdateDatabase", map[string]string{
				"DatabaseName": tt.updateDB,
				"KmsKeyId":     tt.kmsKey,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
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

			result := doRequest(t, h, "DeleteTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_UpdateTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "tbl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"DatabaseName": "mydb", "TableName": "missing"},
			wantStatus: http.StatusBadRequest,
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

			result := doRequest(t, h, "UpdateTable", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_TagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTagsForResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTables_MissingDBName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTables", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_CreateBatchLoadTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"TargetDatabaseName": "mydb",
				"TargetTableName":    "tbl",
				"DataSourceConfiguration": map[string]any{
					"DataFormat":                "CSV",
					"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing target database",
			body:       map[string]string{"TargetTableName": "tbl"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing target table",
			body:       map[string]string{"TargetDatabaseName": "mydb"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "database not found",
			body:       map[string]string{"TargetDatabaseName": "missing-db", "TargetTableName": "tbl"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "table not found",
			body:       map[string]string{"TargetDatabaseName": "mydb", "TargetTableName": "missing-tbl"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "CreateBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["TaskId"])
			}
		})
	}
}

func TestHandler_DescribeBatchLoadTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "mydb",
		"TargetTableName":    "tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	taskID, _ := createResp["TaskId"].(string)
	require.NotEmpty(t, taskID)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]string{"TaskId": taskID},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			body:       map[string]string{"TaskId": "nonexistent-task"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing task id",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "DescribeBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &resp))
				desc, ok := resp["BatchLoadTaskDescription"].(map[string]any)
				assert.True(t, ok)
				assert.Equal(t, taskID, desc["TaskId"])
				assert.Equal(t, "mydb", desc["TargetDatabaseName"])
				assert.Equal(t, "tbl", desc["TargetTableName"])
				assert.Equal(t, "CREATED", desc["TaskStatus"])
			}
		})
	}
}

func TestHandler_ListBatchLoadTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	for range 3 {
		rec = doRequest(t, h, "CreateBatchLoadTask", map[string]any{
			"TargetDatabaseName": "mydb",
			"TargetTableName":    "tbl",
			"DataSourceConfiguration": map[string]any{
				"DataFormat":                "CSV",
				"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body       any
		name       string
		wantLen    int
		wantStatus int
	}{
		{
			name:       "list all",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantLen:    3,
		},
		{
			name:       "filter by status CREATED",
			body:       map[string]string{"TaskStatus": "CREATED"},
			wantStatus: http.StatusOK,
			wantLen:    3,
		},
		{
			name:       "filter by status IN_PROGRESS returns none",
			body:       map[string]string{"TaskStatus": "IN_PROGRESS"},
			wantStatus: http.StatusOK,
			wantLen:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "ListBatchLoadTasks", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(result.Body.Bytes(), &resp))
			tasks, ok := resp["BatchLoadTasks"].([]any)
			assert.True(t, ok)
			assert.Len(t, tasks, tt.wantLen)
		})
	}
}

func TestHandler_ResumeBatchLoadTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateTable", map[string]string{"DatabaseName": "mydb", "TableName": "tbl"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "mydb",
		"TargetTableName":    "tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	taskID, _ := createResp["TaskId"].(string)
	require.NotEmpty(t, taskID)

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "task not in resumable state",
			body:       map[string]string{"TaskId": taskID},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "task not found",
			body:       map[string]string{"TaskId": "nonexistent"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing task id",
			body:       map[string]string{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, "ResumeBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_ResumeBatchLoadTask_Success(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	_, err := b.CreateDatabase("db", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "tbl", nil, nil)
	require.NoError(t, err)

	task, err := b.CreateBatchLoadTask("db", "tbl", nil, nil)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task.TaskID, "PROGRESS_STOPPED")
	require.NoError(t, err)

	rec := doRequest(t, h, "ResumeBatchLoadTask", map[string]string{"TaskId": task.TaskID})
	assert.Equal(t, http.StatusOK, rec.Code)

	described, err := b.DescribeBatchLoadTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "CREATED", described.TaskStatus)
}
