package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

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

	_, err := b.CreateDatabase("db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "tbl", nil, nil)
	require.NoError(t, err)

	task, err := b.CreateBatchLoadTask("db", "tbl", nil, nil, nil, 0)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task.TaskID, "PROGRESS_STOPPED")
	require.NoError(t, err)

	rec := doRequest(t, h, "ResumeBatchLoadTask", map[string]string{"TaskId": task.TaskID})
	assert.Equal(t, http.StatusOK, rec.Code)

	described, err := b.DescribeBatchLoadTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "CREATED", described.TaskStatus)
}

// TestHandler_CreateBatchLoadTask_DataSourceConfigRequired verifies that
// DataSourceConfiguration is a required field on CreateBatchLoadTask.
func TestHandler_CreateBatchLoadTask_DataSourceConfigRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing_DataSourceConfiguration_rejected",
			body: map[string]any{
				"TargetDatabaseName": "blt-req-db",
				"TargetTableName":    "blt-req-tbl",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_DataSourceConfiguration_accepted",
			body: map[string]any{
				"TargetDatabaseName": "blt-req-db",
				"TargetTableName":    "blt-req-tbl",
				"DataSourceConfiguration": map[string]any{
					"DataFormat":                "CSV",
					"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "blt-req-db"})
			doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "blt-req-db",
				"TableName":    "blt-req-tbl",
			})

			rec := doRequest(t, h, "CreateBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				var errBody map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
				assert.Equal(t, "ValidationException", errBody["__type"])
			}
		})
	}
}

// TestHandler_CreateBatchLoadTask_WithDataSourceConfig verifies that
// DataSourceConfiguration is stored and returned by DescribeBatchLoadTask.
func TestHandler_CreateBatchLoadTask_WithDataSourceConfig(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "blt-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "blt-db", "TableName": "blt-tbl"})

	createRec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "blt-db",
		"TargetTableName":    "blt-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat": "CSV",
			"DataSourceS3Configuration": map[string]any{
				"BucketName":      "my-bucket",
				"ObjectKeyPrefix": "prefix/",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)
	require.NotEmpty(t, taskID)

	descRec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	desc := descOut["BatchLoadTaskDescription"].(map[string]any)
	dsc := desc["DataSourceConfiguration"].(map[string]any)
	assert.Equal(t, "CSV", dsc["DataFormat"])

	s3cfg := dsc["DataSourceS3Configuration"].(map[string]any)
	assert.Equal(t, "my-bucket", s3cfg["BucketName"])
	assert.Equal(t, "prefix/", s3cfg["ObjectKeyPrefix"])
}

// TestHandler_CreateBatchLoadTask_WithReportConfig verifies that
// ReportConfiguration is stored and returned.
func TestHandler_CreateBatchLoadTask_WithReportConfig(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "blr-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "blr-db", "TableName": "blr-tbl"})

	createRec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "blr-db",
		"TargetTableName":    "blr-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
		"ReportConfiguration": map[string]any{
			"ReportS3Configuration": map[string]any{
				"BucketName": "report-bucket",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	descRec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	desc := descOut["BatchLoadTaskDescription"].(map[string]any)
	rpt := desc["ReportConfiguration"].(map[string]any)
	rptS3 := rpt["ReportS3Configuration"].(map[string]any)
	assert.Equal(t, "report-bucket", rptS3["BucketName"])
}

// TestHandler_CreateBatchLoadTask_MissingTargetDatabase verifies that
// creating a batch load task for a non-existent database returns an error.
func TestHandler_CreateBatchLoadTask_MissingTargetDatabase(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	rec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "no-db",
		"TargetTableName":    "no-tbl",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_CreateBatchLoadTask_MissingTargetTable verifies that creating a
// batch load task for a non-existent table returns an error.
func TestHandler_CreateBatchLoadTask_MissingTargetTable(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "btdb"})

	rec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "btdb",
		"TargetTableName":    "no-tbl",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ListBatchLoadTasks_Pagination verifies NextToken pagination for
// ListBatchLoadTasks.
func TestHandler_ListBatchLoadTasks_Pagination(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "blt-pg-db",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/blt-pg-db",
		CreationTime: now, LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "blt-pg-db", TableName: "blt-pg-tbl",
		TableStatus: "ACTIVE", ARN: "arn:aws:timestream:us-east-1:000000000000:database/blt-pg-db/table/blt-pg-tbl",
		CreationTime: now, LastUpdatedTime: now,
	})

	for i := range 4 {
		b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
			TaskID:             "task-" + string(rune('a'+i)),
			TargetDatabaseName: "blt-pg-db",
			TargetTableName:    "blt-pg-tbl",
			TaskStatus:         "CREATED",
			CreationTime:       now.Add(time.Duration(i) * time.Second),
			LastUpdatedTime:    now,
		})
	}

	rec1 := doRequest(t, h, "ListBatchLoadTasks", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	tasks1 := out1["BatchLoadTasks"].([]any)
	assert.Len(t, tasks1, 2)

	nextToken := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListBatchLoadTasks", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	tasks2 := out2["BatchLoadTasks"].([]any)
	assert.Len(t, tasks2, 2)
	assert.Empty(t, out2["NextToken"])
}

// TestHandler_ListBatchLoadTasks_StatusFilter verifies status filtering.
func TestHandler_ListBatchLoadTasks_StatusFilter(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "lbt-db", ARN: "arn:lbt-db", CreationTime: now, LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "lbt-db", TableName: "lbt-tbl", ARN: "arn:lbt-tbl",
		TableStatus: "ACTIVE", CreationTime: now, LastUpdatedTime: now,
	})

	task1, err := b.CreateBatchLoadTask("lbt-db", "lbt-tbl", nil, nil, nil, 0)
	require.NoError(t, err)

	task2, err := b.CreateBatchLoadTask("lbt-db", "lbt-tbl", nil, nil, nil, 0)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task2.TaskID, timestreamwrite.BatchLoadStatusSucceeded)
	require.NoError(t, err)

	// Filter for CREATED
	rec := doRequest(t, h, "ListBatchLoadTasks", map[string]any{
		"TaskStatus": timestreamwrite.BatchLoadStatusCreated,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tasks := out["BatchLoadTasks"].([]any)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.TaskID, tasks[0].(map[string]any)["TaskId"])
}

// TestHandler_ListBatchLoadTasks_SummaryResumableUntil verifies that
// ResumableUntil is populated when set on the task.
func TestHandler_ListBatchLoadTasks_SummaryResumableUntil(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(
		&timestreamwrite.Database{
			DatabaseName: "ru-db",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/ru-db",
		},
	)
	b.AddTableInternal(
		&timestreamwrite.Table{
			DatabaseName: "ru-db",
			TableName:    "ru-tbl",
			TableStatus:  "ACTIVE",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/ru-db/table/ru-tbl",
		},
	)

	resumable := time.Now().Add(24 * time.Hour)
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "ru-task", TargetDatabaseName: "ru-db", TargetTableName: "ru-tbl",
		TaskStatus: "FAILED", CreationTime: time.Now(), ResumableUntil: &resumable,
	})

	h := timestreamwrite.NewHandler(b)
	rec := doRequest(t, h, "ListBatchLoadTasks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tasks := out["BatchLoadTasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	assert.NotNil(t, task["ResumableUntil"])
}

// TestHandler_DescribeBatchLoadTask_ErrorMessage verifies ErrorMessage is
// included in the describe response when set.
func TestHandler_DescribeBatchLoadTask_ErrorMessage(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(
		&timestreamwrite.Database{
			DatabaseName: "em-db",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/em-db",
		},
	)
	b.AddTableInternal(
		&timestreamwrite.Table{
			DatabaseName: "em-db",
			TableName:    "em-tbl",
			TableStatus:  "ACTIVE",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/em-db/table/em-tbl",
		},
	)

	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "em-task", TargetDatabaseName: "em-db", TargetTableName: "em-tbl",
		TaskStatus: "FAILED", CreationTime: time.Now(), ErrorMessage: "S3 object not found",
	})

	h := timestreamwrite.NewHandler(b)
	rec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": "em-task"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	assert.Equal(t, "S3 object not found", desc["ErrorMessage"])
}

// TestHandler_BatchLoadTaskDescriptionView_MissingResumableUntilOmitted
// verifies that a task without ResumableUntil omits the field.
func TestHandler_BatchLoadTaskDescriptionView_MissingResumableUntilOmitted(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ruf-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "ruf-db", "TableName": "ruf-tbl"})

	cr := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "ruf-db",
		"TargetTableName":    "ruf-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, cr.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	dr := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, dr.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(dr.Body.Bytes(), &descOut))

	desc := descOut["BatchLoadTaskDescription"].(map[string]any)
	_, hasResumable := desc["ResumableUntil"]
	assert.False(t, hasResumable)
}

// TestHandler_DescribeBatchLoadTask_RecordVersion verifies that the
// RecordVersion field on a BatchLoadTask description view is populated when
// set on the task.
func TestHandler_DescribeBatchLoadTask_RecordVersion(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()

	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "rv-task",
		TargetDatabaseName: "rv-db",
		TargetTableName:    "rv-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusCreated,
		CreationTime:       now,
		LastUpdatedTime:    now,
		RecordVersion:      42,
	})

	h := timestreamwrite.NewHandler(b)
	rec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": "rv-task"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	assert.InDelta(t, float64(42), desc["RecordVersion"], 0,
		"RecordVersion should be returned in DescribeBatchLoadTask response")
}

// TestHandler_DescribeBatchLoadTask_RecordVersionZeroOmitted verifies that
// RecordVersion is omitted from the response when it is zero.
func TestHandler_DescribeBatchLoadTask_RecordVersionZeroOmitted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "rv0-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "rv0-db", "TableName": "rv0-tbl"})

	cr := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "rv0-db",
		"TargetTableName":    "rv0-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, cr.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	dr := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, dr.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(dr.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	_, hasRV := desc["RecordVersion"]
	// RecordVersion of 0 should be omitted (omitempty).
	assert.False(t, hasRV, "RecordVersion of 0 should be omitted from response")
}

// TestHandler_DescribeBatchLoadTask_ProgressReport verifies ProgressReport is
// returned when set on a task.
func TestHandler_DescribeBatchLoadTask_ProgressReport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	now := time.Now()
	pr := &timestreamwrite.BatchLoadProgressReport{
		RecordsProcessed:        1000,
		RecordsIngested:         950,
		RecordIngestionFailures: 50,
		ParseFailures:           5,
		BytesMetered:            1024 * 1024,
	}
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "pr-task",
		TargetDatabaseName: "pr-db",
		TargetTableName:    "pr-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusInProgress,
		CreationTime:       now,
		LastUpdatedTime:    now,
		ProgressReport:     pr,
	})

	rec := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": "pr-task"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	report := desc["ProgressReport"].(map[string]any)
	assert.Equal(t, 1000, int(report["RecordsProcessed"].(float64)))
	assert.Equal(t, 950, int(report["RecordsIngested"].(float64)))
	assert.Equal(t, 50, int(report["RecordIngestionFailures"].(float64)))
	assert.Equal(t, 5, int(report["ParseFailures"].(float64)))
	assert.Equal(t, 1024*1024, int(report["BytesMetered"].(float64)))
}

// TestHandler_DescribeBatchLoadTask_NoProgressReport verifies ProgressReport
// is omitted when not set.
func TestHandler_DescribeBatchLoadTask_NoProgressReport(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "npr-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "npr-db", "TableName": "npr-tbl"})

	cr := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "npr-db",
		"TargetTableName":    "npr-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, cr.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	dr := doRequest(t, h, "DescribeBatchLoadTask", map[string]string{"TaskId": taskID})
	require.Equal(t, http.StatusOK, dr.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(dr.Body.Bytes(), &out))

	desc := out["BatchLoadTaskDescription"].(map[string]any)
	_, hasPR := desc["ProgressReport"]
	assert.False(t, hasPR, "ProgressReport should be absent when not set")
}

// TestHandler_DescribeBatchLoadTask_TimestampsAreFloats verifies batch task
// timestamps are float64.
func TestHandler_DescribeBatchLoadTask_TimestampsAreFloats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "blt-ts-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "blt-ts-db",
		"TableName":    "blt-ts-tbl",
	})
	rec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "blt-ts-db",
		"TargetTableName":    "blt-ts-tbl",
		"DataSourceConfiguration": map[string]any{
			"DataFormat":                "CSV",
			"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	taskID := createOut["TaskId"].(string)

	rec2 := doRequest(t, h, "DescribeBatchLoadTask", map[string]any{"TaskId": taskID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &raw))

	desc := raw["BatchLoadTaskDescription"].(map[string]any)
	ct, ok := desc["CreationTime"].(float64)
	require.True(t, ok, "CreationTime should be float64, got %T", desc["CreationTime"])
	assert.Greater(t, ct, float64(0))
}
