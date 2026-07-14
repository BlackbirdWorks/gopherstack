package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestRefinement2_TimestampsAreFloats verifies that timestamps are returned as
// float64 Unix epoch seconds (not strings), which is required by the AWS SDK v2.
func TestRefinement2_TimestampsAreFloats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "ts-float-db",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	db := raw["Database"].(map[string]any)
	ct, ok := db["CreationTime"].(float64)
	require.True(t, ok, "CreationTime should be float64, got %T", db["CreationTime"])
	assert.Greater(t, ct, float64(0))

	lu, ok := db["LastUpdatedTime"].(float64)
	require.True(t, ok, "LastUpdatedTime should be float64, got %T", db["LastUpdatedTime"])
	assert.Greater(t, lu, float64(0))
}

// TestRefinement2_TableTimestampsAreFloats verifies table timestamps are float64.
func TestRefinement2_TableTimestampsAreFloats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "ts-tbl-db"})
	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ts-tbl-db",
		"TableName":    "ts-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	tbl := raw["Table"].(map[string]any)
	ct, ok := tbl["CreationTime"].(float64)
	require.True(t, ok, "table CreationTime should be float64, got %T", tbl["CreationTime"])
	assert.Greater(t, ct, float64(0))
}

// TestRefinement2_BatchLoadTaskTimestampsAreFloats verifies batch task timestamps are float64.
func TestRefinement2_BatchLoadTaskTimestampsAreFloats(t *testing.T) {
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

// TestRefinement2_CreateDatabaseWithTags verifies tags are stored on creation.
func TestRefinement2_CreateDatabaseWithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseName": "tagged-db",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Fetch the ARN then list tags
	db := out["Database"].(map[string]any)
	arn := db["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	assert.Len(t, tags, 2)
}

// TestRefinement2_CreateTableWithTags verifies tags are stored on table creation.
func TestRefinement2_CreateTableWithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "tagged-tbl-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "tagged-tbl-db",
		"TableName":    "tagged-tbl",
		"Tags": []map[string]string{
			{"Key": "tier", "Value": "silver"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	arn := tbl["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	assert.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "tier", tag["Key"])
	assert.Equal(t, "silver", tag["Value"])
}

// TestRefinement2_ListTagsForResourceSorted verifies tags are sorted by key.
func TestRefinement2_ListTagsForResourceSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "sort-db"})

	descRec := doRequest(t, h, "DescribeDatabase", map[string]any{"DatabaseName": "sort-db"})
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	arn := descOut["Database"].(map[string]any)["Arn"].(string)

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "zzz", "Value": "last"},
			{"Key": "aaa", "Value": "first"},
			{"Key": "mmm", "Value": "middle"},
		},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].(map[string]any)["Key"])
	assert.Equal(t, "mmm", tags[1].(map[string]any)["Key"])
	assert.Equal(t, "zzz", tags[2].(map[string]any)["Key"])
}

// TestRefinement2_HandlerReset verifies that Reset clears all backend state.
func TestRefinement2_HandlerReset(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "reset-db"})
	assert.Equal(t, 1, timestreamwrite.DatabaseCount(b))

	h.Reset()
	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))
}

// TestRefinement2_BackendReset verifies Backend.Reset clears state.
func TestRefinement2_BackendReset(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("reset-db", "", nil)
	require.NoError(t, err)

	b.Reset()
	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))
}

// TestRefinement2_AccountID verifies AccountID returns expected value.
func TestRefinement2_AccountID(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// TestRefinement2_Region verifies Region returns expected value.
func TestRefinement2_Region(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.NotEmpty(t, b.Region())
}

// TestRefinement2_ExportDatabaseCount verifies DatabaseCount export.
func TestRefinement2_ExportDatabaseCount(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))

	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "db1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1",
	})
	assert.Equal(t, 1, timestreamwrite.DatabaseCount(b))
}

// TestRefinement2_ExportTableCount verifies TableCount export.
func TestRefinement2_ExportTableCount(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "db1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1",
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "db1",
		TableName:    "tbl1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1/table/tbl1",
	})
	assert.Equal(t, 1, timestreamwrite.TableCount(b))
}

// TestRefinement2_ExportBatchLoadTaskCount verifies BatchLoadTaskCount export.
func TestRefinement2_ExportBatchLoadTaskCount(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "task-001",
		TargetDatabaseName: "db1",
		TargetTableName:    "tbl1",
		TaskStatus:         timestreamwrite.BatchLoadStatusCreated,
		CreationTime:       time.Now(),
		LastUpdatedTime:    time.Now(),
	})
	assert.Equal(t, 1, timestreamwrite.BatchLoadTaskCount(b))
}

// TestRefinement2_ExportTagCount verifies TagCount export.
func TestRefinement2_ExportTagCount(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("db1", "", nil)
	require.NoError(t, err)

	err = b.TagResource("arn:aws:timestream:us-east-1:000000000000:database/db1", map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.Equal(t, 1, timestreamwrite.TagCount(b))
}

// TestRefinement2_ExportHandlerOpsLen verifies HandlerOpsLen matches GetSupportedOperations.
func TestRefinement2_ExportHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)
	assert.Equal(t, 19, timestreamwrite.HandlerOpsLen(h))
}

// TestRefinement2_AddDatabaseInternal seeds data directly for test setup.
func TestRefinement2_AddDatabaseInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName:    "seeded-db",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seeded-db",
		CreationTime:    now,
		LastUpdatedTime: now,
	})

	db, err := b.DescribeDatabase("seeded-db")
	require.NoError(t, err)
	assert.Equal(t, "seeded-db", db.DatabaseName)
}

// TestRefinement2_AddTableInternal seeds a table directly.
func TestRefinement2_AddTableInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName:    "seed-db",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seed-db",
		CreationTime:    now,
		LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName:    "seed-db",
		TableName:       "seed-tbl",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seed-db/table/seed-tbl",
		TableStatus:     "ACTIVE",
		CreationTime:    now,
		LastUpdatedTime: now,
	})

	tbl, err := b.DescribeTable("seed-db", "seed-tbl")
	require.NoError(t, err)
	assert.Equal(t, "seed-tbl", tbl.TableName)
	assert.Equal(t, 1, timestreamwrite.TableCount(b))
}

// TestRefinement2_AddBatchLoadTaskInternal seeds a batch load task directly.
func TestRefinement2_AddBatchLoadTaskInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "seeded-task",
		TargetDatabaseName: "db",
		TargetTableName:    "tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusFailed,
		CreationTime:       now,
		LastUpdatedTime:    now,
	})

	task, err := b.DescribeBatchLoadTask("seeded-task")
	require.NoError(t, err)
	assert.Equal(t, timestreamwrite.BatchLoadStatusFailed, task.TaskStatus)
}

// TestRefinement2_PersistenceSnapshotRestore verifies Snapshot/Restore round-trip.
func TestRefinement2_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("snap-db", "", map[string]string{"key": "value"})
	require.NoError(t, err)
	_, err = b.CreateTable("snap-db", "snap-tbl", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateBatchLoadTask("snap-db", "snap-tbl", nil, nil)
	require.NoError(t, err)

	data, err := b.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 1, timestreamwrite.DatabaseCount(b2))
	assert.Equal(t, 1, timestreamwrite.TableCount(b2))
	assert.Equal(t, 1, timestreamwrite.BatchLoadTaskCount(b2))

	db, err := b2.DescribeDatabase("snap-db")
	require.NoError(t, err)
	assert.Equal(t, "snap-db", db.DatabaseName)
}

// TestRefinement2_ResumeBatchLoadTask_FromFailed verifies resume from FAILED.
func TestRefinement2_ResumeBatchLoadTask_FromFailed(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "rbt-db", ARN: "arn:xxx", CreationTime: now, LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "rbt-db", TableName: "rbt-tbl", ARN: "arn:xxx2",
		TableStatus: "ACTIVE", CreationTime: now, LastUpdatedTime: now,
	})

	task, err := b.CreateBatchLoadTask("rbt-db", "rbt-tbl", nil, nil)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task.TaskID, timestreamwrite.BatchLoadStatusFailed)
	require.NoError(t, err)

	err = b.ResumeBatchLoadTask(task.TaskID)
	require.NoError(t, err)

	resumed, err := b.DescribeBatchLoadTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, timestreamwrite.BatchLoadStatusCreated, resumed.TaskStatus)
}

// TestRefinement2_ResumeBatchLoadTask_InvalidState verifies error when task not resumable.
func TestRefinement2_ResumeBatchLoadTask_InvalidState(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:     "in-progress-task",
		TaskStatus: timestreamwrite.BatchLoadStatusInProgress,
	})

	err := b.ResumeBatchLoadTask("in-progress-task")
	require.Error(t, err)
	require.ErrorIs(t, err, timestreamwrite.ErrInvalidBatchLoadStatus)
}

// TestRefinement2_ListBatchLoadTasks_StatusFilter verifies status filtering.
func TestRefinement2_ListBatchLoadTasks_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := timestreamwrite.NewInMemoryBackend()
	h2 := timestreamwrite.NewHandler(b)

	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "lbt-db", ARN: "arn:lbt-db", CreationTime: now, LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "lbt-db", TableName: "lbt-tbl", ARN: "arn:lbt-tbl",
		TableStatus: "ACTIVE", CreationTime: now, LastUpdatedTime: now,
	})

	task1, err := b.CreateBatchLoadTask("lbt-db", "lbt-tbl", nil, nil)
	require.NoError(t, err)

	task2, err := b.CreateBatchLoadTask("lbt-db", "lbt-tbl", nil, nil)
	require.NoError(t, err)

	err = b.SetBatchLoadTaskStatus(task2.TaskID, timestreamwrite.BatchLoadStatusSucceeded)
	require.NoError(t, err)

	// Filter for CREATED
	rec := doRequestWithHandler(t, h2, "ListBatchLoadTasks", map[string]any{
		"TaskStatus": timestreamwrite.BatchLoadStatusCreated,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tasks := out["BatchLoadTasks"].([]any)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.TaskID, tasks[0].(map[string]any)["TaskId"])

	_ = h
}

// TestRefinement2_BatchLoadStatusConstants verifies exported status constants.
func TestRefinement2_BatchLoadStatusConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "CREATED", timestreamwrite.BatchLoadStatusCreated)
	assert.Equal(t, "IN_PROGRESS", timestreamwrite.BatchLoadStatusInProgress)
	assert.Equal(t, "FAILED", timestreamwrite.BatchLoadStatusFailed)
	assert.Equal(t, "SUCCEEDED", timestreamwrite.BatchLoadStatusSucceeded)
	assert.Equal(t, "PROGRESS_STOPPED", timestreamwrite.BatchLoadStatusProgressStopped)
	assert.Equal(t, "PENDING_RESUME", timestreamwrite.BatchLoadStatusPendingResume)
}

// TestRefinement2_DatabaseNotFound verifies 400 for missing database.
func TestRefinement2_DatabaseNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeDatabase", map[string]any{"DatabaseName": "no-db"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_TableNotFound verifies 400 for missing table.
func TestRefinement2_TableNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "exists-db"})
	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "exists-db",
		"TableName":    "no-tbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_ListDatabasesSorted verifies databases are sorted by name.
func TestRefinement2_ListDatabasesSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "zdb"})
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "adb"})
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "mdb"})

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	dbs := out["Databases"].([]any)
	require.Len(t, dbs, 3)
	assert.Equal(t, "adb", dbs[0].(map[string]any)["DatabaseName"])
	assert.Equal(t, "mdb", dbs[1].(map[string]any)["DatabaseName"])
	assert.Equal(t, "zdb", dbs[2].(map[string]any)["DatabaseName"])
}

// TestRefinement2_ListTablesSorted verifies tables are sorted by name.
func TestRefinement2_ListTablesSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "srt-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "srt-db", "TableName": "ztbl"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "srt-db", "TableName": "atbl"})

	rec := doRequest(t, h, "ListTables", map[string]any{"DatabaseName": "srt-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbls := out["Tables"].([]any)
	require.Len(t, tbls, 2)
	assert.Equal(t, "atbl", tbls[0].(map[string]any)["TableName"])
	assert.Equal(t, "ztbl", tbls[1].(map[string]any)["TableName"])
}

// TestRefinement2_DatabaseTableCountTracking verifies table count increments/decrements.
func TestRefinement2_DatabaseTableCountTracking(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("cnt-db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("cnt-db", "t1", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("cnt-db", "t2", nil, nil)
	require.NoError(t, err)

	db, err := b.DescribeDatabase("cnt-db")
	require.NoError(t, err)
	assert.Equal(t, 2, db.TableCount)

	require.NoError(t, b.DeleteTable("cnt-db", "t1"))

	db, err = b.DescribeDatabase("cnt-db")
	require.NoError(t, err)
	assert.Equal(t, 1, db.TableCount)
}

// TestRefinement2_DeleteDatabaseCleansUpTags verifies cascading tag cleanup.
func TestRefinement2_DeleteDatabaseCleansUpTags(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	db, err := b.CreateDatabase("cleanup-db", "", nil)
	require.NoError(t, err)

	err = b.TagResource(db.ARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	require.NoError(t, b.DeleteDatabase("cleanup-db"))
	assert.Empty(t, b.ListTagsForResource(db.ARN))
}

// TestRefinement2_DescribeEndpointsReturnsEndpoint verifies endpoint response.
func TestRefinement2_DescribeEndpointsReturnsEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	endpoints := out["Endpoints"].([]any)
	require.NotEmpty(t, endpoints)
	ep := endpoints[0].(map[string]any)
	assert.NotEmpty(t, ep["Address"])
	assert.Greater(t, ep["CachePeriodInMinutes"].(float64), float64(0))
}

// TestRefinement2_WriteRecordsReturnsIngested verifies records ingested count.
func TestRefinement2_WriteRecordsReturnsIngested(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "wr-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "wr-db", "TableName": "wr-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "wr-db",
		"TableName":    "wr-tbl",
		"Records": []map[string]any{
			{
				"MeasureName": "cpu", "MeasureValue": "45", "MeasureValueType": "DOUBLE",
				"Time": "1609459200000", "TimeUnit": "MILLISECONDS",
			},
			{
				"MeasureName": "mem", "MeasureValue": "80", "MeasureValueType": "DOUBLE",
				"Time": "1609459200000", "TimeUnit": "MILLISECONDS",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 2, int(ingested["Total"].(float64)))
}

// TestRefinement2_StorageBackendInterface verifies all methods implemented.
func TestRefinement2_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ timestreamwrite.StorageBackend = (*timestreamwrite.InMemoryBackend)(nil)
}

// doRequestWithHandler is like doRequest but accepts an explicit handler.
func doRequestWithHandler(
	t *testing.T,
	h *timestreamwrite.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	return doRequest(t, h, target, body)
}
