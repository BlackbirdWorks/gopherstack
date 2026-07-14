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

// assertInt64Field asserts that a field in a map[string]any equals the expected
// int64 value, converting from the JSON-unmarshalled float64 representation.
func assertInt64Field(t *testing.T, m map[string]any, key string, expected int64) {
	t.Helper()

	raw, ok := m[key]
	require.True(t, ok, "field %q missing from map", key)
	assert.Equal(t, expected, int64(raw.(float64)))
}

// TestRefinement3_TagResourceRequiresExistingResource verifies that TagResource
// rejects ARNs that do not belong to any known database or table.
func TestRefinement3_TagResourceRequiresExistingResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*timestreamwrite.InMemoryBackend)
		arn     string
		wantErr bool
	}{
		{
			name:    "unknown ARN returns not found",
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/no-such-db",
			wantErr: true,
		},
		{
			name: "known database ARN succeeds",
			setup: func(b *timestreamwrite.InMemoryBackend) {
				_, _ = b.CreateDatabase("known-db", "", nil)
			},
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/known-db",
			wantErr: false,
		},
		{
			name: "known table ARN succeeds",
			setup: func(b *timestreamwrite.InMemoryBackend) {
				_, _ = b.CreateDatabase("tbl-db", "", nil)
				_, _ = b.CreateTable("tbl-db", "tbl", nil, nil)
			},
			arn:     "arn:aws:timestream:us-east-1:000000000000:database/tbl-db/table/tbl",
			wantErr: false,
		},
		{
			name: "scheduled-query ARN accepted for cross-service tags",
			arn:  "arn:aws:timestream:us-east-1:000000000000:scheduled-query/my-query",
			// scheduled-query ARNs are accepted unconditionally so the TimestreamWrite
			// handler can serve as the unified tag store for all Timestream resources.
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.TagResource(tt.arn, map[string]string{"k": "v"})

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, timestreamwrite.ErrResourceNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement3_TagResourceHTTP400OnUnknownARN verifies the handler returns
// 400 (ResourceNotFoundException) when the ARN does not exist.
func TestRefinement3_TagResourceHTTP400OnUnknownARN(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:timestream:us-east-1:000000000000:database/ghost-db",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestRefinement3_TableRetentionProperties verifies that RetentionProperties
// are stored and returned when creating a table.
func TestRefinement3_TableRetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ret-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ret-db",
		"TableName":    "ret-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  24,
			"MagneticStoreRetentionPeriodInDays": 365,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 24)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 365)
}

// TestRefinement3_MagneticStoreWriteProperties verifies that
// MagneticStoreWriteProperties are stored and returned when creating a table.
func TestRefinement3_MagneticStoreWriteProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mag-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "mag-db",
		"TableName":    "mag-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.Equal(t, true, mswp["EnableMagneticStoreWrites"])
}

// TestRefinement3_UpdateTableRetentionProperties verifies that UpdateTable
// updates RetentionProperties correctly.
func TestRefinement3_UpdateTableRetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "upd-db", "TableName": "upd-tbl"})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd-db",
		"TableName":    "upd-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  48,
			"MagneticStoreRetentionPeriodInDays": 730,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 48)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 730)
}

// TestRefinement3_UpdateTableMagneticStoreProps verifies MagneticStoreWriteProperties
// are preserved through UpdateTable.
func TestRefinement3_UpdateTableMagneticStoreProps(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ms-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ms-db",
		"TableName":    "ms-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": false,
		},
	})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "ms-db",
		"TableName":    "ms-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.Equal(t, true, mswp["EnableMagneticStoreWrites"])
}

// TestRefinement3_WriteRecordsReturnsMemoryStoreCount verifies that
// WriteRecords returns both Total and MemoryStore ingested counts.
func TestRefinement3_WriteRecordsReturnsMemoryStoreCount(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "wrc-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "wrc-db", "TableName": "wrc-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "wrc-db",
		"TableName":    "wrc-tbl",
		"Records": []map[string]any{
			{"MeasureName": "m1", "MeasureValue": "1", "Time": "1609459200000", "TimeUnit": "MILLISECONDS"},
			{"MeasureName": "m2", "MeasureValue": "2", "Time": "1609459200001", "TimeUnit": "MILLISECONDS"},
			{"MeasureName": "m3", "MeasureValue": "3", "Time": "1609459200002", "TimeUnit": "MILLISECONDS"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, int(3), int(ingested["Total"].(float64)))
	assert.Equal(t, int(3), int(ingested["MemoryStore"].(float64)))
}

// TestRefinement3_CreateBatchLoadTaskWithDataSourceConfig verifies that
// DataSourceConfiguration is stored and returned by DescribeBatchLoadTask.
func TestRefinement3_CreateBatchLoadTaskWithDataSourceConfig(t *testing.T) {
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

// TestRefinement3_CreateBatchLoadTaskWithReportConfig verifies that
// ReportConfiguration is stored and returned.
func TestRefinement3_CreateBatchLoadTaskWithReportConfig(t *testing.T) {
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

// TestRefinement3_ListBatchLoadTasksSortedByCreationTime verifies that tasks
// are returned in creation-time order (oldest first).
func TestRefinement3_ListBatchLoadTasksSortedByCreationTime(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(
		&timestreamwrite.Database{
			DatabaseName: "sort-db",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/sort-db",
		},
	)
	b.AddTableInternal(
		&timestreamwrite.Table{
			DatabaseName: "sort-db",
			TableName:    "sort-tbl",
			TableStatus:  "ACTIVE",
			ARN:          "arn:aws:timestream:us-east-1:000000000000:database/sort-db/table/sort-tbl",
		},
	)

	now := time.Now()
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "z-task", TargetDatabaseName: "sort-db", TargetTableName: "sort-tbl",
		TaskStatus: "CREATED", CreationTime: now.Add(1 * time.Second),
	})
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID: "a-task", TargetDatabaseName: "sort-db", TargetTableName: "sort-tbl",
		TaskStatus: "CREATED", CreationTime: now,
	})

	tasks := b.ListBatchLoadTasks("")
	require.Len(t, tasks, 2)
	// Older task (a-task) should come first.
	assert.Equal(t, "a-task", tasks[0].TaskID)
	assert.Equal(t, "z-task", tasks[1].TaskID)
}

// TestRefinement3_BatchLoadTaskSummaryViewResumableUntil verifies that
// ResumableUntil is populated when set on the task.
func TestRefinement3_BatchLoadTaskSummaryViewResumableUntil(t *testing.T) {
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

// TestRefinement3_DescribeBatchLoadTaskErrorMessage verifies ErrorMessage is
// included in the describe response when set.
func TestRefinement3_DescribeBatchLoadTaskErrorMessage(t *testing.T) {
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

// TestRefinement3_CreateTableNoPropertiesReturnsDefaults verifies that
// when no RetentionProperties are specified, real AWS defaults are applied (6h / 73d).
func TestRefinement3_CreateTableNoPropertiesReturnsDefaults(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "np-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "np-db",
		"TableName":    "np-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp, hasRP := tbl["RetentionProperties"]
	assert.True(t, hasRP, "RetentionProperties should be set to AWS defaults")

	rpMap, ok := rp.(map[string]any)
	assert.True(t, ok)
	assert.InDelta(t, float64(6), rpMap["MemoryStoreRetentionPeriodInHours"], 0)
	assert.InDelta(t, float64(73), rpMap["MagneticStoreRetentionPeriodInDays"], 0)

	_, hasMSWP := tbl["MagneticStoreWriteProperties"]
	assert.False(t, hasMSWP)
}

// TestRefinement3_BackendRetentionPropertiesRoundTrip verifies backend stores
// and returns RetentionProperties correctly.
func TestRefinement3_BackendRetentionPropertiesRoundTrip(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("rp-db", "", nil)
	require.NoError(t, err)

	inp := &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  12,
			MagneticStoreRetentionPeriodInDays: 180,
		},
	}

	tbl, err := b.CreateTable("rp-db", "rp-tbl", nil, inp)
	require.NoError(t, err)
	require.NotNil(t, tbl.RetentionProperties)
	assert.Equal(t, int64(12), tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours)
	assert.Equal(t, int64(180), tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays)

	// Verify DescribeTable also returns the properties.
	described, err := b.DescribeTable("rp-db", "rp-tbl")
	require.NoError(t, err)
	require.NotNil(t, described.RetentionProperties)
	assert.Equal(t, int64(12), described.RetentionProperties.MemoryStoreRetentionPeriodInHours)
}

// TestRefinement3_BackendUpdateTableMagneticStoreProps verifies UpdateTable
// modifies MagneticStoreWriteProperties.
func TestRefinement3_BackendUpdateTableMagneticStoreProps(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("ms2-db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("ms2-db", "ms2-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{EnableMagneticStoreWrites: false},
	})
	require.NoError(t, err)

	updated, err := b.UpdateTable("ms2-db", "ms2-tbl", &timestreamwrite.UpdateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{EnableMagneticStoreWrites: true},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MagneticStoreWriteProperties)
	assert.True(t, updated.MagneticStoreWriteProperties.EnableMagneticStoreWrites)
}

// TestRefinement3_WriteRecordsOutputTypeIsInt32 verifies that WriteRecords
// returns a *WriteRecordsOutput with int32 fields.
func TestRefinement3_WriteRecordsOutputTypeIsInt32(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("wr2-db", "", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("wr2-db", "wr2-tbl", nil, nil)
	require.NoError(t, err)

	out, err := b.WriteRecords("wr2-db", "wr2-tbl", []timestreamwrite.Record{
		{MeasureName: "m", MeasureValue: "1"},
		{MeasureName: "m2", MeasureValue: "2"},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), out.Total)
	assert.Equal(t, int32(2), out.MemoryStore)
}

// TestRefinement3_ScheduledQueryARNTagging verifies that scheduled-query ARNs
// can be tagged via the write service's unified tag store.
func TestRefinement3_ScheduledQueryARNTagging(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	sqARN := "arn:aws:timestream:us-east-1:000000000000:scheduled-query/my-query"

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": sqARN,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": sqARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "owner", tag["Key"])
	assert.Equal(t, "alice", tag["Value"])
}

// TestRefinement3_CreateBatchLoadTaskMissingTargetDatabase verifies that
// creating a batch load task for a non-existent database returns an error.
func TestRefinement3_CreateBatchLoadTaskMissingTargetDatabase(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	rec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "no-db",
		"TargetTableName":    "no-tbl",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_CreateBatchLoadTaskMissingTargetTable verifies that
// creating a batch load task for a non-existent table returns an error.
func TestRefinement3_CreateBatchLoadTaskMissingTargetTable(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "btdb"})

	rec := doRequest(t, h, "CreateBatchLoadTask", map[string]any{
		"TargetDatabaseName": "btdb",
		"TargetTableName":    "no-tbl",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_DescribeTableReturnsRetentionAfterUpdate verifies that
// after an UpdateTable call the DescribeTable response reflects updated props.
func TestRefinement3_DescribeTableReturnsRetentionAfterUpdate(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd2-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "upd2-db", "TableName": "upd2-tbl"})

	doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd2-db",
		"TableName":    "upd2-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  6,
			"MagneticStoreRetentionPeriodInDays": 90,
		},
	})

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "upd2-db",
		"TableName":    "upd2-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 6)
	assertInt64Field(t, rp, "MagneticStoreRetentionPeriodInDays", 90)
}

// TestRefinement3_ListTablesIncludesRetentionProperties verifies that the
// ListTables response includes RetentionProperties when set.
func TestRefinement3_ListTablesIncludesRetentionProperties(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "lt-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "lt-db",
		"TableName":    "lt-tbl",
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  8,
			"MagneticStoreRetentionPeriodInDays": 60,
		},
	})

	rec := doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "lt-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables := out["Tables"].([]any)
	require.Len(t, tables, 1)
	tbl := tables[0].(map[string]any)
	rp := tbl["RetentionProperties"].(map[string]any)
	assertInt64Field(t, rp, "MemoryStoreRetentionPeriodInHours", 8)
}

// TestRefinement3_DeleteDatabaseCascadesTableTags verifies that deleting a
// database also removes tags for all its tables.
func TestRefinement3_DeleteDatabaseCascadesTableTags(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("casc-db", "", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("casc-db", "casc-tbl", nil, nil)
	require.NoError(t, err)

	tblARN := "arn:aws:timestream:us-east-1:000000000000:database/casc-db/table/casc-tbl"
	err = b.TagResource(tblARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	// Tags should exist.
	tags := b.ListTagsForResource(tblARN)
	assert.Equal(t, "test", tags["env"])

	// Delete the database.
	err = b.DeleteDatabase("casc-db")
	require.NoError(t, err)

	// Tags for the table should be gone.
	tags = b.ListTagsForResource(tblARN)
	assert.Empty(t, tags)
}

// TestRefinement3_DeleteTableCleansUpTags verifies that deleting a table
// removes its associated tags.
func TestRefinement3_DeleteTableCleansUpTags(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("dtag-db", "", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("dtag-db", "dtag-tbl", nil, nil)
	require.NoError(t, err)

	tblARN := "arn:aws:timestream:us-east-1:000000000000:database/dtag-db/table/dtag-tbl"
	err = b.TagResource(tblARN, map[string]string{"k": "v"})
	require.NoError(t, err)

	err = b.DeleteTable("dtag-db", "dtag-tbl")
	require.NoError(t, err)

	tags := b.ListTagsForResource(tblARN)
	assert.Empty(t, tags)
}

// TestRefinement3_WriteRecordsInvalidTable verifies that writing to a
// non-existent table returns an error.
func TestRefinement3_WriteRecordsInvalidTable(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "wri-db"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "wri-db",
		"TableName":    "nonexistent",
		"Records":      []map[string]any{{"MeasureName": "m", "MeasureValue": "1"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_WriteRecordsInvalidDatabase verifies that writing to a
// non-existent database returns an error.
func TestRefinement3_WriteRecordsInvalidDatabase(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "no-db",
		"TableName":    "no-tbl",
		"Records":      []map[string]any{{"MeasureName": "m", "MeasureValue": "1"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_BackendScheduledQueryARNPassesValidation verifies that
// isKnownARNLocked accepts any ARN containing "scheduled-query/".
func TestRefinement3_BackendScheduledQueryARNPassesValidation(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	// No databases or tables created; yet scheduled-query ARNs should be accepted.
	err := b.TagResource(
		"arn:aws:timestream:eu-west-1:123456789012:scheduled-query/daily-report",
		map[string]string{"managed": "true"},
	)
	require.NoError(t, err)

	tags := b.ListTagsForResource("arn:aws:timestream:eu-west-1:123456789012:scheduled-query/daily-report")
	assert.Equal(t, "true", tags["managed"])
}

// TestRefinement3_BatchLoadTaskDescriptionViewMissingResumableUntil verifies
// that a task without ResumableUntil omits the field.
func TestRefinement3_BatchLoadTaskDescriptionViewMissingResumableUntil(t *testing.T) {
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

// TestRefinement3_PersistenceRetentionPropertiesRoundTrip verifies that table
// RetentionProperties survive a snapshot/restore cycle.
func TestRefinement3_PersistenceRetentionPropertiesRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-rp-db", "", nil)
	require.NoError(t, err)
	_, err = b1.CreateTable("snap-rp-db", "snap-rp-tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  72,
			MagneticStoreRetentionPeriodInDays: 400,
		},
	})
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	tbl, err := b2.DescribeTable("snap-rp-db", "snap-rp-tbl")
	require.NoError(t, err)
	require.NotNil(t, tbl.RetentionProperties)
	assert.Equal(t, int64(72), tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours)
	assert.Equal(t, int64(400), tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays)
}

// TestRefinement3_PersistenceDataSourceConfigRoundTrip verifies that
// BatchLoadTask DataSourceConfiguration survives a snapshot/restore cycle.
func TestRefinement3_PersistenceDataSourceConfigRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-blt-db", "", nil)
	require.NoError(t, err)
	_, err = b1.CreateTable("snap-blt-db", "snap-blt-tbl", nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateBatchLoadTask(
		"snap-blt-db", "snap-blt-tbl",
		&timestreamwrite.DataSourceConfiguration{
			DataFormat: "CSV",
			DataSourceS3Configuration: &timestreamwrite.DataSourceS3Configuration{
				BucketName: "snap-bucket",
			},
		},
		nil,
	)
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	tasks := b2.ListBatchLoadTasks("")
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].DataSourceConfiguration)
	require.NotNil(t, tasks[0].DataSourceConfiguration.DataSourceS3Configuration)
	assert.Equal(t, "snap-bucket", tasks[0].DataSourceConfiguration.DataSourceS3Configuration.BucketName)
}

// TestRefinement3_WriteRecordsReturnsParsableJSON verifies the RecordsIngested
// JSON can be parsed and contains the expected field names.
func TestRefinement3_WriteRecordsReturnsParsableJSON(t *testing.T) {
	t.Parallel()

	h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "pj-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "pj-db", "TableName": "pj-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "pj-db",
		"TableName":    "pj-tbl",
		"Records": []map[string]any{
			{"MeasureName": "cpu", "MeasureValue": "99.9"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"Total"`)
	assert.Contains(t, body, `"MemoryStore"`)
	// Should NOT contain old field name.
	assert.NotContains(t, body, `"MemStore"`)
}
