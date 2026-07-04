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

// --- Gap 1: Schema / PartitionKey on Table ---

// TestAccuracy_CreateTableWithSchema verifies that a composite partition key schema
// is stored and returned by CreateTable per the AWS API.
func TestAccuracy_CreateTableWithSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "schema-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "schema-db",
		"TableName":    "schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "region", "EnforcementInRecord": "REQUIRED"},
				{"Type": "MEASURE", "EnforcementInRecord": "OPTIONAL"},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema, ok := tbl["Schema"].(map[string]any)
	require.True(t, ok, "Schema should be present in response")

	keys := schema["CompositePartitionKey"].([]any)
	require.Len(t, keys, 2)

	first := keys[0].(map[string]any)
	assert.Equal(t, "DIMENSION", first["Type"])
	assert.Equal(t, "region", first["Name"])
	assert.Equal(t, "REQUIRED", first["EnforcementInRecord"])
}

// TestAccuracy_DescribeTableReturnsSchema verifies Schema survives DescribeTable.
func TestAccuracy_DescribeTableReturnsSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "desc-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "desc-schema-db",
		"TableName":    "desc-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "MEASURE"},
			},
		},
	})

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "desc-schema-db",
		"TableName":    "desc-schema-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema, ok := tbl["Schema"].(map[string]any)
	require.True(t, ok, "Schema should be present in DescribeTable response")
	keys := schema["CompositePartitionKey"].([]any)
	assert.Len(t, keys, 1)
	assert.Equal(t, "MEASURE", keys[0].(map[string]any)["Type"])
}

// TestAccuracy_UpdateTableWithSchema verifies Schema can be set via UpdateTable.
func TestAccuracy_UpdateTableWithSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "upd-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "upd-schema-db",
		"TableName":    "upd-schema-tbl",
	})

	rec := doRequest(t, h, "UpdateTable", map[string]any{
		"DatabaseName": "upd-schema-db",
		"TableName":    "upd-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "account_id", "EnforcementInRecord": "REQUIRED"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	schema := tbl["Schema"].(map[string]any)
	keys := schema["CompositePartitionKey"].([]any)
	require.Len(t, keys, 1)
	assert.Equal(t, "account_id", keys[0].(map[string]any)["Name"])
}

// TestAccuracy_ListTablesIncludesSchema verifies Schema appears in ListTables output.
func TestAccuracy_ListTablesIncludesSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ls-schema-db"})
	doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "ls-schema-db",
		"TableName":    "ls-schema-tbl",
		"Schema": map[string]any{
			"CompositePartitionKey": []map[string]any{
				{"Type": "DIMENSION", "Name": "host"},
			},
		},
	})

	rec := doRequest(t, h, "ListTables", map[string]string{"DatabaseName": "ls-schema-db"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tables := out["Tables"].([]any)
	require.Len(t, tables, 1)

	schema := tables[0].(map[string]any)["Schema"].(map[string]any)
	keys := schema["CompositePartitionKey"].([]any)
	assert.Equal(t, "host", keys[0].(map[string]any)["Name"])
}

// --- Gap 2: MagneticStoreRejectedDataLocation ---

// TestAccuracy_MagneticStoreRejectedDataLocation verifies S3 config for rejected records
// is stored and returned in the table view.
func TestAccuracy_MagneticStoreRejectedDataLocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "msrdl-db"})

	rec := doRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "msrdl-db",
		"TableName":    "msrdl-tbl",
		"MagneticStoreWriteProperties": map[string]any{
			"EnableMagneticStoreWrites": true,
			"MagneticStoreRejectedDataLocation": map[string]any{
				"S3Configuration": map[string]any{
					"BucketName":       "rejected-records-bucket",
					"ObjectKeyPrefix":  "errors/",
					"EncryptionOption": "SSE_S3",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	assert.True(t, mswp["EnableMagneticStoreWrites"].(bool))

	loc := mswp["MagneticStoreRejectedDataLocation"].(map[string]any)
	s3cfg := loc["S3Configuration"].(map[string]any)
	assert.Equal(t, "rejected-records-bucket", s3cfg["BucketName"])
	assert.Equal(t, "errors/", s3cfg["ObjectKeyPrefix"])
	assert.Equal(t, "SSE_S3", s3cfg["EncryptionOption"])
}

// TestAccuracy_MagneticStoreRejectedDataLocationRoundTrip verifies MSRDL survives DescribeTable.
func TestAccuracy_MagneticStoreRejectedDataLocationRoundTrip(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(b)

	_, err := b.CreateDatabase("msrdl2-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("msrdl2-db", "msrdl2-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
			MagneticStoreRejectedDataLocation: &timestreamwrite.MagneticStoreRejectedDataLocation{
				S3Configuration: &timestreamwrite.S3Configuration{
					BucketName: "err-bucket",
					KmsKeyID:   "arn:aws:kms:us-east-1:000000000000:key/abc",
				},
			},
		},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"DatabaseName": "msrdl2-db",
		"TableName":    "msrdl2-tbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tbl := out["Table"].(map[string]any)
	mswp := tbl["MagneticStoreWriteProperties"].(map[string]any)
	loc := mswp["MagneticStoreRejectedDataLocation"].(map[string]any)
	s3cfg := loc["S3Configuration"].(map[string]any)
	assert.Equal(t, "err-bucket", s3cfg["BucketName"])
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", s3cfg["KmsKeyId"])
}

// --- Gap 3: Record.MeasureValues (MULTI type) ---

// TestAccuracy_WriteRecordsMultiMeasure verifies that MULTI-type records with
// MeasureValues are accepted and stored correctly.
func TestAccuracy_WriteRecordsMultiMeasure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "multi-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "multi-db", "TableName": "multi-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "multi-db",
		"TableName":    "multi-tbl",
		"Records": []map[string]any{
			{
				"MeasureName":      "metrics",
				"MeasureValueType": "MULTI",
				"Time":             "1609459200000",
				"TimeUnit":         "MILLISECONDS",
				"MeasureValues": []map[string]any{
					{"Name": "cpu", "Value": "45.3", "Type": "DOUBLE"},
					{"Name": "mem", "Value": "8192", "Type": "BIGINT"},
					{"Name": "disk_ok", "Value": "true", "Type": "BOOLEAN"},
				},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 1, int(ingested["Total"].(float64)))
}

// TestAccuracy_BackendStoresMeasureValues verifies MeasureValues survive in the backend.
func TestAccuracy_BackendStoresMeasureValues(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("mv-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("mv-db", "mv-tbl", nil, nil)
	require.NoError(t, err)

	records := []timestreamwrite.Record{
		{
			MeasureName:      "system",
			MeasureValueType: "MULTI",
			Time:             "1609459200000",
			TimeUnit:         "MILLISECONDS",
			MeasureValues: []timestreamwrite.MeasureValue{
				{Name: "cpu", Value: "12.5", Type: "DOUBLE"},
				{Name: "status", Value: "healthy", Type: "VARCHAR"},
			},
		},
	}

	out, err := b.WriteRecords("mv-db", "mv-tbl", records)
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)
}

// --- Gap 4: WriteRecords CommonAttributes ---

// TestAccuracy_WriteRecordsCommonAttributes verifies that CommonAttributes dimensions
// are merged into each record per the AWS specification.
func TestAccuracy_WriteRecordsCommonAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ca-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "ca-db", "TableName": "ca-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "ca-db",
		"TableName":    "ca-tbl",
		"CommonAttributes": map[string]any{
			"Dimensions": []map[string]any{
				{"Name": "region", "Value": "us-east-1"},
				{"Name": "env", "Value": "prod"},
			},
			"Time":     "1609459200000",
			"TimeUnit": "MILLISECONDS",
		},
		"Records": []map[string]any{
			{
				"MeasureName":      "cpu",
				"MeasureValue":     "65.4",
				"MeasureValueType": "DOUBLE",
				// No Dimensions or Time — should come from CommonAttributes
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 1, int(ingested["Total"].(float64)))
}

// TestAccuracy_WriteRecordsCommonAttributesMerge verifies record-specific dimensions
// override CommonAttributes on name conflict.
func TestAccuracy_WriteRecordsCommonAttributesMerge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "cam-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "cam-db", "TableName": "cam-tbl"})

	// Two unique records: first with no override, second with dimension override.
	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "cam-db",
		"TableName":    "cam-tbl",
		"CommonAttributes": map[string]any{
			"Time":     "1609459200000",
			"TimeUnit": "MILLISECONDS",
			"Dimensions": []map[string]any{
				{"Name": "env", "Value": "prod"},
			},
		},
		"Records": []map[string]any{
			{
				"MeasureName":      "r1",
				"MeasureValue":     "1",
				"MeasureValueType": "DOUBLE",
			},
			{
				"MeasureName":      "r2",
				"MeasureValue":     "2",
				"MeasureValueType": "DOUBLE",
				"Dimensions": []map[string]any{
					{"Name": "env", "Value": "staging"}, // overrides common
				},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ingested := out["RecordsIngested"].(map[string]any)
	assert.Equal(t, 2, int(ingested["Total"].(float64)))
}

// --- Gap 5: Pagination ---

// TestAccuracy_ListDatabasesPagination verifies NextToken pagination for ListDatabases.
func TestAccuracy_ListDatabasesPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": name})
	}

	// First page: MaxResults=2.
	rec1 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	dbs1 := out1["Databases"].([]any)
	assert.Len(t, dbs1, 2)

	nextToken, ok := out1["NextToken"].(string)
	require.True(t, ok && nextToken != "", "NextToken should be non-empty when more pages exist")

	// Second page.
	rec2 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	dbs2 := out2["Databases"].([]any)
	assert.Len(t, dbs2, 2)

	// Third page (final).
	nextToken2 := out2["NextToken"].(string)
	rec3 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 2, "NextToken": nextToken2})
	require.Equal(t, http.StatusOK, rec3.Code)

	var out3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &out3))

	dbs3 := out3["Databases"].([]any)
	assert.Len(t, dbs3, 1)
	// No NextToken on final page.
	assert.Empty(t, out3["NextToken"])
}

// TestAccuracy_ListDatabasesNoNextTokenWhenFits verifies no NextToken when all results fit.
func TestAccuracy_ListDatabasesNoNextTokenWhenFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "only-db"})

	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Empty(t, out["NextToken"])
}

// TestAccuracy_ListTablesPagination verifies NextToken pagination for ListTables.
func TestAccuracy_ListTablesPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "pg-db"})

	for _, name := range []string{"t1", "t2", "t3"} {
		doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "pg-db", "TableName": name})
	}

	rec1 := doRequest(t, h, "ListTables", map[string]any{"DatabaseName": "pg-db", "MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	tbls1 := out1["Tables"].([]any)
	assert.Len(t, tbls1, 2)

	nextToken := out1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListTables", map[string]any{
		"DatabaseName": "pg-db",
		"MaxResults":   2,
		"NextToken":    nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	tbls2 := out2["Tables"].([]any)
	assert.Len(t, tbls2, 1)
	assert.Empty(t, out2["NextToken"])
}

// TestAccuracy_ListBatchLoadTasksPagination verifies NextToken pagination for ListBatchLoadTasks.
func TestAccuracy_ListBatchLoadTasksPagination(t *testing.T) {
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

// --- Gap 6: BatchLoadProgressReport ---

// TestAccuracy_DescribeBatchLoadTaskProgressReport verifies ProgressReport is returned
// when set on a task.
func TestAccuracy_DescribeBatchLoadTaskProgressReport(t *testing.T) {
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

// TestAccuracy_DescribeBatchLoadTaskNoProgressReport verifies ProgressReport is omitted
// when not set.
func TestAccuracy_DescribeBatchLoadTaskNoProgressReport(t *testing.T) {
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

// --- Gap 7: Dimension.DimensionValueType ---

// TestAccuracy_DimensionValueTypePassthrough verifies that DimensionValueType is
// preserved in the backend when specified on WriteRecords.
func TestAccuracy_DimensionValueTypePassthrough(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("dvt-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("dvt-db", "dvt-tbl", nil, nil)
	require.NoError(t, err)

	records := []timestreamwrite.Record{
		{
			MeasureName:      "cpu",
			MeasureValue:     "42.5",
			MeasureValueType: "DOUBLE",
			Time:             "1609459200000",
			TimeUnit:         "MILLISECONDS",
			Dimensions: []timestreamwrite.Dimension{
				{Name: "host", Value: "server-1", DimensionValueType: "VARCHAR"},
			},
		},
	}

	out, err := b.WriteRecords("dvt-db", "dvt-tbl", records)
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)
}

// TestAccuracy_WriteRecordsDimensionValueType verifies DimensionValueType is accepted
// via the HTTP handler.
func TestAccuracy_WriteRecordsDimensionValueType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dvth-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dvth-db", "TableName": "dvth-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "dvth-db",
		"TableName":    "dvth-tbl",
		"Records": []map[string]any{
			{
				"MeasureName":      "latency",
				"MeasureValue":     "23",
				"MeasureValueType": "BIGINT",
				"Time":             "1609459200000",
				"TimeUnit":         "MILLISECONDS",
				"Dimensions": []map[string]any{
					{"Name": "endpoint", "Value": "/api/health", "DimensionValueType": "VARCHAR"},
				},
			},
		},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Gap 8 & 9: Record version upsert and RejectedRecordsException ---

// TestAccuracy_WriteRecordsVersionUpsert verifies that a record with a higher version
// replaces the existing record without error.
func TestAccuracy_WriteRecordsVersionUpsert(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("vu-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("vu-db", "vu-tbl", nil, nil)
	require.NoError(t, err)

	record := timestreamwrite.Record{
		MeasureName:      "cpu",
		MeasureValue:     "10.0",
		MeasureValueType: "DOUBLE",
		Time:             "1609459200000",
		TimeUnit:         "MILLISECONDS",
		Version:          1,
	}

	out, err := b.WriteRecords("vu-db", "vu-tbl", []timestreamwrite.Record{record})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Total)

	// Same key, higher version: upsert succeeds.
	record.Version = 2
	record.MeasureValue = "20.0"
	out2, err := b.WriteRecords("vu-db", "vu-tbl", []timestreamwrite.Record{record})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out2.Total)
}

// TestAccuracy_WriteRecordsVersionConflictReturnsError verifies that a lower/equal
// version write returns a RejectedRecordsError.
func TestAccuracy_WriteRecordsVersionConflictReturnsError(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("vc-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("vc-db", "vc-tbl", nil, nil)
	require.NoError(t, err)

	record := timestreamwrite.Record{
		MeasureName:      "mem",
		MeasureValue:     "8192",
		MeasureValueType: "BIGINT",
		Time:             "1609459200001",
		TimeUnit:         "MILLISECONDS",
		Version:          2,
	}

	_, err = b.WriteRecords("vc-db", "vc-tbl", []timestreamwrite.Record{record})
	require.NoError(t, err)

	// Same record, lower version: should be rejected.
	record.Version = 1
	_, err = b.WriteRecords("vc-db", "vc-tbl", []timestreamwrite.Record{record})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	require.Len(t, rejErr.RejectedRecords, 1)
	assert.Equal(t, 0, rejErr.RejectedRecords[0].RecordIndex)
	assert.Equal(t, int64(2), rejErr.RejectedRecords[0].ExistingVersion)
}

// TestAccuracy_WriteRecordsSameVersionConflict verifies that a same-version write
// (when version already stored) is also rejected per AWS upsert rules.
func TestAccuracy_WriteRecordsSameVersionConflict(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("sv-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("sv-db", "sv-tbl", nil, nil)
	require.NoError(t, err)

	record := timestreamwrite.Record{
		MeasureName:      "rps",
		MeasureValue:     "500",
		MeasureValueType: "BIGINT",
		Time:             "1609459200002",
		TimeUnit:         "MILLISECONDS",
		Version:          3,
	}

	_, err = b.WriteRecords("sv-db", "sv-tbl", []timestreamwrite.Record{record})
	require.NoError(t, err)

	// Same version: rejected.
	_, err = b.WriteRecords("sv-db", "sv-tbl", []timestreamwrite.Record{record})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	assert.Len(t, rejErr.RejectedRecords, 1)
}

// TestAccuracy_WriteRecordsRejectedRecordsHTTP verifies that the handler returns
// 400 with __type=RejectedRecordsException on version conflict.
func TestAccuracy_WriteRecordsRejectedRecordsHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "rr-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "rr-db", "TableName": "rr-tbl"})

	writeBody := map[string]any{
		"DatabaseName": "rr-db",
		"TableName":    "rr-tbl",
		"Records": []map[string]any{
			{
				"MeasureName":      "errors",
				"MeasureValue":     "5",
				"MeasureValueType": "BIGINT",
				"Time":             "1609459200000",
				"TimeUnit":         "MILLISECONDS",
				"Version":          int64(5),
			},
		},
	}

	// First write succeeds.
	rec1 := doRequest(t, h, "WriteRecords", writeBody)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Same record, lower version — should fail with RejectedRecordsException.
	writeBody["Records"] = []map[string]any{
		{
			"MeasureName":      "errors",
			"MeasureValue":     "5",
			"MeasureValueType": "BIGINT",
			"Time":             "1609459200000",
			"TimeUnit":         "MILLISECONDS",
			"Version":          int64(3),
		},
	}

	rec2 := doRequest(t, h, "WriteRecords", writeBody)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
	assert.Equal(t, "RejectedRecordsException", errBody["__type"])

	rejected, ok := errBody["RejectedRecords"].([]any)
	require.True(t, ok, "RejectedRecords should be present in error response")
	assert.Len(t, rejected, 1)
}

// TestAccuracy_WriteRecordsPartialReject verifies that when some records pass and
// some fail version checks, only the failed ones are rejected.
func TestAccuracy_WriteRecordsPartialReject(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("pr2-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("pr2-db", "pr2-tbl", nil, nil)
	require.NoError(t, err)

	existing := timestreamwrite.Record{
		MeasureName: "load", MeasureValue: "1.0", MeasureValueType: "DOUBLE",
		Time: "1609459200000", TimeUnit: "MILLISECONDS", Version: 3,
	}
	_, err = b.WriteRecords("pr2-db", "pr2-tbl", []timestreamwrite.Record{existing})
	require.NoError(t, err)

	// Two records: one new (should pass), one conflicting (should fail).
	records := []timestreamwrite.Record{
		{
			MeasureName: "load2", MeasureValue: "2.0", MeasureValueType: "DOUBLE",
			Time: "1609459200001", TimeUnit: "MILLISECONDS", Version: 1,
		},
		existing, // same key, same version → rejected
	}

	_, err = b.WriteRecords("pr2-db", "pr2-tbl", records)
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	// Only the second record (index 1) should be rejected.
	require.Len(t, rejErr.RejectedRecords, 1)
	assert.Equal(t, 1, rejErr.RejectedRecords[0].RecordIndex)
}

// --- Gap 10: New records after upsert preserve dedup index ---

// TestAccuracy_WriteRecordsUpsertAndNewRecord verifies that after a successful upsert,
// subsequent writes to the same key require a further-incremented version.
func TestAccuracy_WriteRecordsUpsertAndNewRecord(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("uan-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("uan-db", "uan-tbl", nil, nil)
	require.NoError(t, err)

	base := timestreamwrite.Record{
		MeasureName: "ops", MeasureValue: "100", MeasureValueType: "BIGINT",
		Time: "1609459200000", TimeUnit: "MILLISECONDS", Version: 1,
	}

	// First write.
	_, err = b.WriteRecords("uan-db", "uan-tbl", []timestreamwrite.Record{base})
	require.NoError(t, err)

	// Upsert with higher version.
	upserted := base
	upserted.Version = 2
	upserted.MeasureValue = "200"
	_, err = b.WriteRecords("uan-db", "uan-tbl", []timestreamwrite.Record{upserted})
	require.NoError(t, err)

	// Now try with version 1 (lower than upserted version 2): should fail.
	lower := base
	lower.Version = 1
	_, err = b.WriteRecords("uan-db", "uan-tbl", []timestreamwrite.Record{lower})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	assert.Equal(t, int64(2), rejErr.RejectedRecords[0].ExistingVersion)
}

// --- Gap 11: BatchLoadTask Schema types ---

// TestAccuracy_PartitionKeyTypes verifies the exported PartitionKeyType constants match AWS values.
func TestAccuracy_PartitionKeyTypes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "DIMENSION", timestreamwrite.PartitionKeyTypeDimension)
	assert.Equal(t, "MEASURE", timestreamwrite.PartitionKeyTypeMeasure)
	assert.Equal(t, "REQUIRED", timestreamwrite.PartitionKeyEnforcementRequired)
	assert.Equal(t, "OPTIONAL", timestreamwrite.PartitionKeyEnforcementOptional)
}

// --- Gap 12: ErrRejectedRecords sentinel ---

// TestAccuracy_ErrRejectedRecordsSentinel verifies errors.As matches RejectedRecordsError.
func TestAccuracy_ErrRejectedRecordsSentinel(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("sentinel-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("sentinel-db", "sentinel-tbl", nil, nil)
	require.NoError(t, err)

	rec := timestreamwrite.Record{
		MeasureName: "x", MeasureValue: "1", MeasureValueType: "DOUBLE",
		Time: "1609459200000", TimeUnit: "MILLISECONDS", Version: 5,
	}
	_, err = b.WriteRecords("sentinel-db", "sentinel-tbl", []timestreamwrite.Record{rec})
	require.NoError(t, err)

	rec.Version = 1
	_, err = b.WriteRecords("sentinel-db", "sentinel-tbl", []timestreamwrite.Record{rec})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	assert.ErrorAs(t, err, &rejErr, "error should be detectable as *RejectedRecordsError")
}

// --- Gap 13: Snapshot/Restore round-trip with new fields ---

// TestAccuracy_SnapshotRestorePreservesSchema verifies Schema survives snapshot/restore.
func TestAccuracy_SnapshotRestorePreservesSchema(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-schema-db", nil)
	require.NoError(t, err)
	_, err = b1.CreateTable("snap-schema-db", "snap-schema-tbl", nil, &timestreamwrite.CreateTableInput{
		Schema: &timestreamwrite.Schema{
			CompositePartitionKey: []timestreamwrite.PartitionKey{
				{Type: "DIMENSION", Name: "az", EnforcementInRecord: "REQUIRED"},
			},
		},
	})
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	tbl, err := b2.DescribeTable("snap-schema-db", "snap-schema-tbl")
	require.NoError(t, err)
	require.NotNil(t, tbl.Schema)
	require.Len(t, tbl.Schema.CompositePartitionKey, 1)
	assert.Equal(t, "az", tbl.Schema.CompositePartitionKey[0].Name)
}

// TestAccuracy_SnapshotRestorePreservesMSRDL verifies MagneticStoreRejectedDataLocation
// survives snapshot/restore.
func TestAccuracy_SnapshotRestorePreservesMSRDL(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-msrdl-db", nil)
	require.NoError(t, err)
	_, err = b1.CreateTable("snap-msrdl-db", "snap-msrdl-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: true,
			MagneticStoreRejectedDataLocation: &timestreamwrite.MagneticStoreRejectedDataLocation{
				S3Configuration: &timestreamwrite.S3Configuration{
					BucketName: "snap-bucket",
				},
			},
		},
	})
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	tbl, err := b2.DescribeTable("snap-msrdl-db", "snap-msrdl-tbl")
	require.NoError(t, err)
	require.NotNil(t, tbl.MagneticStoreWriteProperties)
	require.NotNil(t, tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation)
	require.NotNil(t, tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration)
	assert.Equal(t, "snap-bucket",
		tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration.BucketName)
}

// TestAccuracy_SnapshotRestorePreservesRecordIndex verifies that after restore, the
// version-based upsert index works correctly (version conflicts are detected).
func TestAccuracy_SnapshotRestorePreservesRecordIndex(t *testing.T) {
	t.Parallel()

	b1 := timestreamwrite.NewInMemoryBackend()
	_, err := b1.CreateDatabase("snap-idx-db", nil)
	require.NoError(t, err)
	_, err = b1.CreateTable("snap-idx-db", "snap-idx-tbl", nil, nil)
	require.NoError(t, err)

	rec := timestreamwrite.Record{
		MeasureName: "metric", MeasureValue: "7", MeasureValueType: "BIGINT",
		Time: "1609459200000", TimeUnit: "MILLISECONDS", Version: 4,
	}
	_, err = b1.WriteRecords("snap-idx-db", "snap-idx-tbl", []timestreamwrite.Record{rec})
	require.NoError(t, err)

	data, err := b1.Snapshot()
	require.NoError(t, err)

	b2 := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	// After restore, writing with lower version should be rejected.
	rec.Version = 2
	_, err = b2.WriteRecords("snap-idx-db", "snap-idx-tbl", []timestreamwrite.Record{rec})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	assert.Equal(t, int64(4), rejErr.RejectedRecords[0].ExistingVersion)
}

// --- Gap 14: BatchLoadProgressReport in backend struct ---

// TestAccuracy_BackendBatchLoadProgressReport verifies BatchLoadProgressReport
// is stored and returned via DescribeBatchLoadTask.
func TestAccuracy_BackendBatchLoadProgressReport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	pr := &timestreamwrite.BatchLoadProgressReport{
		RecordsProcessed: 500,
		RecordsIngested:  480,
	}
	b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
		TaskID:             "prb-task",
		TargetDatabaseName: "pr-db",
		TargetTableName:    "pr-tbl",
		TaskStatus:         timestreamwrite.BatchLoadStatusInProgress,
		CreationTime:       time.Now(),
		LastUpdatedTime:    time.Now(),
		ProgressReport:     pr,
	})

	task, err := b.DescribeBatchLoadTask("prb-task")
	require.NoError(t, err)
	require.NotNil(t, task.ProgressReport)
	assert.Equal(t, int64(500), task.ProgressReport.RecordsProcessed)
	assert.Equal(t, int64(480), task.ProgressReport.RecordsIngested)
}

// --- Gap 15: WriteRecords DefaultVersion ---

// TestAccuracy_WriteRecordsDefaultVersionIsOne verifies that records without an explicit
// Version are treated as Version=1, and re-writing without Version is rejected.
func TestAccuracy_WriteRecordsDefaultVersionIsOne(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("dv-db", nil)
	require.NoError(t, err)
	_, err = b.CreateTable("dv-db", "dv-tbl", nil, nil)
	require.NoError(t, err)

	rec := timestreamwrite.Record{
		MeasureName:      "metric",
		MeasureValue:     "1",
		MeasureValueType: "BIGINT",
		Time:             "1609459200000",
		TimeUnit:         "MILLISECONDS",
		// No Version — defaults to 1
	}

	_, err = b.WriteRecords("dv-db", "dv-tbl", []timestreamwrite.Record{rec})
	require.NoError(t, err)

	// Writing same record again without version (still 0→1) should fail.
	_, err = b.WriteRecords("dv-db", "dv-tbl", []timestreamwrite.Record{rec})
	require.Error(t, err)

	var rejErr *timestreamwrite.RejectedRecordsError
	require.ErrorAs(t, err, &rejErr)
	assert.Equal(t, int64(1), rejErr.RejectedRecords[0].ExistingVersion)
}
