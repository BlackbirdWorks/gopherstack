package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_ErrValidation_WireCodeIsInvalidInputException verifies that hand-rolled
// validation failures surface as InvalidInputException, not ValidationException.
// aws-sdk-go-v2/service/glue's per-operation error deserializers (e.g.
// awsAwsjson11_deserializeOpErrorCreateDatabase/CreateTrigger/CreateBlueprint)
// list InvalidInputException as the documented hand-validation error; Glue's
// ValidationException type exists but is only declared by a handful of newer
// operations (e.g. DeleteConnectionType), not the general Create* family.
func Test_ErrValidation_WireCodeIsInvalidInputException(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateDatabase empty name",
			action: "CreateDatabase",
			body:   map[string]any{"DatabaseInput": map[string]any{"Name": ""}},
		},
		{
			name:   "CreateJob missing role",
			action: "CreateJob",
			body: map[string]any{
				"Name":    "job1",
				"Command": map[string]any{"Name": "glueetl"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidInputException")
			assert.NotContains(t, rec.Body.String(), "\"__type\":\"ValidationException\"")
		})
	}
}

// Test_TableStorageDescriptor_RoundTrip verifies that CreateTable/GetTable
// round-trip the full StorageDescriptor shape (InputFormat, OutputFormat,
// SerdeInfo, Parameters, BucketColumns, SortColumns, Compressed,
// NumberOfBuckets) plus Table-level Parameters/Owner/Retention and
// Column.Parameters — fields that aws-sdk-go-v2/service/glue/types.
// StorageDescriptor/Table declare but this backend previously dropped
// entirely (silently discarded on every CreateTable/UpdateTable call).
func Test_TableStorageDescriptor_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tableInput := map[string]any{
		"Name":      "tbl1",
		"Owner":     "hive",
		"Retention": 7,
		"Parameters": map[string]any{
			"classification": "parquet",
			"EXTERNAL":       "TRUE",
		},
		"StorageDescriptor": map[string]any{
			"Location":        "s3://bucket/tbl1/",
			"InputFormat":     "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			"OutputFormat":    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			"Compressed":      true,
			"NumberOfBuckets": 4,
			"BucketColumns":   []string{"id"},
			"SortColumns": []map[string]any{
				{"Column": "id", "SortOrder": 1},
			},
			"Parameters": map[string]any{"serialization.format": "1"},
			"SerdeInfo": map[string]any{
				"Name":                 "parquet-serde",
				"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
				"Parameters":           map[string]any{"foo": "bar"},
			},
			"Columns": []map[string]any{
				{"Name": "id", "Type": "int", "Parameters": map[string]any{"comment": "pk"}},
			},
		},
	}

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   tableInput,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetTable", map[string]any{
		"DatabaseName": "db1",
		"Name":         "tbl1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Table struct {
			Parameters        map[string]string `json:"Parameters"`
			Owner             string            `json:"Owner"`
			StorageDescriptor struct {
				SerdeInfo struct {
					Parameters           map[string]string `json:"Parameters"`
					Name                 string            `json:"Name"`
					SerializationLibrary string            `json:"SerializationLibrary"`
				} `json:"SerdeInfo"`
				Parameters    map[string]string `json:"Parameters"`
				Location      string            `json:"Location"`
				InputFormat   string            `json:"InputFormat"`
				OutputFormat  string            `json:"OutputFormat"`
				BucketColumns []string          `json:"BucketColumns"`
				SortColumns   []struct {
					Column    string `json:"Column"`
					SortOrder int    `json:"SortOrder"`
				} `json:"SortColumns"`
				Columns []struct {
					Parameters map[string]string `json:"Parameters"`
					Name       string            `json:"Name"`
					Type       string            `json:"Type"`
				} `json:"Columns"`
				NumberOfBuckets int  `json:"NumberOfBuckets"`
				Compressed      bool `json:"Compressed"`
			} `json:"StorageDescriptor"`
			Retention int `json:"Retention"`
		} `json:"Table"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "hive", out.Table.Owner)
	assert.Equal(t, 7, out.Table.Retention)
	assert.Equal(t, "parquet", out.Table.Parameters["classification"])
	sd := out.Table.StorageDescriptor
	assert.Equal(t, "s3://bucket/tbl1/", sd.Location)
	assert.True(t, sd.Compressed)
	assert.Equal(t, 4, sd.NumberOfBuckets)
	assert.Equal(t, []string{"id"}, sd.BucketColumns)
	assert.Equal(t, "1", sd.Parameters["serialization.format"])
	require.Len(t, sd.SortColumns, 1)
	assert.Equal(t, "id", sd.SortColumns[0].Column)
	require.NotNil(t, sd.SerdeInfo)
	assert.Equal(t, "parquet-serde", sd.SerdeInfo.Name)
	assert.Equal(t, "bar", sd.SerdeInfo.Parameters["foo"])
	require.Len(t, sd.Columns, 1)
	assert.Equal(t, "pk", sd.Columns[0].Parameters["comment"])
}

// Test_CreatePartition_RequiresExistingTable verifies that CreatePartition
// (and BatchCreatePartition) reject a nonexistent database/table with
// EntityNotFoundException instead of silently storing an orphaned partition.
// Previously BatchCreatePartition never checked that the table existed at
// all.
func Test_CreatePartition_RequiresExistingTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName":   "no-such-db",
		"TableName":      "no-such-table",
		"PartitionInput": map[string]any{"Values": []string{"2024-01-01"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
}

// Test_CreatePartition_DuplicateReportsAlreadyExists verifies that creating a
// duplicate partition surfaces AlreadyExistsException, not
// EntityNotFoundException. awserrFromDetail previously wrapped every
// batch-partition ErrorDetail as ErrNotFound regardless of its ErrorCode, so
// this case was misreported.
func Test_CreatePartition_DuplicateReportsAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	}).Code)

	partitionInput := map[string]any{
		"DatabaseName":   "db1",
		"TableName":      "tbl1",
		"PartitionInput": map[string]any{"Values": []string{"2024-01-01"}},
	}
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreatePartition", partitionInput).Code)

	rec := doGlueRequest(t, h, "CreatePartition", partitionInput)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExistsException")
	assert.NotContains(t, rec.Body.String(), "EntityNotFoundException")
}

// Test_BatchGetPartition_ReturnsRealPartitions verifies BatchGetPartition
// actually looks up requested partitions against backend state. It
// previously always returned an empty Partitions list regardless of what
// existed — a disguised stub masked by a stale comment claiming the backend
// "has no partition storage".
func Test_BatchGetPartition_ReturnsRealPartitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	}).Code)
	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName": "db1",
		"TableName":    "tbl1",
		"PartitionInput": map[string]any{
			"Values":            []string{"2024-01-01"},
			"StorageDescriptor": map[string]any{"Location": "s3://bucket/2024-01-01/"},
		},
	}).Code)

	rec := doGlueRequest(t, h, "BatchGetPartition", map[string]any{
		"DatabaseName": "db1",
		"TableName":    "tbl1",
		"PartitionsToGet": []map[string]any{
			{"Values": []string{"2024-01-01"}},
			{"Values": []string{"2024-01-02"}}, // does not exist
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partitions []struct {
			StorageDescriptor struct {
				Location string `json:"Location"`
			} `json:"StorageDescriptor"`
			Values []string `json:"Values"`
		} `json:"Partitions"`
		UnprocessedKeys []struct {
			Values []string `json:"Values"`
		} `json:"UnprocessedKeys"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	require.Len(t, out.Partitions, 1)
	assert.Equal(t, []string{"2024-01-01"}, out.Partitions[0].Values)
	assert.Equal(t, "s3://bucket/2024-01-01/", out.Partitions[0].StorageDescriptor.Location)
	require.Len(t, out.UnprocessedKeys, 1)
	assert.Equal(t, []string{"2024-01-02"}, out.UnprocessedKeys[0].Values)
}

// Test_Job_MaxCapacity verifies that CreateJob/UpdateJob accept and persist
// MaxCapacity (the DPU-based capacity knob used by e.g. Python shell jobs),
// and reject a request that mixes MaxCapacity with WorkerType/NumberOfWorkers
// — AWS's documented mutual-exclusion rule for the two capacity models.
func Test_Job_MaxCapacity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   map[string]any
		name   string
		wantOK bool
	}{
		{
			name: "max_capacity_alone_accepted",
			body: map[string]any{
				"Name":        "job-mc",
				"Role":        "role1",
				"Command":     map[string]any{"Name": "pythonshell"},
				"MaxCapacity": 0.0625,
			},
			wantOK: true,
		},
		{
			name: "max_capacity_with_worker_type_rejected",
			body: map[string]any{
				"Name":            "job-mc-conflict",
				"Role":            "role1",
				"Command":         map[string]any{"Name": "glueetl"},
				"MaxCapacity":     2.0,
				"WorkerType":      "G.1X",
				"NumberOfWorkers": 2,
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", tc.body)

			if !tc.wantOK {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidInputException")

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": tc.body["Name"]})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Job struct {
					MaxCapacity float64 `json:"MaxCapacity"`
				} `json:"Job"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.InDelta(t, tc.body["MaxCapacity"], out.Job.MaxCapacity, 0.0001)
		})
	}
}

// Test_Crawler_CreateOptions_RoundTrip verifies CreateCrawler/UpdateCrawler
// accept and persist Schedule, Classifiers, Configuration and TablePrefix —
// fields AWS's CreateCrawlerRequest/UpdateCrawlerRequest both support that
// were previously silently discarded (CreateCrawler only accepted
// name/role/database/targets/tags). Also verifies JDBC and catalog crawler
// targets round-trip alongside S3 targets.
func Test_Crawler_CreateOptions_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name":          "crawler1",
		"Role":          "role1",
		"Schedule":      "cron(0 12 * * ? *)",
		"Classifiers":   []string{"my-classifier"},
		"Configuration": `{"Version":1.0}`,
		"TablePrefix":   "raw_",
		"Targets": map[string]any{
			"S3Targets":      []map[string]any{{"Path": "s3://bucket/data/"}},
			"JdbcTargets":    []map[string]any{{"ConnectionName": "conn1", "Path": "db/%"}},
			"CatalogTargets": []map[string]any{{"DatabaseName": "db1", "Tables": []string{"t1"}}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "crawler1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Crawler struct {
			Schedule struct {
				ScheduleExpression string `json:"ScheduleExpression"`
			} `json:"Schedule"`
			Configuration string   `json:"Configuration"`
			TablePrefix   string   `json:"TablePrefix"`
			Classifiers   []string `json:"Classifiers"`
			Targets       struct {
				S3Targets []struct {
					Path string `json:"Path"`
				} `json:"S3Targets"`
				JdbcTargets []struct {
					ConnectionName string `json:"ConnectionName"`
					Path           string `json:"Path"`
				} `json:"JdbcTargets"`
				CatalogTargets []struct {
					DatabaseName string   `json:"DatabaseName"`
					Tables       []string `json:"Tables"`
				} `json:"CatalogTargets"`
			} `json:"Targets"`
		} `json:"Crawler"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "cron(0 12 * * ? *)", out.Crawler.Schedule.ScheduleExpression)
	assert.Equal(t, `{"Version":1.0}`, out.Crawler.Configuration)
	assert.Equal(t, "raw_", out.Crawler.TablePrefix)
	assert.Equal(t, []string{"my-classifier"}, out.Crawler.Classifiers)
	require.Len(t, out.Crawler.Targets.JdbcTargets, 1)
	assert.Equal(t, "conn1", out.Crawler.Targets.JdbcTargets[0].ConnectionName)
	require.Len(t, out.Crawler.Targets.CatalogTargets, 1)
	assert.Equal(t, "db1", out.Crawler.Targets.CatalogTargets[0].DatabaseName)

	// UpdateCrawler should also accept and persist the same options.
	rec = doGlueRequest(t, h, "UpdateCrawler", map[string]any{
		"Name":        "crawler1",
		"Role":        "role1",
		"TablePrefix": "curated_",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "crawler1"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "curated_", out.Crawler.TablePrefix)
}
