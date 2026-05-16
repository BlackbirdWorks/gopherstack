package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// createTestDB is a helper that creates a database and a table for partition tests.
func createTestDB(t *testing.T, h *glue.Handler, dbName, tableName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": dbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": dbName,
		"TableInput":   map[string]any{"Name": tableName},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// createTestPartition creates a single partition via the API.
func createTestPartition(t *testing.T, h *glue.Handler, dbName, tableName string, values []string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName": dbName,
		"TableName":    tableName,
		"PartitionInput": map[string]any{
			"Values": values,
			"StorageDescriptor": map[string]any{
				"Location": "s3://bucket/" + tableName,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAccuracy_GetPartition_Found(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db1", "tbl1")
	createTestPartition(t, h, "db1", "tbl1", []string{"2024", "01"})

	rec := doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "db1",
		"TableName":       "tbl1",
		"PartitionValues": []string{"2024", "01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partition struct {
			Values []string `json:"Values"`
		} `json:"Partition"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, []string{"2024", "01"}, out.Partition.Values)
}

func TestAccuracy_GetPartition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "nodb",
		"TableName":       "notbl",
		"PartitionValues": []string{"x"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_GetPartitions_List(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db2", "tbl2")
	createTestPartition(t, h, "db2", "tbl2", []string{"2024"})
	createTestPartition(t, h, "db2", "tbl2", []string{"2025"})

	rec := doGlueRequest(t, h, "GetPartitions", map[string]any{
		"DatabaseName": "db2",
		"TableName":    "tbl2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partitions []struct {
			Values []string `json:"Values"`
		} `json:"Partitions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Partitions, 2)
}

func TestAccuracy_GetPartitions_TableNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetPartitions", map[string]any{
		"DatabaseName": "nodb",
		"TableName":    "notbl",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_UpdatePartition_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db3", "tbl3")
	createTestPartition(t, h, "db3", "tbl3", []string{"v1"})

	rec := doGlueRequest(t, h, "UpdatePartition", map[string]any{
		"DatabaseName":       "db3",
		"TableName":          "tbl3",
		"PartitionValueList": []string{"v1"},
		"PartitionInput": map[string]any{
			"Values": []string{"v1"},
			"StorageDescriptor": map[string]any{
				"Location": "s3://updated-bucket/tbl3",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetPartition", map[string]any{
		"DatabaseName":    "db3",
		"TableName":       "tbl3",
		"PartitionValues": []string{"v1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Partition struct {
			StorageDescriptor struct {
				Location string `json:"Location"`
			} `json:"StorageDescriptor"`
		} `json:"Partition"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "s3://updated-bucket/tbl3", out.Partition.StorageDescriptor.Location)
}

func TestAccuracy_UpdatePartition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdatePartition", map[string]any{
		"DatabaseName":       "nodb",
		"TableName":          "notbl",
		"PartitionValueList": []string{"x"},
		"PartitionInput":     map[string]any{"Values": []string{"x"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_BatchUpdatePartition_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "db4", "tbl4")
	createTestPartition(t, h, "db4", "tbl4", []string{"p1"})
	createTestPartition(t, h, "db4", "tbl4", []string{"p2"})

	rec := doGlueRequest(t, h, "BatchUpdatePartition", map[string]any{
		"DatabaseName": "db4",
		"TableName":    "tbl4",
		"Entries": []map[string]any{
			{
				"PartitionValueList": []string{"p1"},
				"PartitionInput": map[string]any{
					"Values": []string{"p1"},
					"StorageDescriptor": map[string]any{
						"Location": "s3://new/p1",
					},
				},
			},
			{
				"PartitionValueList": []string{"p2"},
				"PartitionInput": map[string]any{
					"Values": []string{"p2"},
					"StorageDescriptor": map[string]any{
						"Location": "s3://new/p2",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Errors []any `json:"Errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.Errors)
}

func TestAccuracy_GetCustomEntityType_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{
		"Name": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_GetCustomEntityType_Found(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.AddCustomEntityTypeInternal(&glue.CustomEntityType{
		Name:        "MY_TYPE",
		RegexString: `\d+`,
	})
	h := glue.NewHandler(b)

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{
		"Name": "MY_TYPE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Name        string `json:"Name"`
		RegexString string `json:"RegexString"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "MY_TYPE", out.Name)
	assert.Equal(t, `\d+`, out.RegexString)
}

func TestAccuracy_GetDataQualityResult_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityResult", map[string]any{
		"ResultId": "nonexistent-id",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_GetDataQualityResult_Found(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDataQualityResultInternal(&glue.DataQualityResult{
		ResultID: "result-abc",
		Score:    0.95,
	})
	h := glue.NewHandler(b)

	rec := doGlueRequest(t, h, "GetDataQualityResult", map[string]any{
		"ResultId": "result-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultID string  `json:"ResultId"`
		Score    float64 `json:"Score"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "result-abc", out.ResultID)
	assert.InDelta(t, 0.95, out.Score, 0.001)
}

func TestAccuracy_SearchTables_EmptyFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "searchdb", "alpha")

	rec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "searchdb",
		"TableInput":   map[string]any{"Name": "beta"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "SearchTables", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.TableList, 2)
}

func TestAccuracy_SearchTables_WithFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDB(t, h, "filterdb", "orders")

	rec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "filterdb",
		"TableInput":   map[string]any{"Name": "customers"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "SearchTables", map[string]any{
		"SearchText": "order",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableList []struct {
			Name string `json:"Name"`
		} `json:"TableList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.TableList, 1)
	assert.Equal(t, "orders", out.TableList[0].Name)
}

func TestAccuracy_UpdateConnection_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "my-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "my-conn",
		"ConnectionInput": map[string]any{
			"Name":           "my-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://newhost:3306/db",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetConnection", map[string]any{
		"Name": "my-conn",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Connection struct {
			ConnectionProperties map[string]string `json:"ConnectionProperties"`
		} `json:"Connection"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "jdbc:mysql://newhost:3306/db", out.Connection.ConnectionProperties["JDBC_CONNECTION_URL"])
}

func TestAccuracy_UpdateConnection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "nonexistent",
		"ConnectionInput": map[string]any{
			"Name":           "nonexistent",
			"ConnectionType": "JDBC",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateCrawler_WithoutDatabase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "my-crawler",
		"Role": "arn:aws:iam::000000000000:role/GlueCrawlerRole",
		"Targets": map[string]any{
			"S3Targets": []map[string]any{
				{"Path": "s3://my-bucket/data/"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
