package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetUnfilteredTableMetadata exercises the GetUnfilteredTableMetadata handler.
func TestGetUnfilteredTableMetadata(t *testing.T) {
	t.Parallel()

	t.Run("empty_identifiers_returns_ok_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetUnfilteredTableMetadata", map[string]any{
			"DatabaseName": "",
			"Name":         "",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct{ AuthorizedColumns []string `json:"AuthorizedColumns"` }
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotNil(t, out.AuthorizedColumns)
	})

	t.Run("table_not_found_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetUnfilteredTableMetadata", map[string]any{
			"DatabaseName": "no-db",
			"Name":         "no-tbl",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("existing_table_returns_table_data", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufdb", "uftbl")

		rec := doGlueRequest(t, h, "GetUnfilteredTableMetadata", map[string]any{
			"DatabaseName": "ufdb",
			"Name":         "uftbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Table struct {
				Name string `json:"Name"`
			} `json:"Table"`
			AuthorizedColumns []string `json:"AuthorizedColumns"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "uftbl", out.Table.Name)
		assert.NotNil(t, out.AuthorizedColumns)
	})

	t.Run("not_registered_with_lake_formation", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "lfdb", "lftbl")

		rec := doGlueRequest(t, h, "GetUnfilteredTableMetadata", map[string]any{
			"DatabaseName": "lfdb",
			"Name":         "lftbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			IsRegisteredWithLakeFormation bool `json:"IsRegisteredWithLakeFormation"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.False(t, out.IsRegisteredWithLakeFormation)
	})
}

// TestGetUnfilteredPartitionMetadata exercises the GetUnfilteredPartitionMetadata handler.
func TestGetUnfilteredPartitionMetadata(t *testing.T) {
	t.Parallel()

	t.Run("empty_identifiers_returns_ok_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetUnfilteredPartitionMetadata", map[string]any{
			"DatabaseName":    "",
			"TableName":       "",
			"PartitionValues": []string{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct{ AuthorizedColumns []string `json:"AuthorizedColumns"` }
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotNil(t, out.AuthorizedColumns)
	})

	t.Run("partition_not_found_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufpdb-nf", "ufptbl-nf")

		rec := doGlueRequest(t, h, "GetUnfilteredPartitionMetadata", map[string]any{
			"DatabaseName":    "ufpdb-nf",
			"TableName":       "ufptbl-nf",
			"PartitionValues": []string{"no-such-value"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("existing_partition_returns_partition_data", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufpdb", "ufptbl")
		createTestPartition(t, h, "ufpdb", "ufptbl", []string{"2024", "01"})

		rec := doGlueRequest(t, h, "GetUnfilteredPartitionMetadata", map[string]any{
			"DatabaseName":    "ufpdb",
			"TableName":       "ufptbl",
			"PartitionValues": []string{"2024", "01"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Partition struct {
				Values []string `json:"Values"`
			} `json:"Partition"`
			AuthorizedColumns             []string `json:"AuthorizedColumns"`
			IsRegisteredWithLakeFormation bool     `json:"IsRegisteredWithLakeFormation"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, []string{"2024", "01"}, out.Partition.Values)
		assert.NotNil(t, out.AuthorizedColumns)
		assert.False(t, out.IsRegisteredWithLakeFormation)
	})
}

// TestGetUnfilteredPartitionsMetadata exercises the GetUnfilteredPartitionsMetadata handler.
func TestGetUnfilteredPartitionsMetadata(t *testing.T) {
	t.Parallel()

	t.Run("empty_identifiers_returns_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
			"DatabaseName": "",
			"TableName":    "",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct{ UnfilteredPartitions []any `json:"UnfilteredPartitions"` }
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.UnfilteredPartitions)
	})

	t.Run("table_not_found_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
			"DatabaseName": "no-db",
			"TableName":    "no-tbl",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("table_with_no_partitions_returns_empty_list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufpm-db", "ufpm-tbl")

		rec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
			"DatabaseName": "ufpm-db",
			"TableName":    "ufpm-tbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct{ UnfilteredPartitions []any `json:"UnfilteredPartitions"` }
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.UnfilteredPartitions)
	})

	t.Run("table_with_partitions_returns_all", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufp2-db", "ufp2-tbl")
		createTestPartition(t, h, "ufp2-db", "ufp2-tbl", []string{"2024"})
		createTestPartition(t, h, "ufp2-db", "ufp2-tbl", []string{"2025"})

		rec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
			"DatabaseName": "ufp2-db",
			"TableName":    "ufp2-tbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct{ UnfilteredPartitions []any `json:"UnfilteredPartitions"` }
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.UnfilteredPartitions, 2)
	})

	t.Run("partition_entries_contain_authorized_columns", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestDB(t, h, "ufpac-db", "ufpac-tbl")
		createTestPartition(t, h, "ufpac-db", "ufpac-tbl", []string{"2024"})

		rec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
			"DatabaseName": "ufpac-db",
			"TableName":    "ufpac-tbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			UnfilteredPartitions []struct {
				Partition struct {
					Values []string `json:"Values"`
				} `json:"Partition"`
				AuthorizedColumns []string `json:"AuthorizedColumns"`
			} `json:"UnfilteredPartitions"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.Len(t, out.UnfilteredPartitions, 1)
		assert.Equal(t, []string{"2024"}, out.UnfilteredPartitions[0].Partition.Values)
		assert.NotNil(t, out.UnfilteredPartitions[0].AuthorizedColumns)
	})
}

// TestUnfilteredMetadata_IntegrationFlow runs a combined create-query flow.
func TestUnfilteredMetadata_IntegrationFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup: DB + table + partition
	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "integ-uf-db"},
	})
	require.Equal(t, http.StatusOK, dbRec.Code)

	tblRec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "integ-uf-db",
		"TableInput":   map[string]any{"Name": "integ-uf-tbl"},
	})
	require.Equal(t, http.StatusOK, tblRec.Code)

	partRec := doGlueRequest(t, h, "CreatePartition", map[string]any{
		"DatabaseName": "integ-uf-db",
		"TableName":    "integ-uf-tbl",
		"PartitionInput": map[string]any{
			"Values": []string{"2024", "06"},
		},
	})
	require.Equal(t, http.StatusOK, partRec.Code)

	// GetUnfilteredTableMetadata returns the table.
	tblMetaRec := doGlueRequest(t, h, "GetUnfilteredTableMetadata", map[string]any{
		"DatabaseName": "integ-uf-db",
		"Name":         "integ-uf-tbl",
	})
	require.Equal(t, http.StatusOK, tblMetaRec.Code)

	var tblMeta struct {
		Table struct{ Name string `json:"Name"` } `json:"Table"`
	}
	require.NoError(t, json.Unmarshal(tblMetaRec.Body.Bytes(), &tblMeta))
	assert.Equal(t, "integ-uf-tbl", tblMeta.Table.Name)

	// GetUnfilteredPartitionsMetadata lists all partitions.
	partsMetaRec := doGlueRequest(t, h, "GetUnfilteredPartitionsMetadata", map[string]any{
		"DatabaseName": "integ-uf-db",
		"TableName":    "integ-uf-tbl",
	})
	require.Equal(t, http.StatusOK, partsMetaRec.Code)

	var partsMeta struct{ UnfilteredPartitions []any `json:"UnfilteredPartitions"` }
	require.NoError(t, json.Unmarshal(partsMetaRec.Body.Bytes(), &partsMeta))
	assert.Len(t, partsMeta.UnfilteredPartitions, 1)

	// GetUnfilteredPartitionMetadata retrieves the specific partition.
	partMetaRec := doGlueRequest(t, h, "GetUnfilteredPartitionMetadata", map[string]any{
		"DatabaseName":    "integ-uf-db",
		"TableName":       "integ-uf-tbl",
		"PartitionValues": []string{"2024", "06"},
	})
	require.Equal(t, http.StatusOK, partMetaRec.Code)

	var partMeta struct {
		Partition struct{ Values []string `json:"Values"` } `json:"Partition"`
	}
	require.NoError(t, json.Unmarshal(partMetaRec.Body.Bytes(), &partMeta))
	assert.Equal(t, []string{"2024", "06"}, partMeta.Partition.Values)
}
