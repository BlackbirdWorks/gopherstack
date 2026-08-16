package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestListTableStorageOptimizers_TypeFilter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Add compaction + retention optimizers
	postJSON(t, h, "/UpdateTableStorageOptimizer", map[string]any{
		"CatalogId":    "123",
		"DatabaseName": "db",
		"TableName":    "tbl",
		"StorageOptimizerConfig": map[string]any{
			"COMPACTION": map[string]any{"enabled": "true"},
			"RETENTION":  map[string]any{"enabled": "true"},
		},
	})

	// Filter for COMPACTION only
	rec := postJSON(t, h, "/ListTableStorageOptimizers", map[string]any{
		"CatalogId":            "123",
		"DatabaseName":         "db",
		"TableName":            "tbl",
		"StorageOptimizerType": "COMPACTION",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	opts := out["StorageOptimizerList"].([]any)
	assert.Len(t, opts, 1)
	assert.Equal(t, "COMPACTION", opts[0].(map[string]any)["StorageOptimizerType"])
}

// --- GetDataCellsFilter ---

func TestGetTableObjects_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetTableObjects", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.Nil(t, out["Objects"])
}
