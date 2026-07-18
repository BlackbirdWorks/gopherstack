package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListTableOptimizerRuns_ReturnsRealHistory verifies ListTableOptimizerRuns
// validates required params and reads real run history seeded by
// CreateTableOptimizer.
func TestListTableOptimizerRuns_ReturnsRealHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db",
		"TableInput":   map[string]any{"Name": "tbl"},
	})

	rec := doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing required params")

	rec = doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{
		"DatabaseName": "db", "TableName": "tbl", "Type": "compaction",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "no optimizer created yet")

	rec = doGlueRequest(t, h, "CreateTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "db",
		"TableName":    "tbl",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "ListTableOptimizerRuns", map[string]any{
		"DatabaseName": "db", "TableName": "tbl", "Type": "compaction",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TableOptimizerRuns []map[string]any `json:"TableOptimizerRuns"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.TableOptimizerRuns, 1)
	assert.Equal(t, "completed", out.TableOptimizerRuns[0]["eventType"])
}

// TestTableOptimizer tests TableOptimizer CRUD.
func TestTableOptimizer(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateTableOptimizer
	dispatchNewOp(t, h, "CreateTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": true,
		},
	})

	// GetTableOptimizer
	out := dispatchNewOp(t, h, "GetTableOptimizer", map[string]any{
		"CatalogId":    "123456789012",
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})
	to := out["TableOptimizer"].(map[string]any)
	if to["Type"] != "compaction" {
		t.Errorf("Type mismatch: %v", to["Type"])
	}
	config := to["Configuration"].(map[string]any)
	if config["Enabled"] != true {
		t.Errorf("Enabled mismatch: %v", config["Enabled"])
	}

	// UpdateTableOptimizer
	dispatchNewOp(t, h, "UpdateTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
		"TableOptimizerConfiguration": map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/GlueRole",
			"Enabled": false,
		},
	})

	// BatchGetTableOptimizer
	out2 := dispatchNewOp(t, h, "BatchGetTableOptimizer", map[string]any{
		"Entries": []any{
			map[string]any{
				"DatabaseName": "mydb",
				"TableName":    "mytable",
				"Type":         "compaction",
			},
		},
	})
	opts, _ := out2["TableOptimizers"].([]any)
	if len(opts) != 1 {
		t.Errorf("expected 1 optimizer, got %d", len(opts))
	}

	// DeleteTableOptimizer
	dispatchNewOp(t, h, "DeleteTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetTableOptimizer", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"Type":         "compaction",
	})
}
