package glue_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchGetPartition_TableValidation verifies that BatchGetPartition mirrors
// the AWS Glue contract: missing required identifiers and unknown tables both
// produce client errors instead of an empty success.
func TestBatchGetPartition_TableValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		setupTable bool
		wantOK     bool
	}{
		{
			name:       "missing_database_and_table",
			body:       map[string]any{},
			setupTable: false,
			wantOK:     false,
		},
		{
			name:       "unknown_table_returns_client_error",
			body:       map[string]any{"DatabaseName": "db1", "TableName": "missing"},
			setupTable: false,
			wantOK:     false,
		},
		{
			name:       "existing_table_returns_empty_partitions",
			body:       map[string]any{"DatabaseName": "db1", "TableName": "tbl1"},
			setupTable: true,
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setupTable {
				rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
					"DatabaseInput": map[string]any{"Name": "db1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": "db1",
					"TableInput":   map[string]any{"Name": "tbl1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "BatchGetPartition", tt.body)
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				assert.NotEqual(t, http.StatusOK, rec.Code)
				assert.NotEqual(t, http.StatusInternalServerError, rec.Code)
			}
		})
	}
}
