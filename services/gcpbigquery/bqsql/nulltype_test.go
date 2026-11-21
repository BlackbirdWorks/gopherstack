package bqsql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An untyped NULL or empty ARRAY literal selected as a column is INT64
// (https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#literals).
func TestUntypedNullColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		typ  string
		mode string
	}{
		{"null", "SELECT NULL AS x", "INTEGER", "NULLABLE"},
		{"empty array", "SELECT [] AS x", "INTEGER", "REPEATED"},
		{"typed null", "SELECT CAST(NULL AS STRING) AS x", "STRING", "NULLABLE"},
		{"string", "SELECT 'a' AS x", "STRING", "NULLABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, newCatalog(t), tt.sql)
			require.NoError(t, err)
			require.Len(t, res.Fields, 1)
			assert.Equal(t, tt.typ, res.Fields[0].Type)
			assert.Equal(t, tt.mode, res.Fields[0].Mode)
		})
	}
}
