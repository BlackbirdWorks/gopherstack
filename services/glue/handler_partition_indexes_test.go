package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func createPartitionedTable(t *testing.T, h *glue.Handler, dbName, tableName string) {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": dbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": dbName,
		"TableInput": map[string]any{
			"Name": tableName,
			"PartitionKeys": []map[string]any{
				{"Name": "year", "Type": "string"},
				{"Name": "month", "Type": "string"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestPartitionIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		request    map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_index",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "ym", "Keys": []string{"year", "month"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "unknown_partition_key",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "bad", "Keys": []string{"region"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_key",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "events",
				"PartitionIndex": map[string]any{"IndexName": "duplicate", "Keys": []string{"year", "year"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "table_missing",
			request: map[string]any{
				"DatabaseName":   "db",
				"TableName":      "missing",
				"PartitionIndex": map[string]any{"IndexName": "ym", "Keys": []string{"year"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createPartitionedTable(t, h, "db", "events")
			rec := doGlueRequest(t, h, "CreatePartitionIndex", tt.request)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestPartitionIndexLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createPartitionedTable(t, h, "db", "events")

	for _, index := range []map[string]any{
		{"IndexName": "year_idx", "Keys": []string{"year"}},
		{"IndexName": "ym_idx", "Keys": []string{"year", "month"}},
	} {
		rec := doGlueRequest(t, h, "CreatePartitionIndex", map[string]any{
			"DatabaseName": "db", "TableName": "events", "PartitionIndex": index,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doGlueRequest(t, h, "GetPartitionIndexes", map[string]any{
		"DatabaseName": "db", "TableName": "events",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out struct {
		Indexes []*glue.PartitionIndex `json:"PartitionIndexDescriptorList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Indexes, 2)
	assert.Equal(t, "year_idx", out.Indexes[0].IndexName)
	assert.Equal(t, "ACTIVE", out.Indexes[0].IndexStatus)

	rec = doGlueRequest(t, h, "DeletePartitionIndex", map[string]any{
		"DatabaseName": "db", "TableName": "events", "IndexName": "year_idx",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGlueRequest(t, h, "DeletePartitionIndex", map[string]any{
		"DatabaseName": "db", "TableName": "events", "IndexName": "year_idx",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(t, h, "DeleteTable", map[string]any{"DatabaseName": "db", "Name": "events"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doGlueRequest(t, h, "GetPartitionIndexes", map[string]any{
		"DatabaseName": "db", "TableName": "events",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
