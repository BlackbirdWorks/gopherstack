package glue_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionOps_CatalogID(t *testing.T) {
	t.Parallel()

	const other = "999999999999"

	partInput := map[string]any{"Values": []string{"p"}, "StorageDescriptor": map[string]any{"Location": "s3://b/p"}}

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name: "create_partition", action: "CreatePartition",
			body: map[string]any{"PartitionInput": partInput},
		},
		{
			name: "batch_create_partition", action: "BatchCreatePartition",
			body: map[string]any{"PartitionInputList": []any{partInput}},
		},
		{
			name: "batch_update_partition", action: "BatchUpdatePartition",
			body: map[string]any{"Entries": []any{map[string]any{
				"PartitionValueList": []string{"p"}, "PartitionInput": partInput,
			}}},
		},
		{
			name: "create_partition_index", action: "CreatePartitionIndex",
			body: map[string]any{"PartitionIndex": map[string]any{"IndexName": "i", "Keys": []string{"k"}}},
		},
		{name: "delete_partition_index", action: "DeletePartitionIndex", body: map[string]any{"IndexName": "i"}},
		{name: "get_partition_indexes", action: "GetPartitionIndexes", body: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "db", "tbl")

			body := map[string]any{"DatabaseName": "db", "TableName": "tbl", "CatalogId": other}
			maps.Copy(body, tt.body)

			rec := doGlueRequest(t, h, tt.action, body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "EntityNotFoundException")
		})
	}
}
