package glue_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPartitions_Segment(t *testing.T) {
	t.Parallel()

	const total = 5

	tests := []struct {
		segment   map[string]any
		name      string
		wantCount int
		wantErr   bool
	}{
		{name: "no_segment", wantCount: total},
		{name: "first_of_two", segment: map[string]any{"SegmentNumber": 0, "TotalSegments": 2}, wantCount: 3},
		{name: "second_of_two", segment: map[string]any{"SegmentNumber": 1, "TotalSegments": 2}, wantCount: 2},
		{name: "number_out_of_range", segment: map[string]any{"SegmentNumber": 2, "TotalSegments": 2}, wantErr: true},
		{name: "negative_number", segment: map[string]any{"SegmentNumber": -1, "TotalSegments": 2}, wantErr: true},
		{name: "missing_total", segment: map[string]any{"SegmentNumber": 0}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "db1", "tbl1")

			for i := range total {
				createTestPartition(t, h, "db1", "tbl1", []string{strconv.Itoa(i)})
			}

			body := map[string]any{"DatabaseName": "db1", "TableName": "tbl1"}
			if tt.segment != nil {
				body["Segment"] = tt.segment
			}

			rec := doGlueRequest(t, h, "GetPartitions", body)
			if tt.wantErr {
				require.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Partitions []map[string]any `json:"Partitions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Partitions, tt.wantCount)
		})
	}
}

func TestGetPartitions_ExcludeColumnSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		exclude     bool
		wantColumns int
	}{
		{name: "default", wantColumns: 1},
		{name: "excluded", exclude: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "db1", "tbl1")

			rec := doGlueRequest(t, h, "CreatePartition", map[string]any{
				"DatabaseName": "db1", "TableName": "tbl1",
				"PartitionInput": map[string]any{
					"Values": []string{"p"},
					"StorageDescriptor": map[string]any{
						"Location": "s3://bucket/p",
						"Columns":  []map[string]any{{"Name": "c", "Type": "string"}},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "GetPartitions", map[string]any{
				"DatabaseName": "db1", "TableName": "tbl1", "ExcludeColumnSchema": tt.exclude,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Partitions []struct {
					StorageDescriptor struct {
						Location string           `json:"Location"`
						Columns  []map[string]any `json:"Columns"`
					} `json:"StorageDescriptor"`
				} `json:"Partitions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.Partitions, 1)
			assert.Len(t, out.Partitions[0].StorageDescriptor.Columns, tt.wantColumns)
			assert.Equal(t, "s3://bucket/p", out.Partitions[0].StorageDescriptor.Location)
		})
	}
}
