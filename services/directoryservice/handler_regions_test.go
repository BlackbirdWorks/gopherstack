package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "add describe remove cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Add
			rec1 := doRequest(t, h, "AddRegion", map[string]any{
				"DirectoryId": dirID,
				"RegionName":  "us-west-2",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeRegions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			regions, _ := r2["RegionsDescription"].([]any)
			assert.Len(t, regions, 1)

			// Remove
			rec3 := doRequest(t, h, "RemoveRegion", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after remove
			rec4 := doRequest(t, h, "DescribeRegions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			regions2, _ := r4["RegionsDescription"].([]any)
			assert.Empty(t, regions2)

			_ = tc
		})
	}
}
