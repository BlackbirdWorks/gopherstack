package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridAD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create update describe cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// CreateHybridAD
			rec1 := doRequest(t, h, "CreateHybridAD", map[string]any{
				"Name":     "hybrid.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			dirID, _ := r1["DirectoryId"].(string)
			assert.NotEmpty(t, dirID)
			assert.NotEmpty(t, r1["RequestId"])

			// UpdateHybridAD
			rec2 := doRequest(t, h, "UpdateHybridAD", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assert.NotEmpty(t, r2["RequestId"])

			// DescribeHybridADUpdate
			rec3 := doRequest(t, h, "DescribeHybridADUpdate", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			updates, _ := r3["HybridADUpdateInfo"].([]any)
			assert.NotEmpty(t, updates)

			_ = tc
		})
	}
}
