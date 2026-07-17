package directoryservice_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListIpRoutes_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("paginate through IP routes", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		routes := make([]any, 5)
		for i := range 5 {
			routes[i] = map[string]any{"CidrIp": fmt.Sprintf("10.%d.0.0/24", i), "Description": "r"}
		}
		doRequest(t, h, "AddIpRoutes", map[string]any{"DirectoryId": dirID, "IpRoutes": routes})

		rec := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID, "Limit": 2})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["IpRoutesInfo"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		rec2 := doRequest(t, h, "ListIpRoutes", map[string]any{
			"DirectoryId": dirID, "Limit": 3, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["IpRoutesInfo"].([]any)
		assert.Len(t, page2, 3)
		_, hasMore := body2["NextToken"]
		assert.False(t, hasMore)
	})
}

func TestIpRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "add list remove cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Add
			rec1 := doRequest(t, h, "AddIpRoutes", map[string]any{
				"DirectoryId": dirID,
				"IpRoutes":    []any{map[string]any{"CidrIp": "10.0.0.0/24", "Description": "test"}},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// List
			rec2 := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			routes, _ := r2["IpRoutesInfo"].([]any)
			assert.Len(t, routes, 1)

			// Remove
			rec3 := doRequest(t, h, "RemoveIpRoutes", map[string]any{
				"DirectoryId": dirID,
				"CidrIps":     []any{"10.0.0.0/24"},
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// List after remove
			rec4 := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			routes2, _ := r4["IpRoutesInfo"].([]any)
			assert.Empty(t, routes2)

			_ = tc
		})
	}
}
