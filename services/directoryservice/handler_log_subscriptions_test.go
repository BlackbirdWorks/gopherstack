package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListLogSubscriptions_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("all subscriptions returned without pagination", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateLogSubscription", map[string]any{
			"DirectoryId":  dirID,
			"LogGroupName": "/aws/directoryservice/corp",
		})

		rec := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		subs, _ := body["LogSubscriptions"].([]any)
		assert.Len(t, subs, 1)
	})
}

// --- DescribeDirectories response field fidelity ---

func TestLogSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create list delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Create
			rec1 := doRequest(t, h, "CreateLogSubscription", map[string]any{
				"DirectoryId":  dirID,
				"LogGroupName": "/aws/directoryservice/test",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// List
			rec2 := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			subs, _ := r2["LogSubscriptions"].([]any)
			assert.Len(t, subs, 1)

			// Delete
			rec3 := doRequest(t, h, "DeleteLogSubscription", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// List after delete
			rec4 := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			subs2, _ := r4["LogSubscriptions"].([]any)
			assert.Empty(t, subs2)

			_ = tc
		})
	}
}
