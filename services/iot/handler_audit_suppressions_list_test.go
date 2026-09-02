package iot_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAuditSuppressions_FilterOrderAndPagination proves
// ListAuditSuppressions honors CheckName, ResourceIdentifier, AscendingOrder
// (JSON-body-bound: iot@v1.77.4 serializers.go
// awsRestjson1_serializeOpDocumentListAuditSuppressionsInput), and
// MaxResults/NextToken -- previously the handler read no request fields at
// all and always returned every suppression.
func TestListAuditSuppressions_FilterOrderAndPagination(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	require.NoError(t, b.CreateAuditSuppression(
		"CHECK_A", map[string]any{"account": "111111111111"}, "", false, 300,
	))
	require.NoError(t, b.CreateAuditSuppression(
		"CHECK_A", map[string]any{"account": "222222222222"}, "", false, 100,
	))
	require.NoError(t, b.CreateAuditSuppression(
		"CHECK_B", map[string]any{"account": "333333333333"}, "", false, 200,
	))

	t.Run("checkName filter", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/audit/suppressions/list",
			map[string]any{"checkName": "CHECK_B"}, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		suppressions, _ := out["suppressions"].([]any)
		require.Len(t, suppressions, 1)
	})

	t.Run("resourceIdentifier filter", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/audit/suppressions/list",
			map[string]any{"resourceIdentifier": map[string]any{"account": "222222222222"}}, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		suppressions, _ := out["suppressions"].([]any)
		require.Len(t, suppressions, 1)
	})

	t.Run("ascending order default when omitted", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/audit/suppressions/list", map[string]any{}, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		suppressions, _ := out["suppressions"].([]any)
		require.Len(t, suppressions, 3)

		expirations := suppressionExpirations(t, suppressions)
		assert.Equal(t, []float64{100, 200, 300}, expirations)
	})

	t.Run("explicit descending order", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/audit/suppressions/list",
			map[string]any{"ascendingOrder": false}, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		suppressions, _ := out["suppressions"].([]any)
		require.Len(t, suppressions, 3)

		expirations := suppressionExpirations(t, suppressions)
		assert.Equal(t, []float64{300, 200, 100}, expirations)
	})

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/audit/suppressions/list",
			map[string]any{"maxResults": 2}, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		suppressions, _ := out["suppressions"].([]any)
		require.Len(t, suppressions, 2)
		assert.NotEmpty(t, out["nextToken"])
	})
}

func suppressionExpirations(t *testing.T, suppressions []any) []float64 {
	t.Helper()

	out := make([]float64, len(suppressions))

	for i, s := range suppressions {
		entry, ok := s.(map[string]any)
		require.True(t, ok)
		out[i], ok = entry["expirationDate"].(float64)
		require.True(t, ok)
	}

	return out
}
