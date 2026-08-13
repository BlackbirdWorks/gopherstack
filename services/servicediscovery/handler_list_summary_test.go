package servicediscovery_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListServices_SummaryShape locks that ListServices emits the
// real types.ServiceSummary shape instead of reusing GetService's converter
// unscoped (gopherstack-tuh5): NamespaceId is a real member of the full
// types.Service but is not declared on types.ServiceSummary. This
// assertion reads the raw JSON response body rather than going through an
// AWS SDK client, since the SDK deserializer silently drops keys it does
// not recognise and cannot observe an over-wide response.
func TestHandler_ListServices_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	nsID := createNamespaceHelper(t, h, "lss-ns")

	doSDRequest(t, h, "CreateService", map[string]any{"Name": "lss-svc", "NamespaceId": nsID})

	rec := doSDRequest(t, h, "ListServices", map[string]any{})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Services"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item, ok := items[0].(map[string]any)
	require.True(t, ok)

	for _, k := range []string{"Id", "Arn", "Name", "CreateDate", "InstanceCount"} {
		assert.Contains(t, item, k, "expected real ServiceSummary member %q", k)
	}
	assert.NotContains(t, item, "NamespaceId", "leaked Get-only member \"NamespaceId\"")
}
