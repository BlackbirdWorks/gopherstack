package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeRecommendations(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// No recommendations before batch start.
	pre := parseJSON(t, doDMS(t, h, "DescribeRecommendations", map[string]any{}))
	assert.Empty(t, pre["Recommendations"])

	// Create a source endpoint (BatchStartRecommendations uses existing endpoints).
	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)

	// Trigger recommendations.
	rec := doDMS(t, h, "BatchStartRecommendations", map[string]any{"Data": []any{}})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now recommendations exist.
	post := parseJSON(t, doDMS(t, h, "DescribeRecommendations", map[string]any{}))
	recs, ok := post["Recommendations"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, recs)
	r0, ok := recs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, r0["DatabaseId"])
	assert.Equal(t, "aurora-mysql", r0["EngineName"])
	assert.Equal(t, "active", r0["Status"])
}

func TestHandler_BatchStartRecommendations(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "BatchStartRecommendations", map[string]any{
		"Data": []any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec)
	entries, ok := resp["ErrorEntries"].([]any)
	require.True(t, ok)
	assert.Empty(t, entries)
}
