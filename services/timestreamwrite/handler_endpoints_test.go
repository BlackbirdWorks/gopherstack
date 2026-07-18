package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]string{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["Endpoints"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.Equal(t, "localhost", ep["Address"])
}

// TestHandler_DescribeEndpoints_CachePeriodIs1440 verifies the endpoint cache
// period matches the real AWS Timestream value.
func TestHandler_DescribeEndpoints_CachePeriodIs1440(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	endpoints := out["Endpoints"].([]any)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.InDelta(t, float64(1440), ep["CachePeriodInMinutes"], 0,
		"CachePeriodInMinutes must match real AWS value of 1440")
}

// TestHandler_DescribeEndpoints_CachePeriodPositive verifies that the
// endpoint response includes a positive CachePeriodInMinutes per the AWS API.
func TestHandler_DescribeEndpoints_CachePeriodPositive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	endpoints, ok := out["Endpoints"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	cache := ep["CachePeriodInMinutes"].(float64)
	assert.Greater(t, cache, float64(0), "CachePeriodInMinutes should be positive")
	assert.LessOrEqual(t, cache, float64(1440), "CachePeriodInMinutes should not exceed one day")
}
