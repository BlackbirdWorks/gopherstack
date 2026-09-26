package timestreamwrite_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
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
	assert.Equal(t, "http://example.com", ep["Address"],
		"Address must echo the request's own Host so mandatory endpoint discovery routes back here")
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

// TestHandler_DescribeEndpoints_EchoesRequestHost verifies the address tracks
// the actual incoming Host, not a hardcoded value -- required for
// aws-sdk-go-v2's mandatory endpoint-discovery middleware to route
// subsequent calls (CreateDatabase, WriteRecords, ...) back to this server.
func TestHandler_DescribeEndpoints_EchoesRequestHost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{}")))
	req.Host = "127.0.0.1:54321"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Timestream_20181101.DescribeEndpoints")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints := resp["Endpoints"].([]any)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.Equal(t, "http://127.0.0.1:54321", ep["Address"])
}
