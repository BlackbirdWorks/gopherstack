package cosmosdb_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_AccountRoot covers the database-account root resource ("" or
// "/"): every real Cosmos SDK -- azcosmos included -- issues a GET here
// before it will make a single data-plane call, to discover the account's
// regional endpoints. Omitting this resource makes the service unreachable
// by any unmodified SDK -- see account_ops.go's handleAccountRoot doc
// comment.
func TestHandler_AccountRoot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := decodeBody(t, rec)
	assert.Equal(t, "gopherstack", body["id"])
	assert.NotEmpty(t, body["_rid"])
	assert.Equal(t, false, body["enableMultipleWriteLocations"])

	policy, ok := body["userConsistencyPolicy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Session", policy["defaultConsistencyLevel"])

	for _, key := range []string{"writableLocations", "readableLocations"} {
		locations, listOK := body[key].([]any)
		require.True(t, listOK, key)
		require.Len(t, locations, 1, key)

		loc, locOK := locations[0].(map[string]any)
		require.True(t, locOK, key)
		assert.NotEmpty(t, loc["name"], key)
		assert.NotEmpty(t, loc["databaseAccountEndpoint"], key)
	}
}

// TestHandler_AccountRoot_EndpointEchoesRequestHost is the regression guard
// for the exact bug class that made the whole service unreachable: the
// databaseAccountEndpoint in every location entry MUST be built from the
// REQUEST's own Host header, never from the server's own configured port.
// Under testcontainers (or any other port-mapping/proxy setup), the
// container's configured port is frequently NOT the port a client actually
// connects through -- if this handler advertised its own configured port
// instead of echoing back exactly how the client reached it, an SDK would
// dutifully "discover" a location pointing at a port nothing is listening
// on and redirect every subsequent request there.
func TestHandler_AccountRoot_EndpointEchoesRequestHost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Port = 8081 // the server's own configured port, deliberately different from the Host below

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Host = "localhost:32771" // simulates a testcontainers-mapped host port

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	const wantEndpoint = "http://localhost:32771/"

	for _, key := range []string{"writableLocations", "readableLocations"} {
		locations, ok := body[key].([]any)
		require.True(t, ok, key)
		require.Len(t, locations, 1, key)

		loc, ok := locations[0].(map[string]any)
		require.True(t, ok, key)
		assert.Equal(t, wantEndpoint, loc["databaseAccountEndpoint"], key)
	}
}

func TestHandler_AccountRoot_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/", nil, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
