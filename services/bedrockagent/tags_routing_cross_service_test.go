package bedrockagent_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

// newTestBedrockAgentFISRegistryServer wires up bedrockagent's real Handler
// alongside FIS, both of which claim the shared "/tags/" prefix. bedrockagent
// registers first (higher MatchPriority) so a routing bug in its RouteMatcher
// would silently swallow FIS's tag requests.
func newTestBedrockAgentFISRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		bedrockagent.NewHandler(bedrockagent.NewInMemoryBackend("us-east-1", "000000000000")),
	))
	require.NoError(t, registry.Register(
		fis.NewHandler(fis.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_FISNotShadowedByBedrockAgent reproduces gopherstack-0y8bi:
// bedrockagent's RouteMatcher fell through to an unconditional HasPrefix(path,
// "/tags/") whenever ExtractServiceFromRequest couldn't identify a signing
// service (empty scope) -- the exact case for an unsigned request, like the
// raw http.Client calls FIS's own integration test makes. Any /tags/{arn}
// request with no recognized signing scope was swallowed by bedrockagent
// regardless of the ARN's owning service. Asserts an unsigned POST to FIS's
// own tag endpoint still reaches FIS.
func TestTagsRouting_FISNotShadowedByBedrockAgent(t *testing.T) {
	t.Parallel()

	srv := newTestBedrockAgentFISRegistryServer(t)

	unknownARN := "arn:aws:fis:us-east-1:000000000000:experiment-template/EXTdoesnotexist00000000"
	body := bytes.NewReader([]byte(`{"tags":{"key":"value"}}`))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/tags/"+unknownARN, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(
		t, http.StatusNotFound, resp.StatusCode,
		"must be FIS's own not-found response, not bedrockagent swallowing the path",
	)
}
