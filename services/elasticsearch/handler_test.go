package elasticsearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func newTestHandler() *elasticsearch.Handler {
	bk := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	return elasticsearch.NewHandler(bk)
}

func doRequest(t *testing.T, h *elasticsearch.Handler, method, path string, body any) *http.Response {
	t.Helper()

	var reqBody io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		reqBody = bytes.NewReader(b)
	}

	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	return rw.Result()
}

func createDomainAndGetARN(t *testing.T, h *elasticsearch.Handler, domainName string) string {
	t.Helper()

	createResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": domainName,
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)

	return status["ARN"].(string)
}

func newEchoContext(method, path string) *echo.Context {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()

	return e.NewContext(req, rec)
}

// readJSONBody decodes resp's JSON body into a map, closing the body when done.
func readJSONBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))

	return m
}

// createTestDomainName creates a domain and returns its name (discarding the ARN).
func createTestDomainName(t *testing.T, h *elasticsearch.Handler, name string) string {
	t.Helper()
	createDomainAndGetARN(t, h, name)

	return name
}

// createTestPackage creates a TXT-DICTIONARY package and returns its ID.
func createTestPackage(t *testing.T, h *elasticsearch.Handler, name string) string {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages", map[string]any{
		"PackageName":        name,
		"PackageType":        "TXT-DICTIONARY",
		"PackageDescription": "test",
		"PackageSource":      map[string]any{"S3BucketName": "test-bucket", "S3Key": "test-key"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := readJSONBody(t, resp)
	id, _ := body["PackageDetails"].(map[string]any)["PackageID"].(string)
	require.NotEmpty(t, id)

	return id
}

func TestElasticsearchHandler_Metadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.Region = "us-east-1"

	assert.Equal(t, "Elasticsearch", h.Name())
	assert.NotZero(t, h.MatchPriority())
	assert.Equal(t, "es", h.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Len(t, h.GetSupportedOperations(), 51)

	c := newEchoContext(http.MethodGet, "/2015-01-01/es/domain/my-domain")
	assert.Equal(t, "my-domain", h.ExtractResource(c))

	c2 := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.Empty(t, h.ExtractResource(c2))

	matcher := h.RouteMatcher()
	c3 := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.True(t, matcher(c3))

	c4 := newEchoContext(http.MethodGet, "/other/path")
	assert.False(t, matcher(c4))

	echoCtx := newEchoContext(http.MethodGet, "/2015-01-01/es/domain")
	assert.NoError(t, h.Handle(echoCtx))
}

func TestElasticsearchHandler_RouteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPut, "/2015-01-01/es/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestElasticsearchHandler_PostDomainRoute_NotConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/some-domain/other", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestElasticsearchHandler_SupportedOperationsIncludeTags verifies both
// ListTags and RemoveTags are present in the supported operations list.
func TestElasticsearchHandler_SupportedOperationsIncludeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "ListTags")
	assert.Contains(t, ops, "RemoveTags")
	assert.Len(t, ops, 51)
}
