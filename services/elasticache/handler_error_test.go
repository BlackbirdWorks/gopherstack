package elasticache_test

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// errorEnvelope mirrors the AWS query-protocol error response so the wire shape
// can be asserted directly.
type errorEnvelope struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Error   struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestID string `xml:"RequestId"`
}

func newErrorTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)
	handler.Region = "us-east-1"

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func postAction(t *testing.T, srvURL string, form url.Values) (int, errorEnvelope) {
	t.Helper()

	form.Set("Version", "2015-02-02")
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		srvURL,
		strings.NewReader(form.Encode()),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var env errorEnvelope
	require.NoError(t, xml.Unmarshal(body, &env), "response body: %s", body)

	return resp.StatusCode, env
}

func TestXMLErrorEnvelopeShape(t *testing.T) {
	t.Parallel()

	srv := newErrorTestServer(t)

	tests := []struct {
		form     url.Values
		name     string
		wantCode string
		wantType string
	}{
		{
			name: "not_found_is_sender",
			form: url.Values{
				"Action":         {"DescribeCacheClusters"},
				"CacheClusterId": {"missing-cluster"},
			},
			wantCode: "CacheClusterNotFound",
			wantType: "Sender",
		},
		{
			name: "delete_missing_cluster_is_sender",
			form: url.Values{
				"Action":         {"DeleteCacheCluster"},
				"CacheClusterId": {"nope"},
			},
			wantCode: "CacheClusterNotFound",
			wantType: "Sender",
		},
		{
			name: "snapshot_not_found_is_sender",
			form: url.Values{
				"Action":       {"DeleteSnapshot"},
				"SnapshotName": {"ghost"},
			},
			wantCode: "SnapshotNotFoundFault",
			wantType: "Sender",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, env := postAction(t, srv.URL, tt.form)

			assert.GreaterOrEqual(t, status, http.StatusBadRequest)
			assert.Equal(t, tt.wantType, env.Error.Type, "fault Type element must be present and classified")
			assert.Equal(t, tt.wantCode, env.Error.Code, "error Code must match")
			assert.NotEmpty(t, env.Error.Message, "error Message must be present")
			assert.NotEmpty(t, env.Xmlns, "ErrorResponse must carry the elasticache namespace")

			// RequestId must be a fresh UUID, not a static stub.
			require.NotEmpty(t, env.RequestID)
			_, parseErr := uuid.Parse(env.RequestID)
			require.NoError(t, parseErr, "RequestId should be a UUID, got %q", env.RequestID)
			assert.NotEqual(t, "elasticache-stub", env.RequestID)
		})
	}
}

func TestXMLErrorRequestIDsAreUnique(t *testing.T) {
	t.Parallel()

	srv := newErrorTestServer(t)

	form := url.Values{"Action": {"DescribeCacheClusters"}, "CacheClusterId": {"missing"}}

	_, first := postAction(t, srv.URL, form)
	_, second := postAction(t, srv.URL, form)

	require.NotEmpty(t, first.RequestID)
	require.NotEmpty(t, second.RequestID)
	assert.NotEqual(t, first.RequestID, second.RequestID, "each error response must carry a distinct RequestId")
}
