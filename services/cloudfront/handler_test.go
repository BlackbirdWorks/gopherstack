package cloudfront_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponseHasCFIDHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cf_id_header_present"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-hdr", "test", true))
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)
			assert.NotEmpty(t, rec.Header().Get("X-Amz-Cf-Id"), tc.name)
		})
	}
}

// TestCFHandlerStringManipulations exercises path parsing paths with line-coverage gaps.
func TestCFHandlerStringManipulations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown_resource_type",
			method:     http.MethodGet,
			path:       "/2020-05-31/totally-unknown-resource",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "post_to_realtime_log_config_root",
			method:     http.MethodPost,
			path:       "/2020-05-31/realtime-log-config",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var body []byte
			if strings.Contains(tt.path, "realtime-log-config") {
				body = []byte(
					`<RealtimeLogConfig><Name>test-rlc</Name><SamplingRate>100</SamplingRate></RealtimeLogConfig>`,
				)
			}

			rec := doXML(t, h, tt.method, tt.path, body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandlerName verifies the handler name and service metadata.
func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "CloudFront", h.Name())
	assert.Equal(t, "cloudfront", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher verifies RouteMatcher and MatchPriority.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()

	tests := []struct {
		name    string
		path    string
		wantHit bool
	}{
		{name: "matches_prefix", path: "/2020-05-31/distribution", wantHit: true},
		{name: "matches_prefix_subpath", path: "/2020-05-31/origin-access-identity/cloudfront", wantHit: true},
		{name: "no_match", path: "/api/other", wantHit: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantHit, h.RouteMatcher()(c))
		})
	}

	assert.Positive(t, h.MatchPriority())
}

// TestXMLResponseFormat verifies XML content-type and structure.
func TestXMLResponseFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "xml")

	// Verify the response is valid XML.
	var v any
	err := xml.Unmarshal(rec.Body.Bytes(), &v)
	require.NoError(t, err)
}

// TestHandlerReset verifies Reset() clears all backend state.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend

	_, err := b.CreateDistribution("ref-r1", "reset-dist", true, nil)
	require.NoError(t, err)

	_, err = b.CreateOAI("ref-oai-r1", "reset-oai")
	require.NoError(t, err)

	h.Reset()

	dists := b.ListDistributions()
	assert.Empty(t, dists)

	oais := b.ListOAIs()
	assert.Empty(t, oais)
}
