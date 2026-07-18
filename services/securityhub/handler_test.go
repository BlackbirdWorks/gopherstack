package securityhub_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) *securityhub.Handler {
	t.Helper()
	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	return securityhub.NewHandler(b)
}

func doRequest(t *testing.T, h *securityhub.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/securityhub/aws4_request")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// enableHub is a helper that enables SecurityHub on a fresh handler.
func enableHub(t *testing.T, h *securityhub.Handler) {
	t.Helper()
	rec := doRequest(t, h, http.MethodPost, "/accounts", map[string]any{
		"EnableDefaultStandards": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: RouteMatcher must match SecurityHub-specific paths.
func TestRouteMatcherMatchesSecurityHubPaths(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	securityHubPaths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/accounts"},
		{http.MethodGet, "/accounts"},
		{http.MethodDelete, "/accounts"},
		{http.MethodPatch, "/accounts"},
		{http.MethodPost, "/insights"},
		{http.MethodPost, "/insights/get"},
		{http.MethodGet, "/insights/results/arn:aws:securityhub:us-east-1:000000000000:insight/000000000000/1"},
		{http.MethodPost, "/actionTargets"},
		{http.MethodPost, "/actionTargets/get"},
		{http.MethodGet, "/standards"},
		{http.MethodPost, "/standards/register"},
		{http.MethodPost, "/standards/deregister"},
		{http.MethodPost, "/standards/get"},
		{http.MethodGet, "/associations"},
		{http.MethodGet, "/products"},
		{http.MethodGet, "/productSubscriptions"},
		{http.MethodPost, "/productSubscriptions"},
		{http.MethodGet, "/securityControls/definitions"},
		{http.MethodGet, "/automationrules/list"},
		{http.MethodPost, "/automationrules/create"},
		{http.MethodPost, "/automationrules/get"},
		{http.MethodPost, "/automationrules/delete"},
	}

	for _, tt := range securityHubPaths {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.True(t, matcher(c), "must match %s %s", tt.method, tt.path)
		})
	}
}

// Batch-1 accuracy gap: RouteMatcher for /tags/ only matches SecurityHub ARNs.
func TestRouteMatcherTagsOnlyMatchesSecurityHubARNs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	tests := []struct {
		path string
		want bool
	}{
		{"/tags/arn:aws:securityhub:us-east-1:000000000000:hub/default", true},
		{"/tags/arn:aws:securityhub:us-east-1:000000000000:insight/000000000000/1", true},
		{"/tags/arn:aws:macie2:us-east-1:000000000000:allow-list/id", false},
		{"/tags/arn:aws:guardduty:us-east-1:000000000000:detector/id", false},
		{"/tags/arn:aws:s3:::my-bucket", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
