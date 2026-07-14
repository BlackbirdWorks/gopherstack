package cloudfront_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// newTestHandler builds a fresh CloudFront handler backed by a new in-memory backend.
func newTestHandler() *cloudfront.Handler {
	backend := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return cloudfront.NewHandler(backend)
}

// doXML fires an XML-body request through the handler and returns the recorder.
func doXML(
	t *testing.T,
	h *cloudfront.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	return doXMLWithHeaders(t, h, method, path, body, nil)
}

// doXMLWithHeaders fires an XML-body request through the handler with optional extra headers.
func doXMLWithHeaders(
	t *testing.T,
	h *cloudfront.Handler,
	method, path string,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "text/xml")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// minimalDistConfig builds a minimal DistributionConfig XML body.
func minimalDistConfig(callerRef, comment string, enabled bool) []byte {
	tmpl := `<DistributionConfig>` +
		`<CallerReference>%s</CallerReference>` +
		`<Comment>%s</Comment>` +
		`<Enabled>%v</Enabled>` +
		`</DistributionConfig>`

	return fmt.Appendf([]byte(nil), tmpl, callerRef, comment, enabled)
}

// minimalOAIConfig builds a minimal CloudFrontOriginAccessIdentityConfig XML body.
func minimalOAIConfig(callerRef, comment string) []byte {
	tmpl := `<CloudFrontOriginAccessIdentityConfig>` +
		`<CallerReference>%s</CallerReference>` +
		`<Comment>%s</Comment>` +
		`</CloudFrontOriginAccessIdentityConfig>`

	return fmt.Appendf([]byte(nil), tmpl, callerRef, comment)
}

// newCFHandler builds a fresh CloudFront handler backed by a new in-memory backend.
func newCFHandler(t *testing.T) *cloudfront.Handler {
	t.Helper()
	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	return cloudfront.NewHandler(b)
}

// cfRequest fires a request through the handler and returns the recorder.
func cfRequest(t *testing.T, h *cloudfront.Handler, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != "" {
		bodyReader = bytes.NewBufferString(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	e := echo.New()
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/xml")
	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}

// cfOK fires a request through the handler and requires a 2xx response, returning the body.
func cfOK(t *testing.T, h *cloudfront.Handler, method, path, body string) string {
	t.Helper()
	rr := cfRequest(t, h, method, path, body)
	if rr.Code != http.StatusOK && rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("%s %s: want 2xx, got %d body: %s", method, path, rr.Code, rr.Body.String())
	}

	return rr.Body.String()
}

// extractXMLID extracts the <Id>...</Id> value from an XML response body.
func extractXMLID(t *testing.T, xmlStr string) string {
	t.Helper()
	var m struct {
		Inner string `xml:",innerxml"`
	}
	// Simple: look for field between tags
	start := "<Id>"
	end := "</Id>"
	si := strings.Index(xmlStr, start)
	ei := strings.Index(xmlStr, end)
	if si < 0 || ei < 0 {
		return ""
	}
	_ = m

	return xmlStr[si+len(start) : ei]
}

// cfRequestWithHeader fires a request through the handler with optional extra headers.
func cfRequestWithHeader(
	t *testing.T,
	h interface{ Handler() echo.HandlerFunc },
	method, path string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Content-Type", "application/xml")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)

	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}

// doReq fires a request through the handler and returns the recorder.
func doReq(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/xml")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doJSONReq fires a JSON-body request through the handler.
func doJSONReq(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// newAuditBackend creates a fresh backend for testing.
func newAuditBackend() *cloudfront.InMemoryBackend {
	return cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
}

// newTestBackend creates a fresh in-memory backend for testing.
func newTestBackend() *cloudfront.InMemoryBackend {
	return cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

// newB creates a fresh in-memory backend for testing.
func newB(t *testing.T) *cloudfront.InMemoryBackend {
	t.Helper()

	return cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
}

// cfRequestWithBodyHeaders issues an HTTP request with a body and headers and
// returns the response recorder.
func cfRequestWithBodyHeaders(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader io.Reader = bytes.NewReader(nil)
	if body != "" {
		bodyReader = bytes.NewBufferString(body)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/xml")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}
