package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCORSActualResponseHeaders is a regression test for gopherstack-ozl0:
// only the OPTIONS preflight response carried Access-Control-Allow-* headers,
// so a browser that passed preflight then blocked the real GET/PUT that
// followed it -- CORS didn't work end to end even with a correct config.
func TestCORSActualResponseHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		corsXML       string
		reqMethod     string
		wantAllowOrig string
		wantExpose    string
		putCORS       bool
	}{
		{
			name: "get response carries allow origin when rule matches",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			putCORS:       true,
			reqMethod:     http.MethodGet,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "response omits allow origin when method not covered by rule",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			putCORS:       true,
			reqMethod:     http.MethodHead,
			wantAllowOrig: "",
		},
		{
			name:          "response omits allow origin when no cors configured",
			putCORS:       false,
			reqMethod:     http.MethodGet,
			wantAllowOrig: "",
		},
		{
			name: "expose headers reflected on actual response when rule declares them",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`<ExposeHeader>ETag</ExposeHeader>` +
				`</CORSRule></CORSConfiguration>`,
			putCORS:       true,
			reqMethod:     http.MethodGet,
			wantAllowOrig: "https://example.com",
			wantExpose:    "ETag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			bucket := "cors-actual-bucket"
			mustCreateBucket(t, backend, bucket)
			mustPutObject(t, backend, bucket, "obj", []byte("body"))

			if tt.putCORS {
				req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(tt.corsXML))
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			req := httptest.NewRequest(tt.reqMethod, "/"+bucket+"/obj", nil)
			req.Header.Set("Origin", "https://example.com")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantAllowOrig, rec.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tt.wantExpose, rec.Header().Get("Access-Control-Expose-Headers"))
		})
	}
}

// TestCORSWildcardOriginMatching is a regression test for gopherstack-ozl0:
// AllowedOrigin only matched a literal origin or a bare "*", not AWS's
// single-embedded-wildcard form (e.g. "https://*.example.com"). Each case
// proves both directions -- an origin the wildcard should admit is admitted,
// and an origin it should not (including a malformed multi-wildcard rule)
// still isn't, so the fix doesn't loosen matching beyond the documented form.
func TestCORSWildcardOriginMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		allowedOrigin string
		origin        string
		wantMatch     bool
	}{
		{
			name:          "single embedded wildcard matches subdomain",
			allowedOrigin: "https://*.example.com",
			origin:        "https://foo.example.com",
			wantMatch:     true,
		},
		{
			name:          "single embedded wildcard rejects unrelated domain",
			allowedOrigin: "https://*.example.com",
			origin:        "https://evil.com",
			wantMatch:     false,
		},
		{
			name:          "single embedded wildcard rejects domain missing separator",
			allowedOrigin: "https://*.example.com",
			origin:        "https://notexample.com",
			wantMatch:     false,
		},
		{
			name:          "multiple wildcards fail closed rather than over match",
			allowedOrigin: "https://*.*.example.com",
			origin:        "https://a.b.example.com",
			wantMatch:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			bucket := "cors-wildcard-bucket"
			mustCreateBucket(t, backend, bucket)

			corsXML := `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>` + tt.allowedOrigin + `</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(corsXML))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
			req.Header.Set("Origin", tt.origin)
			req.Header.Set("Access-Control-Request-Method", "GET")
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			if tt.wantMatch {
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, tt.origin, rec.Header().Get("Access-Control-Allow-Origin"))

				return
			}

			require.Equal(t, http.StatusForbidden, rec.Code)
		})
	}
}
