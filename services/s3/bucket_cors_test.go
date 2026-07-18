package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3BucketCORSCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "cors-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	corsXML := `<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>` +
		`<AllowedOrigin>https://example.com</AllowedOrigin></CORSRule></CORSConfiguration>`

	// PutBucketCORS
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(corsXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBucketCORS
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?cors", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AllowedOrigin")

	// OPTIONS preflight (CORS configured)
	req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))

	// DeleteBucketCORS
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?cors", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// OPTIONS preflight after delete → 403
	req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// TestS3CORSPreflightRuleEnforcement verifies that preflight requests are
// evaluated against the configured CORS rules and rejected when no rule matches.

func TestS3CORSPreflightRuleEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		corsXML       string
		origin        string
		method        string
		reqHeaders    string
		wantAllowOrig string
		wantCode      int
		wantForbidden bool
	}{
		{
			name: "matching origin and method allowed",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>PUT</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "PUT",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "wildcard origin matches any origin",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>*</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://any-site.example",
			method:        "GET",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://any-site.example",
		},
		{
			name: "non-matching origin rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://evil.example",
			method:        "GET",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "non-matching method rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "DELETE",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "wildcard allowed headers passes any header",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>*</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "Content-Type, X-Custom-Header",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "specific allowed header matches",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>Content-Type</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "Content-Type",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "disallowed request header rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>Content-Type</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "X-Forbidden-Header",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "second matching rule used when first does not match",
			corsXML: `<CORSConfiguration>` +
				`<CORSRule>` +
				`<AllowedOrigin>https://other.example</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule>` +
				`<CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>PUT</AllowedMethod>` +
				`</CORSRule>` +
				`</CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "PUT",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "MaxAgeSeconds reflected in response",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`<MaxAgeSeconds>600</MaxAgeSeconds>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "GET",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "empty Origin rejected even with wildcard rule",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>*</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "",
			method:        "GET",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "empty Method rejected even with matching origin",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, sdkClient := newTestHandler(t)
			bucket := "cors-enforce-bucket"

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			// Put CORS config
			req := httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?cors",
				strings.NewReader(tt.corsXML),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			// Send OPTIONS preflight
			req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
			req.Header.Set("Origin", tt.origin)
			req.Header.Set("Access-Control-Request-Method", tt.method)

			if tt.reqHeaders != "" {
				req.Header.Set("Access-Control-Request-Headers", tt.reqHeaders)
			}

			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			if !tt.wantForbidden {
				assert.Equal(t, tt.wantAllowOrig, rec.Header().Get("Access-Control-Allow-Origin"))
				assert.Equal(t, tt.method, rec.Header().Get("Access-Control-Allow-Methods"))
			}
		})
	}
}

// TestS3BucketLifecycleCRUD verifies put/get/delete lifecycle configuration.

func TestS3BucketCORSValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		corsXML     string
		wantContain string
		wantCode    int
	}{
		{
			name: "valid CORS XML accepted",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			wantCode: http.StatusOK,
		},
		{
			name:        "malformed CORS XML rejected with 400",
			corsXML:     `<CORSConfiguration><NotClosed>`,
			wantCode:    http.StatusBadRequest,
			wantContain: "MalformedXML",
		},
		{
			name:        "completely invalid CORS XML rejected with 400",
			corsXML:     `this is not xml at all`,
			wantCode:    http.StatusBadRequest,
			wantContain: "MalformedXML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, sdkClient := newTestHandler(t)
			bucket := "cors-validation-bucket"

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?cors",
				strings.NewReader(tt.corsXML),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// TestS3BucketLifecycleCRUD verifies put/get/delete lifecycle configuration.
