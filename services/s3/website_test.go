package s3_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketWebsiteConfiguration(t *testing.T) {
	t.Parallel()

	const websiteXML = `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<IndexDocument><Suffix>index.html</Suffix></IndexDocument>` +
		`<ErrorDocument><Key>error.html</Key></ErrorDocument>` +
		`</WebsiteConfiguration>`

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, backend *s3.InMemoryBackend, bucket string)
		name    string
		bucket  string
		want    string
	}{
		{
			name:   "get returns NoSuchWebsiteConfiguration when not set",
			bucket: "website-test-bucket",
			setup: func(_ *testing.T, _ *s3.InMemoryBackend, _ string) {
				// No website config stored.
			},
			wantErr: s3.ErrNoWebsiteConfig,
		},
		{
			name:   "put then get returns stored config",
			bucket: "website-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketWebsite(t.Context(), bucket, websiteXML)
				require.NoError(t, err)
			},
			want: websiteXML,
		},
		{
			name:   "delete clears the config",
			bucket: "website-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketWebsite(t.Context(), bucket, websiteXML)
				require.NoError(t, err)
				err = backend.DeleteBucketWebsite(t.Context(), bucket)
				require.NoError(t, err)
			},
			wantErr: s3.ErrNoWebsiteConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			tt.setup(t, backend, tt.bucket)

			got, err := backend.GetBucketWebsite(t.Context(), tt.bucket)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPutBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketWebsite(t.Context(), "nonexistent-bucket", "<WebsiteConfiguration/>")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestGetBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketWebsite(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestDeleteBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.DeleteBucketWebsite(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestS3BucketWebsiteCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "website-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	websiteXML := `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<IndexDocument><Suffix>index.html</Suffix></IndexDocument>` +
		`<ErrorDocument><Key>error.html</Key></ErrorDocument>` +
		`</WebsiteConfiguration>`

	// GetBucketWebsite before any config → 404 NoSuchWebsiteConfiguration
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchWebsiteConfiguration")

	// PutBucketWebsite
	req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?website", strings.NewReader(websiteXML))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBucketWebsite returns stored config
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "IndexDocument")
	assert.Contains(t, rec.Body.String(), "index.html")

	// DeleteBucketWebsite
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketWebsite after delete → 404 NoSuchWebsiteConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchWebsiteConfiguration")
}

// TestS3BucketWebsite_MalformedXML verifies that PutBucketWebsite rejects invalid XML.

func TestS3BucketWebsite_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "website-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?website",
		strings.NewReader("not-valid-xml"),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3PublicAccessBlockCRUD verifies put/get/delete public access block configuration.

func TestHandler_ServeWebsite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, backend *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "serves index document for root path",
			bucket: "site-bucket",
			key:    "",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "site-bucket")
				mustPutObject(t, backend, "site-bucket", "index.html", []byte("<html>hello</html>"))

				xmlCfg := `<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>`
				err := backend.PutBucketWebsite(t.Context(), "site-bucket", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantBody:   "hello",
		},
		{
			name:   "returns 404 for missing object with error document",
			bucket: "site-err",
			key:    "notfound.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "site-err")
				mustPutObject(t, backend, "site-err", "error.html", []byte("not found page"))

				xmlCfg := "<WebsiteConfiguration>" +
					"<IndexDocument><Suffix>index.html</Suffix></IndexDocument>" +
					"<ErrorDocument><Key>error.html</Key></ErrorDocument>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "site-err", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "not found page",
		},
		{
			name:   "returns 404 JSON when no website config",
			bucket: "no-cfg-bucket",
			key:    "page.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "no-cfg-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchWebsiteConfiguration",
		},
		{
			name:   "redirect all requests to host",
			bucket: "redirect-bucket",
			key:    "page.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "redirect-bucket")

				xmlCfg := "<WebsiteConfiguration>" +
					"<RedirectAllRequestsTo>" +
					"<HostName>example.com</HostName>" +
					"<Protocol>https</Protocol>" +
					"</RedirectAllRequestsTo>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "redirect-bucket", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusMovedPermanently,
		},
		{
			// A routing rule scoped ONLY to HttpErrorCodeReturnedEquals=404 must not
			// fire for a request that resolves successfully — it is not an
			// unconditional or prefix-based rule.
			name:   "error-code-only routing rule does not redirect an existing object",
			bucket: "err-code-scoped-hit",
			key:    "page.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "err-code-scoped-hit")
				mustPutObject(t, backend, "err-code-scoped-hit", "page.html", []byte("real page"))

				xmlCfg := "<WebsiteConfiguration>" +
					"<IndexDocument><Suffix>index.html</Suffix></IndexDocument>" +
					"<RoutingRules><RoutingRule>" +
					"<Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>" +
					"<Redirect><HostName>fallback.example.com</HostName></Redirect>" +
					"</RoutingRule></RoutingRules>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "err-code-scoped-hit", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantBody:   "real page",
		},
		{
			// The same error-code-only rule must fire once the object genuinely
			// fails to resolve, confirming the post-error phase still works.
			name:   "error-code-only routing rule redirects a missing object",
			bucket: "err-code-scoped-miss",
			key:    "missing.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "err-code-scoped-miss")

				xmlCfg := "<WebsiteConfiguration>" +
					"<IndexDocument><Suffix>index.html</Suffix></IndexDocument>" +
					"<RoutingRules><RoutingRule>" +
					"<Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>" +
					"<Redirect><HostName>fallback.example.com</HostName></Redirect>" +
					"</RoutingRule></RoutingRules>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "err-code-scoped-miss", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			e := echo.New()
			req := httptest.NewRequest(
				http.MethodGet,
				"/_gopherstack/website/"+tt.bucket+"/"+tt.key,
				nil,
			)
			logCtx := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(logCtx)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetPathValues(echo.PathValues{
				{Name: "bucket", Value: tt.bucket},
				{Name: "key", Value: tt.key},
			})

			require.NoError(t, handler.ServeWebsite(c))

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_BucketAnalyticsConfig verifies put/get/list/delete bucket analytics configuration.
