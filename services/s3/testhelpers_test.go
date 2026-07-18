package s3_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// serveS3Handler is a test helper that invokes an S3 Echo handler with a raw HTTP request.
func serveS3Handler(handler *s3.S3Handler, w http.ResponseWriter, r *http.Request) {
	ctx := logger.Save(r.Context(), slog.Default())
	r = r.WithContext(ctx)
	e := echo.New()
	c := e.NewContext(r, w)
	_ = handler.Handler()(c)
}

func newTestHandler(t *testing.T) (*s3.S3Handler, *s3.InMemoryBackend) {
	t.Helper()

	// WithSkipMultipartSizeCheck disables the 5 MiB minimum non-last-part size
	// check so existing tests can use small parts without hitting EntityTooSmall.
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})

	return handler, backend
}

func mustCreateBucket(t *testing.T, b s3.StorageBackend, bucket string) {
	t.Helper()

	_, err := b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func mustPutObject(t *testing.T, b s3.StorageBackend, bucket, key string, data []byte) {
	t.Helper()

	_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
}

func enableVersioning(t *testing.T, handler *s3.S3Handler, bucket string) {
	t.Helper()

	xmlBody := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?versioning", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

// doRequest builds and serves an httptest request against handler, returning the recorder.
func doRequest(
	handler *s3.S3Handler,
	method, path string,
	body io.Reader,
	headers map[string]string,
) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	return rec
}
