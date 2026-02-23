package s3_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalHeaders(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "cond-bucket")

	// Put a known object
	putOut, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("cond-bucket"),
		Key:    aws.String("obj"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag) // e.g. "\"5d41402abc4b2a76b9719d911017c592\""

	// Get last-modified via HeadObject
	headOut, err := backend.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
		Bucket: aws.String("cond-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	lastMod := headOut.LastModified

	future := lastMod.Add(time.Hour).Format(http.TimeFormat)
	past := lastMod.Add(-time.Hour).Format(http.TimeFormat)

	tests := []struct {
		name           string
		method         string
		headerKey      string
		headerVal      string
		wantStatusCode int
	}{
		{
			name:           "If-None-Match matching ETag returns 304",
			method:         http.MethodGet,
			headerKey:      "If-None-Match",
			headerVal:      etag,
			wantStatusCode: http.StatusNotModified,
		},
		{
			name:           "If-None-Match non-matching ETag returns 200",
			method:         http.MethodGet,
			headerKey:      "If-None-Match",
			headerVal:      "\"different-etag\"",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "If-Modified-Since future date returns 304",
			method:         http.MethodGet,
			headerKey:      "If-Modified-Since",
			headerVal:      future,
			wantStatusCode: http.StatusNotModified,
		},
		{
			name:           "If-Modified-Since past date returns 200",
			method:         http.MethodGet,
			headerKey:      "If-Modified-Since",
			headerVal:      past,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "If-Match matching ETag returns 200",
			method:         http.MethodGet,
			headerKey:      "If-Match",
			headerVal:      etag,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "If-Match non-matching ETag returns 412",
			method:         http.MethodGet,
			headerKey:      "If-Match",
			headerVal:      "\"wrong-etag\"",
			wantStatusCode: http.StatusPreconditionFailed,
		},
		{
			name:           "If-Unmodified-Since future date returns 200",
			method:         http.MethodGet,
			headerKey:      "If-Unmodified-Since",
			headerVal:      future,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "If-Unmodified-Since past date returns 412",
			method:         http.MethodGet,
			headerKey:      "If-Unmodified-Since",
			headerVal:      past,
			wantStatusCode: http.StatusPreconditionFailed,
		},
		// HEAD method conditional checks
		{
			name:           "HEAD If-None-Match matching ETag returns 304",
			method:         http.MethodHead,
			headerKey:      "If-None-Match",
			headerVal:      etag,
			wantStatusCode: http.StatusNotModified,
		},
		{
			name:           "HEAD If-Match non-matching ETag returns 412",
			method:         http.MethodHead,
			headerKey:      "If-Match",
			headerVal:      "\"wrong-etag\"",
			wantStatusCode: http.StatusPreconditionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			url := "/cond-bucket/obj"
			req := httptest.NewRequest(tt.method, url, nil)
			req.Header.Set(tt.headerKey, tt.headerVal)
			w := httptest.NewRecorder()

			serveS3Handler(handler, w, req)

			assert.Equal(t, tt.wantStatusCode, w.Code)
		})
	}
}
