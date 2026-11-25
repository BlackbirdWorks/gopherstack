package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalHeaders_SecondPrecisionBoundary(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "boundary-bucket")
	mustPutObject(t, backend, "boundary-bucket", "obj", []byte("hello"))

	headRec := httptest.NewRecorder()
	serveS3Handler(handler, headRec, httptest.NewRequest(http.MethodHead, "/boundary-bucket/obj", nil))
	require.Equal(t, http.StatusOK, headRec.Code)

	lm := headRec.Header().Get("Last-Modified")
	require.NotEmpty(t, lm)

	lmTime, err := http.ParseTime(lm)
	require.NoError(t, err)

	before := lmTime.Add(-time.Second).Format(http.TimeFormat)

	tests := []struct {
		name       string
		method     string
		header     string
		value      string
		wantStatus int
	}{
		{"get ims equal", http.MethodGet, "If-Modified-Since", lm, http.StatusNotModified},
		{"head ims equal", http.MethodHead, "If-Modified-Since", lm, http.StatusNotModified},
		{"get ims earlier", http.MethodGet, "If-Modified-Since", before, http.StatusOK},
		{"get ius equal", http.MethodGet, "If-Unmodified-Since", lm, http.StatusOK},
		{"head ius equal", http.MethodHead, "If-Unmodified-Since", lm, http.StatusOK},
		{"get ius earlier", http.MethodGet, "If-Unmodified-Since", before, http.StatusPreconditionFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, "/boundary-bucket/obj", nil)
			req.Header.Set(tt.header, tt.value)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
