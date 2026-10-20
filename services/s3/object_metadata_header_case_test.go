package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real S3 returns user metadata headers lowercase; SDKs that read the raw header
// name (boto3) surface the key case as sent, so Go's canonicalised "X-Amz-Meta-K" breaks them.
func TestHandler_GetHeadObject_UserMetadataHeaderIsLowercase(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			put := httptest.NewRequest(http.MethodPut, "/bkt/obj", strings.NewReader("x"))
			put.Header.Set("X-Amz-Meta-Mixed", "v")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, put)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, httptest.NewRequest(method, "/bkt/obj", nil))
			require.Equal(t, http.StatusOK, rec.Code)

			var names []string
			for k := range rec.Header() {
				if strings.EqualFold(k, "x-amz-meta-mixed") {
					names = append(names, k)
				}
			}

			assert.Equal(t, []string{"x-amz-meta-mixed"}, names)
		})
	}
}
