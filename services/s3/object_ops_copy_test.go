package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CopyObject_NoSuchSource verifies that CopyObject returns 404
// when the source key does not exist.
func TestHandler_CopyObject_NoSuchSource(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "copy-src")
	mustCreateBucket(t, backend, "copy-dst")

	req := httptest.NewRequest(http.MethodPut, "/copy-dst/dest-key", nil)
	req.Header.Set("X-Amz-Copy-Source", "copy-src/nonexistent-key")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCopyObject_SourceVersionIDHeader verifies that CopyObject returns the
// x-amz-copy-source-version-id response header exactly when the source object
// has a real (non-null) version ID.
func TestCopyObject_SourceVersionIDHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		srcBucket       string
		dstBucket       string
		enableVersioned bool
		wantHeaderSet   bool
	}{
		{
			name:            "versioned_source_returns_header",
			srcBucket:       "b2-copy-src",
			dstBucket:       "b2-copy-dst",
			enableVersioned: true,
			wantHeaderSet:   true,
		},
		{
			name:            "unversioned_source_omits_header",
			srcBucket:       "b2-copy-novsr-src",
			dstBucket:       "b2-copy-novsr-dst",
			enableVersioned: false,
			wantHeaderSet:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.srcBucket)
			mustCreateBucket(t, backend, tt.dstBucket)

			if tt.enableVersioned {
				enableVersioning(t, handler, tt.srcBucket)
			}

			mustPutObject(t, backend, tt.srcBucket, "srckey", []byte("hello"))

			req := httptest.NewRequest(http.MethodPut, "/"+tt.dstBucket+"/dstkey", nil)
			req.Header.Set("X-Amz-Copy-Source", "/"+tt.srcBucket+"/srckey")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			srcVersionID := rec.Header().Get("X-Amz-Copy-Source-Version-Id")
			if tt.wantHeaderSet {
				assert.NotEmpty(t, srcVersionID,
					"CopyObject must return x-amz-copy-source-version-id when source has a version ID")
			} else {
				assert.Empty(t, srcVersionID,
					"CopyObject must NOT return x-amz-copy-source-version-id for null-versioned objects")
			}
		})
	}
}

// TestCopyObject_IfModifiedSince_NotModified_Returns412 verifies that a
// CopyObject request with a copy-source-if-modified-since header set in the
// future (i.e. the source was not modified after that time) is rejected with
// 412 Precondition Failed.
func TestCopyObject_IfModifiedSince_NotModified_Returns412(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "csm-src")
	mustCreateBucket(t, backend, "csm-dst")
	mustPutObject(t, backend, "csm-src", "obj", []byte("source"))

	// If-Modified-Since set far in the future → object was NOT modified after → 412.
	rec := doRequest(handler, http.MethodPut, "/csm-dst/dst", nil,
		map[string]string{
			"X-Amz-Copy-Source":                   "csm-src/obj",
			"X-Amz-Copy-Source-If-Modified-Since": "Thu, 01 Jan 2099 00:00:00 GMT",
		})

	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
}

// TestCopyObject_MetadataReplace_FormURLEncodedContentType verifies that a
// REPLACE metadata directive with an application/x-www-form-urlencoded
// content-type falls back to preserving the source content-type.
func TestCopyObject_MetadataReplace_FormURLEncodedContentType(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ct-src")
	mustCreateBucket(t, backend, "ct-dst")
	mustPutObject(t, backend, "ct-src", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodPut, "/ct-dst/dst", nil,
		map[string]string{
			"X-Amz-Copy-Source":        "ct-src/obj",
			"X-Amz-Metadata-Directive": "REPLACE",
			"Content-Type":             "application/x-www-form-urlencoded",
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}
