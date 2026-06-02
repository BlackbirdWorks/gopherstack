package s3_test

// accuracy_batch2_test.go — AWS-accuracy fixes for S3 batch-2 ops audit (go-ztq9):
//   - GetBucketLocation: empty LocationConstraint for us-east-1 (AWS classic region)
//   - ListObjects V1 NextMarker: only returned when delimiter is specified
//   - CopyObject x-amz-copy-source-version-id response header

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// GetBucketLocation — empty LocationConstraint for us-east-1
// ---------------------------------------------------------------------------

// TestBatch2_GetBucketLocation_USEast1_EmptyConstraint verifies that
// GetBucketLocation returns an empty LocationConstraint element for buckets
// created in us-east-1, matching AWS S3 behaviour.
func TestBatch2_GetBucketLocation_USEast1_EmptyConstraint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		wantEmptyBody bool
	}{
		{
			name:          "default region bucket has empty LocationConstraint",
			bucket:        "b2-loc-default",
			wantEmptyBody: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?location", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "LocationConstraint")

			// For us-east-1, the constraint value must be empty (not "us-east-1").
			if tt.wantEmptyBody {
				assert.NotContains(t, body, "us-east-1",
					"us-east-1 bucket LocationConstraint must be empty, not \"us-east-1\"")
			}
		})
	}
}

// TestBatch2_GetBucketLocation_NonDefaultRegion_HasConstraint verifies that
// buckets created in non-default regions still return their region string.
func TestBatch2_GetBucketLocation_NonDefaultRegion_HasConstraint(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	// Create bucket in eu-west-1 by setting a regional bucket name and creating
	// it via the handler with a LocationConstraint body.
	bucket := "b2-loc-euwest"
	body := `<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/"+bucket, strings.NewReader(body))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	_ = backend // backend available for future assertions

	req2 := httptest.NewRequest(http.MethodGet, "/"+bucket+"?location", nil)
	req2.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/s3/aws4_request")
	rec2 := httptest.NewRecorder()
	serveS3Handler(handler, rec2, req2)

	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "eu-west-1")
}

// ---------------------------------------------------------------------------
// ListObjects V1 — NextMarker only with delimiter
// ---------------------------------------------------------------------------

type listBucketResultXML struct {
	NextMarker  string `xml:"NextMarker"`
	IsTruncated bool   `xml:"IsTruncated"`
}

// TestBatch2_ListObjects_NextMarker_OnlyWithDelimiter verifies that NextMarker
// is returned only when a delimiter is specified AND the response is truncated.
func TestBatch2_ListObjects_NextMarker_OnlyWithDelimiter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		delimiter      string
		wantNextMarker bool
	}{
		{
			name:           "no delimiter: NextMarker absent even when truncated",
			delimiter:      "",
			wantNextMarker: false,
		},
		{
			name:           "with delimiter: NextMarker present when truncated",
			delimiter:      "/",
			wantNextMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b2-nm-bkt")

			// Put 3 objects so max-keys=2 triggers truncation.
			for _, key := range []string{"a/obj1", "b/obj2", "c/obj3"} {
				mustPutObject(t, backend, "b2-nm-bkt", key, []byte("data"))
			}

			url := "/b2-nm-bkt?max-keys=2"
			if tt.delimiter != "" {
				url += "&delimiter=" + tt.delimiter
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			var result listBucketResultXML
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
			assert.True(t, result.IsTruncated, "response should be truncated")

			if tt.wantNextMarker {
				assert.NotEmpty(t, result.NextMarker,
					"NextMarker must be present when delimiter is specified and truncated")
			} else {
				assert.Empty(t, result.NextMarker,
					"NextMarker must be absent when no delimiter is specified")
			}
		})
	}
}

// TestBatch2_ListObjects_NextMarker_NotTruncated verifies that NextMarker is
// absent when the response is not truncated, regardless of delimiter.
func TestBatch2_ListObjects_NextMarker_NotTruncated(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b2-nm-complete")

	mustPutObject(t, backend, "b2-nm-complete", "key1", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/b2-nm-complete?delimiter=/", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result listBucketResultXML
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.False(t, result.IsTruncated)
	assert.Empty(t, result.NextMarker, "NextMarker must be absent when not truncated")
}

// ---------------------------------------------------------------------------
// CopyObject — x-amz-copy-source-version-id response header
// ---------------------------------------------------------------------------

// TestBatch2_CopyObject_SourceVersionID_Header verifies that CopyObject returns
// x-amz-copy-source-version-id when the source object has a real version ID.
func TestBatch2_CopyObject_SourceVersionID_Header(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b2-copy-src")
	mustCreateBucket(t, backend, "b2-copy-dst")

	// Enable versioning so objects get real version IDs.
	enableVersioning(t, handler, "b2-copy-src")

	mustPutObject(t, backend, "b2-copy-src", "srckey", []byte("hello"))

	req := httptest.NewRequest(http.MethodPut, "/b2-copy-dst/dstkey", nil)
	req.Header.Set("X-Amz-Copy-Source", "/b2-copy-src/srckey")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	srcVersionID := rec.Header().Get("X-Amz-Copy-Source-Version-Id")
	assert.NotEmpty(t, srcVersionID,
		"CopyObject must return x-amz-copy-source-version-id when source has a version ID")
}

// TestBatch2_CopyObject_NoSourceVersionID_Header verifies that CopyObject does
// NOT return x-amz-copy-source-version-id when the source bucket has no
// versioning (null version).
func TestBatch2_CopyObject_NoSourceVersionID_Header(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b2-copy-novsr-src")
	mustCreateBucket(t, backend, "b2-copy-novsr-dst")

	// No versioning enabled — objects get null version IDs.
	mustPutObject(t, backend, "b2-copy-novsr-src", "srckey", []byte("hello"))

	req := httptest.NewRequest(http.MethodPut, "/b2-copy-novsr-dst/dstkey", nil)
	req.Header.Set("X-Amz-Copy-Source", "/b2-copy-novsr-src/srckey")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	srcVersionID := rec.Header().Get("X-Amz-Copy-Source-Version-Id")
	assert.Empty(t, srcVersionID,
		"CopyObject must NOT return x-amz-copy-source-version-id for null-versioned objects")
}
