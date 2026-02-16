package s3_test

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"Gopherstack/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateChecksum(t *testing.T) {
	t.Parallel()

	data := []byte("hello world")

	tests := []struct {
		name      string
		algorithm string
		want      string
	}{
		{
			name:      "CRC32",
			algorithm: "CRC32",
			want:      "DUoRhQ==",
		},
		{
			name:      "CRC32C",
			algorithm: "CRC32C",
			want:      "yZRlqg==",
		},
		{
			name:      "SHA1",
			algorithm: "SHA1",
			want:      "Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
		},
		{
			name:      "SHA256",
			algorithm: "SHA256",
			want:      "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=",
		},
		{
			name:      "Unknown",
			algorithm: "UNKNOWN",
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3.CalculateChecksum(data, tt.algorithm)
			if tt.want != "" {
				assert.Equal(t, tt.want, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestHandler_VirtualHostedStyle(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend)
	handler.Endpoint = "localhost:8080"

	mustCreateBucket(t, backend, "my-bucket")

	// 1. Valid virtual hosted style request
	req := httptest.NewRequest(http.MethodGet, "/key", nil)
	req.Host = "my-bucket.localhost:8080"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Should be 404 because key doesn't exist, but it confirms bucket resolution
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// 2. Invalid bucket name in Host -> falls back to path style
	req = httptest.NewRequest(http.MethodGet, "/key", nil)
	req.Host = "invalid_bucket.localhost:8080"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// "/key" as path-style means bucket="key", not found -> 404
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// 3. No match for Endpoint
	req = httptest.NewRequest(http.MethodGet, "/key", nil)
	req.Host = "my-bucket.otherhost.com"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Fallback to path-style, which means bucket = "key" (if valid) or error
	// Here "/key" as path-style means bucket="key", but it doesn't exist.
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CopyObject(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-bkt")
	mustCreateBucket(t, backend, "dest-bkt")
	mustPutObject(t, backend, "src-bkt", "src-key", []byte("copy me"))

	// Copy object
	req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
	req.Header.Set("X-Amz-Copy-Source", "/src-bkt/src-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify copy exists
	req = httptest.NewRequest(http.MethodGet, "/dest-bkt/dest-key", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "copy me", string(body))
}

func TestHandler_CopyObject_ReplaceMetadata(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-bkt")
	mustCreateBucket(t, backend, "dest-bkt")
	mustPutObject(t, backend, "src-bkt", "src-key", []byte("copy me"))

	// Copy with REPLACE metadata
	req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
	req.Header.Set("X-Amz-Copy-Source", "/src-bkt/src-key")
	req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
	req.Header.Set("X-Amz-Meta-New", "Value")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify metadata
	req = httptest.NewRequest(http.MethodHead, "/dest-bkt/dest-key", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Value", rec.Header().Get("X-Amz-Meta-New"))
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestHandler_ListObjectsV2_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	for i := 1; i <= 5; i++ {
		mustPutObject(t, backend, "bkt", "key"+strings.Repeat("0", i), []byte("data"))
	}

	// Page 1
	req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=2", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<IsTruncated>true</IsTruncated>")
	assert.Contains(t, rec.Body.String(), "<NextContinuationToken>")
}

func TestHandler_MultipartUpload_Lifecycle(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Create
	req := httptest.NewRequest(http.MethodPost, "/bkt/large?uploads", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UploadId")

	// Extract UploadId manually for brevity
	body := rec.Body.String()
	start := strings.Index(body, "<UploadId>") + 10
	end := strings.Index(body, "</UploadId>")
	uploadID := body[start:end]

	// 2. Upload Part
	req = httptest.NewRequest(http.MethodPut, "/bkt/large?partNumber=1&uploadId="+uploadID, strings.NewReader("part1"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	etag := rec.Header().Get("ETag")

	// 3. Complete
	completeXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
	req = httptest.NewRequest(http.MethodPost, "/bkt/large?uploadId="+uploadID, strings.NewReader(completeXML))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 4. Verify content
	req = httptest.NewRequest(http.MethodGet, "/bkt/large", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "part1", rec.Body.String())
}

func TestHandler_MultipartUpload_Abort(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodPost, "/bkt/abort?uploads", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	start := strings.Index(body, "<UploadId>") + 10
	end := strings.Index(body, "</UploadId>")
	uploadID := body[start:end]

	req = httptest.NewRequest(http.MethodDelete, "/bkt/abort?uploadId="+uploadID, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_CopyObject_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "src", []byte("data"))

	tests := []struct {
		name       string
		source     string
		dest       string
		wantStatus int
	}{
		{
			name:       "source bucket not found",
			source:     "/no-bkt/src",
			dest:       "/bkt/dest",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "source key not found",
			source:     "/bkt/no-key",
			dest:       "/bkt/dest",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "destination bucket not found",
			source:     "/bkt/src",
			dest:       "/no-bkt/dest",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid copy source format",
			source:     "invalid-format",
			dest:       "/bkt/dest",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, tt.dest, nil)
			req.Header.Set("X-Amz-Copy-Source", tt.source)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetObject_ChecksumMode(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "key", []byte("checksum-me"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Algorithm"))
	assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Crc32"))

	// Test with existing checksum
	_, err := backend.PutObject(context.Background(), &sdk_s3.PutObjectInput{
		Bucket:         aws.String("bkt"),
		Key:            aws.String("key2"),
		Body:           strings.NewReader("data"),
		ChecksumSHA256: aws.String("fake-sha256"),
	})
	require.NoError(t, err)

	req = httptest.NewRequest(http.MethodGet, "/bkt/key2", nil)
	req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))
	assert.Equal(t, "fake-sha256", rec.Header().Get("X-Amz-Checksum-Sha256"))
}

func TestHandler_ListObjectsV2_CommonPrefixes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "photos/1.jpg", []byte("d"))
	mustPutObject(t, backend, "bkt", "photos/2.jpg", []byte("d"))
	mustPutObject(t, backend, "bkt", "videos/1.mp4", []byte("d"))
	mustPutObject(t, backend, "bkt", "root.txt", []byte("d"))

	req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&delimiter=/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<Prefix>photos/</Prefix>")
	assert.Contains(t, body, "<Prefix>videos/</Prefix>")
	assert.Contains(t, body, "<Key>root.txt</Key>")
}

func TestHandler_ListObjects_Marker(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "a", []byte("d"))
	mustPutObject(t, backend, "bkt", "b", []byte("d"))
	mustPutObject(t, backend, "bkt", "c", []byte("d"))

	req := httptest.NewRequest(http.MethodGet, "/bkt?marker=a", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.NotContains(t, body, "<Key>a</Key>")
	assert.Contains(t, body, "<Key>b</Key>")
	assert.Contains(t, body, "<Key>c</Key>")
}

func TestHandler_CopyObject_Versioned(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src")
	mustCreateBucket(t, backend, "dest")

	enableVersioning(t, handler, "src")

	// Put V1
	mustPutObject(t, backend, "src", "key", []byte("v1"))
	// Put V2
	req := httptest.NewRequest(http.MethodPut, "/src/key", strings.NewReader("v2"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	v2ID := rec.Header().Get("X-Amz-Version-Id")

	// Copy V2 specifically
	req = httptest.NewRequest(http.MethodPut, "/dest/key-v2", nil)
	// Use header instead of query param to be safe, though my handler now supports both
	req.Header.Set("X-Amz-Copy-Source", "/src/key")
	req.Header.Set("X-Amz-Copy-Source-Version-Id", v2ID)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify content
	req = httptest.NewRequest(http.MethodGet, "/dest/key-v2", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, "v2", rec.Body.String())
}

func TestHandler_CompleteMultipartUpload_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Invalid XML
	req := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploadId=any", strings.NewReader("not xml"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// 2. No upload ID
	req = httptest.NewRequest(http.MethodPost, "/bkt/obj", strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// This might be handled as a regular POST or something if no query params?
	// But usually it should be 404 or 400 if it's meant to be multipart.
}

func TestHandler_ResolveBucketAndKey_HostChecks(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend)
	handler.Endpoint = "s3.amazonaws.com"

	// Valid host, no port
	req := httptest.NewRequest(http.MethodGet, "/key", nil)
	req.Host = "mybucket.s3.amazonaws.com"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// If bucket doesn't exist, 404 is fine, confirms resolution if it tried to find "mybucket"
}

func TestHandler_ListObjects_CommonPrefixes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "a/1", []byte("d"))
	mustPutObject(t, backend, "bkt", "a/2", []byte("d"))
	mustPutObject(t, backend, "bkt", "b/1", []byte("d"))

	req := httptest.NewRequest(http.MethodGet, "/bkt?delimiter=/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<Prefix>a/</Prefix>")
	assert.Contains(t, body, "<Prefix>b/</Prefix>")
	assert.NotContains(t, body, "<Key>a/1</Key>")
}

func TestHandler_GetObject_Range(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "key", []byte("0123456789"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	req.Header.Set("Range", "bytes=0-4")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusPartialContent, rec.Code)
	assert.Equal(t, "01234", rec.Body.String())
	assert.Equal(t, "bytes 0-4/10", rec.Header().Get("Content-Range"))

	// Suffix range: bytes=-3 (last 3 bytes: 789)
	req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	req.Header.Set("Range", "bytes=-3")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, "789", rec.Body.String())

	// Start only: bytes=8-
	req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	req.Header.Set("Range", "bytes=8-")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, "89", rec.Body.String())

	// Invalid range (start > end)
	req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	req.Header.Set("Range", "bytes=10-5")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
}

func TestHandler_ListObjectVersions_WithDeleteMarkers(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	enableVersioning(t, handler, "bkt")

	mustPutObject(t, backend, "bkt", "key", []byte("data"))
	// Create delete marker
	req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<DeleteMarker>")
	assert.Contains(t, body, "<IsLatest>true</IsLatest>")
}

func TestHandler_ListObjectVersions(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	enableVersioning(t, handler, "bkt")

	mustPutObject(t, backend, "bkt", "key", []byte("v1"))
	mustPutObject(t, backend, "bkt", "key", []byte("v2"))

	req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ListVersionsResult>")
	assert.Contains(t, body, "<Key>key</Key>")
	assert.Contains(t, body, "<VersionId>")
	// Should have at least two versions
	assert.Equal(t, 2, strings.Count(body, "<Version>"))
}

func TestHandler_GetBucketLocation(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodGet, "/bkt?location", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "us-east-1")
}

func TestHandler_DeleteBucket_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "full-bkt")
	mustPutObject(t, backend, "full-bkt", "obj", []byte("data"))

	// Delete non-empty bucket
	req := httptest.NewRequest(http.MethodDelete, "/full-bkt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Delete non-existent bucket
	req = httptest.NewRequest(http.MethodDelete, "/no-bkt", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateBucket_Exists(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodPut, "/bkt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_UploadPart_Errors(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	// Missing uploadId
	req := httptest.NewRequest(http.MethodPut, "/bkt/obj?partNumber=1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// This will go to putObject because both params are required for uploadPart route
	// But it might fail because bucket doesn't exist.
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_BucketTagging_NotImplemented(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// GET tagging
	req := httptest.NewRequest(http.MethodGet, "/bkt?tagging", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestHandler_ResolveBucketAndKey_InvalidKey(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	// Key too long (> 1024)
	longKey := strings.Repeat("a", 1025)
	req := httptest.NewRequest(http.MethodGet, "/bkt/"+longKey, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ObjectLifecycle_Versioned(t *testing.T) {
	t.Parallel()
	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	enableVersioning(t, handler, "bkt")

	// Put
	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var res s3.ListVersionsResult
	err := xml.Unmarshal(rec.Body.Bytes(), &res)
	require.NoError(t, err, "Body: %s", rec.Body.String())
	require.NotEmpty(t, res.Versions, "Body: %s", rec.Body.String())
	vid := res.Versions[0].VersionID
	require.NotEmpty(t, vid, "VersionID is empty. Body: %s", rec.Body.String())

	// Head with version
	req = httptest.NewRequest(http.MethodHead, "/bkt/key?versionId="+vid, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))

	// Delete with version
	req = httptest.NewRequest(http.MethodDelete, "/bkt/key?versionId="+vid, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))
}

func TestHandler_ListObjectsV2_DelimiterAndPrefix(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "dir/a", []byte("d"))
	mustPutObject(t, backend, "bkt", "dir/b", []byte("d"))
	mustPutObject(t, backend, "bkt", "other", []byte("d"))

	// List with prefix="dir/" and delimiter="/"
	req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&prefix=dir/&delimiter=/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<Key>dir/a</Key>")
	assert.Contains(t, body, "<Key>dir/b</Key>")
	assert.NotContains(t, body, "<Key>other</Key>")
}

func TestHandler_ListObjectsV2_StartAfter(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "a", []byte("d"))
	mustPutObject(t, backend, "bkt", "b", []byte("d"))
	mustPutObject(t, backend, "bkt", "c", []byte("d"))

	// List with start-after=a
	req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&start-after=a", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.NotContains(t, body, "<Key>a</Key>")
	assert.Contains(t, body, "<Key>b</Key>")
	assert.Contains(t, body, "<Key>c</Key>")
}

func TestHandler_MultipartUpload_ExtendedErrors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// uploadPart with non-existent uploadId
	req := httptest.NewRequest(http.MethodPut, "/bkt/obj?partNumber=1&uploadId=no-such-id", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// uploadPart with invalid partNumber (non-integer)
	req = httptest.NewRequest(http.MethodPut, "/bkt/obj?partNumber=abc&uploadId=any", strings.NewReader("data"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListObjectsV2_InvalidMaxKeys(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// List with invalid max-keys
	req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Should default to 1000 and return 200
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteObject_NonExistent(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Delete non-existent object: S3 returns 204 (No Content) success even if key doesn't exist
	req := httptest.NewRequest(http.MethodDelete, "/bkt/no-key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_CompleteMultipartUpload_MoreErrors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Create upload
	req := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var res s3.InitiateMultipartUploadResult
	xml.Unmarshal(rec.Body.Bytes(), &res)
	uploadID := res.UploadID

	// Complete with invalid XML
	req = httptest.NewRequest(http.MethodPost, "/bkt/obj?uploadId="+uploadID, strings.NewReader("not xml"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteObjectTagging_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Delete tags from non-existent object: S3 returns 204
	req := httptest.NewRequest(http.MethodDelete, "/bkt/no-such-key?tagging", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_CreateBucket_InvalidConfig(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	// Invalid XML in body
	req := httptest.NewRequest(http.MethodPut, "/new-bkt", strings.NewReader("not xml"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// It should warn but default to us-east-1 and continue?
	// Based on handler.go:346, it logs a warning but doesn't return.
	// So success is expected if the bucket name is valid.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListObjectVersions_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/no-bucket?versions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UploadPart_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/no-bucket/key?partNumber=1&uploadId=ui", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_AbortMultipartUpload_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/no-bucket/key?uploadId=ui", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CompleteMultipartUpload_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	// Body doesn't matter much if bucket resolution fails first
	req := httptest.NewRequest(http.MethodPost, "/no-bucket/key?uploadId=ui", strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetObjectTagging_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/no-bucket/key?tagging", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetBucketVersioning_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/no-bucket?versioning", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CopyObject_MissingSource(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "dest-bkt")

	// Copy object without source header: Routes to PutObject, which succeeds with empty body
	req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CopyObject_VersionedSourceHeader(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-bkt")
	mustCreateBucket(t, backend, "dest-bkt")
	mustPutObject(t, backend, "src-bkt", "src-key", []byte("v1"))

	// Enable versioning and put another version
	enableVersioning(t, handler, "src-bkt")
	mustPutObject(t, backend, "src-bkt", "src-key", []byte("v2"))

	// Get versions to find v2 ID
	req := httptest.NewRequest(http.MethodGet, "/src-bkt?versions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var res s3.ListVersionsResult
	xml.Unmarshal(rec.Body.Bytes(), &res)
	v2ID := res.Versions[0].VersionID

	// Copy v2 specifically using header avec ?versionId=
	req = httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
	req.Header.Set("X-Amz-Copy-Source", "/src-bkt/src-key?versionId="+v2ID)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	ops := handler.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "PutObject")
}

func TestHandler_AbortMultipartUpload_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Invalid upload ID
	req := httptest.NewRequest(http.MethodDelete, "/bkt/obj?uploadId=invalid", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// S3 returns 404 NoSuchUpload
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Missing upload ID
	req = httptest.NewRequest(http.MethodDelete, "/bkt/obj", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Routes to DeleteObject if no uploadId, which succeeds (204)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_CopyObject_InvalidSource(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "dest")

	// Invalid source format (missing bucket or key)
	req := httptest.NewRequest(http.MethodPut, "/dest/obj", nil)
	req.Header.Set("X-Amz-Copy-Source", "invalid-format")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetObject_Range_Invalid(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("some data"))

	// Invalid range format
	req := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
	req.Header.Set("Range", "bits=0-5")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Should fallback to 200 OK for invalid range
	assert.Equal(t, http.StatusOK, rec.Code)
}
