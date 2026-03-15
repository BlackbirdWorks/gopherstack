package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_ChecksumMismatch_Rejected verifies that PutObject via the HTTP
// handler returns 400 BadDigest when the supplied checksum does not match the
// actual content.
func TestHandler_ChecksumMismatch_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		algo           string
		checksumHeader string
	}{
		{name: "sha256_mismatch", algo: "SHA256", checksumHeader: "X-Amz-Checksum-Sha256"},
		{name: "crc32_mismatch", algo: "CRC32", checksumHeader: "X-Amz-Checksum-Crc32"},
		{name: "sha1_mismatch", algo: "SHA1", checksumHeader: "X-Amz-Checksum-Sha1"},
		{name: "crc32c_mismatch", algo: "CRC32C", checksumHeader: "X-Amz-Checksum-Crc32c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := "test data for checksum"
			// Compute correct checksum then corrupt it.
			correct := s3.CalculateChecksum([]byte(body), tt.algo)
			corrupt := correct[:len(correct)-1] + "X"

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader(body))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.algo)
			req.Header.Set(tt.checksumHeader, corrupt)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"expected 400 BadDigest for corrupted %s checksum", tt.algo)
			assert.Contains(t, rec.Body.String(), "BadDigest")
		})
	}
}

// TestHandler_ChecksumValid_Accepted verifies that PutObject via the HTTP
// handler returns 200 when the supplied checksum matches the content.
func TestHandler_ChecksumValid_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		algo           string
		checksumHeader string
	}{
		{name: "sha256_valid", algo: "SHA256", checksumHeader: "X-Amz-Checksum-Sha256"},
		{name: "crc32_valid", algo: "CRC32", checksumHeader: "X-Amz-Checksum-Crc32"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := "valid checksum data"
			correct := s3.CalculateChecksum([]byte(body), tt.algo)

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader(body))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.algo)
			req.Header.Set(tt.checksumHeader, correct)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusOK, rec.Code,
				"expected 200 for valid %s checksum", tt.algo)
		})
	}
}

// TestHandler_GetObjectAcl_NotFound verifies that GetObjectAcl returns 404 when
// the object does not exist.
func TestHandler_GetObjectAcl_NotFound(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodGet, "/bkt/nonexistent?acl", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_GetObjectAcl_ReturnsOwnerGrant verifies that GetObjectAcl returns a
// valid XML ACL with FULL_CONTROL for an existing object.
func TestHandler_GetObjectAcl_ReturnsOwnerGrant(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/key?acl", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "AccessControlPolicy")
	assert.Contains(t, body, "FULL_CONTROL")
	assert.Contains(t, body, "gopherstack")
	assert.Contains(t, body, "CanonicalUser")
}

// TestHandler_MultipartUploadTagging verifies that the X-Amz-Tagging header on
// CreateMultipartUpload is forwarded to the backend and applied on completion.
func TestHandler_MultipartUploadTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taggingHdr string
		wantBody   string
	}{
		{
			name:       "tags_in_header_applied_on_complete",
			taggingHdr: "color=blue&size=large",
			wantBody:   "color",
		},
		{
			name:       "no_tagging_header",
			taggingHdr: "",
			wantBody:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			// Initiate with X-Amz-Tagging header.
			reqInit := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
			if tt.taggingHdr != "" {
				reqInit.Header.Set("X-Amz-Tagging", tt.taggingHdr)
			}
			recInit := httptest.NewRecorder()
			serveS3Handler(handler, recInit, reqInit)
			require.Equal(t, http.StatusOK, recInit.Code)

			// Parse upload ID from XML response.
			var initResp struct {
				UploadID string `xml:"UploadId"`
			}
			require.NoError(t, xml.NewDecoder(recInit.Body).Decode(&initResp))
			uploadID := initResp.UploadID
			require.NotEmpty(t, uploadID)

			// Upload one part (the in-memory backend has no minimum part size).
			partBody := strings.Repeat("x", 1024) // 1 KiB is enough for the mock
			reqPart := httptest.NewRequest(
				http.MethodPut, "/bkt/obj?partNumber=1&uploadId="+uploadID,
				strings.NewReader(partBody),
			)
			recPart := httptest.NewRecorder()
			serveS3Handler(handler, recPart, reqPart)
			require.Equal(t, http.StatusOK, recPart.Code)
			etag := recPart.Header().Get("ETag")
			require.NotEmpty(t, etag)

			// Complete the multipart upload.
			completeXML := `<CompleteMultipartUpload>` +
				`<Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part>` +
				`</CompleteMultipartUpload>`
			reqComplete := httptest.NewRequest(
				http.MethodPost, "/bkt/obj?uploadId="+uploadID,
				strings.NewReader(completeXML),
			)
			recComplete := httptest.NewRecorder()
			serveS3Handler(handler, recComplete, reqComplete)
			require.Equal(t, http.StatusOK, recComplete.Code)

			// Retrieve object tags.
			reqTags := httptest.NewRequest(http.MethodGet, "/bkt/obj?tagging", nil)
			recTags := httptest.NewRecorder()
			serveS3Handler(handler, recTags, reqTags)

			if tt.wantBody != "" {
				require.Equal(t, http.StatusOK, recTags.Code)
				assert.Contains(t, recTags.Body.String(), tt.wantBody)
			}

			// Verify object exists.
			reqGet := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
			recGet := httptest.NewRecorder()
			serveS3Handler(handler, recGet, reqGet)
			assert.Equal(t, http.StatusOK, recGet.Code)
		})
	}
}
