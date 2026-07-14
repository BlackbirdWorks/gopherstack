package s3_test

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_PutObject_ContentMD5 verifies that PutObject validates an
// optional Content-MD5 header against the uploaded body.
func TestHandler_PutObject_ContentMD5(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucket    string
		url       string
		body      string
		md5Header string
		wantCode  int
	}{
		{
			name:      "valid_md5",
			bucket:    "md5-valid-bucket",
			url:       "/md5-valid-bucket/hello.txt",
			body:      "hello world",
			md5Header: "XrY7u+Ae7tCTyyK7j1rNww==",
			wantCode:  http.StatusOK,
		},
		{
			name:      "invalid_md5",
			bucket:    "md5-invalid-bucket",
			url:       "/md5-invalid-bucket/hello.txt",
			body:      "hello world",
			md5Header: "AAAAAAAAAAAAAAAAAAAAAA==",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodPut, tt.url, strings.NewReader(tt.body))
			req.Header.Set("Content-Md5", tt.md5Header)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ─── checksum helpers shared by the UploadPart checksum table below ──────────

func checksumB64CRC32(data []byte) string {
	sum := crc32.ChecksumIEEE(data)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)

	return base64.StdEncoding.EncodeToString(b)
}

func checksumB64CRC32C(data []byte) string {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(data)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, h.Sum32())

	return base64.StdEncoding.EncodeToString(b)
}

func checksumB64SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func checksumB64SHA256(data []byte) string {
	h := sha256.New()
	h.Write(data)

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// doMultipartUpload initiates a multipart upload and returns its upload ID.
func doMultipartUpload(
	t *testing.T,
	handler *s3.S3Handler,
	backend s3.StorageBackend,
	bucket string,
) string {
	t.Helper()
	mustCreateBucket(t, backend, bucket)

	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"/obj?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(rec.Body).Decode(&resp))

	return resp.UploadID
}

// TestUploadPart_ChecksumHandling verifies that UploadPart accepts a matching
// checksum for every supported algorithm, rejects a mismatched checksum, and
// computes the checksum server-side when none is supplied.
func TestUploadPart_ChecksumHandling(t *testing.T) {
	t.Parallel()

	partData := []byte("hello world part data")

	tests := []struct {
		checksum   func() string
		name       string
		bucket     string
		algo       string
		headerName string
		wantCode   int
	}{
		{
			name:       "crc32",
			bucket:     "chksum-crc32",
			algo:       "CRC32",
			headerName: "X-Amz-Checksum-Crc32",
			checksum:   func() string { return checksumB64CRC32(partData) },
			wantCode:   http.StatusOK,
		},
		{
			name:       "crc32c",
			bucket:     "chksum-crc32c",
			algo:       "CRC32C",
			headerName: "X-Amz-Checksum-Crc32c",
			checksum:   func() string { return checksumB64CRC32C(partData) },
			wantCode:   http.StatusOK,
		},
		{
			name:       "sha1",
			bucket:     "chksum-sha1",
			algo:       "SHA1",
			headerName: "X-Amz-Checksum-Sha1",
			checksum:   func() string { return checksumB64SHA1(partData) },
			wantCode:   http.StatusOK,
		},
		{
			name:       "sha256",
			bucket:     "chksum-sha256",
			algo:       "SHA256",
			headerName: "X-Amz-Checksum-Sha256",
			checksum:   func() string { return checksumB64SHA256(partData) },
			wantCode:   http.StatusOK,
		},
		{
			name:       "wrong_checksum_rejected",
			bucket:     "chksum-wrong",
			algo:       "CRC32",
			headerName: "X-Amz-Checksum-Crc32",
			checksum:   func() string { return base64.StdEncoding.EncodeToString(make([]byte, 4)) },
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "server_computed_when_absent",
			bucket:     "chksum-computed",
			algo:       "SHA256",
			headerName: "", // no checksum value header — server computes it
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			uploadID := doMultipartUpload(t, handler, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodPut,
				fmt.Sprintf("/%s/obj?partNumber=1&uploadId=%s", tt.bucket, uploadID),
				bytes.NewReader(partData))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.algo)
			if tt.headerName != "" {
				req.Header.Set(tt.headerName, tt.checksum())
			}
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
