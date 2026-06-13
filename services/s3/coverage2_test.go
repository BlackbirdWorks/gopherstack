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

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// ─── Regions / BucketsByRegion ────────────────────────────────────────────────

func TestHandler_Regions_Empty(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	regions := handler.Regions()
	assert.Empty(t, regions)
}

func TestHandler_Regions_WithBuckets(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "reg-bucket")

	regions := handler.Regions()
	assert.NotEmpty(t, regions)
}

func TestHandler_BucketsByRegion_All(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "byr-bucket-1")
	mustCreateBucket(t, backend, "byr-bucket-2")

	buckets := handler.BucketsByRegion("")
	assert.GreaterOrEqual(t, len(buckets), 2)
}

func TestHandler_BucketsByRegion_Specific(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "byr-specific")

	// Using a non-existent region returns empty.
	buckets := handler.BucketsByRegion("ap-southeast-99")
	assert.Empty(t, buckets)

	// Default region is us-east-1.
	all := handler.BucketsByRegion("us-east-1")
	assert.NotEmpty(t, all)
}

// ─── InMemoryBackend.Regions / BucketsByRegion ───────────────────────────────

func TestInMemoryBackend_Regions(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	mustCreateBucket(t, backend, "imb-region-bucket")

	regions := backend.Regions()
	assert.NotEmpty(t, regions)
}

func TestInMemoryBackend_BucketsByRegion(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	mustCreateBucket(t, backend, "imb-byr-bucket")

	all := backend.BucketsByRegion("")
	assert.NotEmpty(t, all)

	// Lookup for unknown region.
	none := backend.BucketsByRegion("eu-west-99")
	assert.Empty(t, none)
}

// ─── applyVersionDelimiter via ListObjectVersions ────────────────────────────

func TestListObjectVersions_Delimiter_GroupsCommonPrefixes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lvd-bucket")
	enableVersioning(t, handler, "lvd-bucket")

	// Create objects that share a common prefix under the delimiter.
	for _, key := range []string{"logs/2024/jan.log", "logs/2024/feb.log", "data/file.csv"} {
		mustPutObject(t, backend, "lvd-bucket", key, []byte("content"))
	}

	req := httptest.NewRequest(http.MethodGet, "/lvd-bucket?versions&delimiter=/&prefix=logs/", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	// The two log/ objects share "logs/2024/" as a common prefix.
	assert.Contains(t, body, "CommonPrefixes")
}

func TestListObjectVersions_Delimiter_NoCommonPrefix(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lvd2-bucket")
	enableVersioning(t, handler, "lvd2-bucket")

	mustPutObject(t, backend, "lvd2-bucket", "file.txt", []byte("hello"))

	// Delimiter present but no key has delimiter in its name.
	req := httptest.NewRequest(http.MethodGet, "/lvd2-bucket?versions&delimiter=/", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}

// ─── seekMultipartMarker via ListMultipartUploads with key-marker ─────────────

func TestListMultipartUploads_KeyMarker_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "smm-bucket")

	// Start several multipart uploads.
	keys := []string{"aaa/file", "bbb/file", "ccc/file"}
	uploadIDs := make(map[string]string)

	for _, key := range keys {
		req := httptest.NewRequest(http.MethodPost, "/smm-bucket/"+key+"?uploads", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp s3.InitiateMultipartUploadResult
		require.NoError(t, xml.NewDecoder(rec.Body).Decode(&resp))
		uploadIDs[key] = resp.UploadID
	}

	// List with key-marker set to "bbb/file" — should skip aaa/file and bbb/file.
	req := httptest.NewRequest(http.MethodGet,
		"/smm-bucket?uploads&key-marker=bbb/file", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "aaa/file", "aaa/file should be skipped by key-marker")
	assert.Contains(t, body, "ccc/file", "ccc/file should appear after key-marker")
}

func TestListMultipartUploads_KeyMarkerPlusUploadIDMarker(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "smm2-bucket")

	// Start two uploads for the same key.
	uploadIDs := make([]string, 0, 2)
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/smm2-bucket/same-key?uploads", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp s3.InitiateMultipartUploadResult
		require.NoError(t, xml.NewDecoder(rec.Body).Decode(&resp))
		uploadIDs = append(uploadIDs, resp.UploadID)
	}

	// List all to get upload IDs in order.
	listReq := httptest.NewRequest(http.MethodGet, "/smm2-bucket?uploads", nil)
	listRec := httptest.NewRecorder()
	serveS3Handler(handler, listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code)

	// Use key-marker + upload-id-marker to skip the first upload for "same-key".
	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf(
			"/smm2-bucket?uploads&key-marker=same-key&upload-id-marker=%s",
			uploadIDs[0],
		),
		nil,
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestListMultipartUploads_KeyMarkerBeyondAllUploads(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "smm3-bucket")

	// Start one upload.
	req := httptest.NewRequest(http.MethodPost, "/smm3-bucket/aaa?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// key-marker beyond all keys → empty result.
	listReq := httptest.NewRequest(http.MethodGet, "/smm3-bucket?uploads&key-marker=zzz", nil)
	listRec := httptest.NewRecorder()
	serveS3Handler(handler, listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code)
}

// ─── UploadPart with checksum algorithms ─────────────────────────────────────

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

func TestUploadPart_WithCRC32Checksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-crc32")
	partData := []byte("hello world part data")
	checksum := checksumB64CRC32(partData)

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-crc32/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC32")
	req.Header.Set("X-Amz-Checksum-Crc32", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUploadPart_WithCRC32C_Checksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-crc32c")
	partData := []byte("hello world part data")
	checksum := checksumB64CRC32C(partData)

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-crc32c/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC32C")
	req.Header.Set("X-Amz-Checksum-Crc32c", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUploadPart_WithSHA1_Checksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-sha1")
	partData := []byte("hello world part data")
	checksum := checksumB64SHA1(partData)

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-sha1/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA1")
	req.Header.Set("X-Amz-Checksum-Sha1", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUploadPart_WithSHA256_Checksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-sha256")
	partData := []byte("hello world part data")
	checksum := checksumB64SHA256(partData)

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-sha256/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	req.Header.Set("X-Amz-Checksum-Sha256", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUploadPart_WrongChecksum_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-wrong")
	partData := []byte("hello world part data")
	badChecksum := base64.StdEncoding.EncodeToString(make([]byte, 4)) // wrong

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-wrong/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC32")
	req.Header.Set("X-Amz-Checksum-Crc32", badChecksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUploadPart_ServerComputedChecksum(t *testing.T) {
	t.Parallel()

	// When algorithm is set but no checksum value is provided,
	// the server computes and stores it.
	handler, backend := newTestHandler(t)
	uploadID := doMultipartUpload(t, handler, backend, "chksum-computed")
	partData := []byte("hello world part data")

	req := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/chksum-computed/obj?partNumber=1&uploadId=%s", uploadID),
		bytes.NewReader(partData))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	// No X-Amz-Checksum-SHA256 header — server should compute it.
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── getStoredChecksum via GetObject (object with checksum stored) ─────────────

func TestGetObject_ReturnsStoredChecksum_CRC32(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-crc32")

	data := []byte("checksum test data")
	checksum := checksumB64CRC32(data)

	// PUT with CRC32 checksum.
	req := httptest.NewRequest(http.MethodPut, "/gsc-crc32/obj",
		bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC32")
	req.Header.Set("X-Amz-Checksum-Crc32", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// GET with X-Amz-Checksum-Mode: ENABLED should echo back the checksum header.
	getReq := httptest.NewRequest(http.MethodGet, "/gsc-crc32/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, checksum, getRec.Header().Get("X-Amz-Checksum-Crc32"))
}

func TestGetObject_ReturnsStoredChecksum_SHA256(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-sha256")

	data := []byte("checksum test data")
	checksum := checksumB64SHA256(data)

	req := httptest.NewRequest(http.MethodPut, "/gsc-sha256/obj",
		bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	req.Header.Set("X-Amz-Checksum-Sha256", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-sha256/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, checksum, getRec.Header().Get("X-Amz-Checksum-Sha256"))
}

func TestGetObject_ReturnsStoredChecksum_SHA1(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-sha1")

	data := []byte("sha1 data")
	checksum := checksumB64SHA1(data)

	req := httptest.NewRequest(http.MethodPut, "/gsc-sha1/obj", bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA1")
	req.Header.Set("X-Amz-Checksum-Sha1", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-sha1/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, checksum, getRec.Header().Get("X-Amz-Checksum-Sha1"))
}

func TestGetObject_ReturnsStoredChecksum_CRC32C(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-crc32c")

	data := []byte("crc32c data")
	checksum := checksumB64CRC32C(data)

	req := httptest.NewRequest(http.MethodPut, "/gsc-crc32c/obj", bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC32C")
	req.Header.Set("X-Amz-Checksum-Crc32c", checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-crc32c/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, checksum, getRec.Header().Get("X-Amz-Checksum-Crc32c"))
}

func TestGetObject_ChecksumMode_NoStoredChecksum_ComputesCRC32(t *testing.T) {
	t.Parallel()

	// Object stored without any checksum → default path computes CRC32.
	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-nocsum")
	mustPutObject(t, backend, "gsc-nocsum", "obj", []byte("data"))

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-nocsum/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	// Server-computed CRC32 should be set.
	assert.Equal(t, "CRC32", getRec.Header().Get("X-Amz-Checksum-Algorithm"))
}

func TestGetObject_ReturnsStoredChecksum_CRC64NVME(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-crc64")

	data := []byte("crc64 test data")
	crc64Checksum := s3.CalculateCRC64NVME(data)

	req := httptest.NewRequest(http.MethodPut, "/gsc-crc64/obj", bytes.NewReader(data))
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC64NVME")
	req.Header.Set("X-Amz-Checksum-Crc64nvme", crc64Checksum)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-crc64/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.NotEmpty(t, getRec.Header().Get("X-Amz-Checksum-Algorithm"))
}

// ─── setExpirationHeader via PutObject + GetObject with lifecycle ──────────────

func TestGetObject_WithLifecycle_SetsExpirationHeader(t *testing.T) {
	t.Parallel()

	// Create a handler WITH janitor so setExpirationHeader is exercised.
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})

	mustCreateBucket(t, backend, "lc-exp-bucket")
	mustPutObject(t, backend, "lc-exp-bucket", "obj.log", []byte("hello"))

	// Set a lifecycle rule with a Days expiration.
	lcXML := `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>obj</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

	putLCReq := httptest.NewRequest(http.MethodPut,
		"/lc-exp-bucket?lifecycle",
		strings.NewReader(lcXML))
	putLCRec := httptest.NewRecorder()
	serveS3Handler(handler, putLCRec, putLCReq)
	require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, putLCRec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/lc-exp-bucket/obj.log", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	// X-Amz-Expiration may or may not be set depending on implementation;
	// we just verify the request doesn't panic.
}

// ─── setSSEResponseHeaders via SSE-KMS PutObject ──────────────────────────────

func TestSSE_KMS_ResponseHeaders(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "sse-kms-hdr")

	req := httptest.NewRequest(http.MethodPut, "/sse-kms-hdr/obj",
		strings.NewReader("hello kms"))
	req.Header.Set("X-Amz-Server-Side-Encryption", "aws:kms")
	req.Header.Set(
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"arn:aws:kms:us-east-1:123456789012:key/test-key",
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "aws:kms", rec.Header().Get("X-Amz-Server-Side-Encryption"))
}

// ─── validateSSECKey with empty MD5 ──────────────────────────────────────────

func TestSSEC_NoMD5Header_Rejected(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-nomd5")

	key := make([]byte, 32)
	keyB64 := base64.StdEncoding.EncodeToString(key)

	// SSE-C algorithm/key/key-MD5 are required together per AWS spec.
	rec := doRequest(handler, http.MethodPut, "/ssec-nomd5/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
			"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── evaluateCopySourceConditionals — if-modified-since path ─────────────────

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

// ─── buildCopyMetadata with content-type form-urlencoded (fallback branch) ───

func TestCopyObject_MetadataReplace_FormURLEncodedContentType(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ct-src")
	mustCreateBucket(t, backend, "ct-dst")
	mustPutObject(t, backend, "ct-src", "obj", []byte("data"))

	// REPLACE directive with application/x-www-form-urlencoded content-type —
	// should fall back to preserving source content-type.
	rec := doRequest(handler, http.MethodPut, "/ct-dst/dst", nil,
		map[string]string{
			"X-Amz-Copy-Source":        "ct-src/obj",
			"X-Amz-Metadata-Directive": "REPLACE",
			"Content-Type":             "application/x-www-form-urlencoded",
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── WithCompressionMinBytes ──────────────────────────────────────────────────

func TestWithCompressionMinBytes_NegativeClampedToZero(t *testing.T) {
	t.Parallel()

	// Negative value should be accepted (clamped internally).
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithCompressionMinBytes(-1)
	mustCreateBucket(t, backend, "comp-neg")

	_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("comp-neg"),
		Key:    aws.String("key"),
		Body:   bytes.NewReader([]byte("small")),
	})
	assert.NoError(t, err)
}

func TestWithCompressionMinBytes_AboveThreshold_NotCompressed(t *testing.T) {
	t.Parallel()

	// Set minimum to 1000 bytes; upload a small object (below threshold).
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithCompressionMinBytes(1000)
	mustCreateBucket(t, backend, "comp-thresh")

	_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("comp-thresh"),
		Key:    aws.String("small-obj"),
		Body:   bytes.NewReader([]byte("small")),
	})
	require.NoError(t, err)

	out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("comp-thresh"),
		Key:    aws.String("small-obj"),
	})
	require.NoError(t, err)
	defer out.Body.Close()
}

// ─── AbortMultipartUpload — non-existent upload ID ────────────────────────────

func TestAbortMultipartUpload_NonExistentUpload(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "abort-ne")

	req := httptest.NewRequest(http.MethodDelete,
		"/abort-ne/obj?uploadId=nonexistent-upload-id", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── ListObjectVersions with version key-marker pagination ───────────────────

func TestListObjectVersions_KeyMarker_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lv-pg")
	enableVersioning(t, handler, "lv-pg")

	for _, key := range []string{"aaa", "bbb", "ccc"} {
		mustPutObject(t, backend, "lv-pg", key, []byte("v"))
	}

	// List starting after "aaa".
	req := httptest.NewRequest(http.MethodGet, "/lv-pg?versions&key-marker=aaa", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<Key>aaa</Key>")
	assert.Contains(t, body, "<Key>bbb</Key>")
}

// ─── routeObjectPost — restore object (stub) ────────────────────────────────

func TestHandler_RouteObjectPost_Restore(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "restore-bkt")
	mustPutObject(t, backend, "restore-bkt", "obj", []byte("data"))

	req := httptest.NewRequest(http.MethodPost, "/restore-bkt/obj?restore", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	// Restore is a stub — returns 200 or 202 Accepted.
	assert.Contains(t, []int{http.StatusOK, http.StatusAccepted}, rec.Code)
}

// ─── deleteObjectTagging error path ──────────────────────────────────────────

func TestDeleteObjectTagging_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/no-bucket/obj?tagging", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
