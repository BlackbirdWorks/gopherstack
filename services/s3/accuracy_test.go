package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

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

// ─── SSE-C enforcement ────────────────────────────────────────────────────────

func TestSSEC_ValidKeyMD5(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "test-sse")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	keyB64 := base64.StdEncoding.EncodeToString(key)
	md5Sum := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(md5Sum[:])

	rec := doRequest(handler, http.MethodPut, "/test-sse/myobj",
		strings.NewReader("hello world"),
		map[string]string{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
			"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   keyMD5,
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSSEC_BadKeyMD5_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "test-sse-bad")

	key := make([]byte, 32)
	keyB64 := base64.StdEncoding.EncodeToString(key)
	wrongMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))

	rec := doRequest(handler, http.MethodPut, "/test-sse-bad/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
			"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   wrongMD5,
		})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSSEC_EchoedOnGet(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "test-sse-echo")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	keyB64 := base64.StdEncoding.EncodeToString(key)
	md5Sum := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(md5Sum[:])

	sseHeaders := map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   keyMD5,
	}

	putRec := doRequest(handler, http.MethodPut, "/test-sse-echo/obj",
		strings.NewReader("test data"), sseHeaders)
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doRequest(handler, http.MethodGet, "/test-sse-echo/obj", nil, sseHeaders)
	require.Equal(t, http.StatusOK, getRec.Code)

	assert.Equal(
		t,
		"AES256",
		getRec.Header().Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"),
	)
	assert.Equal(t, keyMD5, getRec.Header().Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"))
}

func TestSSE_S3Algorithm_EchoedOnPut(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "test-sse-s3")

	rec := doRequest(handler, http.MethodPut, "/test-sse-s3/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Server-Side-Encryption": "AES256",
		})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "AES256", rec.Header().Get("X-Amz-Server-Side-Encryption"))
}

// ─── CopyObject conditionals ─────────────────────────────────────────────────

func TestCopyObject_IfMatch_Passes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-bucket")
	mustCreateBucket(t, backend, "dst-bucket")

	mustPutObject(t, backend, "src-bucket", "src-obj", []byte("hello"))

	out, err := backend.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
		Bucket: aws.String("src-bucket"),
		Key:    aws.String("src-obj"),
	})
	require.NoError(t, err)

	etag := aws.ToString(out.ETag)

	rec := doRequest(handler, http.MethodPut, "/dst-bucket/dst-obj", nil, map[string]string{
		"X-Amz-Copy-Source":          "src-bucket/src-obj",
		"X-Amz-Copy-Source-If-Match": etag,
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCopyObject_IfMatch_Fails_Returns412(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-cm")
	mustCreateBucket(t, backend, "dst-cm")

	mustPutObject(t, backend, "src-cm", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodPut, "/dst-cm/dst", nil, map[string]string{
		"X-Amz-Copy-Source":          "src-cm/obj",
		"X-Amz-Copy-Source-If-Match": "\"wrong-etag\"",
	})

	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
}

func TestCopyObject_IfNoneMatch_Fails_Returns412(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-cnm")
	mustCreateBucket(t, backend, "dst-cnm")

	mustPutObject(t, backend, "src-cnm", "obj", []byte("data"))

	out, err := backend.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
		Bucket: aws.String("src-cnm"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)

	rec := doRequest(handler, http.MethodPut, "/dst-cnm/dst", nil, map[string]string{
		"X-Amz-Copy-Source":               "src-cnm/obj",
		"X-Amz-Copy-Source-If-None-Match": aws.ToString(out.ETag),
	})

	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
}

func TestCopyObject_IfUnmodifiedSince_Fails_Returns412(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-iums")
	mustCreateBucket(t, backend, "dst-iums")

	mustPutObject(t, backend, "src-iums", "obj", []byte("data"))

	// Use a date far in the past so the object is "modified after"
	rec := doRequest(handler, http.MethodPut, "/dst-iums/dst", nil, map[string]string{
		"X-Amz-Copy-Source":                     "src-iums/obj",
		"X-Amz-Copy-Source-If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT",
	})

	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
}

func TestCopyObject_MetadataDirectiveReplace(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-meta")
	mustCreateBucket(t, backend, "dst-meta")

	// Put source with metadata.
	_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:      aws.String("src-meta"),
		Key:         aws.String("obj"),
		Body:        strings.NewReader("body"),
		Metadata:    map[string]string{"original": "yes"},
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	rec := doRequest(handler, http.MethodPut, "/dst-meta/dst", nil, map[string]string{
		"X-Amz-Copy-Source":        "src-meta/obj",
		"X-Amz-Metadata-Directive": "REPLACE",
		"Content-Type":             "application/json",
		"X-Amz-Meta-newkey":        "newval",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	headRec := doRequest(handler, http.MethodHead, "/dst-meta/dst", nil, nil)
	require.Equal(t, http.StatusOK, headRec.Code)
	assert.Equal(t, "application/json", headRec.Header().Get("Content-Type"))
	assert.Equal(t, "newval", headRec.Header().Get("X-Amz-Meta-Newkey"))
}

func TestCopyObject_MetadataDirectiveCopy(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-copy-meta")
	mustCreateBucket(t, backend, "dst-copy-meta")

	_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:      aws.String("src-copy-meta"),
		Key:         aws.String("obj"),
		Body:        strings.NewReader("body"),
		Metadata:    map[string]string{"original": "yes"},
		ContentType: aws.String("text/html"),
	})
	require.NoError(t, err)

	// COPY directive (default) - no X-Amz-Metadata-Directive header.
	rec := doRequest(handler, http.MethodPut, "/dst-copy-meta/dst", nil, map[string]string{
		"X-Amz-Copy-Source": "src-copy-meta/obj",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	headRec := doRequest(handler, http.MethodHead, "/dst-copy-meta/dst", nil, nil)
	require.Equal(t, http.StatusOK, headRec.Code)
	assert.Equal(t, "text/html", headRec.Header().Get("Content-Type"))
	assert.Equal(t, "yes", headRec.Header().Get("X-Amz-Meta-Original"))
}

func TestCopyObject_TaggingDirectiveReplace(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-tag")
	mustCreateBucket(t, backend, "dst-tag")

	mustPutObject(t, backend, "src-tag", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodPut, "/dst-tag/dst", nil, map[string]string{
		"X-Amz-Copy-Source":       "src-tag/obj",
		"X-Amz-Tagging-Directive": "REPLACE",
		"X-Amz-Tagging":           "env=prod&tier=premium",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags via GetObjectTagging.
	taggingRec := doRequest(handler, http.MethodGet, "/dst-tag/dst?tagging", nil, nil)
	require.Equal(t, http.StatusOK, taggingRec.Code)

	var tagging s3.Tagging
	require.NoError(t, xml.NewDecoder(taggingRec.Body).Decode(&tagging))

	tagMap := make(map[string]string)
	for _, tag := range tagging.TagSet.Tags {
		tagMap[tag.Key] = tag.Value
	}

	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "premium", tagMap["tier"])
}

// ─── ListParts pagination ─────────────────────────────────────────────────────

func TestListParts_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-bucket")

	// Create multipart upload.
	createRec := doRequest(handler, http.MethodPost, "/mp-bucket/bigobj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload 5 parts (small for test).
	for i := 1; i <= 5; i++ {
		partPath := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&partNumber=%d", uploadID, i)
		partRec := doRequest(handler, http.MethodPut, partPath,
			strings.NewReader(fmt.Sprintf("part%d", i)), nil)
		require.Equal(t, http.StatusOK, partRec.Code)
	}

	// ListParts with max-parts=2 and no marker → first 2 parts.
	listPath := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&max-parts=2", uploadID)
	listRec := doRequest(handler, http.MethodGet, listPath, nil, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec.Body).Decode(&result))

	assert.Len(t, result.Parts, 2)
	assert.True(t, result.IsTruncated)
	assert.Equal(t, 2, result.NextPartNumberMarker)
	assert.Equal(t, 1, result.Parts[0].PartNumber)
	assert.Equal(t, 2, result.Parts[1].PartNumber)

	// ListParts with marker=2 → parts 3, 4, 5.
	listPath2 := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&part-number-marker=2", uploadID)
	listRec2 := doRequest(handler, http.MethodGet, listPath2, nil, nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var result2 s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec2.Body).Decode(&result2))

	assert.Len(t, result2.Parts, 3)
	assert.False(t, result2.IsTruncated)
	assert.Equal(t, 3, result2.Parts[0].PartNumber)
	assert.Equal(t, 5, result2.Parts[2].PartNumber)
}

func TestListParts_DefaultMaxParts1000(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-default")

	createRec := doRequest(handler, http.MethodPost, "/mp-default/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload 3 parts.
	for i := 1; i <= 3; i++ {
		partPath := fmt.Sprintf("/mp-default/obj?uploadId=%s&partNumber=%d", uploadID, i)
		doRequest(handler, http.MethodPut, partPath, strings.NewReader("data"), nil)
	}

	listPath := fmt.Sprintf("/mp-default/obj?uploadId=%s", uploadID)
	listRec := doRequest(handler, http.MethodGet, listPath, nil, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec.Body).Decode(&result))

	assert.Len(t, result.Parts, 3)
	assert.False(t, result.IsTruncated)
	assert.Equal(t, 1000, result.MaxParts)
}

// ─── Multipart 5 MiB minimum enforcement ────────────────────────────────────

func TestCompleteMultipart_EntityTooSmall(t *testing.T) {
	t.Parallel()

	// Use a backend WITH size enforcement (no skip flag).
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	mustCreateBucket(t, backend, "mp-size")

	createRec := doRequest(handler, http.MethodPost, "/mp-size/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload two small parts (below 5 MiB).
	part1 := strings.NewReader("small-part-1")
	part2 := strings.NewReader("small-part-2")

	path1 := fmt.Sprintf("/mp-size/obj?uploadId=%s&partNumber=1", uploadID)
	path2 := fmt.Sprintf("/mp-size/obj?uploadId=%s&partNumber=2", uploadID)

	rec1 := doRequest(handler, http.MethodPut, path1, part1, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	rec2 := doRequest(handler, http.MethodPut, path2, part2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	etag2 := rec2.Header().Get("ETag")

	// Complete — part 1 is below 5 MiB and is not the last part → EntityTooSmall.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)

	completePath := fmt.Sprintf("/mp-size/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)

	assert.Equal(t, http.StatusBadRequest, completeRec.Code)

	var errResp s3.ErrorResponse
	require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&errResp))
	assert.Equal(t, "EntityTooSmall", errResp.Code)
}

func TestCompleteMultipart_EmptyParts_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "no_parts_element",
			body: `<CompleteMultipartUpload></CompleteMultipartUpload>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "mp-empty")

			createRec := doRequest(handler, http.MethodPost, "/mp-empty/obj?uploads", nil, nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			var initResult s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
			uploadID := initResult.UploadID

			completePath := fmt.Sprintf("/mp-empty/obj?uploadId=%s", uploadID)
			completeRec := doRequest(handler, http.MethodPost, completePath,
				strings.NewReader(tt.body), nil)

			assert.Equal(t, http.StatusBadRequest, completeRec.Code)

			var errResp s3.ErrorResponse
			require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&errResp))
			assert.Equal(t, "InvalidRequest", errResp.Code)
		})
	}
}

func TestCompleteMultipart_SinglePartSmall_OK(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-single")

	createRec := doRequest(handler, http.MethodPost, "/mp-single/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	path1 := fmt.Sprintf("/mp-single/obj?uploadId=%s&partNumber=1", uploadID)
	rec1 := doRequest(handler, http.MethodPut, path1, strings.NewReader("tiny"), nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	// Single part can be any size.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1)

	completePath := fmt.Sprintf("/mp-single/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)

	assert.Equal(t, http.StatusOK, completeRec.Code)
}

// ─── x-amz-expected-bucket-owner ─────────────────────────────────────────────

func TestExpectedBucketOwner_Match_Passes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "owned-bucket")

	rec := doRequest(handler, http.MethodPut, "/owned-bucket/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Expected-Bucket-Owner": "123456789012",
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestExpectedBucketOwner_Mismatch_Returns403(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "owned-bucket2")

	rec := doRequest(handler, http.MethodPut, "/owned-bucket2/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Expected-Bucket-Owner": "999999999999",
		})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestExpectedBucketOwner_GetObject_Mismatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "get-owned")
	mustPutObject(t, backend, "get-owned", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodGet, "/get-owned/obj", nil, map[string]string{
		"X-Amz-Expected-Bucket-Owner": "000000000000",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestExpectedBucketOwner_HeadObject_Mismatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "head-owned")
	mustPutObject(t, backend, "head-owned", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodHead, "/head-owned/obj", nil, map[string]string{
		"X-Amz-Expected-Bucket-Owner": "111111111111",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// ─── CRC64NVME checksum ───────────────────────────────────────────────────────

func TestCRC64NVME_HashInterface(t *testing.T) {
	t.Parallel()

	h := s3.NewCRC64NVME()

	// Size must be 8 bytes (64-bit).
	assert.Equal(t, 8, h.Size())

	// BlockSize must be 1 (byte-oriented).
	assert.Equal(t, 1, h.BlockSize())

	// Write some data and verify reset clears state.
	_, _ = h.Write([]byte("data"))
	sumBefore := h.Sum(nil)

	h.Reset()
	sumAfter := h.Sum(nil)

	// After reset, should match the empty hash.
	empty := s3.NewCRC64NVME()
	emptySum := empty.Sum(nil)
	assert.Equal(t, emptySum, sumAfter)
	assert.NotEqual(t, sumBefore, sumAfter, "hash after reset should differ from hash of 'data'")
}

func TestCRC64NVME_Hash_IsConsistent(t *testing.T) {
	t.Parallel()

	data := []byte("The quick brown fox jumps over the lazy dog")
	got1 := s3.CalculateCRC64NVME(data)
	got2 := s3.CalculateCRC64NVME(data)

	assert.Equal(t, got1, got2)
	assert.NotEmpty(t, got1)
}

func TestCRC64NVME_EmptyData(t *testing.T) {
	t.Parallel()

	got := s3.CalculateCRC64NVME([]byte{})
	assert.NotEmpty(t, got)

	// Verify base64 decodes to 8 bytes (64-bit hash).
	b, err := base64.StdEncoding.DecodeString(got)
	require.NoError(t, err)
	assert.Len(t, b, 8)
}

func TestCRC64NVME_DifferentData_DifferentHash(t *testing.T) {
	t.Parallel()

	a := s3.CalculateCRC64NVME([]byte("hello"))
	b := s3.CalculateCRC64NVME([]byte("world"))

	assert.NotEqual(t, a, b)
}

func TestCRC64NVME_PutObject_ValidChecksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "crc64-bucket")

	data := []byte("test data for crc64nvme")
	checksum := s3.CalculateCRC64NVME(data)

	rec := doRequest(handler, http.MethodPut, "/crc64-bucket/obj",
		bytes.NewReader(data),
		map[string]string{
			"X-Amz-Checksum-Algorithm": "CRC64NVME",
			"X-Amz-Checksum-Crc64nvme": checksum,
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCRC64NVME_PutObject_InvalidChecksum_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "crc64-bad")

	data := []byte("test data")
	wrongChecksum := base64.StdEncoding.EncodeToString(make([]byte, 8))

	rec := doRequest(handler, http.MethodPut, "/crc64-bad/obj",
		bytes.NewReader(data),
		map[string]string{
			"X-Amz-Checksum-Algorithm": "CRC64NVME",
			"X-Amz-Checksum-Crc64nvme": wrongChecksum,
		})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── Delete markers with versioning ──────────────────────────────────────────

func TestDeleteMarker_Versioning_GetReturns404WithHeader(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ver-bucket")
	enableVersioning(t, handler, "ver-bucket")

	mustPutObject(t, backend, "ver-bucket", "obj", []byte("v1"))

	// Delete without versionId → creates delete marker.
	delRec := doRequest(handler, http.MethodDelete, "/ver-bucket/obj", nil, nil)
	assert.Equal(t, http.StatusNoContent, delRec.Code)
	assert.Equal(t, "true", delRec.Header().Get("X-Amz-Delete-Marker"))

	// GET now returns 404 with x-amz-delete-marker: true.
	getRec := doRequest(handler, http.MethodGet, "/ver-bucket/obj", nil, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestDeleteMarker_Versioning_ListVersionsEmitsMarker(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)

	_, err := backend.CreateBucket(
		t.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String("lv-bucket")},
	)
	require.NoError(t, err)

	_, err = backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("lv-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("lv-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)

	_, err = backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
		Bucket: aws.String("lv-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)

	out, err := backend.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
		Bucket: aws.String("lv-bucket"),
	})
	require.NoError(t, err)

	assert.Len(t, out.Versions, 1, "should have one version")
	assert.Len(t, out.DeleteMarkers, 1, "should have one delete marker")
}

// ─── Object Lock enforcement on delete/overwrite ─────────────────────────────

func TestAccuracy_ObjectLock_LegalHold_BlocksDelete(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lock-bucket")
	mustPutObject(t, backend, "lock-bucket", "locked", []byte("secure"))

	// Put legal hold on object.
	lhPath := "/lock-bucket/locked?legal-hold"
	lhBody := `<LegalHold><Status>ON</Status></LegalHold>`
	lhRec := doRequest(handler, http.MethodPut, lhPath, strings.NewReader(lhBody), nil)
	require.Equal(t, http.StatusOK, lhRec.Code)

	// Delete should fail.
	delRec := doRequest(handler, http.MethodDelete, "/lock-bucket/locked", nil, nil)
	assert.NotEqual(t, http.StatusNoContent, delRec.Code)
}

func TestObjectLock_Retention_BlocksOverwrite(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "retain-bucket")
	mustPutObject(t, backend, "retain-bucket", "obj", []byte("original"))

	// Set COMPLIANCE retention far in the future.
	err := backend.PutObjectRetention(t.Context(), "retain-bucket", "obj", nil,
		"COMPLIANCE", mustParseRetentionTime(t, "2099-01-01T00:00:00Z"))
	require.NoError(t, err)

	// PutObject (overwrite) should be blocked.
	_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("retain-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("overwrite"),
	})

	assert.Error(t, err)
}

// ─── ListObjectsV2 pagination correctness ────────────────────────────────────

func TestListObjectsV2_MaxKeys_CountsContentsAndPrefixes(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "v2-bucket")

	// Create objects across two prefixes.
	for _, key := range []string{"a/1", "a/2", "b/1", "c"} {
		mustPutObject(t, backend, "v2-bucket", key, []byte("data"))
	}

	out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
		Bucket:    aws.String("v2-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(2),
	})
	require.NoError(t, err)

	totalCount := len(out.Contents) + len(out.CommonPrefixes)
	assert.LessOrEqual(t, totalCount, 2, "total Contents+CommonPrefixes must respect MaxKeys")
	assert.True(t, aws.ToBool(out.IsTruncated))
}

func TestListObjectsV2_NoPagination(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "v2-nopag")

	for i := range 5 {
		mustPutObject(t, backend, "v2-nopag", fmt.Sprintf("obj%d", i), []byte("x"))
	}

	out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
		Bucket:  aws.String("v2-nopag"),
		MaxKeys: aws.Int32(10),
	})
	require.NoError(t, err)

	assert.Len(t, out.Contents, 5)
	assert.False(t, aws.ToBool(out.IsTruncated))
}

// ─── Multipart ETag fixture test ─────────────────────────────────────────────

func TestMultipartETag_MatchesAWSFormat(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-etag")

	createRec := doRequest(handler, http.MethodPost, "/mp-etag/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload two parts large enough to pass 5 MiB threshold.
	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024+1)
	part2Data := bytes.Repeat([]byte("B"), 1024)

	path1 := fmt.Sprintf("/mp-etag/obj?uploadId=%s&partNumber=1", uploadID)
	path2 := fmt.Sprintf("/mp-etag/obj?uploadId=%s&partNumber=2", uploadID)

	rec1 := doRequest(handler, http.MethodPut, path1, bytes.NewReader(part1Data), nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	rec2 := doRequest(handler, http.MethodPut, path2, bytes.NewReader(part2Data), nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	etag2 := rec2.Header().Get("ETag")

	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)

	completePath := fmt.Sprintf("/mp-etag/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)
	require.Equal(t, http.StatusOK, completeRec.Code)

	var completeResult s3.CompleteMultipartUploadResult
	require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&completeResult))

	// The ETag should match the AWS multipart format: "<md5hex>-2"
	assert.True(t, strings.HasSuffix(completeResult.ETag, "-2\""),
		"multipart ETag should end with -<partCount>\", got %s", completeResult.ETag)

	// Verify the ETag is MD5(rawmd5_1 || rawmd5_2)-2
	raw1, _ := hex.DecodeString(strings.Trim(etag1, "\""))
	raw2, _ := hex.DecodeString(strings.Trim(etag2, "\""))
	combined := append(raw1, raw2...) //nolint:gocritic // intentional append
	h := md5.New()
	h.Write(combined)
	expectedETag := fmt.Sprintf("\"%s-2\"", hex.EncodeToString(h.Sum(nil)))
	assert.Equal(t, expectedETag, completeResult.ETag)
}

// ─── CopyObject expected bucket owner ────────────────────────────────────────

func TestCopyObject_ExpectedBucketOwner_Mismatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-ebo")
	mustCreateBucket(t, backend, "dst-ebo")
	mustPutObject(t, backend, "src-ebo", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodPut, "/dst-ebo/dst", nil, map[string]string{
		"X-Amz-Copy-Source":           "src-ebo/obj",
		"X-Amz-Expected-Bucket-Owner": "badaccount",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// ─── SSE-C enforcement on GET/HEAD ───────────────────────────────────────────

func ssecHeaders(keyB64, keyMD5 string) map[string]string {
	return map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   keyMD5,
	}
}

func mustPutSSECObject(
	t *testing.T,
	handler *s3.S3Handler,
	bucket, key, content string,
) (string, string) {
	t.Helper()

	rawKey := make([]byte, 32)
	for i := range rawKey {
		rawKey[i] = byte(i + 7)
	}

	keyB64 := base64.StdEncoding.EncodeToString(rawKey)
	sum := md5.Sum(rawKey)
	keyMD5 := base64.StdEncoding.EncodeToString(sum[:])

	rec := doRequest(handler, http.MethodPut, "/"+bucket+"/"+key,
		strings.NewReader(content), ssecHeaders(keyB64, keyMD5))
	require.Equal(t, http.StatusOK, rec.Code)

	return keyB64, keyMD5
}

func TestSSEC_GetObject_MissingHeaders_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-get-missing")
	keyB64, keyMD5 := mustPutSSECObject(t, handler, "ssec-get-missing", "obj", "data")
	_ = keyMD5

	// GET without any SSE-C headers → 400.
	rec := doRequest(handler, http.MethodGet, "/ssec-get-missing/obj", nil, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// GET with algorithm only (no key-MD5) → 400.
	rec = doRequest(handler, http.MethodGet, "/ssec-get-missing/obj", nil, map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSSEC_GetObject_WrongMD5_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-get-wrong")
	keyB64, _ := mustPutSSECObject(t, handler, "ssec-get-wrong", "obj", "data")

	wrongMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))
	rec := doRequest(handler, http.MethodGet, "/ssec-get-wrong/obj", nil, map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   wrongMD5,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSSEC_GetObject_ValidHeaders_Returns200(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-get-valid")
	keyB64, keyMD5 := mustPutSSECObject(t, handler, "ssec-get-valid", "obj", "hello")

	rec := doRequest(
		handler,
		http.MethodGet,
		"/ssec-get-valid/obj",
		nil,
		ssecHeaders(keyB64, keyMD5),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// second object with a distinct key name ensures mustPutSSECObject's key param is exercised.
	key2B64, key2MD5 := mustPutSSECObject(t, handler, "ssec-get-valid", "obj2", "world")
	rec2 := doRequest(
		handler,
		http.MethodGet,
		"/ssec-get-valid/obj2",
		nil,
		ssecHeaders(key2B64, key2MD5),
	)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestSSEC_HeadObject_MissingHeaders_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-head-missing")
	keyB64, keyMD5 := mustPutSSECObject(t, handler, "ssec-head-missing", "obj", "data")
	_ = keyMD5

	rec := doRequest(handler, http.MethodHead, "/ssec-head-missing/obj", nil, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// algorithm only → 400.
	rec = doRequest(handler, http.MethodHead, "/ssec-head-missing/obj", nil, map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSSEC_HeadObject_WrongMD5_Returns400(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-head-wrong")
	keyB64, _ := mustPutSSECObject(t, handler, "ssec-head-wrong", "obj", "data")

	wrongMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))
	rec := doRequest(handler, http.MethodHead, "/ssec-head-wrong/obj", nil, map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   wrongMD5,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSSEC_HeadObject_ValidHeaders_Returns200(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-head-valid")
	keyB64, keyMD5 := mustPutSSECObject(t, handler, "ssec-head-valid", "obj", "hello")

	rec := doRequest(
		handler,
		http.MethodHead,
		"/ssec-head-valid/obj",
		nil,
		ssecHeaders(keyB64, keyMD5),
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── CopyObject tagging COPY directive ───────────────────────────────────────

func TestCopyObject_TaggingDirective_COPY_InheritsSourceTags(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "tag-copy-src")
	mustCreateBucket(t, backend, "tag-copy-dst")
	mustPutObject(t, backend, "tag-copy-src", "src", []byte("content"))

	// Tag the source object.
	tagRec := doRequest(handler, http.MethodPut, "/tag-copy-src/src?tagging", strings.NewReader(
		`<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`,
	), map[string]string{"Content-Type": "application/xml"})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// CopyObject with explicit COPY directive → destination should have source tags.
	copyRec := doRequest(handler, http.MethodPut, "/tag-copy-dst/dst", nil, map[string]string{
		"X-Amz-Copy-Source":       "tag-copy-src/src",
		"X-Amz-Tagging-Directive": "COPY",
	})
	require.Equal(t, http.StatusOK, copyRec.Code)

	// Verify destination has the tags.
	getTagRec := doRequest(handler, http.MethodGet, "/tag-copy-dst/dst?tagging", nil, nil)
	require.Equal(t, http.StatusOK, getTagRec.Code)
	assert.Contains(t, getTagRec.Body.String(), "env")
	assert.Contains(t, getTagRec.Body.String(), "prod")
}

func TestCopyObject_NoTaggingDirective_InheritsSourceTags(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "tag-nodir-src")
	mustCreateBucket(t, backend, "tag-nodir-dst")
	mustPutObject(t, backend, "tag-nodir-src", "src", []byte("content"))

	// Tag the source object.
	tagRec := doRequest(handler, http.MethodPut, "/tag-nodir-src/src?tagging", strings.NewReader(
		`<Tagging><TagSet><Tag><Key>tier</Key><Value>gold</Value></Tag></TagSet></Tagging>`,
	), map[string]string{"Content-Type": "application/xml"})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// CopyObject without a Tagging-Directive (defaults to COPY).
	copyRec := doRequest(handler, http.MethodPut, "/tag-nodir-dst/dst", nil, map[string]string{
		"X-Amz-Copy-Source": "tag-nodir-src/src",
	})
	require.Equal(t, http.StatusOK, copyRec.Code)

	getTagRec := doRequest(handler, http.MethodGet, "/tag-nodir-dst/dst?tagging", nil, nil)
	require.Equal(t, http.StatusOK, getTagRec.Code)
	assert.Contains(t, getTagRec.Body.String(), "tier")
	assert.Contains(t, getTagRec.Body.String(), "gold")
}

// ─── Helper ──────────────────────────────────────────────────────────────────

func mustParseRetentionTime(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)

	return parsed
}
