package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestSSE_S3_RoundTripEncryptsAtRest verifies that an SSE-S3 PUT actually
// stores ciphertext (not plaintext) and that GET returns the original bytes.
// This goes beyond LocalStack, which only echoes the SSE metadata.
func TestSSE_S3_RoundTripEncryptsAtRest(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "sse-bkt")

	plaintext := []byte("super secret payload that should not be visible at rest")

	req := httptest.NewRequest(http.MethodPut, "/sse-bkt/secret.txt", bytes.NewReader(plaintext))
	req.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// At-rest check: read the raw stored bytes and assert they don't contain
	// the plaintext substring. With AES-256-GCM the ciphertext is
	// indistinguishable from random, so the substring will be absent.
	stored := s3.PeekStoredBytes(backend, "sse-bkt", "secret.txt")
	require.NotEmpty(t, stored, "expected stored bytes")
	require.NotContains(t, string(stored), "super secret payload",
		"plaintext should not appear in stored bytes after SSE-S3 encryption")

	// Round-trip: GET returns the original plaintext.
	out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("sse-bkt"),
		Key:    aws.String("secret.txt"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	require.Equal(t, plaintext, got)
}

// TestSSE_S3_SurvivesSnapshotRestore is a regression test for the persistence
// data-loss bug where StoredObjectVersion.EncryptionDEK / EncryptionNonce were
// tagged json:"-": the ciphertext persisted but its key did not, so every
// SSE-S3/SSE-KMS object became permanently undecryptable after a restart. It
// PUTs an SSE-S3 object, snapshots the backend, restores into a fresh backend,
// and asserts the object still decrypts to the original plaintext.
func TestSSE_S3_SurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "sse-persist")

	plaintext := []byte("payload that must survive a restart intact")
	req := httptest.NewRequest(http.MethodPut, "/sse-persist/k.txt", bytes.NewReader(plaintext))
	req.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	require.NoError(t, fresh.Restore(t.Context(), snap))

	out, err := fresh.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("sse-persist"),
		Key:    aws.String("k.txt"),
	})
	require.NoError(t, err, "restored SSE object must remain decryptable")
	got, _ := io.ReadAll(out.Body)
	require.Equal(t, plaintext, got)
}

// TestSSE_S3_MultipartEncryptsAtRest verifies that a multipart upload created
// with SSE-S3 produces a stored version whose Data is ciphertext, and that
// CompleteMultipartUpload + GetObject still returns the original concatenation.
func TestSSE_S3_MultipartEncryptsAtRest(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-sse-bkt")

	// 1. CreateMultipartUpload with SSE-S3
	req := httptest.NewRequest(http.MethodPost, "/mp-sse-bkt/big.bin?uploads", nil)
	req.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	uploadID := extractUploadID(t, rec.Body.String())
	require.NotEmpty(t, uploadID)

	// 2. Upload two parts. Each part must be >= 5 MB to satisfy AWS's
	// non-last-part minimum, but we configured the test backend with
	// WithSkipMultipartSizeCheck so smaller parts are accepted.
	part1 := bytes.Repeat([]byte("aaaaaaaaaa"), 100) // 1 KB
	part2 := bytes.Repeat([]byte("bbbbbbbbbb"), 100) // 1 KB

	uploadPart := func(num int, body []byte) string {
		u := "/mp-sse-bkt/big.bin?partNumber=" + strconv.Itoa(num) + "&uploadId=" + uploadID
		r := httptest.NewRequest(http.MethodPut, u, bytes.NewReader(body))
		rr := httptest.NewRecorder()
		serveS3Handler(handler, rr, r)
		require.Equal(t, http.StatusOK, rr.Code)

		return rr.Header().Get("ETag")
	}

	etag1 := uploadPart(1, part1)
	etag2 := uploadPart(2, part2)

	// 3. CompleteMultipartUpload with both parts
	completeBody := "<CompleteMultipartUpload>" +
		"<Part><PartNumber>1</PartNumber><ETag>" + etag1 + "</ETag></Part>" +
		"<Part><PartNumber>2</PartNumber><ETag>" + etag2 + "</ETag></Part>" +
		"</CompleteMultipartUpload>"
	completeURL := "/mp-sse-bkt/big.bin?uploadId=" + uploadID
	req = httptest.NewRequest(http.MethodPost, completeURL, bytes.NewReader([]byte(completeBody)))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// At-rest assertion: the stored body must NOT contain the plaintext
	// fingerprints from either part.
	stored := s3.PeekStoredBytes(backend, "mp-sse-bkt", "big.bin")
	require.NotEmpty(t, stored)
	require.NotContains(t, string(stored), "aaaaaaaaaa")
	require.NotContains(t, string(stored), "bbbbbbbbbb")

	// Round-trip: GET returns the concatenated plaintext.
	out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("mp-sse-bkt"),
		Key:    aws.String("big.bin"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	require.Equal(t, append(append([]byte{}, part1...), part2...), got)
}

// extractUploadID pulls the <UploadId> value out of an InitiateMultipartUpload
// XML response body. We do a literal substring match — adequate for the
// hand-written response above and avoids pulling in a full XML decoder.
func extractUploadID(t *testing.T, body string) string {
	t.Helper()

	const openTag, closeTag = "<UploadId>", "</UploadId>"

	i := strings.Index(body, openTag)
	require.GreaterOrEqual(t, i, 0)

	rest := body[i+len(openTag):]

	j := strings.Index(rest, closeTag)
	require.GreaterOrEqual(t, j, 0)

	return rest[:j]
}

// TestSSE_C_RoundTripRequiresKey verifies that SSE-C encrypts with the
// customer-supplied key and refuses to decrypt for a different key.
func TestSSE_C_RoundTripRequiresKey(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-bkt")

	rawKey := make([]byte, 32)
	for i := range rawKey {
		rawKey[i] = byte(i + 1)
	}
	keyB64 := base64.StdEncoding.EncodeToString(rawKey)
	sum := md5.Sum(rawKey)
	keyMD5 := base64.StdEncoding.EncodeToString(sum[:])

	plaintext := []byte("customer-managed payload")

	// PUT with SSE-C
	req := httptest.NewRequest(http.MethodPut, "/ssec-bkt/c-obj", bytes.NewReader(plaintext))
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key", keyB64)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", keyMD5)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// At-rest is ciphertext (no DEK persisted for SSE-C).
	stored := s3.PeekStoredBytes(backend, "ssec-bkt", "c-obj")
	require.NotContains(t, string(stored), "customer-managed payload")

	// GET with the correct key → plaintext.
	req = httptest.NewRequest(http.MethodGet, "/ssec-bkt/c-obj", nil)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key", keyB64)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", keyMD5)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, plaintext, rec.Body.Bytes())

	// GET with a DIFFERENT key (same MD5 won't match, so handler short-circuits
	// at 400). We test the MD5-mismatch path here; tampering-detection happens
	// at the GCM layer when MD5s coincidentally collide, which we don't
	// exercise.
	wrongKey := make([]byte, 32)
	for i := range wrongKey {
		wrongKey[i] = byte(i + 99)
	}
	wrongB64 := base64.StdEncoding.EncodeToString(wrongKey)
	wrongSum := md5.Sum(wrongKey)
	wrongMD5 := base64.StdEncoding.EncodeToString(wrongSum[:])

	req = httptest.NewRequest(http.MethodGet, "/ssec-bkt/c-obj", nil)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key", wrongB64)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", wrongMD5)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.NotEqual(t, http.StatusOK, rec.Code, "wrong-key GET must not return plaintext")
}
