package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
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
	sum := md5.Sum(rawKey) //nolint:gosec // MD5 mandated by SSE-C
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
	wrongSum := md5.Sum(wrongKey) //nolint:gosec
	wrongMD5 := base64.StdEncoding.EncodeToString(wrongSum[:])

	req = httptest.NewRequest(http.MethodGet, "/ssec-bkt/c-obj", nil)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key", wrongB64)
	req.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5", wrongMD5)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.NotEqual(t, http.StatusOK, rec.Code, "wrong-key GET must not return plaintext")
}
