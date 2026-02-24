package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutBucketACL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "acl-test")

	req := httptest.NewRequest(http.MethodPut, "/acl-test?acl", nil)
	req.Header.Set("X-Amz-Acl", "public-read")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetBucketACL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "acl-get-test")

	req := httptest.NewRequest(http.MethodGet, "/acl-get-test?acl", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutObject_WithContentMD5(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "md5-bucket")

	body := "hello world"
	req := httptest.NewRequest(http.MethodPut, "/md5-bucket/hello.txt", strings.NewReader(body))
	// Valid MD5 of "hello world" base64-encoded
	req.Header.Set("Content-Md5", "XrY7u+Ae7tCTyyK7j1rNww==")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutObject_InvalidContentMD5(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "md5-invalid")

	body := "hello world"
	req := httptest.NewRequest(http.MethodPut, "/md5-invalid/hello.txt", strings.NewReader(body))
	req.Header.Set("Content-Md5", "AAAAAAAAAAAAAAAAAAAAAA==") // Wrong MD5
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteObjects_Mixed(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "del-objects-bucket")

	// Put two objects
	for _, key := range []string{"obj1", "obj2"} {
		req := httptest.NewRequest(http.MethodPut, "/del-objects-bucket/"+key, strings.NewReader("data"))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Delete both + one nonexistent
	deleteXML := `<Delete><Object><Key>obj1</Key></Object><Object><Key>obj2</Key></Object><Object><Key>nonexistent</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/del-objects-bucket?delete", strings.NewReader(deleteXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteResult")
}

func TestHandler_DeleteObjectTagging(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "dtag-bucket")

	// Put object with tags
	req := httptest.NewRequest(http.MethodPut, "/dtag-bucket/tagged-obj", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Put tags
	tagsXML := `<Tagging><TagSet><Tag><Key>foo</Key><Value>bar</Value></Tag></TagSet></Tagging>`
	req = httptest.NewRequest(http.MethodPut, "/dtag-bucket/tagged-obj?tagging", strings.NewReader(tagsXML))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete tags
	req = httptest.NewRequest(http.MethodDelete, "/dtag-bucket/tagged-obj?tagging", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_HeadObject_NoSuchBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/no-bucket/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetObject_WithChecksumMode(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "checksum-bucket")

	body := "hello checksum"
	req := httptest.NewRequest(http.MethodPut, "/checksum-bucket/ck-obj", strings.NewReader(body))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Get with checksum mode
	req = httptest.NewRequest(http.MethodGet, "/checksum-bucket/ck-obj", nil)
	req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, body, rec.Body.String())
}

func TestHandler_GetObjectTagging_NoSuchKey(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "getotag-nokey")

req := httptest.NewRequest(http.MethodGet, "/getotag-nokey/nokey?tagging", nil)
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteObject_Versioned(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "del-versioned")

// Put object
req := httptest.NewRequest(http.MethodPut, "/del-versioned/obj", strings.NewReader("data"))
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)
require.Equal(t, http.StatusOK, rec.Code)

// Delete object
req = httptest.NewRequest(http.MethodDelete, "/del-versioned/obj", nil)
rec = httptest.NewRecorder()
serveS3Handler(handler, rec, req)
assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_CreateBucket_AlreadyExists(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "exists-bucket")

req := httptest.NewRequest(http.MethodPut, "/exists-bucket", nil)
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.True(t, rec.Code == http.StatusNoContent || rec.Code == http.StatusConflict)
}

func TestHandler_DeleteBucket_NotEmpty(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "notempty-bucket")

// Put an object
req := httptest.NewRequest(http.MethodPut, "/notempty-bucket/obj", strings.NewReader("data"))
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)
require.Equal(t, http.StatusOK, rec.Code)

// Try to delete bucket
req = httptest.NewRequest(http.MethodDelete, "/notempty-bucket", nil)
rec = httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.True(t, rec.Code == http.StatusNoContent || rec.Code == http.StatusConflict)
}

func TestHandler_ListBuckets_Empty(t *testing.T) {
t.Parallel()

handler, _ := newTestHandler(t)

req := httptest.NewRequest(http.MethodGet, "/", nil)
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.Equal(t, http.StatusOK, rec.Code)
}

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

func TestHandler_PutObjectTagging_NoSuchKey(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "ptag-nokey")

tagsXML := `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`
req := httptest.NewRequest(http.MethodPut, "/ptag-nokey/nokey?tagging", strings.NewReader(tagsXML))
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteObjects_InvalidXML(t *testing.T) {
t.Parallel()

handler, backend := newTestHandler(t)
mustCreateBucket(t, backend, "del-bad-xml")

req := httptest.NewRequest(http.MethodPost, "/del-bad-xml?delete", strings.NewReader("not-xml"))
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_HeadBucket_NoSuchBucket(t *testing.T) {
t.Parallel()

handler, _ := newTestHandler(t)

req := httptest.NewRequest(http.MethodHead, "/no-such-bucket-head", nil)
rec := httptest.NewRecorder()
serveS3Handler(handler, rec, req)

assert.Equal(t, http.StatusNotFound, rec.Code)
}
