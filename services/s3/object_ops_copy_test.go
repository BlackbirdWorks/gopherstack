package s3_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
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

func TestHandler_CopyObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "copy object and verify content"},
		{name: "copy with replace metadata"},
		{name: "missing source header routes to put object"},
		{name: "invalid source format returns 400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "src-bkt")
			mustCreateBucket(t, backend, "dest-bkt")
			mustPutObject(t, backend, "src-bkt", "src-key", []byte("copy me"))

			switch tt.name {
			case "copy object and verify content":
				req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
				req.Header.Set("X-Amz-Copy-Source", "/src-bkt/src-key")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/dest-bkt/dest-key", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body, err := io.ReadAll(rec.Body)
				require.NoError(t, err)
				assert.Equal(t, "copy me", string(body))

			case "copy with replace metadata":
				req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
				req.Header.Set("X-Amz-Copy-Source", "/src-bkt/src-key")
				req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
				req.Header.Set("X-Amz-Meta-New", "Value")
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodHead, "/dest-bkt/dest-key", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "Value", rec.Header().Get("X-Amz-Meta-New"))
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			case "missing source header routes to put object":
				req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)

			case "invalid source format returns 400":
				req := httptest.NewRequest(http.MethodPut, "/dest-bkt/dest-key", nil)
				req.Header.Set("X-Amz-Copy-Source", "invalid-format")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestHandler_CopyObject_Errors(t *testing.T) {
	t.Parallel()

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
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "src", []byte("data"))

			req := httptest.NewRequest(http.MethodPut, tt.dest, nil)
			req.Header.Set("X-Amz-Copy-Source", tt.source)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CopyObject_Versioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "copy specific version using version-id header"},
		{name: "copy specific version using versionId query param in source"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "src")
			mustCreateBucket(t, backend, "dest")
			enableVersioning(t, handler, "src")
			mustPutObject(t, backend, "src", "key", []byte("v1"))

			req := httptest.NewRequest(http.MethodPut, "/src/key", strings.NewReader("v2"))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			v2ID := rec.Header().Get("X-Amz-Version-Id")

			switch tt.name {
			case "copy specific version using version-id header":
				req = httptest.NewRequest(http.MethodPut, "/dest/key-v2", nil)
				req.Header.Set("X-Amz-Copy-Source", "/src/key")
				req.Header.Set("X-Amz-Copy-Source-Version-Id", v2ID)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/dest/key-v2", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, "v2", rec.Body.String())

			case "copy specific version using versionId query param in source":
				req = httptest.NewRequest(http.MethodPut, "/dest/dest-key", nil)
				req.Header.Set("X-Amz-Copy-Source", "/src/key?versionId="+v2ID)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestCopyObject_SSEAndChecksum is a table test covering three related
// CopyObject wire-shape fixes:
//   - checksum propagation: CopyObjectResult now includes the destination's
//     checksum, matching real S3's types.CopyObjectResult (ChecksumCRC32/
//     CRC32C/SHA1/SHA256/CRC64NVME alongside ETag/LastModified);
//   - destination SSE-KMS: CopyObject now honors destination-side
//     server-side-encryption headers (independent of whatever encryption, if
//     any, the source object used) and echoes the SSE-KMS response headers
//     exactly like PutObject does;
//   - copy-source SSE-C: copying an SSE-C encrypted source object now fails
//     with 400 InvalidRequest when the caller omits the
//     x-amz-copy-source-server-side-encryption-customer-* headers, and 400
//     BadDigest when the supplied key-MD5 is wrong, instead of the pre-fix
//     behavior where decryptVersionForGet silently handed back ciphertext and
//     the copy "succeeded" with corrupted, unreadable data at the
//     destination; supplying the correct key decrypts correctly.
//
// Fixture buckets/objects (checksum source, plaintext source, SSE-C source)
// are created once and shared read-only across parallel subtests, each of
// which copies to its own distinct destination key.
func TestCopyObject_SSEAndChecksum(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "copy-fixture-src")
	mustCreateBucket(t, backend, "copy-fixture-dst")

	checksumPutRec := doRequest(handler, http.MethodPut, "/copy-fixture-src/checksum-obj",
		strings.NewReader("hello checksum"), map[string]string{
			"X-Amz-Checksum-Algorithm": "CRC32",
		})
	require.Equal(t, http.StatusOK, checksumPutRec.Code)
	wantChecksum := checksumPutRec.Header().Get("X-Amz-Checksum-Crc32")
	require.NotEmpty(t, wantChecksum, "PutObject must have computed a CRC32 checksum")

	mustPutObject(t, backend, "copy-fixture-src", "plain-obj", []byte("unencrypted source"))

	const ssecPlaintext = "sensitive payload"
	ssecKeyB64, ssecKeyMD5 := mustPutSSECObject(t, handler, "copy-fixture-src", "secret-obj", ssecPlaintext)

	tests := []struct {
		check          func(t *testing.T, rec *httptest.ResponseRecorder, destKey string)
		name           string
		destKey        string
		copyHeaders    map[string]string
		wantBodySubstr string
		wantStatus     int
	}{
		{
			name:    "checksum propagated from source to CopyObjectResult",
			destKey: "checksum-copy",
			copyHeaders: map[string]string{
				"X-Amz-Copy-Source": "copy-fixture-src/checksum-obj",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()

				var result s3.CopyObjectResult
				require.NoError(t, xml.NewDecoder(rec.Body).Decode(&result))
				assert.Equal(t, wantChecksum, result.ChecksumCRC32)
			},
		},
		{
			name:    "destination SSE-KMS honored and round-trips",
			destKey: "kms-copy",
			copyHeaders: map[string]string{
				"X-Amz-Copy-Source":                           "copy-fixture-src/plain-obj",
				"X-Amz-Server-Side-Encryption":                "aws:kms",
				"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "arn:aws:kms:us-east-1:000000000000:key/test-key",
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, destKey string) {
				t.Helper()

				assert.Equal(t, "aws:kms", rec.Header().Get("X-Amz-Server-Side-Encryption"))
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test-key",
					rec.Header().Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))

				// The destination object must actually be readable back
				// (round-trips through the KMS-envelope encryption path, not
				// left as plaintext).
				getRec := doRequest(handler, http.MethodGet, "/copy-fixture-dst/"+destKey, nil, nil)
				require.Equal(t, http.StatusOK, getRec.Code)
				assert.Equal(t, "unencrypted source", getRec.Body.String())
				assert.Equal(t, "aws:kms", getRec.Header().Get("X-Amz-Server-Side-Encryption"))
			},
		},
		{
			name:    "SSE-C source without copy-source key rejected",
			destKey: "ssec-no-key-copy",
			copyHeaders: map[string]string{
				"X-Amz-Copy-Source": "copy-fixture-src/secret-obj",
			},
			wantStatus:     http.StatusBadRequest,
			wantBodySubstr: "InvalidRequest",
		},
		{
			name:    "SSE-C source with wrong copy-source key-MD5 rejected",
			destKey: "ssec-wrong-md5-copy",
			copyHeaders: map[string]string{
				"X-Amz-Copy-Source": "copy-fixture-src/secret-obj",
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": "AES256",
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       ssecKeyB64,
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   "bm90dGhlcmlnaHRtZDU=",
			},
			wantStatus:     http.StatusBadRequest,
			wantBodySubstr: "BadDigest",
		},
		{
			name:    "SSE-C source with correct copy-source key decrypts",
			destKey: "ssec-with-key-copy",
			copyHeaders: map[string]string{
				"X-Amz-Copy-Source": "copy-fixture-src/secret-obj",
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": "AES256",
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       ssecKeyB64,
				"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   ssecKeyMD5,
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, _ *httptest.ResponseRecorder, destKey string) {
				t.Helper()

				// Destination itself is unencrypted here, so a plain GET
				// reads back the decrypted plaintext directly.
				getRec := doRequest(handler, http.MethodGet, "/copy-fixture-dst/"+destKey, nil, nil)
				require.Equal(t, http.StatusOK, getRec.Code)
				assert.Equal(t, ssecPlaintext, getRec.Body.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(handler, http.MethodPut, "/copy-fixture-dst/"+tt.destKey, nil, tt.copyHeaders)
			require.Equal(t, tt.wantStatus, rec.Code, "body=%s", rec.Body.String())

			if tt.wantBodySubstr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodySubstr)
			}

			if tt.check != nil {
				tt.check(t, rec, tt.destKey)
			}
		})
	}
}
