package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
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

func newTestHandler(t *testing.T) (*s3.Handler, *s3.InMemoryBackend) {
	t.Helper()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend)

	return handler, backend
}

func mustCreateBucket(t *testing.T, b s3.StorageBackend, bucket string) {
	t.Helper()

	_, err := b.CreateBucket(context.Background(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func mustPutObject(t *testing.T, b s3.StorageBackend, bucket, key string, data []byte) {
	t.Helper()

	_, err := b.PutObject(context.Background(), &sdk_s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
}

func TestHandler_ListBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(context.Context, *s3.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "no buckets",
			setup:   func(_ context.Context, _ *s3.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one bucket",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "test")
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/xml")
		})
	}
}

func TestHandler_CreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(context.Context, *s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:       "create new bucket",
			bucket:     "new-bucket",
			setup:      func(_ context.Context, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusOK,
		},
		{
			name:   "create duplicate bucket",
			bucket: "existing",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "existing")
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(context.Context, *s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:   "delete empty bucket",
			bucket: "my-bucket",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "my-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ context.Context, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "delete non-empty bucket",
			bucket: "full-bucket",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "full-bucket")
				mustPutObject(t, b, "full-bucket", "k", []byte("d"))
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HeadBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(context.Context, *s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:   "existing bucket",
			bucket: "my-bucket",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "my-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ context.Context, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(context.Context, *s3.InMemoryBackend)
		bucket     string
		key        string
		body       string
		wantStatus int
	}{
		{
			name:   "put object success",
			bucket: "bkt",
			key:    "key",
			body:   "hello world",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "put object to non-existent bucket",
			bucket:     "no-bkt",
			key:        "key",
			body:       "data",
			setup:      func(_ context.Context, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+tt.key, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(context.Context, *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "get existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "key", []byte("content"))
			},
			wantStatus: http.StatusOK,
			wantBody:   "content",
		},
		{
			name:   "get non-existent key",
			bucket: "bkt",
			key:    "no-key",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				body, _ := io.ReadAll(rec.Body)
				assert.Equal(t, tt.wantBody, string(body))
			}
		})
	}
}

func TestHandler_DeleteObject(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_HeadObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(context.Context, *s3.InMemoryBackend)
		bucket     string
		key        string
		wantStatus int
	}{
		{
			name:   "existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(testCtx context.Context, b *s3.InMemoryBackend) {
				_, err := b.CreateBucket(testCtx, &sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})
				require.NoError(t, err)

				_, err = b.PutObject(testCtx, &sdk_s3.PutObjectInput{
					Bucket:   aws.String("bkt"),
					Key:      aws.String("key"),
					Body:     bytes.NewReader([]byte("data")),
					Metadata: map[string]string{},
				})
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "non-existent object",
			bucket: "bkt",
			key:    "no-key",
			setup: func(_ context.Context, b *s3.InMemoryBackend) {
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(context.Background(), backend)

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HeadObjectWithMetadata(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Put object with metadata and content-type
	req := httptest.NewRequest(http.MethodPut, "/bkt/meta", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Amz-Meta-Author", "Antigravity")
	req.Header.Set("X-Amz-Meta-Priority", "High")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 2. Head object and verify headers
	req = httptest.NewRequest(http.MethodHead, "/bkt/meta", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
	assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))

	// 3. Get object and verify headers (should also work)
	req = httptest.NewRequest(http.MethodGet, "/bkt/meta", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
	assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))
}

func TestHandler_ObjectTagging(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	// Put tagging
	taggingXML := `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`
	req := httptest.NewRequest(http.MethodPut, "/bkt/key?tagging", strings.NewReader(taggingXML))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get tagging
	req = httptest.NewRequest(http.MethodGet, "/bkt/key?tagging", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod")

	// Delete tagging
	req = httptest.NewRequest(http.MethodDelete, "/bkt/key?tagging", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_BucketVersioning(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Put versioning
	versioningXML := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/bkt?versioning", strings.NewReader(versioningXML))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get versioning
	req = httptest.NewRequest(http.MethodGet, "/bkt?versioning", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var conf s3.VersioningConfiguration
	err := xml.NewDecoder(rec.Body).Decode(&conf)
	require.NoError(t, err)
	assert.Equal(t, "Enabled", conf.Status)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "PATCH on bucket",
			method: http.MethodPatch,
			path:   "/bkt",
		},
		{
			name:   "PATCH on object",
			method: http.MethodPatch,
			path:   "/bkt/key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandler_BucketLocationQuery(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodGet, "/bkt?location", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "LocationConstraint")
}

func TestHandler_BucketTaggingNotImplemented(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	// PUT bucket tagging
	req := httptest.NewRequest(http.MethodPut, "/bkt?tagging", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)

	// GET bucket tagging
	req = httptest.NewRequest(http.MethodGet, "/bkt?tagging", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestHandler_ObjectACLIgnored(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	// PUT ACL
	req := httptest.NewRequest(http.MethodPut, "/bkt/key?acl", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GET ACL
	req = httptest.NewRequest(http.MethodGet, "/bkt/key?acl", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestHandler_ListObjects(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "file1.txt", []byte("a"))
	mustPutObject(t, backend, "bkt", "file2.txt", []byte("b"))

	req := httptest.NewRequest(http.MethodGet, "/bkt", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "file1.txt")
	assert.Contains(t, rec.Body.String(), "file2.txt")
}

func TestHandler_PutBucketVersioning_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	versioningXML := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/no-bucket?versioning", strings.NewReader(versioningXML))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_PutBucketVersioning_InvalidXML(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodPut, "/bkt?versioning", strings.NewReader("not xml"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func enableVersioning(t *testing.T, handler *s3.Handler, bucket string) {
	t.Helper()

	xmlBody := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?versioning", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_VersionedObjectLifecycle(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	enableVersioning(t, handler, "bkt")

	// Put V1
	req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v1 data"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	v1ID := rec.Header().Get("X-Amz-Version-Id")
	assert.NotEmpty(t, v1ID)
	assert.NotEqual(t, s3.NullVersion, v1ID)

	// Put V2
	req = httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v2 data"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	v2ID := rec.Header().Get("X-Amz-Version-Id")
	assert.NotEmpty(t, v2ID)
	assert.NotEqual(t, v1ID, v2ID)

	// Get latest should return V2
	req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "v2 data", string(body))

	// Get V1 by version ID
	req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, _ = io.ReadAll(rec.Body)
	assert.Equal(t, "v1 data", string(body))

	// Delete (creates delete marker)
	req = httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
	dmID := rec.Header().Get("X-Amz-Version-Id")
	assert.NotEmpty(t, dmID)

	// Get latest should now 404
	req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// But specific version V1 still works
	req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutObjectWithTaggingHeader(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("data"))
	req.Header.Set("X-Amz-Tagging", "env=prod&team=alpha")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags were stored
	out, err := backend.GetObjectTagging(
		context.Background(),
		&sdk_s3.GetObjectTaggingInput{Bucket: aws.String("bkt"), Key: aws.String("key")},
	)
	require.NoError(t, err)
	// Output tags are []types.Tag
	assert.Len(t, out.TagSet, 2)
	// Helper to find tag
	getTag := func(key string) string {
		for _, tag := range out.TagSet {
			if *tag.Key == key {
				return *tag.Value
			}
		}

		return ""
	}
	assert.Equal(t, "prod", getTag("env"))
	assert.Equal(t, "alpha", getTag("team"))
}

func TestHandler_PutObjectTagging_InvalidXML(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	req := httptest.NewRequest(http.MethodPut, "/bkt/key?tagging", strings.NewReader("not xml"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetObject_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListObjects_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteObject_NonExistentBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/nonexistent/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func TestHandler_ChecksumSupport(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Put object with SHA256 checksum
	body := "checksum test data"
	req := httptest.NewRequest(http.MethodPut, "/bkt/check", strings.NewReader(body))
	req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	req.Header.Set("X-Amz-Checksum-Sha256", "fake-sha256-value")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify ETag is MD5 (hex encoded)
	expectedHash := md5.Sum([]byte(body))
	expectedETag := "\"" + hex.EncodeToString(expectedHash[:]) + "\""
	assert.Equal(t, expectedETag, rec.Header().Get("ETag"))
	assert.Equal(t, "fake-sha256-value", rec.Header().Get("X-Amz-Checksum-Sha256"))

	// 2. Get object and verify checksum header
	req = httptest.NewRequest(http.MethodGet, "/bkt/check", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "fake-sha256-value", rec.Header().Get("X-Amz-Checksum-Sha256"))
	assert.Equal(
		t,
		"SHA256",
		rec.Header().Get("X-Amz-Checksum-Algorithm"),
	)
}

func TestHandler_VirtualHostedStyle_Final(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	handler.Endpoint = "localhost:9000"
	mustCreateBucket(t, backend, "my-vh-bucket")

	// 1. Valid virtual hosted request
	req := httptest.NewRequest(http.MethodGet, "/somekey", nil)
	req.Host = "my-vh-bucket.localhost:9000"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Bucket should be found, but key is missing
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchKey")

	// 2. Invalid bucket in host fallback to path-style /
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "invalid_bucket.localhost:9000"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Fallback to list buckets (200 OK)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 3. Host doesn't match endpoint fallback to path-style /
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "my-vh-bucket.other:9000"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Fallback to list buckets (200 OK)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ChecksumAlgorithms_Coverage(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	algos := []struct {
		name   string
		header string
	}{
		{name: "CRC32", header: "X-Amz-Checksum-Crc32"},
		{name: "CRC32C", header: "X-Amz-Checksum-Crc32c"},
		{name: "SHA1", header: "X-Amz-Checksum-Sha1"},
	}

	for _, algo := range algos {
		t.Run(algo.name, func(t *testing.T) {
			key := "check-" + algo.name
			req := httptest.NewRequest(http.MethodPut, "/bkt/"+key, strings.NewReader("data"))
			req.Header.Set("X-Amz-Checksum-Algorithm", algo.name)
			req.Header.Set(algo.header, "fake-value")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Get and verify
			req = httptest.NewRequest(http.MethodGet, "/bkt/"+key, nil)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "fake-value", rec.Header().Get(algo.header))
			assert.Equal(t, algo.name, rec.Header().Get("X-Amz-Checksum-Algorithm"))
		})
	}
}

func TestHandler_InvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "invalid bucket name (too short)",
			method:     http.MethodPut,
			path:       "/ab",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (uppercase)",
			method:     http.MethodPut,
			path:       "/InvalidBucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (underscore)",
			method:     http.MethodPut,
			path:       "/my_bucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (starts with hyphen)",
			method:     http.MethodPut,
			path:       "/-bucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (IP address)",
			method:     http.MethodPut,
			path:       "/192.168.1.1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid object key (too long)",
			method:     http.MethodPut,
			path:       "/valid-bucket/" + strings.Repeat("a", 1025),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if strings.Contains(tt.path, "/valid-bucket/") {
				mustCreateBucket(t, backend, "valid-bucket")
			}

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, "case: %s", tt.name)
		})
	}
}

func TestHandler_MultipartUpload(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Initiate
	req := httptest.NewRequest(http.MethodPost, "/bkt/mp?uploads", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var initResp s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(rec.Body).Decode(&initResp))
	uploadID := initResp.UploadID
	assert.NotEmpty(t, uploadID)
	assert.Equal(t, "bkt", initResp.Bucket)
	assert.Equal(t, "mp", initResp.Key)

	// 2. Upload Part 1
	part1Data := "part1"
	req = httptest.NewRequest(http.MethodPut, "/bkt/mp?partNumber=1&uploadId="+uploadID, strings.NewReader(part1Data))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	etag1 := rec.Header().Get("ETag")
	assert.NotEmpty(t, etag1)

	// 3. Upload Part 2
	part2Data := "part2"
	req = httptest.NewRequest(http.MethodPut, "/bkt/mp?partNumber=2&uploadId="+uploadID, strings.NewReader(part2Data))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	etag2 := rec.Header().Get("ETag")
	assert.NotEmpty(t, etag2)

	// 4. Complete
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)
	req = httptest.NewRequest(http.MethodPost, "/bkt/mp?uploadId="+uploadID, strings.NewReader(completeXML))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// 5. Verify object content
	req = httptest.NewRequest(http.MethodGet, "/bkt/mp", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "part1part2", string(body))
}

func TestHandler_MultipartUpload_Errors(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// 1. Upload Part to invalid UploadID
	req := httptest.NewRequest(http.MethodPut, "/bkt/mp?partNumber=1&uploadId=invalid-id", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchUpload")

	// 2. Abort invalid UploadID
	req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId=invalid-id", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// 3. Initiate valid upload
	req = httptest.NewRequest(http.MethodPost, "/bkt/mp?uploads", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var initResp s3.InitiateMultipartUploadResult
	_ = xml.NewDecoder(rec.Body).Decode(&initResp)
	uploadID := initResp.UploadID

	// 4. Complete with invalid part (wrong ETag)
	completeXML := `<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"wrong-etag"</ETag></Part>
	</CompleteMultipartUpload>`
	req = httptest.NewRequest(http.MethodPost, "/bkt/mp?uploadId="+uploadID, strings.NewReader(completeXML))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidPart")

	// 5. Abort valid upload
	req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId="+uploadID, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// 6. Verify upload is gone (Abort again fails)
	req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId="+uploadID, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
