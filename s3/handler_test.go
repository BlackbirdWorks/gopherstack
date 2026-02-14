package s3_test

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"Gopherstack/s3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) (*s3.Handler, *s3.InMemoryBackend) {
	t.Helper()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend)

	return handler, backend
}

func TestHandler_ListBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*s3.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "no buckets",
			setup:   func(_ *s3.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("test"))
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
		setup      func(*s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:       "create new bucket",
			bucket:     "new-bucket",
			setup:      func(_ *s3.InMemoryBackend) {},
			wantStatus: http.StatusOK,
		},
		{
			name:   "create duplicate bucket",
			bucket: "existing",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("existing"))
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
		setup      func(*s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:   "delete empty bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "delete non-empty bucket",
			bucket: "full-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("full-bucket"))

				_, err := b.PutObject("full-bucket", "k", []byte("d"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
		setup      func(*s3.InMemoryBackend)
		bucket     string
		wantStatus int
	}{
		{
			name:   "existing bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
		setup      func(*s3.InMemoryBackend)
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
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "put object to non-existent bucket",
			bucket:     "no-bkt",
			key:        "key",
			body:       "data",
			setup:      func(_ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
		setup      func(*s3.InMemoryBackend)
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
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "key", []byte("content"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantBody:   "content",
		},
		{
			name:   "get non-existent key",
			bucket: "bkt",
			key:    "no-key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

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
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "key", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_HeadObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*s3.InMemoryBackend)
		bucket     string
		key        string
		wantStatus int
	}{
		{
			name:   "existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "key", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "non-existent object",
			bucket: "bkt",
			key:    "no-key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(backend)

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ObjectTagging(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "key", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

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
	require.NoError(t, backend.CreateBucket("bkt"))

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
	require.NoError(t, backend.CreateBucket("bkt"))

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
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "key", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

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
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "file1.txt", []byte("a"), s3.ObjectMetadata{})
	require.NoError(t, err)

	_, err = backend.PutObject("bkt", "file2.txt", []byte("b"), s3.ObjectMetadata{})
	require.NoError(t, err)

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
	require.NoError(t, backend.CreateBucket("bkt"))

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
	require.NoError(t, backend.CreateBucket("bkt"))

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
	require.NoError(t, backend.CreateBucket("bkt"))

	req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("data"))
	req.Header.Set("X-Amz-Tagging", "env=prod&team=alpha")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags were stored
	tags, err := backend.GetObjectTagging("bkt", "key", "")
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "alpha", tags["team"])
}

func TestHandler_PutObjectTagging_InvalidXML(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "key", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

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
	require.NoError(t, backend.CreateBucket("bkt"))

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
	) // Wait, I didn't return algorithm header? Let me check.
}
