package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3"
)

func newTestHandler(t *testing.T) (*s3.S3Handler, *s3.InMemoryBackend) {
	t.Helper()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend, slog.Default())

	return handler, backend
}

func mustCreateBucket(t *testing.T, b s3.StorageBackend, bucket string) {
	t.Helper()

	_, err := b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func mustPutObject(t *testing.T, b s3.StorageBackend, bucket, key string, data []byte) {
	t.Helper()

	_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
}

func enableVersioning(t *testing.T, handler *s3.S3Handler, bucket string) {
	t.Helper()

	xmlBody := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?versioning", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *s3.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "no buckets",
			setup:   func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "test")
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/xml")
		})
	}
}

func TestHandler_CreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		wantStatus int
	}{
		{
			name:       "create new bucket",
			bucket:     "new-bucket",
			setup:      func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusOK,
		},
		{
			name:   "create duplicate bucket",
			bucket: "existing",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "existing")
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateBucket_ReturnsLocation(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/test-bucket", rec.Header().Get("Location"), "Location header should be set")
}

func TestHandler_DeleteBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		wantStatus int
	}{
		{
			name:   "delete empty bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "delete non-empty bucket",
			bucket: "full-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "full-bucket")
				mustPutObject(t, b, "full-bucket", "k", []byte("d"))
			},
			// Async deletion: non-empty buckets are now queued for background deletion.
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HeadBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		wantStatus int
	}{
		{
			name:   "existing bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent bucket",
			bucket:     "no-bucket",
			setup:      func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
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
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "put object to non-existent bucket",
			bucket:     "no-bkt",
			key:        "key",
			body:       "data",
			setup:      func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"/"+tt.key,
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
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
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
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
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

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

	tests := []struct {
		name string
	}{
		{name: "delete existing object returns 204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestHandler_HeadObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantStatus int
	}{
		{
			name:   "existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "key", []byte("data"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "non-existent object",
			bucket: "bkt",
			key:    "no-key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HeadObjectWithMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "head and get object return metadata headers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/meta", strings.NewReader("data"))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-Amz-Meta-Author", "Antigravity")
			req.Header.Set("X-Amz-Meta-Priority", "High")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodHead, "/bkt/meta", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
			assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
			assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))

			req = httptest.NewRequest(http.MethodGet, "/bkt/meta", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
			assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
			assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))
		})
	}
}

func TestHandler_ObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put get delete tagging lifecycle"},
		{name: "invalid XML returns 400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			switch tt.name {
			case "put get delete tagging lifecycle":
				body := `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`
				req := httptest.NewRequest(
					http.MethodPut,
					"/bkt/key?tagging",
					strings.NewReader(body),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/bkt/key?tagging", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "prod")

				req = httptest.NewRequest(http.MethodDelete, "/bkt/key?tagging", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusNoContent, rec.Code)

			case "invalid XML returns 400":
				req := httptest.NewRequest(
					http.MethodPut,
					"/bkt/key?tagging",
					strings.NewReader("not xml"),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestHandler_BucketVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantConf   *s3.VersioningConfiguration
		name       string
		bucket     string
		xmlBody    string
		wantStatus int
	}{
		{
			name:       "put and get versioning",
			bucket:     "bkt",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: http.StatusOK,
			wantConf:   &s3.VersioningConfiguration{Status: "Enabled"},
		},
		{
			name:       "non-existent bucket returns 404",
			bucket:     "no-bucket",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid XML returns 400",
			bucket:     "bkt",
			xmlBody:    "not xml",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.bucket == "bkt" {
				mustCreateBucket(t, backend, "bkt")
			}

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"?versioning",
				strings.NewReader(tt.xmlBody),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantConf != nil {
				req = httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?versioning", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				var got s3.VersioningConfiguration
				require.NoError(t, xml.NewDecoder(rec.Body).Decode(&got))

				if diff := cmp.Diff(
					*tt.wantConf, got,
					cmpopts.IgnoreFields(s3.VersioningConfiguration{}, "XMLName"),
				); diff != "" {
					t.Errorf("VersioningConfiguration mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
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
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandler_BucketLocationQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "bucket location returns LocationConstraint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodGet, "/bkt?location", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "LocationConstraint")
		})
	}
}

func TestHandler_BucketTaggingNotImplemented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
	}{
		{name: "PUT bucket tagging returns 501", method: http.MethodPut},
		{name: "GET bucket tagging returns 501", method: http.MethodGet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(tt.method, "/bkt?tagging", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusNotImplemented, rec.Code)
		})
	}
}

func TestHandler_ObjectACLIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{name: "PUT ACL returns 200", method: http.MethodPut, wantStatus: http.StatusOK},
		{
			name:       "GET ACL returns 501",
			method:     http.MethodGet,
			wantStatus: http.StatusNotImplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(tt.method, "/bkt/key?acl", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list objects returns all keys"},
		{name: "marker excludes items at or before it"},
		{name: "common prefixes with delimiter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "list objects returns all keys":
				mustPutObject(t, backend, "bkt", "file1.txt", []byte("a"))
				mustPutObject(t, backend, "bkt", "file2.txt", []byte("b"))

				req := httptest.NewRequest(http.MethodGet, "/bkt", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "file1.txt")
				assert.Contains(t, rec.Body.String(), "file2.txt")

			case "marker excludes items at or before it":
				mustPutObject(t, backend, "bkt", "a", []byte("d"))
				mustPutObject(t, backend, "bkt", "b", []byte("d"))
				mustPutObject(t, backend, "bkt", "c", []byte("d"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?marker=a", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.NotContains(t, body, "<Key>a</Key>")
				assert.Contains(t, body, "<Key>b</Key>")
				assert.Contains(t, body, "<Key>c</Key>")

			case "common prefixes with delimiter":
				mustPutObject(t, backend, "bkt", "a/1", []byte("d"))
				mustPutObject(t, backend, "bkt", "a/2", []byte("d"))
				mustPutObject(t, backend, "bkt", "b/1", []byte("d"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?delimiter=/", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Prefix>a/</Prefix>")
				assert.Contains(t, body, "<Prefix>b/</Prefix>")
				assert.NotContains(t, body, "<Key>a/1</Key>")
			}
		})
	}
}

func TestHandler_VersionedObjectLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "versioned object full lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v1 data"))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			v1ID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, v1ID)
			assert.NotEqual(t, s3.NullVersion, v1ID)

			req = httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v2 data"))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			v2ID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, v2ID)
			assert.NotEqual(t, v1ID, v2ID)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			body, _ := io.ReadAll(rec.Body)
			assert.Equal(t, "v2 data", string(body))

			req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			body, _ = io.ReadAll(rec.Body)
			assert.Equal(t, "v1 data", string(body))

			req = httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
			dmID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, dmID)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_PutObjectWithTaggingHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put with tagging header stores tags"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("data"))
			req.Header.Set("X-Amz-Tagging", "env=prod&team=alpha")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			out, err := backend.GetObjectTagging(t.Context(), &sdk_s3.GetObjectTaggingInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)
			assert.Len(t, out.TagSet, 2)

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
		})
	}
}

func TestHandler_ChecksumSupport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "SHA256 checksum header roundtrip"},
		{name: "existing checksum preserved on get with checksum mode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "SHA256 checksum header roundtrip":
				body := "checksum test data"
				req := httptest.NewRequest(http.MethodPut, "/bkt/check", strings.NewReader(body))
				req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
				req.Header.Set("X-Amz-Checksum-Sha256", "fake-sha256-value")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)

				expectedHash := md5.Sum([]byte(body))
				expectedETag := "\"" + hex.EncodeToString(expectedHash[:]) + "\""
				assert.Equal(t, expectedETag, rec.Header().Get("ETag"))
				assert.Equal(t, "fake-sha256-value", rec.Header().Get("X-Amz-Checksum-Sha256"))

				req = httptest.NewRequest(http.MethodGet, "/bkt/check", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "fake-sha256-value", rec.Header().Get("X-Amz-Checksum-Sha256"))
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))

			case "existing checksum preserved on get with checksum mode":
				_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket:         aws.String("bkt"),
					Key:            aws.String("key2"),
					Body:           strings.NewReader("data"),
					ChecksumSHA256: aws.String("fake-sha256"),
				})
				require.NoError(t, err)

				req := httptest.NewRequest(http.MethodGet, "/bkt/key2", nil)
				req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.Equal(t, "fake-sha256", rec.Header().Get("X-Amz-Checksum-Sha256"))
			}
		})
	}
}

func TestHandler_VirtualHostedStyle_Final(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		path       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "valid virtual hosted bucket key not found",
			host:       "my-vh-bucket.localhost:9000",
			path:       "/somekey",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchKey",
		},
		{
			name:       "invalid bucket in host falls back to path style",
			host:       "invalid_bucket.localhost:9000",
			path:       "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "host not matching endpoint falls back",
			host:       "my-vh-bucket.other:9000",
			path:       "/",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			handler.Endpoint = "localhost:9000"
			mustCreateBucket(t, backend, "my-vh-bucket")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ChecksumAlgorithms_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
	}{
		{name: "CRC32", header: "X-Amz-Checksum-Crc32"},
		{name: "CRC32C", header: "X-Amz-Checksum-Crc32c"},
		{name: "SHA1", header: "X-Amz-Checksum-Sha1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			key := "check-" + tt.name
			req := httptest.NewRequest(http.MethodPut, "/bkt/"+key, strings.NewReader("data"))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.name)
			req.Header.Set(tt.header, "fake-value")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodGet, "/bkt/"+key, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "fake-value", rec.Header().Get(tt.header))
			assert.Equal(t, tt.name, rec.Header().Get("X-Amz-Checksum-Algorithm"))
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
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, "case: %s", tt.name)
		})
	}
}

func TestHandler_MultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "multipart upload full lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPost, "/bkt/mp?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var initResp s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(rec.Body).Decode(&initResp))

			if diff := cmp.Diff(
				s3.InitiateMultipartUploadResult{Bucket: "bkt", Key: "mp", UploadID: initResp.UploadID},
				initResp,
				cmpopts.IgnoreFields(s3.InitiateMultipartUploadResult{}, "UploadID", "XMLName"),
			); diff != "" {
				t.Errorf("InitiateMultipartUploadResult mismatch (-want +got):\n%s", diff)
			}
			assert.NotEmpty(t, initResp.UploadID)

			uploadID := initResp.UploadID

			part1Data := "part1"
			req = httptest.NewRequest(
				http.MethodPut,
				"/bkt/mp?partNumber=1&uploadId="+uploadID,
				strings.NewReader(part1Data),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			etag1 := rec.Header().Get("ETag")
			assert.NotEmpty(t, etag1)

			part2Data := "part2"
			req = httptest.NewRequest(
				http.MethodPut,
				"/bkt/mp?partNumber=2&uploadId="+uploadID,
				strings.NewReader(part2Data),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			etag2 := rec.Header().Get("ETag")
			assert.NotEmpty(t, etag2)

			completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
	<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
	<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
</CompleteMultipartUpload>`, etag1, etag2)
			req = httptest.NewRequest(
				http.MethodPost,
				"/bkt/mp?uploadId="+uploadID,
				strings.NewReader(completeXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodGet, "/bkt/mp", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			body, _ := io.ReadAll(rec.Body)
			assert.Equal(t, "part1part2", string(body))
		})
	}
}

func TestHandler_MultipartUpload_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "multipart upload error scenarios"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(
				http.MethodPut,
				"/bkt/mp?partNumber=1&uploadId=invalid-id",
				strings.NewReader("data"),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Contains(t, rec.Body.String(), "NoSuchUpload")

			req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId=invalid-id", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			req = httptest.NewRequest(http.MethodPost, "/bkt/mp?uploads", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			var initResp s3.InitiateMultipartUploadResult
			_ = xml.NewDecoder(rec.Body).Decode(&initResp)
			uploadID := initResp.UploadID

			completeXML := `<CompleteMultipartUpload>
	<Part><PartNumber>1</PartNumber><ETag>"wrong-etag"</ETag></Part>
</CompleteMultipartUpload>`
			req = httptest.NewRequest(
				http.MethodPost,
				"/bkt/mp?uploadId="+uploadID,
				strings.NewReader(completeXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidPart")

			req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId="+uploadID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			req = httptest.NewRequest(http.MethodDelete, "/bkt/mp?uploadId="+uploadID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
func TestHandler_DeleteObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		xmlBody    string
		wantStatus int
	}{
		{
			name:   "delete multiple objects",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Object><Key>k1</Key></Object>
				<Object><Key>k2</Key></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
				mustPutObject(t, b, "bkt", "k2", []byte("d2"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete objects with versions",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Object><Key>k1</Key><VersionId>null</VersionId></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "quiet mode",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Quiet>true</Quiet>
				<Object><Key>k1</Key></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+tt.bucket+"?delete",
				strings.NewReader(tt.xmlBody),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusOK {
				assert.Contains(t, rec.Header().Get("Content-Type"), "application/xml")
			}
		})
	}
}

func TestHandler_DeleteObjects_Versioning(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "versioned-bucket"
	mustCreateBucket(t, backend, bucket)
	enableVersioning(t, handler, bucket)

	// Create 3 versions of k1
	v1, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v1"),
	})
	require.NoError(t, err)
	v1ID := *v1.VersionId

	v2, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v2"),
	})
	require.NoError(t, err)
	v2ID := *v2.VersionId

	v3, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v3"),
	})
	require.NoError(t, err)
	v3ID := *v3.VersionId

	// Delete v1 and v3, leave v2
	xmlBody := fmt.Sprintf(`<Delete>
		<Object><Key>k1</Key><VersionId>%s</VersionId></Object>
		<Object><Key>k1</Key><VersionId>%s</VersionId></Object>
	</Delete>`, v1ID, v3ID)

	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify v1 and v3 are gone, v2 remains
	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v1ID),
	})
	require.Error(t, err)

	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v3ID),
	})
	require.Error(t, err)

	out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v2ID),
	})
	require.NoError(t, err)
	data, _ := io.ReadAll(out.Body)
	assert.Equal(t, "v2", string(data))

	// Now delete without version ID - should create a delete marker
	xmlBody = `<Delete><Object><Key>k1</Key></Object></Delete>`
	req = httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(xmlBody))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Head object should now return 404 (due to delete marker)
	req = httptest.NewRequest(http.MethodHead, "/"+bucket+"/k1", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
