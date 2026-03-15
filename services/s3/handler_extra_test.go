package s3_test

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// serveS3Handler is a test helper that invokes an S3 Echo handler with a raw HTTP request.
func serveS3Handler(handler *s3.S3Handler, w http.ResponseWriter, r *http.Request) {
	ctx := logger.Save(r.Context(), slog.Default())
	r = r.WithContext(ctx)
	e := echo.New()
	c := e.NewContext(r, w)
	_ = handler.Handler()(c)
}

func TestCalculateChecksum(t *testing.T) {
	t.Parallel()

	data := []byte("hello world")
	tests := []struct {
		name      string
		algorithm string
		want      string
	}{
		{name: "CRC32", algorithm: "CRC32", want: "DUoRhQ=="},
		{name: "CRC32C", algorithm: "CRC32C", want: "yZRlqg=="},
		{name: "SHA1", algorithm: "SHA1", want: "Kq5sNclPz7QV2+lfQIuc6R7oRu0="},
		{name: "SHA256", algorithm: "SHA256", want: "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="},
		{name: "Unknown", algorithm: "UNKNOWN", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.CalculateChecksum(data, tt.algorithm))
		})
	}
}

func TestHandler_VirtualHostedStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpoint   string
		host       string
		path       string
		bucket     string
		wantStatus int
	}{
		{
			name:       "valid virtual hosted bucket returns 404 for missing key",
			endpoint:   "localhost:8080",
			host:       "my-bucket.localhost:8080",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid bucket name in host falls back to path style",
			endpoint:   "localhost:8080",
			host:       "invalid_bucket.localhost:8080",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "host not matching endpoint falls back to path style",
			endpoint:   "localhost:8080",
			host:       "my-bucket.otherhost.com",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "host without port matches endpoint",
			endpoint:   "s3.amazonaws.com",
			host:       "mybucket.s3.amazonaws.com",
			path:       "/key",
			bucket:     "mybucket",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			handler := s3.NewHandler(backend)
			handler.Endpoint = tt.endpoint
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()

			// Use Echo handler
			ctx := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctx)
			e := echo.New()
			c := e.NewContext(req, rec)
			_ = handler.Handler()(c)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
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

func TestHandler_GetObject_ChecksumMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "auto-computed CRC32 checksum returned"},
		{name: "stored SHA256 checksum returned"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "auto-computed CRC32 checksum returned":
				mustPutObject(t, backend, "bkt", "key", []byte("checksum-me"))
				req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
				req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Crc32"))

			case "stored SHA256 checksum returned":
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
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.Equal(t, "fake-sha256", rec.Header().Get("X-Amz-Checksum-Sha256"))
			}
		})
	}
}

func TestHandler_ListObjectsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "pagination with max-keys returns truncation token"},
		{name: "common prefixes with delimiter"},
		{name: "delimiter and prefix filter"},
		{name: "start-after excludes items before it"},
		{name: "invalid max-keys defaults to 1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "pagination with max-keys returns truncation token":
				for i := 1; i <= 5; i++ {
					mustPutObject(t, backend, "bkt", "key"+strings.Repeat("0", i), []byte("data"))
				}
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=2", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "<IsTruncated>true</IsTruncated>")
				assert.Contains(t, rec.Body.String(), "<NextContinuationToken>")

			case "common prefixes with delimiter":
				mustPutObject(t, backend, "bkt", "photos/1.jpg", []byte("d"))
				mustPutObject(t, backend, "bkt", "photos/2.jpg", []byte("d"))
				mustPutObject(t, backend, "bkt", "videos/1.mp4", []byte("d"))
				mustPutObject(t, backend, "bkt", "root.txt", []byte("d"))
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&delimiter=/", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Prefix>photos/</Prefix>")
				assert.Contains(t, body, "<Prefix>videos/</Prefix>")
				assert.Contains(t, body, "<Key>root.txt</Key>")

			case "delimiter and prefix filter":
				mustPutObject(t, backend, "bkt", "dir/a", []byte("d"))
				mustPutObject(t, backend, "bkt", "dir/b", []byte("d"))
				mustPutObject(t, backend, "bkt", "other", []byte("d"))
				req := httptest.NewRequest(
					http.MethodGet, "/bkt?list-type=2&prefix=dir/&delimiter=/", nil,
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Key>dir/a</Key>")
				assert.Contains(t, body, "<Key>dir/b</Key>")
				assert.NotContains(t, body, "<Key>other</Key>")

			case "start-after excludes items before it":
				mustPutObject(t, backend, "bkt", "a", []byte("d"))
				mustPutObject(t, backend, "bkt", "b", []byte("d"))
				mustPutObject(t, backend, "bkt", "c", []byte("d"))
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&start-after=a", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.NotContains(t, body, "<Key>a</Key>")
				assert.Contains(t, body, "<Key>b</Key>")
				assert.Contains(t, body, "<Key>c</Key>")

			case "invalid max-keys defaults to 1000":
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=-1", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestHandler_GetObject_Range(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeHdr   string
		wantBody   string
		wantRange  string
		wantStatus int
	}{
		{
			name:       "partial range returns 206 and slice",
			rangeHdr:   "bytes=0-4",
			wantStatus: http.StatusPartialContent,
			wantBody:   "01234",
			wantRange:  "bytes 0-4/10",
		},
		{
			name:       "suffix range returns last bytes",
			rangeHdr:   "bytes=-3",
			wantStatus: http.StatusPartialContent,
			wantBody:   "789",
		},
		{
			name:       "open-ended range returns from offset",
			rangeHdr:   "bytes=8-",
			wantStatus: http.StatusPartialContent,
			wantBody:   "89",
		},
		{
			name:       "inverted range returns 416",
			rangeHdr:   "bytes=10-5",
			wantStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:       "unsupported range unit falls back to 200",
			rangeHdr:   "bits=0-5",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("0123456789"))

			req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			req.Header.Set("Range", tt.rangeHdr)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}

			if tt.wantRange != "" {
				assert.Equal(t, tt.wantRange, rec.Header().Get("Content-Range"))
			}
		})
	}
}

func TestHandler_MultipartUpload_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create upload part and complete"},
		{name: "create and abort upload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			start := strings.Index(body, "<UploadId>") + len("<UploadId>")
			end := strings.Index(body, "</UploadId>")
			uploadID := body[start:end]

			switch tt.name {
			case "create upload part and complete":
				req = httptest.NewRequest(
					http.MethodPut,
					"/bkt/obj?partNumber=1&uploadId="+uploadID,
					strings.NewReader("part1"),
				)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				etag := rec.Header().Get("ETag")

				completeXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` +
					etag + `</ETag></Part></CompleteMultipartUpload>`
				req = httptest.NewRequest(
					http.MethodPost, "/bkt/obj?uploadId="+uploadID, strings.NewReader(completeXML),
				)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "part1", rec.Body.String())

			case "create and abort upload":
				req = httptest.NewRequest(http.MethodDelete, "/bkt/obj?uploadId="+uploadID, nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}

func TestHandler_CompleteMultipartUpload_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "invalid XML returns 400"},
		{name: "invalid XML after valid initiate returns 400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "invalid XML returns 400":
				req := httptest.NewRequest(
					http.MethodPost, "/bkt/obj?uploadId=any", strings.NewReader("not xml"),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusBadRequest, rec.Code)

			case "invalid XML after valid initiate returns 400":
				req := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				var res s3.InitiateMultipartUploadResult
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &res))
				uploadID := res.UploadID

				req = httptest.NewRequest(
					http.MethodPost, "/bkt/obj?uploadId="+uploadID, strings.NewReader("not xml"),
				)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestHandler_AbortMultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "invalid upload ID returns 404",
			path:       "/bkt/obj?uploadId=invalid",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing upload ID routes to delete object",
			path:       "/bkt/obj",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "non-existent bucket returns 404",
			path:       "/no-bucket/key?uploadId=ui",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodDelete, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListObjectVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "multiple versions of same key"},
		{name: "delete marker appears as latest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")

			switch tt.name {
			case "multiple versions of same key":
				mustPutObject(t, backend, "bkt", "key", []byte("v1"))
				mustPutObject(t, backend, "bkt", "key", []byte("v2"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<ListVersionsResult>")
				assert.Contains(t, body, "<Key>key</Key>")
				assert.Contains(t, body, "<VersionId>")
				assert.Equal(t, 2, strings.Count(body, "<Version>"))

			case "delete marker appears as latest":
				mustPutObject(t, backend, "bkt", "key", []byte("data"))

				req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusNoContent, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<DeleteMarker>")
				assert.Contains(t, body, "<IsLatest>true</IsLatest>")
			}
		})
	}
}

func TestHandler_ObjectLifecycle_Versioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "head and delete specific version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var res s3.ListVersionsResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &res), "Body: %s", rec.Body.String())
			require.NotEmpty(t, res.Versions, "Body: %s", rec.Body.String())

			vid := res.Versions[0].VersionID
			require.NotEmpty(t, vid, "VersionID is empty. Body: %s", rec.Body.String())

			req = httptest.NewRequest(http.MethodHead, "/bkt/key?versionId="+vid, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))

			req = httptest.NewRequest(http.MethodDelete, "/bkt/key?versionId="+vid, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))
		})
	}
}

func TestHandler_GetBucketLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		path   string
		want   string
	}{
		{
			name:   "returns us-east-1",
			bucket: "bkt",
			path:   "/bkt?location",
			want:   "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestHandler_DeleteBucket_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		populate   bool
		wantStatus int
	}{
		{
			// Async deletion: non-empty buckets are now queued for background deletion.
			name:       "delete non-empty bucket succeeds and queues async deletion",
			bucket:     "full-bkt",
			populate:   true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete non-existent bucket returns 404",
			bucket:     "no-bkt",
			populate:   false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)

			if tt.populate {
				mustCreateBucket(t, backend, tt.bucket)
				mustPutObject(t, backend, tt.bucket, "obj", []byte("data"))
			}

			req := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateBucket_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		body       string
		preCreate  bool
		wantStatus int
	}{
		{
			name:       "duplicate bucket returns 409",
			bucket:     "bkt",
			preCreate:  true,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "invalid XML in body creates bucket with default region",
			bucket:     "new-bkt",
			body:       "not xml",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)

			if tt.preCreate {
				mustCreateBucket(t, backend, tt.bucket)
			}

			var body strings.Reader
			if tt.body != "" {
				body = *strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket, &body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateBucket_BucketAlreadyOwnedByYou_XMLResponse(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "existing")

	req := httptest.NewRequest(http.MethodPut, "/existing", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/xml")

	var errResp s3.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "BucketAlreadyOwnedByYou", errResp.Code)
	assert.NotEmpty(t, errResp.Message)
}

func TestHandler_UploadPart_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing upload ID routes to put object and succeeds",
			path:       "/bkt/obj?partNumber=1",
			body:       "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent upload ID returns 404",
			path:       "/bkt/obj?partNumber=1&uploadId=no-such-id",
			body:       "data",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid part number returns 400",
			path:       "/bkt/obj?partNumber=abc&uploadId=any",
			body:       "data",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-existent bucket returns 404",
			path:       "/no-bucket/key?partNumber=1&uploadId=ui",
			body:       "data",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BucketTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, backend *s3.InMemoryBackend)
		name       string
		method     string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PUT bucket tagging succeeds",
			method: http.MethodPut,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			body:       `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "GET bucket tagging returns tags",
			method: http.MethodGet,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")

				// pre-populate a tag
				req := httptest.NewRequest(http.MethodPut, "/bkt?tagging", strings.NewReader(
					`<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`,
				))
				rec := httptest.NewRecorder()
				handler, _ := newTestHandler(t)
				handler.Backend = backend
				serveS3Handler(handler, rec, req)
			},
			wantStatus: http.StatusOK,
			wantBody:   "env",
		},
		{
			name:   "GET bucket tagging returns 404 when no tags set",
			method: http.MethodGet,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "DELETE bucket tagging succeeds",
			method: http.MethodDelete,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			var reqBody io.Reader
			if tt.body != "" {
				reqBody = strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(tt.method, "/bkt?tagging", reqBody)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ResolveBucketAndKey_InvalidKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{
			name:       "key longer than 1024 bytes returns 400",
			key:        strings.Repeat("a", 1025),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(http.MethodGet, "/bkt/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteObject_NonExistent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{
			name:       "delete non-existent key returns 204",
			key:        "no-key",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodDelete, "/bkt/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteObjectTagging_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "delete tags from non-existent object returns 204",
			path:       "/bkt/no-such-key?tagging",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodDelete, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_NonExistentBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "list object versions",
			method:     http.MethodGet,
			path:       "/no-bucket?versions",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "complete multipart upload",
			method:     http.MethodPost,
			path:       "/no-bucket/key?uploadId=ui",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get object tagging",
			method:     http.MethodGet,
			path:       "/no-bucket/key?tagging",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get bucket versioning",
			method:     http.MethodGet,
			path:       "/no-bucket?versioning",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			var body strings.Reader
			if tt.method == http.MethodPost {
				body = *strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>")
			}

			req := httptest.NewRequest(tt.method, tt.path, &body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantNotEmpty bool
	}{
		{
			name:         "returns non-empty list including PutObject",
			wantContains: "PutObject",
			wantNotEmpty: true,
		},
		{
			name:         "includes PutBucketWebsite",
			wantContains: "PutBucketWebsite",
		},
		{
			name:         "includes GetBucketWebsite",
			wantContains: "GetBucketWebsite",
		},
		{
			name:         "includes DeleteBucketWebsite",
			wantContains: "DeleteBucketWebsite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)
			ops := handler.GetSupportedOperations()

			if tt.wantNotEmpty {
				assert.NotEmpty(t, ops)
			}

			assert.Contains(t, ops, tt.wantContains)
		})
	}
}

// TestHandler_ListObjectsV2Error exercises handleListObjectsV2Error via
// ListObjectsV2 on a non-existent bucket (NoSuchBucket) and a generic backend error.
func TestHandler_ListObjectsV2Error(t *testing.T) {
	t.Parallel()

	t.Run("non-existent bucket returns 404", func(t *testing.T) {
		t.Parallel()
		handler, _ := newTestHandler(t)

		// No bucket created → ListObjectsV2 will get ErrNoSuchBucket.
		req := httptest.NewRequest(http.MethodGet, "/no-such-bucket?list-type=2", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestS3BucketPolicyCRUD verifies put/get/delete bucket policy operations.
func TestS3BucketPolicyCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "policy-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	// PutBucketPolicy
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?policy", strings.NewReader(policy))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketPolicy
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, policy, rec.Body.String())

	// DeleteBucketPolicy
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketPolicy after delete → NoSuchBucketPolicy
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestS3BucketCORSCRUD verifies put/get/delete bucket CORS + OPTIONS preflight.
func TestS3BucketCORSCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "cors-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	corsXML := `<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>` +
		`<AllowedOrigin>https://example.com</AllowedOrigin></CORSRule></CORSConfiguration>`

	// PutBucketCORS
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(corsXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBucketCORS
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?cors", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AllowedOrigin")

	// OPTIONS preflight (CORS configured)
	req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))

	// DeleteBucketCORS
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?cors", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// OPTIONS preflight after delete → 403
	req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// TestS3CORSPreflightRuleEnforcement verifies that preflight requests are
// evaluated against the configured CORS rules and rejected when no rule matches.
func TestS3CORSPreflightRuleEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		corsXML       string
		origin        string
		method        string
		reqHeaders    string
		wantAllowOrig string
		wantCode      int
		wantForbidden bool
	}{
		{
			name: "matching origin and method allowed",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>PUT</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "PUT",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "wildcard origin matches any origin",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>*</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://any-site.example",
			method:        "GET",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://any-site.example",
		},
		{
			name: "non-matching origin rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://evil.example",
			method:        "GET",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "non-matching method rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "DELETE",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "wildcard allowed headers passes any header",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>*</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "Content-Type, X-Custom-Header",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "specific allowed header matches",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>Content-Type</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "Content-Type",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "disallowed request header rejected",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>POST</AllowedMethod>` +
				`<AllowedHeader>Content-Type</AllowedHeader>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "POST",
			reqHeaders:    "X-Forbidden-Header",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "second matching rule used when first does not match",
			corsXML: `<CORSConfiguration>` +
				`<CORSRule>` +
				`<AllowedOrigin>https://other.example</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule>` +
				`<CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>PUT</AllowedMethod>` +
				`</CORSRule>` +
				`</CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "PUT",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "MaxAgeSeconds reflected in response",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`<MaxAgeSeconds>600</MaxAgeSeconds>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "GET",
			wantCode:      http.StatusOK,
			wantAllowOrig: "https://example.com",
		},
		{
			name: "empty Origin rejected even with wildcard rule",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>*</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "",
			method:        "GET",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
		{
			name: "empty Method rejected even with matching origin",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			origin:        "https://example.com",
			method:        "",
			wantCode:      http.StatusForbidden,
			wantForbidden: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, sdkClient := newTestHandler(t)
			bucket := "cors-enforce-bucket"

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			// Put CORS config
			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(tt.corsXML))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			// Send OPTIONS preflight
			req = httptest.NewRequest(http.MethodOptions, "/"+bucket, nil)
			req.Header.Set("Origin", tt.origin)
			req.Header.Set("Access-Control-Request-Method", tt.method)

			if tt.reqHeaders != "" {
				req.Header.Set("Access-Control-Request-Headers", tt.reqHeaders)
			}

			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			if !tt.wantForbidden {
				assert.Equal(t, tt.wantAllowOrig, rec.Header().Get("Access-Control-Allow-Origin"))
				assert.Equal(t, tt.method, rec.Header().Get("Access-Control-Allow-Methods"))
			}
		})
	}
}

// TestS3BucketLifecycleCRUD verifies put/get/delete lifecycle configuration.
func TestS3BucketCORSValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		corsXML     string
		wantContain string
		wantCode    int
	}{
		{
			name: "valid CORS XML accepted",
			corsXML: `<CORSConfiguration><CORSRule>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod>` +
				`</CORSRule></CORSConfiguration>`,
			wantCode: http.StatusOK,
		},
		{
			name:        "malformed CORS XML rejected with 400",
			corsXML:     `<CORSConfiguration><NotClosed>`,
			wantCode:    http.StatusBadRequest,
			wantContain: "MalformedXML",
		},
		{
			name:        "completely invalid CORS XML rejected with 400",
			corsXML:     `this is not xml at all`,
			wantCode:    http.StatusBadRequest,
			wantContain: "MalformedXML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, sdkClient := newTestHandler(t)
			bucket := "cors-validation-bucket"

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?cors", strings.NewReader(tt.corsXML))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// TestS3BucketLifecycleCRUD verifies put/get/delete lifecycle configuration.
func TestS3BucketLifecycleCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "lifecycle-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	lifecycleXML := `<LifecycleConfiguration><Rule><ID>expire-old</ID>` +
		`<Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`

	// PutBucketLifecycleConfiguration
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?lifecycle", strings.NewReader(lifecycleXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketLifecycleConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "expire-old")

	// DeleteBucketLifecycleConfiguration
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketLifecycleConfiguration after delete → NoSuchLifecycleConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestS3BucketNotificationCRUD verifies put/get bucket notification configuration.
func TestS3BucketNotificationCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "notif-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	notifXML := `<NotificationConfiguration><TopicConfiguration>` +
		`<Topic>arn:aws:sns:us-east-1:000000000000:my-topic</Topic>` +
		`<Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>`

	// PutBucketNotificationConfiguration
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?notification", strings.NewReader(notifXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBucketNotificationConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?notification", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TopicConfiguration")

	// GetBucketNotificationConfiguration on bucket without notifications
	bucket2 := "notif-empty-bucket"
	_, err = sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket2)})
	require.NoError(t, err)

	req = httptest.NewRequest(http.MethodGet, "/"+bucket2+"?notification", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "NotificationConfiguration")
}

// TestS3BucketWebsiteCRUD verifies put/get/delete bucket website configuration.
func TestS3BucketWebsiteCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "website-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	websiteXML := `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<IndexDocument><Suffix>index.html</Suffix></IndexDocument>` +
		`<ErrorDocument><Key>error.html</Key></ErrorDocument>` +
		`</WebsiteConfiguration>`

	// GetBucketWebsite before any config → 404 NoSuchWebsiteConfiguration
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchWebsiteConfiguration")

	// PutBucketWebsite
	req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?website", strings.NewReader(websiteXML))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBucketWebsite returns stored config
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "IndexDocument")
	assert.Contains(t, rec.Body.String(), "index.html")

	// DeleteBucketWebsite
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketWebsite after delete → 404 NoSuchWebsiteConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?website", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchWebsiteConfiguration")
}

// TestS3BucketWebsite_MalformedXML verifies that PutBucketWebsite rejects invalid XML.
func TestS3BucketWebsite_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "website-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?website", strings.NewReader("not-valid-xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3PublicAccessBlockCRUD verifies put/get/delete public access block configuration.
func TestS3PublicAccessBlockCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name: "full-block-all",
			configXML: `<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>true</BlockPublicAcls>` +
				`<IgnorePublicAcls>true</IgnorePublicAcls>` +
				`<BlockPublicPolicy>true</BlockPublicPolicy>` +
				`<RestrictPublicBuckets>true</RestrictPublicBuckets>` +
				`</PublicAccessBlockConfiguration>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BlockPublicAcls",
		},
		{
			name: "partial-block",
			configXML: `<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>false</BlockPublicAcls>` +
				`<IgnorePublicAcls>false</IgnorePublicAcls>` +
				`<BlockPublicPolicy>true</BlockPublicPolicy>` +
				`<RestrictPublicBuckets>false</RestrictPublicBuckets>` +
				`</PublicAccessBlockConfiguration>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BlockPublicPolicy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "pab-test-" + tt.name

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			// GetPublicAccessBlock before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// PutPublicAccessBlock
			req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?publicAccessBlock", strings.NewReader(tt.configXML))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetPublicAccessBlock
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeletePublicAccessBlock
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetPublicAccessBlock after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3PublicAccessBlock_MalformedXML verifies that PutPublicAccessBlock rejects invalid XML.
func TestS3PublicAccessBlock_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "pab-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?publicAccessBlock", strings.NewReader("not-valid-xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketOwnershipControlsCRUD verifies put/get/delete ownership controls.
func TestS3BucketOwnershipControlsCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name: "bucket-owner-preferred",
			configXML: `<OwnershipControls>` +
				`<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>` +
				`</OwnershipControls>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BucketOwnerPreferred",
		},
		{
			name: "bucket-owner-enforced",
			configXML: `<OwnershipControls>` +
				`<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>` +
				`</OwnershipControls>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BucketOwnerEnforced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "ownership-test-" + tt.name

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			// GetBucketOwnershipControls before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// PutBucketOwnershipControls
			req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?ownershipControls", strings.NewReader(tt.configXML))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketOwnershipControls
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeleteBucketOwnershipControls
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetBucketOwnershipControls after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3BucketOwnershipControls_MalformedXML verifies that PutBucketOwnershipControls rejects invalid XML.
func TestS3BucketOwnershipControls_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "ownership-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?ownershipControls", strings.NewReader("not-valid-xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketLoggingCRUD verifies put/get bucket logging configuration.
func TestS3BucketLoggingCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configXML string
		wantBody  string
		wantPut   int
		wantGet   int
	}{
		{
			name: "logging-enabled",
			configXML: `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
				`<LoggingEnabled>` +
				`<TargetBucket>my-logs-bucket</TargetBucket>` +
				`<TargetPrefix>logs/</TargetPrefix>` +
				`</LoggingEnabled>` +
				`</BucketLoggingStatus>`,
			wantPut:  http.StatusOK,
			wantGet:  http.StatusOK,
			wantBody: "my-logs-bucket",
		},
		{
			name: "logging-disabled",
			configXML: `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
				`</BucketLoggingStatus>`,
			wantPut:  http.StatusOK,
			wantGet:  http.StatusOK,
			wantBody: "BucketLoggingStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "logging-test-" + tt.name

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			// GetBucketLogging before put → empty BucketLoggingStatus (not an error)
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?logging", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "BucketLoggingStatus")

			// PutBucketLogging
			req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?logging", strings.NewReader(tt.configXML))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketLogging
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?logging", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestS3BucketLogging_MalformedXML verifies that PutBucketLogging rejects invalid XML.
func TestS3BucketLogging_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "logging-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?logging", strings.NewReader("not-valid-xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketReplicationCRUD verifies put/get/delete bucket replication configuration.
func TestS3BucketReplicationCRUD(t *testing.T) {
	t.Parallel()

	replicationXML := `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::123456789012:role/replication-role</Role>` +
		`<Rule>` +
		`<ID>rule1</ID>` +
		`<Status>Enabled</Status>` +
		`<Prefix></Prefix>` +
		`<Destination><Bucket>arn:aws:s3:::dest-bucket</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name:       "valid-replication",
			configXML:  replicationXML,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "dest-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "replication-test-" + tt.name

			_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
			require.NoError(t, err)

			// GetBucketReplication before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Contains(t, rec.Body.String(), "ReplicationConfigurationNotFoundError")

			// PutBucketReplication
			req = httptest.NewRequest(http.MethodPut, "/"+bucket+"?replication", strings.NewReader(tt.configXML))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketReplication
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeleteBucketReplication
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetBucketReplication after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3BucketReplication_MalformedXML verifies that PutBucketReplication rejects invalid XML.
func TestS3BucketReplication_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "replication-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?replication", strings.NewReader("not-valid-xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3NewOperations_GetSupportedOperations verifies that all new operations are listed.
func TestS3NewOperations_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "includes PutPublicAccessBlock", want: "PutPublicAccessBlock"},
		{name: "includes GetPublicAccessBlock", want: "GetPublicAccessBlock"},
		{name: "includes DeletePublicAccessBlock", want: "DeletePublicAccessBlock"},
		{name: "includes PutBucketOwnershipControls", want: "PutBucketOwnershipControls"},
		{name: "includes GetBucketOwnershipControls", want: "GetBucketOwnershipControls"},
		{name: "includes DeleteBucketOwnershipControls", want: "DeleteBucketOwnershipControls"},
		{name: "includes PutBucketLogging", want: "PutBucketLogging"},
		{name: "includes GetBucketLogging", want: "GetBucketLogging"},
		{name: "includes PutBucketReplication", want: "PutBucketReplication"},
		{name: "includes GetBucketReplication", want: "GetBucketReplication"},
		{name: "includes DeleteBucketReplication", want: "DeleteBucketReplication"},
		{name: "includes PutBucketEncryption", want: "PutBucketEncryption"},
		{name: "includes GetBucketEncryption", want: "GetBucketEncryption"},
		{name: "includes DeleteBucketEncryption", want: "DeleteBucketEncryption"},
		{name: "includes PutObjectLockConfiguration", want: "PutObjectLockConfiguration"},
		{name: "includes GetObjectLockConfiguration", want: "GetObjectLockConfiguration"},
		{name: "includes PutObjectRetention", want: "PutObjectRetention"},
		{name: "includes GetObjectRetention", want: "GetObjectRetention"},
		{name: "includes PutObjectLegalHold", want: "PutObjectLegalHold"},
		{name: "includes GetObjectLegalHold", want: "GetObjectLegalHold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)
			ops := handler.GetSupportedOperations()
			assert.Contains(t, ops, tt.want)
		})
	}
}

// TestS3NewOperations_NonExistentBucket verifies that new operations return NoSuchBucket for missing buckets.
func TestS3NewOperations_NonExistentBucket(t *testing.T) {
	t.Parallel()

	publicAccessXML := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls>` +
		`<IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy>` +
		`<RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	ownershipXML := `<OwnershipControls>` +
		`<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>` +
		`</OwnershipControls>`
	loggingXML := `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></BucketLoggingStatus>`
	replicationXML := `<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role>` +
		`<Rule><Status>Enabled</Status><Prefix></Prefix>` +
		`<Destination><Bucket>arn:aws:s3:::dest</Bucket></Destination></Rule></ReplicationConfiguration>`

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "GetPublicAccessBlock_NoSuchBucket", method: http.MethodGet, path: "/missing?publicAccessBlock"},
		{
			name:   "PutPublicAccessBlock_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?publicAccessBlock",
			body:   publicAccessXML,
		},
		{name: "DeletePublicAccessBlock_NoSuchBucket", method: http.MethodDelete, path: "/missing?publicAccessBlock"},
		{name: "GetOwnershipControls_NoSuchBucket", method: http.MethodGet, path: "/missing?ownershipControls"},
		{
			name:   "PutOwnershipControls_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?ownershipControls",
			body:   ownershipXML,
		},
		{name: "DeleteOwnershipControls_NoSuchBucket", method: http.MethodDelete, path: "/missing?ownershipControls"},
		{name: "GetLogging_NoSuchBucket", method: http.MethodGet, path: "/missing?logging"},
		{name: "PutLogging_NoSuchBucket", method: http.MethodPut, path: "/missing?logging", body: loggingXML},
		{name: "GetReplication_NoSuchBucket", method: http.MethodGet, path: "/missing?replication"},
		{
			name:   "PutReplication_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?replication",
			body:   replicationXML,
		},
		{name: "DeleteReplication_NoSuchBucket", method: http.MethodDelete, path: "/missing?replication"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Contains(t, rec.Body.String(), "NoSuchBucket")
		})
	}
}

// TestHandler_ServeWebsite verifies the ServeWebsite method for website hosting.
func TestHandler_ServeWebsite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, backend *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "serves index document for root path",
			bucket: "site-bucket",
			key:    "",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "site-bucket")
				mustPutObject(t, backend, "site-bucket", "index.html", []byte("<html>hello</html>"))

				xmlCfg := `<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>`
				err := backend.PutBucketWebsite(t.Context(), "site-bucket", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantBody:   "hello",
		},
		{
			name:   "returns 404 for missing object with error document",
			bucket: "site-err",
			key:    "notfound.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "site-err")
				mustPutObject(t, backend, "site-err", "error.html", []byte("not found page"))

				xmlCfg := "<WebsiteConfiguration>" +
					"<IndexDocument><Suffix>index.html</Suffix></IndexDocument>" +
					"<ErrorDocument><Key>error.html</Key></ErrorDocument>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "site-err", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "not found page",
		},
		{
			name:   "returns 404 JSON when no website config",
			bucket: "no-cfg-bucket",
			key:    "page.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "no-cfg-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchWebsiteConfiguration",
		},
		{
			name:   "redirect all requests to host",
			bucket: "redirect-bucket",
			key:    "page.html",
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "redirect-bucket")

				xmlCfg := "<WebsiteConfiguration>" +
					"<RedirectAllRequestsTo>" +
					"<HostName>example.com</HostName>" +
					"<Protocol>https</Protocol>" +
					"</RedirectAllRequestsTo>" +
					"</WebsiteConfiguration>"
				err := backend.PutBucketWebsite(t.Context(), "redirect-bucket", xmlCfg)
				require.NoError(t, err)
			},
			wantStatus: http.StatusMovedPermanently,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/_gopherstack/website/"+tt.bucket+"/"+tt.key, nil)
			logCtx := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(logCtx)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetPathValues(echo.PathValues{
				{Name: "bucket", Value: tt.bucket},
				{Name: "key", Value: tt.key},
			})

			serveErr := handler.ServeWebsite(c)
			if serveErr != nil {
				_ = c.JSON(rec.Code, map[string]string{"error": serveErr.Error()})
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}
