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
	"github.com/blackbirdworks/gopherstack/s3"
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
			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}, nil)
			handler := s3.NewHandler(backend, slog.Default())
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

func TestHandler_BucketTagging_NotImplemented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
	}{
		{name: "GET bucket tagging returns 501", method: http.MethodGet},
		{name: "PUT bucket tagging returns 501", method: http.MethodPut},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(tt.method, "/bkt?tagging", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotImplemented, rec.Code)
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
