package s3_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestHandler_HeadObject_StorageClassAndAcceptRanges(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("data"))

	req := httptest.NewRequest(http.MethodHead, "/bkt/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "bytes", rec.Header().Get("Accept-Ranges"))
	// Real S3 omits x-amz-storage-class for STANDARD-class objects.
	assert.Empty(t, rec.Header().Get("X-Amz-Storage-Class"))
}

// TestHandler_GetObject_ResponseHeaderOverrides verifies the AWS response-*
// query parameters override the corresponding response headers on GET (used
// heavily via presigned URLs, e.g. forcing a download filename).

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

func TestHandler_GetObjectAttributes_MultipartParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		key        string
		partCount  int
		wantParts  bool
		wantStatus int
	}{
		{
			name:       "multipart_object_returns_parts",
			bucket:     "mp-bkt",
			key:        "mp-obj",
			partCount:  2,
			wantParts:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "single_put_object_no_parts",
			bucket:     "mp-bkt",
			key:        "single-obj",
			partCount:  0,
			wantParts:  false,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			if tt.wantParts {
				// 1. Initiate multipart upload
				up, err := backend.CreateMultipartUpload(ctx, &sdk_s3.CreateMultipartUploadInput{
					Bucket: &tt.bucket,
					Key:    &tt.key,
				})
				require.NoError(t, err)

				// 2. Upload parts
				p1Num := int32(1)
				p1, err := backend.UploadPart(ctx, &sdk_s3.UploadPartInput{
					Bucket:     &tt.bucket,
					Key:        &tt.key,
					UploadId:   up.UploadId,
					PartNumber: &p1Num,
					Body:       bytes.NewReader([]byte("part1-payload")),
				})
				require.NoError(t, err)

				p2Num := int32(2)
				p2, err := backend.UploadPart(ctx, &sdk_s3.UploadPartInput{
					Bucket:     &tt.bucket,
					Key:        &tt.key,
					UploadId:   up.UploadId,
					PartNumber: &p2Num,
					Body:       bytes.NewReader([]byte("part2-payload")),
				})
				require.NoError(t, err)

				// 3. Complete multipart upload
				_, err = backend.CompleteMultipartUpload(ctx, &sdk_s3.CompleteMultipartUploadInput{
					Bucket:   &tt.bucket,
					Key:      &tt.key,
					UploadId: up.UploadId,
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{PartNumber: &p1Num, ETag: p1.ETag},
							{PartNumber: &p2Num, ETag: p2.ETag},
						},
					},
				})
				require.NoError(t, err)
			} else {
				mustPutObject(t, backend, tt.bucket, tt.key, []byte("single-put-data"))
			}

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key+"?attributes", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			body := rec.Body.String()
			assert.Contains(t, body, "<GetObjectAttributesResult")
			if tt.wantParts {
				assert.Contains(t, body, "<ObjectParts>")
				assert.Contains(t, body, "<TotalPartsCount>2</TotalPartsCount>")
				assert.Contains(t, body, "<PartNumber>1</PartNumber>")
				assert.Contains(t, body, "<PartNumber>2</PartNumber>")
			} else {
				assert.NotContains(t, body, "<ObjectParts>")
			}
		})
	}
}
