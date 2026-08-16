package s3_test

import (
	"bytes"
	"fmt"
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

	type headArgs struct {
		bucket      string
		key         string
		queryString string
		partCount   int
	}

	type headWant struct {
		wantStatus           int
		wantParts            bool
		wantTruncated        bool
		wantNextMarker       int
		wantVisiblePartCount int
	}

	tests := []struct {
		name string
		args headArgs
		want headWant
	}{
		{
			name: "multipart_object_returns_parts",
			args: headArgs{
				bucket:    "mp-bkt",
				key:       "mp-obj",
				partCount: 2,
			},
			want: headWant{
				wantParts:            true,
				wantStatus:           http.StatusOK,
				wantVisiblePartCount: 2,
			},
		},
		{
			name: "multipart_object_pagination_max_parts",
			args: headArgs{
				bucket:      "mp-bkt-pag",
				key:         "mp-obj-pag",
				queryString: "&max-parts=1",
				partCount:   2,
			},
			want: headWant{
				wantParts:            true,
				wantTruncated:        true,
				wantNextMarker:       1,
				wantVisiblePartCount: 1,
				wantStatus:           http.StatusOK,
			},
		},
		{
			name: "multipart_object_pagination_marker",
			args: headArgs{
				bucket:      "mp-bkt-marker",
				key:         "mp-obj-marker",
				queryString: "&part-number-marker=1",
				partCount:   2,
			},
			want: headWant{
				wantParts:            true,
				wantVisiblePartCount: 1,
				wantStatus:           http.StatusOK,
			},
		},
		{
			name: "single_put_object_no_parts",
			args: headArgs{
				bucket:    "mp-bkt",
				key:       "single-obj",
				partCount: 0,
			},
			want: headWant{
				wantParts:  false,
				wantStatus: http.StatusOK,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.args.bucket)

			if tt.args.partCount > 0 {
				up, err := backend.CreateMultipartUpload(ctx, &sdk_s3.CreateMultipartUploadInput{
					Bucket: &tt.args.bucket,
					Key:    &tt.args.key,
				})
				require.NoError(t, err)

				completedParts := make([]types.CompletedPart, 0, tt.args.partCount)
				for i := 1; i <= tt.args.partCount; i++ {
					pNum := int32(i)
					p, uErr := backend.UploadPart(ctx, &sdk_s3.UploadPartInput{
						Bucket:     &tt.args.bucket,
						Key:        &tt.args.key,
						UploadId:   up.UploadId,
						PartNumber: &pNum,
						Body:       bytes.NewReader(fmt.Appendf(nil, "part%d-payload", i)),
					})
					require.NoError(t, uErr)
					completedParts = append(completedParts, types.CompletedPart{
						PartNumber: &pNum,
						ETag:       p.ETag,
					})
				}

				_, err = backend.CompleteMultipartUpload(ctx, &sdk_s3.CompleteMultipartUploadInput{
					Bucket:   &tt.args.bucket,
					Key:      &tt.args.key,
					UploadId: up.UploadId,
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: completedParts,
					},
				})
				require.NoError(t, err)
			} else {
				mustPutObject(t, backend, tt.args.bucket, tt.args.key, []byte("single-put-data"))
			}

			targetURL := "/" + tt.args.bucket + "/" + tt.args.key + "?attributes" + tt.args.queryString
			req := httptest.NewRequest(http.MethodGet, targetURL, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.want.wantStatus, rec.Code)
			body := rec.Body.String()
			assert.Contains(t, body, "<GetObjectAttributesResponse")
			if tt.want.wantParts {
				assert.Contains(t, body, "<ObjectParts>")
				assert.Contains(t, body, fmt.Sprintf("<TotalPartsCount>%d</TotalPartsCount>", tt.args.partCount))
				if tt.want.wantTruncated {
					assert.Contains(t, body, "<IsTruncated>true</IsTruncated>")
					assert.Contains(
						t,
						body,
						fmt.Sprintf("<NextPartNumberMarker>%d</NextPartNumberMarker>", tt.want.wantNextMarker),
					)
				}
				assert.Equal(t, tt.want.wantVisiblePartCount, strings.Count(body, "<Part>"))
			} else {
				assert.NotContains(t, body, "<ObjectParts>")
			}
		})
	}
}
