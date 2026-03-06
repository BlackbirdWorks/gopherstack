package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestHandler_ListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		listURL         string
		wantBucket      string
		wantKeyPrefix   string
		keysToUpload    []string
		wantStatus      int
		wantUploadCount int
		createBucket    bool
	}{
		{
			name:            "empty bucket returns no uploads",
			bucket:          "mpu-empty",
			createBucket:    true,
			listURL:         "/mpu-empty?uploads",
			wantStatus:      http.StatusOK,
			wantBucket:      "mpu-empty",
			wantUploadCount: 0,
		},
		{
			name:            "bucket with two uploads returns both",
			bucket:          "mpu-list",
			createBucket:    true,
			keysToUpload:    []string{"key1", "key2"},
			listURL:         "/mpu-list?uploads",
			wantStatus:      http.StatusOK,
			wantBucket:      "mpu-list",
			wantUploadCount: 2,
		},
		{
			name:            "prefix filter returns only matching uploads",
			bucket:          "mpu-prefix",
			createBucket:    true,
			keysToUpload:    []string{"logs/2024/a", "logs/2024/b", "data/x"},
			listURL:         "/mpu-prefix?uploads&prefix=logs/",
			wantStatus:      http.StatusOK,
			wantUploadCount: 2,
			wantKeyPrefix:   "logs/",
		},
		{
			name:         "nonexistent bucket returns 404",
			bucket:       "no-such-bucket",
			createBucket: false,
			listURL:      "/no-such-bucket?uploads",
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			if tt.createBucket {
				mustCreateBucket(t, backend, tt.bucket)
			}

			for _, key := range tt.keysToUpload {
				req := httptest.NewRequest(http.MethodPost, "/"+tt.bucket+"/"+key+"?uploads", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			req := httptest.NewRequest(http.MethodGet, tt.listURL, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var result s3.ListMultipartUploadsResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))

			if tt.wantBucket != "" {
				assert.Equal(t, tt.wantBucket, result.Bucket)
			}

			assert.Len(t, result.Uploads, tt.wantUploadCount)

			if tt.wantKeyPrefix != "" {
				for _, u := range result.Uploads {
					assert.True(t, strings.HasPrefix(u.Key, tt.wantKeyPrefix))
				}
			}
		})
	}
}

func TestHandler_ListParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		bucket           string
		key              string
		wantBucket       string
		wantKey          string
		partsToUpload    []string
		wantStatus       int
		wantPartCount    int
		useNonexistentID bool
		checkPartDetails bool
	}{
		{
			name:             "upload with two parts lists both",
			bucket:           "lp-bucket",
			key:              "myobj",
			partsToUpload:    []string{"1", "2"},
			wantStatus:       http.StatusOK,
			wantBucket:       "lp-bucket",
			wantKey:          "myobj",
			wantPartCount:    2,
			checkPartDetails: true,
		},
		{
			name:             "nonexistent upload ID returns 404",
			bucket:           "lp-nosuchupload",
			key:              "obj",
			useNonexistentID: true,
			wantStatus:       http.StatusNotFound,
		},
		{
			name:          "upload with no parts returns empty list",
			bucket:        "lp-empty",
			key:           "obj",
			partsToUpload: []string{},
			wantStatus:    http.StatusOK,
			wantPartCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			uploadID := "nonexistent"

			if !tt.useNonexistentID {
				req := httptest.NewRequest(http.MethodPost, "/"+tt.bucket+"/"+tt.key+"?uploads", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				body := rec.Body.String()
				start := strings.Index(body, "<UploadId>") + len("<UploadId>")
				end := strings.Index(body, "</UploadId>")
				uploadID = body[start:end]

				for _, partNum := range tt.partsToUpload {
					req = httptest.NewRequest(
						http.MethodPut,
						"/"+tt.bucket+"/"+tt.key+"?partNumber="+partNum+"&uploadId="+uploadID,
						strings.NewReader("part-data-"+partNum),
					)
					rec = httptest.NewRecorder()
					serveS3Handler(handler, rec, req)
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key+"?uploadId="+uploadID, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var result s3.ListPartsResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))

			if tt.wantBucket != "" {
				assert.Equal(t, tt.wantBucket, result.Bucket)
			}

			if tt.wantKey != "" {
				assert.Equal(t, tt.wantKey, result.Key)
			}

			assert.Equal(t, uploadID, result.UploadID)

			require.Len(t, result.Parts, tt.wantPartCount)

			if tt.checkPartDetails {
				assert.Equal(t, 1, result.Parts[0].PartNumber)
				assert.Equal(t, 2, result.Parts[1].PartNumber)
				assert.Positive(t, result.Parts[0].Size)
				assert.NotEmpty(t, result.Parts[0].ETag)
			}
		})
	}
}
