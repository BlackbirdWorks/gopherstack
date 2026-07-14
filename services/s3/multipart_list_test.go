package s3_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// listMPUResultXML is a minimal struct for parsing ListMultipartUploads responses.
type listMPUResultXML struct {
	XMLName   xml.Name `xml:"ListMultipartUploadsResult"`
	Delimiter string   `xml:"Delimiter"`
}

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
				req := httptest.NewRequest(
					http.MethodPost,
					"/"+tt.bucket+"/"+tt.key+"?uploads",
					nil,
				)
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

			req := httptest.NewRequest(
				http.MethodGet,
				"/"+tt.bucket+"/"+tt.key+"?uploadId="+uploadID,
				nil,
			)
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

// TestListMultipartUploads_DelimiterEchoed verifies that the Delimiter query
// parameter is echoed in the ListMultipartUploads response body, matching AWS S3.
func TestListMultipartUploads_DelimiterEchoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		delimiter     string
		wantDelimiter string
	}{
		{
			name:          "no delimiter: Delimiter element absent",
			bucket:        "b2mu-nodlm",
			delimiter:     "",
			wantDelimiter: "",
		},
		{
			name:          "slash delimiter: echoed in response",
			bucket:        "b2mu-slash",
			delimiter:     "/",
			wantDelimiter: "/",
		},
		{
			name:          "dash delimiter: echoed in response",
			bucket:        "b2mu-dash",
			delimiter:     "-",
			wantDelimiter: "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			url := "/" + tt.bucket + "?uploads"
			if tt.delimiter != "" {
				url += "&delimiter=" + tt.delimiter
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			var result listMPUResultXML
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantDelimiter, result.Delimiter,
				"ListMultipartUploads must echo the delimiter request parameter")
		})
	}
}

// TestListMultipartUploads_KeyMarkerPagination verifies key-marker (and
// key-marker + upload-id-marker) pagination behavior via seekMultipartMarker.
func TestListMultipartUploads_KeyMarkerPagination(t *testing.T) {
	t.Parallel()

	t.Run("key_marker_skips_prior_keys", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "smm-bucket")

		// Start several multipart uploads.
		keys := []string{"aaa/file", "bbb/file", "ccc/file"}
		uploadIDs := make(map[string]string)

		for _, key := range keys {
			req := httptest.NewRequest(http.MethodPost, "/smm-bucket/"+key+"?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(rec.Body).Decode(&resp))
			uploadIDs[key] = resp.UploadID
		}

		// List with key-marker set to "bbb/file" — should skip aaa/file and bbb/file.
		req := httptest.NewRequest(http.MethodGet,
			"/smm-bucket?uploads&key-marker=bbb/file", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		assert.NotContains(t, body, "aaa/file", "aaa/file should be skipped by key-marker")
		assert.Contains(t, body, "ccc/file", "ccc/file should appear after key-marker")
	})

	t.Run("key_marker_plus_upload_id_marker", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "smm2-bucket")

		// Start two uploads for the same key.
		uploadIDs := make([]string, 0, 2)
		for range 2 {
			req := httptest.NewRequest(http.MethodPost, "/smm2-bucket/same-key?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(rec.Body).Decode(&resp))
			uploadIDs = append(uploadIDs, resp.UploadID)
		}

		// List all to get upload IDs in order.
		listReq := httptest.NewRequest(http.MethodGet, "/smm2-bucket?uploads", nil)
		listRec := httptest.NewRecorder()
		serveS3Handler(handler, listRec, listReq)
		require.Equal(t, http.StatusOK, listRec.Code)

		// Use key-marker + upload-id-marker to skip the first upload for "same-key".
		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf(
				"/smm2-bucket?uploads&key-marker=same-key&upload-id-marker=%s",
				uploadIDs[0],
			),
			nil,
		)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("key_marker_beyond_all_uploads", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "smm3-bucket")

		// Start one upload.
		req := httptest.NewRequest(http.MethodPost, "/smm3-bucket/aaa?uploads", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// key-marker beyond all keys → empty result.
		listReq := httptest.NewRequest(http.MethodGet, "/smm3-bucket?uploads&key-marker=zzz", nil)
		listRec := httptest.NewRecorder()
		serveS3Handler(handler, listRec, listReq)
		require.Equal(t, http.StatusOK, listRec.Code)
	})
}

// TestAbortMultipartUpload_NonExistentUpload verifies that aborting a
// nonexistent upload ID returns 404.
func TestAbortMultipartUpload_NonExistentUpload(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "abort-ne")

	req := httptest.NewRequest(http.MethodDelete,
		"/abort-ne/obj?uploadId=nonexistent-upload-id", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
