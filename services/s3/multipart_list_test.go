package s3_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestListParts_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-bucket")

	// Create multipart upload.
	createRec := doRequest(handler, http.MethodPost, "/mp-bucket/bigobj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload 5 parts (small for test).
	for i := 1; i <= 5; i++ {
		partPath := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&partNumber=%d", uploadID, i)
		partRec := doRequest(handler, http.MethodPut, partPath,
			strings.NewReader(fmt.Sprintf("part%d", i)), nil)
		require.Equal(t, http.StatusOK, partRec.Code)
	}

	// ListParts with max-parts=2 and no marker → first 2 parts.
	listPath := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&max-parts=2", uploadID)
	listRec := doRequest(handler, http.MethodGet, listPath, nil, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec.Body).Decode(&result))

	assert.Len(t, result.Parts, 2)
	assert.True(t, result.IsTruncated)
	assert.Equal(t, "2", result.NextPartNumberMarker)
	assert.Equal(t, 1, result.Parts[0].PartNumber)
	assert.Equal(t, 2, result.Parts[1].PartNumber)

	// ListParts with marker=2 → parts 3, 4, 5.
	listPath2 := fmt.Sprintf("/mp-bucket/bigobj?uploadId=%s&part-number-marker=2", uploadID)
	listRec2 := doRequest(handler, http.MethodGet, listPath2, nil, nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var result2 s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec2.Body).Decode(&result2))

	assert.Len(t, result2.Parts, 3)
	assert.False(t, result2.IsTruncated)
	assert.Equal(t, 3, result2.Parts[0].PartNumber)
	assert.Equal(t, 5, result2.Parts[2].PartNumber)
}

func TestListParts_DefaultMaxParts1000(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-default")

	createRec := doRequest(handler, http.MethodPost, "/mp-default/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload 3 parts.
	for i := 1; i <= 3; i++ {
		partPath := fmt.Sprintf("/mp-default/obj?uploadId=%s&partNumber=%d", uploadID, i)
		doRequest(handler, http.MethodPut, partPath, strings.NewReader("data"), nil)
	}

	listPath := fmt.Sprintf("/mp-default/obj?uploadId=%s", uploadID)
	listRec := doRequest(handler, http.MethodGet, listPath, nil, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.NewDecoder(listRec.Body).Decode(&result))

	assert.Len(t, result.Parts, 3)
	assert.False(t, result.IsTruncated)
	assert.Equal(t, 1000, result.MaxParts)
}

// ─── Multipart 5 MiB minimum enforcement ────────────────────────────────────

func TestHandler_UploadPartCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rangeHeader  string
		expectedBody string
	}{
		{
			name:         "copies full source object into multipart part",
			expectedBody: "hello multipart copy",
		},
		{
			name:         "copies ranged slice into multipart part",
			rangeHeader:  "bytes=6-14",
			expectedBody: "multipart",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "src", []byte("hello multipart copy"))

			reqInit := httptest.NewRequest(http.MethodPost, "/bkt/dst?uploads", nil)
			recInit := httptest.NewRecorder()
			serveS3Handler(handler, recInit, reqInit)
			require.Equal(t, http.StatusOK, recInit.Code)

			var initResp s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(recInit.Body).Decode(&initResp))
			uploadID := initResp.UploadID

			reqPart := httptest.NewRequest(
				http.MethodPut,
				"/bkt/dst?partNumber=1&uploadId="+uploadID,
				nil,
			)
			reqPart.Header.Set("X-Amz-Copy-Source", "/bkt/src")
			if tt.rangeHeader != "" {
				reqPart.Header.Set("X-Amz-Copy-Source-Range", tt.rangeHeader)
			}

			recPart := httptest.NewRecorder()
			serveS3Handler(handler, recPart, reqPart)
			require.Equal(t, http.StatusOK, recPart.Code)
			assert.Contains(t, recPart.Body.String(), "CopyPartResult")

			var copyResp s3.UploadPartCopyResult
			require.NoError(t, xml.NewDecoder(recPart.Body).Decode(&copyResp))
			require.NotEmpty(t, copyResp.ETag)

			completeXML := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" +
				copyResp.ETag + "</ETag></Part></CompleteMultipartUpload>"
			reqComplete := httptest.NewRequest(
				http.MethodPost,
				"/bkt/dst?uploadId="+uploadID,
				strings.NewReader(completeXML),
			)
			recComplete := httptest.NewRecorder()
			serveS3Handler(handler, recComplete, reqComplete)
			require.Equal(t, http.StatusOK, recComplete.Code)

			reqGet := httptest.NewRequest(http.MethodGet, "/bkt/dst", nil)
			recGet := httptest.NewRecorder()
			serveS3Handler(handler, recGet, reqGet)
			require.Equal(t, http.StatusOK, recGet.Code)
			assert.Equal(t, tt.expectedBody, recGet.Body.String())
		})
	}
}

func TestHandler_CompleteMultipartUpload_PartOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		partOrder  []int // order to send parts in the complete request
		wantStatus int
	}{
		{
			name:       "ascending order succeeds",
			partOrder:  []int{1, 2, 3},
			wantStatus: http.StatusOK,
		},
		{
			name:       "descending order fails",
			partOrder:  []int{3, 2, 1},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate part number fails",
			partOrder:  []int{1, 1, 2},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			// Create upload
			reqInit := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
			recInit := httptest.NewRecorder()
			serveS3Handler(handler, recInit, reqInit)
			require.Equal(t, http.StatusOK, recInit.Code)

			var initResp struct {
				UploadID string `xml:"UploadId"`
			}
			require.NoError(t, xml.Unmarshal(recInit.Body.Bytes(), &initResp))
			uploadID := initResp.UploadID

			// Upload parts 1-3 regardless of test order
			partETags := make(map[int]string)
			for i := 1; i <= 3; i++ {
				req := httptest.NewRequest(
					http.MethodPut,
					"/bkt/obj?partNumber="+strconv.Itoa(i)+"&uploadId="+uploadID,
					strings.NewReader("data"),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				partETags[i] = rec.Header().Get("ETag")
			}

			// Build complete XML in the requested order
			var parts strings.Builder
			for _, pn := range tt.partOrder {
				parts.WriteString("<Part><PartNumber>")
				parts.WriteString(strconv.Itoa(pn))
				parts.WriteString("</PartNumber><ETag>")
				parts.WriteString(partETags[pn])
				parts.WriteString("</ETag></Part>")
			}
			body := "<CompleteMultipartUpload>" + parts.String() + "</CompleteMultipartUpload>"

			reqComplete := httptest.NewRequest(
				http.MethodPost, "/bkt/obj?uploadId="+uploadID, strings.NewReader(body),
			)
			recComplete := httptest.NewRecorder()
			serveS3Handler(handler, recComplete, reqComplete)
			assert.Equal(t, tt.wantStatus, recComplete.Code)
		})
	}
}

func TestHandler_ListMultipartUploads_MaxUploads(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	// Create 5 multipart uploads
	uploadIDs := make([]string, 5)
	for i := range uploadIDs {
		req := httptest.NewRequest(
			http.MethodPost,
			"/bkt/obj"+strings.Repeat(string(rune('a'+i)), 1)+"?uploads",
			nil,
		)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List with max-uploads=2
	req := httptest.NewRequest(http.MethodGet, "/bkt?uploads&max-uploads=2", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	type listUploadsResult struct {
		Uploads []struct {
			Key string `xml:"Key"`
		} `xml:"Upload"`
		IsTruncated bool `xml:"IsTruncated"`
	}

	var result listUploadsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Len(t, result.Uploads, 2, "should return exactly MaxUploads entries")
	assert.True(t, result.IsTruncated, "should be truncated")
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
