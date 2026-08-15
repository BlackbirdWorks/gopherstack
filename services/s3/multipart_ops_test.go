package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestCompleteMultipartUpload_EmptyParts verifies that CompleteMultipartUpload
// rejects a request with no <Parts> (or an empty <Parts> list).
func TestCompleteMultipartUpload_EmptyParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "nil_parts_rejected",
			body:     `<CompleteMultipartUpload/>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_parts_list_rejected",
			body:     `<CompleteMultipartUpload><Parts></Parts></CompleteMultipartUpload>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "test-bucket")

			// Initiate multipart upload.
			req := httptest.NewRequest(http.MethodPost, "/test-bucket/my-key?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var initResult s3.InitiateMultipartUploadResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
			uploadID := initResult.UploadID
			require.NotEmpty(t, uploadID)

			// Complete with empty parts.
			req = httptest.NewRequest(
				http.MethodPost,
				"/test-bucket/my-key?uploadId="+uploadID,
				strings.NewReader(tt.body),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_MultipartUploadTagging verifies that the X-Amz-Tagging header on
// CreateMultipartUpload is forwarded to the backend and applied on completion.
func TestHandler_MultipartUploadTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taggingHdr string
		wantBody   string
	}{
		{
			name:       "tags_in_header_applied_on_complete",
			taggingHdr: "color=blue&size=large",
			wantBody:   "color",
		},
		{
			name:       "no_tagging_header",
			taggingHdr: "",
			wantBody:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			// Initiate with X-Amz-Tagging header.
			reqInit := httptest.NewRequest(http.MethodPost, "/bkt/obj?uploads", nil)
			if tt.taggingHdr != "" {
				reqInit.Header.Set("X-Amz-Tagging", tt.taggingHdr)
			}
			recInit := httptest.NewRecorder()
			serveS3Handler(handler, recInit, reqInit)
			require.Equal(t, http.StatusOK, recInit.Code)

			// Parse upload ID from XML response.
			var initResp struct {
				UploadID string `xml:"UploadId"`
			}
			require.NoError(t, xml.NewDecoder(recInit.Body).Decode(&initResp))
			uploadID := initResp.UploadID
			require.NotEmpty(t, uploadID)

			// Upload one part (the in-memory backend has no minimum part size).
			partBody := strings.Repeat("x", 1024) // 1 KiB is enough for the mock
			reqPart := httptest.NewRequest(
				http.MethodPut, "/bkt/obj?partNumber=1&uploadId="+uploadID,
				strings.NewReader(partBody),
			)
			recPart := httptest.NewRecorder()
			serveS3Handler(handler, recPart, reqPart)
			require.Equal(t, http.StatusOK, recPart.Code)
			etag := recPart.Header().Get("ETag")
			require.NotEmpty(t, etag)

			// Complete the multipart upload.
			completeXML := `<CompleteMultipartUpload>` +
				`<Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part>` +
				`</CompleteMultipartUpload>`
			reqComplete := httptest.NewRequest(
				http.MethodPost, "/bkt/obj?uploadId="+uploadID,
				strings.NewReader(completeXML),
			)
			recComplete := httptest.NewRecorder()
			serveS3Handler(handler, recComplete, reqComplete)
			require.Equal(t, http.StatusOK, recComplete.Code)

			// Retrieve object tags.
			reqTags := httptest.NewRequest(http.MethodGet, "/bkt/obj?tagging", nil)
			recTags := httptest.NewRecorder()
			serveS3Handler(handler, recTags, reqTags)

			if tt.wantBody != "" {
				require.Equal(t, http.StatusOK, recTags.Code)
				assert.Contains(t, recTags.Body.String(), tt.wantBody)
			}

			// Verify object exists.
			reqGet := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
			recGet := httptest.NewRecorder()
			serveS3Handler(handler, recGet, reqGet)
			assert.Equal(t, http.StatusOK, recGet.Code)
		})
	}
}

// TestMultipartUpload_TaggingPropagated verifies that tags set via the Tagging
// field on CreateMultipartUpload are applied to the resulting object version after
// a successful CompleteMultipartUpload.
func TestMultipartUpload_TaggingPropagated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagging  string
		wantTags int
	}{
		{name: "tags_applied_on_complete", tagging: "env=prod&team=infra", wantTags: 2},
		{name: "no_tags_when_not_specified", tagging: "", wantTags: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			createOut, err := backend.CreateMultipartUpload(
				t.Context(),
				&sdk_s3.CreateMultipartUploadInput{
					Bucket:  aws.String("bkt"),
					Key:     aws.String("key"),
					Tagging: aws.String(tt.tagging),
				},
			)
			require.NoError(t, err)

			uploadID := createOut.UploadId
			// The in-memory backend has no minimum part size; use a small payload.
			partData := bytes.Repeat([]byte("x"), 1024) // 1 KiB

			p1, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			_, err = backend.CompleteMultipartUpload(
				t.Context(),
				&sdk_s3.CompleteMultipartUploadInput{
					Bucket:   aws.String("bkt"),
					Key:      aws.String("key"),
					UploadId: uploadID,
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{PartNumber: aws.Int32(1), ETag: p1.ETag},
						},
					},
				},
			)
			require.NoError(t, err)

			taggingOut, getErr := backend.GetObjectTagging(
				t.Context(),
				&sdk_s3.GetObjectTaggingInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				},
			)

			if tt.wantTags == 0 {
				// No tags set → either NoSuchTagSet or empty tag set.
				if getErr == nil {
					assert.Empty(t, taggingOut.TagSet, "expected no tags on object")
				}
			} else {
				require.NoError(t, getErr)
				assert.Len(t, taggingOut.TagSet, tt.wantTags)
			}
		})
	}
}

func TestCompleteMultipart_EntityTooSmall(t *testing.T) {
	t.Parallel()

	// Use a backend WITH size enforcement (no skip flag).
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	mustCreateBucket(t, backend, "mp-size")

	createRec := doRequest(handler, http.MethodPost, "/mp-size/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload two small parts (below 5 MiB).
	part1 := strings.NewReader("small-part-1")
	part2 := strings.NewReader("small-part-2")

	path1 := fmt.Sprintf("/mp-size/obj?uploadId=%s&partNumber=1", uploadID)
	path2 := fmt.Sprintf("/mp-size/obj?uploadId=%s&partNumber=2", uploadID)

	rec1 := doRequest(handler, http.MethodPut, path1, part1, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	rec2 := doRequest(handler, http.MethodPut, path2, part2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	etag2 := rec2.Header().Get("ETag")

	// Complete — part 1 is below 5 MiB and is not the last part → EntityTooSmall.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)

	completePath := fmt.Sprintf("/mp-size/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)

	assert.Equal(t, http.StatusBadRequest, completeRec.Code)

	var errResp s3.ErrorResponse
	require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&errResp))
	assert.Equal(t, "EntityTooSmall", errResp.Code)
}

func TestCompleteMultipart_EmptyParts_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "no_parts_element",
			body: `<CompleteMultipartUpload></CompleteMultipartUpload>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "mp-empty")

			createRec := doRequest(handler, http.MethodPost, "/mp-empty/obj?uploads", nil, nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			var initResult s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
			uploadID := initResult.UploadID

			completePath := fmt.Sprintf("/mp-empty/obj?uploadId=%s", uploadID)
			completeRec := doRequest(handler, http.MethodPost, completePath,
				strings.NewReader(tt.body), nil)

			assert.Equal(t, http.StatusBadRequest, completeRec.Code)

			var errResp s3.ErrorResponse
			require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&errResp))
			assert.Equal(t, "InvalidRequest", errResp.Code)
		})
	}
}

func TestCompleteMultipart_SinglePartSmall_OK(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-single")

	createRec := doRequest(handler, http.MethodPost, "/mp-single/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	path1 := fmt.Sprintf("/mp-single/obj?uploadId=%s&partNumber=1", uploadID)
	rec1 := doRequest(handler, http.MethodPut, path1, strings.NewReader("tiny"), nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	// Single part can be any size.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1)

	completePath := fmt.Sprintf("/mp-single/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)

	assert.Equal(t, http.StatusOK, completeRec.Code)
}

// ─── x-amz-expected-bucket-owner ─────────────────────────────────────────────

func TestMultipartETag_MatchesAWSFormat(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mp-etag")

	createRec := doRequest(handler, http.MethodPost, "/mp-etag/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	// Upload two parts large enough to pass 5 MiB threshold.
	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024+1)
	part2Data := bytes.Repeat([]byte("B"), 1024)

	path1 := fmt.Sprintf("/mp-etag/obj?uploadId=%s&partNumber=1", uploadID)
	path2 := fmt.Sprintf("/mp-etag/obj?uploadId=%s&partNumber=2", uploadID)

	rec1 := doRequest(handler, http.MethodPut, path1, bytes.NewReader(part1Data), nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	etag1 := rec1.Header().Get("ETag")

	rec2 := doRequest(handler, http.MethodPut, path2, bytes.NewReader(part2Data), nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	etag2 := rec2.Header().Get("ETag")

	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>
	</CompleteMultipartUpload>`, etag1, etag2)

	completePath := fmt.Sprintf("/mp-etag/obj?uploadId=%s", uploadID)
	completeRec := doRequest(handler, http.MethodPost, completePath,
		strings.NewReader(completeBody), nil)
	require.Equal(t, http.StatusOK, completeRec.Code)

	var completeResult s3.CompleteMultipartUploadResult
	require.NoError(t, xml.NewDecoder(completeRec.Body).Decode(&completeResult))

	// The ETag should match the AWS multipart format: "<md5hex>-2"
	assert.True(t, strings.HasSuffix(completeResult.ETag, "-2\""),
		"multipart ETag should end with -<partCount>\", got %s", completeResult.ETag)

	// Verify the ETag is MD5(rawmd5_1 || rawmd5_2)-2
	raw1, _ := hex.DecodeString(strings.Trim(etag1, "\""))
	raw2, _ := hex.DecodeString(strings.Trim(etag2, "\""))
	combined := append(raw1, raw2...) //nolint:gocritic // intentional append
	h := md5.New()
	h.Write(combined)
	expectedETag := fmt.Sprintf("\"%s-2\"", hex.EncodeToString(h.Sum(nil)))
	assert.Equal(t, expectedETag, completeResult.ETag)
}

// ─── CopyObject expected bucket owner ────────────────────────────────────────

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

func TestHandler_MultipartUpload_ETagFormat(t *testing.T) {
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

	// Upload 2 parts
	etags := make([]string, 2)
	for i := 1; i <= 2; i++ {
		req := httptest.NewRequest(
			http.MethodPut,
			"/bkt/obj?partNumber="+strconv.Itoa(i)+"&uploadId="+uploadID,
			strings.NewReader("part-data"),
		)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		etags[i-1] = rec.Header().Get("ETag")
	}

	// Complete
	body := "<CompleteMultipartUpload>" +
		"<Part><PartNumber>1</PartNumber><ETag>" + etags[0] + "</ETag></Part>" +
		"<Part><PartNumber>2</PartNumber><ETag>" + etags[1] + "</ETag></Part>" +
		"</CompleteMultipartUpload>"

	reqComplete := httptest.NewRequest(
		http.MethodPost, "/bkt/obj?uploadId="+uploadID, strings.NewReader(body),
	)
	recComplete := httptest.NewRecorder()
	serveS3Handler(handler, recComplete, reqComplete)
	require.Equal(t, http.StatusOK, recComplete.Code)

	// Get object and check ETag has -2 suffix (2 parts)
	reqGet := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
	recGet := httptest.NewRecorder()
	serveS3Handler(handler, recGet, reqGet)
	require.Equal(t, http.StatusOK, recGet.Code)

	etag := recGet.Header().Get("ETag")
	assert.True(
		t,
		strings.HasSuffix(etag, "-2\""),
		"multipart ETag should end with -2\" got: %s",
		etag,
	)
	assert.True(t, strings.HasPrefix(etag, "\""), "ETag should start with quote, got: %s", etag)
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

			wantInit := s3.InitiateMultipartUploadResult{
				Bucket:   "bkt",
				Key:      "mp",
				UploadID: initResp.UploadID,
			}
			initDiff := cmp.Diff(
				wantInit, initResp,
				cmpopts.IgnoreFields(s3.InitiateMultipartUploadResult{}, "UploadID", "XMLName"),
			)
			assert.Empty(t, initDiff, "InitiateMultipartUploadResult mismatch")
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

// TestMultipartUpload_StorageClassAppliedToObject is a regression test: real S3
// fixes an object's storage class at CreateMultipartUpload time (the
// x-amz-storage-class header, same session-init semantics as SSE) and both
// applies it to the object CompleteMultipartUpload produces and reports it back
// from ListMultipartUploads. gopherstack's CreateMultipartUpload previously read
// input.StorageClass into nothing -- the field was declared on the SDK input and
// never referenced anywhere in multipart.go -- so any multipart upload silently
// landed as STANDARD regardless of what the caller asked for, and
// ListMultipartUploads never populated StorageClass/Owner/Initiator at all
// (real aws-sdk-go-v2/service/s3@v1.106.5 deserializers.go's
// awsRestxml_deserializeDocumentMultipartUpload decodes exactly these fields).
// Driving the real SDK client (not a raw-body substring assertion) proves the
// typed field actually decodes, not just that some XML text is present.
func TestMultipartUpload_StorageClassAppliedToObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		requested    types.StorageClass
		wantReported types.StorageClass
	}{
		{name: "glacier", requested: types.StorageClassGlacier, wantReported: types.StorageClassGlacier},
		{name: "standard ia", requested: types.StorageClassStandardIa, wantReported: types.StorageClassStandardIa},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "mp-storage-class-" + strings.ReplaceAll(tt.name, " ", "-")
			key := "obj.bin"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			created, err := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket:       aws.String(bucket),
				Key:          aws.String(key),
				StorageClass: tt.requested,
			})
			require.NoError(t, err)
			uploadID := created.UploadId

			// ListMultipartUploads must report StorageClass/Owner/Initiator for the
			// still-in-progress upload -- these were never populated before this fix.
			listed, err := client.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)
			require.Len(t, listed.Uploads, 1)
			assert.Equal(t, tt.wantReported, listed.Uploads[0].StorageClass)
			require.NotNil(t, listed.Uploads[0].Owner)
			assert.NotEmpty(t, aws.ToString(listed.Uploads[0].Owner.ID))
			require.NotNil(t, listed.Uploads[0].Initiator)
			assert.NotEmpty(t, aws.ToString(listed.Uploads[0].Initiator.ID))

			part, err := client.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       strings.NewReader("payload"),
			})
			require.NoError(t, err)

			_, err = client.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			require.NoError(t, err)

			head, err := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantReported, head.StorageClass)
		})
	}
}
