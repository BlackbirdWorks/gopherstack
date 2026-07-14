package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DeleteObjectTagging verifies that DeleteObjectTagging removes a
// previously-set tag set and returns 204 No Content.
func TestHandler_DeleteObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		tagsXML  string
		wantCode int
	}{
		{
			name:     "delete_object_tagging",
			bucket:   "dtag-bucket",
			key:      "tagged-obj",
			tagsXML:  `<Tagging><TagSet><Tag><Key>foo</Key><Value>bar</Value></Tag></TagSet></Tagging>`,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"/"+tt.key,
				strings.NewReader("data"),
			)
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			tagReq := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"/"+tt.key+"?tagging",
				strings.NewReader(tt.tagsXML),
			)
			tagRec := httptest.NewRecorder()
			serveS3Handler(handler, tagRec, tagReq)
			require.Equal(t, http.StatusOK, tagRec.Code)

			delReq := httptest.NewRequest(
				http.MethodDelete,
				"/"+tt.bucket+"/"+tt.key+"?tagging",
				nil,
			)
			delRec := httptest.NewRecorder()
			serveS3Handler(handler, delRec, delReq)

			assert.Equal(t, tt.wantCode, delRec.Code)
		})
	}
}

// TestObjectTagging_NoSuchKey verifies that GetObjectTagging/PutObjectTagging/
// DeleteObjectTagging all return 404 for a key or bucket that does not exist.
func TestObjectTagging_NoSuchKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		method   string
		url      string
		body     string
		wantCode int
	}{
		{
			name:     "get_object_tagging_no_such_key",
			bucket:   "getotag-nokey",
			method:   http.MethodGet,
			url:      "/getotag-nokey/nokey?tagging",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "put_object_tagging_no_such_key",
			bucket:   "ptag-nokey",
			method:   http.MethodPut,
			url:      "/ptag-nokey/nokey?tagging",
			body:     `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_object_tagging_no_such_bucket",
			bucket:   "no-bucket",
			method:   http.MethodDelete,
			url:      "/no-bucket/obj?tagging",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.bucket != "no-bucket" {
				mustCreateBucket(t, backend, tt.bucket)
			}

			var bodyReader *strings.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			} else {
				bodyReader = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.url, bodyReader)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
