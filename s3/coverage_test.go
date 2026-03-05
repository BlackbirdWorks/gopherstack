package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_BucketACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucket    string
		method    string
		url       string
		aclHeader string
		wantCode  int
	}{
		{
			name:      "put_bucket_acl",
			bucket:    "acl-put-test",
			method:    http.MethodPut,
			url:       "/acl-put-test?acl",
			aclHeader: "public-read",
			wantCode:  http.StatusOK,
		},
		{
			name:     "get_bucket_acl",
			bucket:   "acl-get-test",
			method:   http.MethodGet,
			url:      "/acl-get-test?acl",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(tt.method, tt.url, nil)
			if tt.aclHeader != "" {
				req.Header.Set("X-Amz-Acl", tt.aclHeader)
			}
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutObject_ContentMD5(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucket    string
		url       string
		body      string
		md5Header string
		wantCode  int
	}{
		{
			name:      "valid_md5",
			bucket:    "md5-valid-bucket",
			url:       "/md5-valid-bucket/hello.txt",
			body:      "hello world",
			md5Header: "XrY7u+Ae7tCTyyK7j1rNww==",
			wantCode:  http.StatusOK,
		},
		{
			name:      "invalid_md5",
			bucket:    "md5-invalid-bucket",
			url:       "/md5-invalid-bucket/hello.txt",
			body:      "hello world",
			md5Header: "AAAAAAAAAAAAAAAAAAAAAA==",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodPut, tt.url, strings.NewReader(tt.body))
			req.Header.Set("Content-Md5", tt.md5Header)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteObjects_BulkOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		deleteBody   string
		wantBody     string
		setupObjects []string
		wantCode     int
	}{
		{
			name:         "mixed_objects_including_nonexistent",
			bucket:       "del-objects-bucket",
			setupObjects: []string{"obj1", "obj2"},
			deleteBody: `<Delete>` +
				`<Object><Key>obj1</Key></Object>` +
				`<Object><Key>obj2</Key></Object>` +
				`<Object><Key>nonexistent</Key></Object>` +
				`</Delete>`,
			wantCode: http.StatusOK,
			wantBody: "DeleteResult",
		},
		{
			name:       "invalid_xml_body",
			bucket:     "del-bad-xml",
			deleteBody: "not-xml",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			for _, key := range tt.setupObjects {
				putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+key, strings.NewReader("data"))
				putRec := httptest.NewRecorder()
				serveS3Handler(handler, putRec, putReq)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			req := httptest.NewRequest(http.MethodPost, "/"+tt.bucket+"?delete", strings.NewReader(tt.deleteBody))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

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

			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+tt.key, strings.NewReader("data"))
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

			delReq := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			delRec := httptest.NewRecorder()
			serveS3Handler(handler, delRec, delReq)

			assert.Equal(t, tt.wantCode, delRec.Code)
		})
	}
}

func TestHandler_HeadObject_NoSuchBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		wantCode int
	}{
		{
			name:     "no_such_bucket",
			url:      "/no-bucket/obj",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(http.MethodHead, tt.url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetObject_WithChecksumMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "get_with_checksum_mode_enabled",
			bucket:   "checksum-bucket",
			key:      "ck-obj",
			body:     "hello checksum",
			wantCode: http.StatusOK,
			wantBody: "hello checksum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+tt.key, strings.NewReader(tt.body))
			putReq.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			getReq := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key, nil)
			getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)

			assert.Equal(t, tt.wantCode, getRec.Code)
			assert.Equal(t, tt.wantBody, getRec.Body.String())
		})
	}
}

func TestHandler_ObjectTagging_NoSuchKey(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

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

func TestHandler_DeleteObject_Versioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		wantCode int
	}{
		{
			name:     "delete_existing_object",
			bucket:   "del-versioned",
			key:      "obj",
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+tt.key, strings.NewReader("data"))
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			delReq := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket+"/"+tt.key, nil)
			delRec := httptest.NewRecorder()
			serveS3Handler(handler, delRec, delReq)

			assert.Equal(t, tt.wantCode, delRec.Code)
		})
	}
}

func TestHandler_CreateBucket_AlreadyExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		wantCodeOneOf []int
	}{
		{
			name:          "bucket_already_exists",
			bucket:        "exists-bucket",
			wantCodeOneOf: []int{http.StatusNoContent, http.StatusConflict},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Contains(t, tt.wantCodeOneOf, rec.Code)
		})
	}
}

func TestHandler_DeleteBucket_NotEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		objectKey     string
		wantCodeOneOf []int
	}{
		{
			name:          "delete_non_empty_bucket",
			bucket:        "notempty-bucket",
			objectKey:     "obj",
			wantCodeOneOf: []int{http.StatusNoContent, http.StatusConflict},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/"+tt.objectKey, strings.NewReader("data"))
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			delReq := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket, nil)
			delRec := httptest.NewRecorder()
			serveS3Handler(handler, delRec, delReq)

			assert.Contains(t, tt.wantCodeOneOf, delRec.Code)
		})
	}
}

func TestHandler_ListBuckets_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "list_empty_buckets",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CopyObject_NoSuchSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		srcBucket  string
		dstBucket  string
		copySource string
		destURL    string
		wantCode   int
	}{
		{
			name:       "source_key_does_not_exist",
			srcBucket:  "copy-src",
			dstBucket:  "copy-dst",
			copySource: "copy-src/nonexistent-key",
			destURL:    "/copy-dst/dest-key",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.srcBucket)
			mustCreateBucket(t, backend, tt.dstBucket)

			req := httptest.NewRequest(http.MethodPut, tt.destURL, nil)
			req.Header.Set("X-Amz-Copy-Source", tt.copySource)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_HeadBucket_NoSuchBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		wantCode int
	}{
		{
			name:     "no_such_bucket",
			url:      "/no-such-bucket-head",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(http.MethodHead, tt.url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
