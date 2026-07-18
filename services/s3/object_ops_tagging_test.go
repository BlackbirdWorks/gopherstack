package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestHandler_ObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put get delete tagging lifecycle"},
		{name: "invalid XML returns 400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			switch tt.name {
			case "put get delete tagging lifecycle":
				body := `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`
				req := httptest.NewRequest(
					http.MethodPut,
					"/bkt/key?tagging",
					strings.NewReader(body),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/bkt/key?tagging", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "prod")

				req = httptest.NewRequest(http.MethodDelete, "/bkt/key?tagging", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusNoContent, rec.Code)

			case "invalid XML returns 400":
				req := httptest.NewRequest(
					http.MethodPut,
					"/bkt/key?tagging",
					strings.NewReader("not xml"),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestHandler_PutObjectWithTaggingHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put with tagging header stores tags"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("data"))
			req.Header.Set("X-Amz-Tagging", "env=prod&team=alpha")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			out, err := backend.GetObjectTagging(t.Context(), &sdk_s3.GetObjectTaggingInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)
			assert.Len(t, out.TagSet, 2)

			getTag := func(key string) string {
				for _, tag := range out.TagSet {
					if *tag.Key == key {
						return *tag.Value
					}
				}

				return ""
			}
			assert.Equal(t, "prod", getTag("env"))
			assert.Equal(t, "alpha", getTag("team"))
		})
	}
}
