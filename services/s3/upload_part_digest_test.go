package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUploadPart_ContentMD5AndETag(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte("part-data"), 1000)
	sum := md5.Sum(data)

	tests := []struct {
		name       string
		contentMD5 string
		wantBody   string
		wantCode   int
	}{
		{name: "none", wantCode: http.StatusOK},
		{name: "valid", contentMD5: base64.StdEncoding.EncodeToString(sum[:]), wantCode: http.StatusOK},
		{
			name:       "mismatch",
			contentMD5: base64.StdEncoding.EncodeToString(make([]byte, md5.Size)),
			wantCode:   http.StatusBadRequest, wantBody: "BadDigest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			uploadID := doMultipartUpload(t, handler, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut,
				fmt.Sprintf("/bkt/obj?partNumber=1&uploadId=%s", uploadID), bytes.NewReader(data))
			if tt.contentMD5 != "" {
				req.Header.Set("Content-MD5", tt.contentMD5)
			}

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)

				return
			}

			assert.Equal(t, `"`+hex.EncodeToString(sum[:])+`"`, rec.Header().Get("ETag"))
		})
	}
}

func TestContentMD5_ErrorCodes(t *testing.T) {
	t.Parallel()

	body := []byte("<Delete><Object><Key>k1</Key></Object></Delete>")
	sum := md5.Sum(body)
	good := base64.StdEncoding.EncodeToString(sum[:])
	wrongSum := base64.StdEncoding.EncodeToString(make([]byte, md5.Size))
	badB64 := "!!!not-base64!!!"
	shortLen := base64.StdEncoding.EncodeToString([]byte("short"))

	tests := []struct {
		name       string
		op         string
		contentMD5 string
		wantCode   string
		wantStatus int
	}{
		{name: "put_bad_b64", op: "put", contentMD5: badB64, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "put_short", op: "put", contentMD5: shortLen, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "put_mismatch", op: "put", contentMD5: wrongSum, wantCode: "BadDigest", wantStatus: 400},
		{name: "put_good", op: "put", contentMD5: good, wantStatus: 200},
		{name: "part_bad_b64", op: "part", contentMD5: badB64, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "part_short", op: "part", contentMD5: shortLen, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "part_mismatch", op: "part", contentMD5: wrongSum, wantCode: "BadDigest", wantStatus: 400},
		{name: "delete_bad_b64", op: "delete", contentMD5: badB64, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "delete_short", op: "delete", contentMD5: shortLen, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "delete_mismatch", op: "delete", contentMD5: wrongSum, wantCode: "BadDigest", wantStatus: 400},
		{name: "delete_good", op: "delete", contentMD5: good, wantStatus: 200},
		{name: "cfg_bad_b64", op: "cfg", contentMD5: badB64, wantCode: "InvalidDigest", wantStatus: 400},
		{name: "cfg_mismatch", op: "cfg", contentMD5: wrongSum, wantCode: "BadDigest", wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			uploadID := doMultipartUpload(t, handler, backend, "bkt")
			mustPutObject(t, backend, "bkt", "k1", []byte("x"))

			var req *http.Request

			switch tt.op {
			case "put":
				req = httptest.NewRequest(http.MethodPut, "/bkt/new", bytes.NewReader(body))
			case "part":
				req = httptest.NewRequest(http.MethodPut,
					fmt.Sprintf("/bkt/obj?partNumber=1&uploadId=%s", uploadID), bytes.NewReader(body))
			case "delete":
				req = httptest.NewRequest(http.MethodPost, "/bkt?delete", bytes.NewReader(body))
			case "cfg":
				req = httptest.NewRequest(http.MethodPut, "/bkt?tagging",
					bytes.NewReader([]byte(`<Tagging><TagSet></TagSet></Tagging>`)))
			}

			req.Header.Set("Content-MD5", tt.contentMD5)

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantCode+"</Code>")
			}
		})
	}
}
