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
