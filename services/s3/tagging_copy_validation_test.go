package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ObjectTagging_Limits verifies AWS object-tag limits: >10 tags →
// 400 BadRequest, over-long key/value → 400 InvalidTag, on both the PutObject
// X-Amz-Tagging header and the PutObjectTagging body.
func TestParity_ObjectTagging_Limits(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "tag-bucket")

	// 11 tags via PutObject header → BadRequest.
	pairs := make([]string, 0, 11)
	for i := range 11 {
		pairs = append(pairs, "k"+string(rune('a'+i))+"=v")
	}
	req := httptest.NewRequest(http.MethodPut, "/tag-bucket/many", strings.NewReader("x"))
	req.Header.Set("X-Amz-Tagging", strings.Join(pairs, "&"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BadRequest")

	// Over-long value via PutObject header → InvalidTag.
	req = httptest.NewRequest(http.MethodPut, "/tag-bucket/long", strings.NewReader("x"))
	req.Header.Set("X-Amz-Tagging", "k="+strings.Repeat("v", 300))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidTag")

	// Valid single tag succeeds.
	req = httptest.NewRequest(http.MethodPut, "/tag-bucket/ok", strings.NewReader("x"))
	req.Header.Set("X-Amz-Tagging", "Environment=prod")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// PutObjectTagging body with 11 tags → BadRequest.
	var tagXML strings.Builder
	tagXML.WriteString("<Tagging><TagSet>")
	for i := range 11 {
		tagXML.WriteString("<Tag><Key>k")
		tagXML.WriteByte(byte('a' + i))
		tagXML.WriteString("</Key><Value>v</Value></Tag>")
	}
	tagXML.WriteString("</TagSet></Tagging>")
	req = httptest.NewRequest(http.MethodPut, "/tag-bucket/ok?tagging", strings.NewReader(tagXML.String()))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestParity_CopyObject_SelfCopyGuard verifies that copying an object onto itself
// without changing any attribute returns 400 InvalidRequest, while a self-copy
// with X-Amz-Metadata-Directive: REPLACE succeeds.
func TestParity_CopyObject_SelfCopyGuard(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "copy-bucket")

	req := httptest.NewRequest(http.MethodPut, "/copy-bucket/k", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Self-copy with no changes → InvalidRequest.
	req = httptest.NewRequest(http.MethodPut, "/copy-bucket/k", nil)
	req.Header.Set("X-Amz-Copy-Source", "/copy-bucket/k")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidRequest")

	// Self-copy with metadata REPLACE → allowed.
	req = httptest.NewRequest(http.MethodPut, "/copy-bucket/k", nil)
	req.Header.Set("X-Amz-Copy-Source", "/copy-bucket/k")
	req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}
