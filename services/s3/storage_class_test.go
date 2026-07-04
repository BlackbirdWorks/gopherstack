package s3_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageClass_PutAndGet verifies X-Amz-Storage-Class is stored and
// returned correctly on PutObject, GetObject, and HeadObject.
func TestStorageClass_PutAndGet(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/test-bucket-sc", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	classes := []string{
		"STANDARD",
		"REDUCED_REDUNDANCY",
		"INTELLIGENT_TIERING",
		"STANDARD_IA",
		"ONEZONE_IA",
		"GLACIER",
		"DEEP_ARCHIVE",
		"GLACIER_IR",
	}

	for _, sc := range classes {
		t.Run(sc, func(t *testing.T) {
			t.Parallel()

			key := "obj-" + strings.ToLower(sc)

			putRR := httptest.NewRecorder()
			putReq := httptest.NewRequest(http.MethodPut, "/test-bucket-sc/"+key,
				strings.NewReader("hello"))
			putReq.Header.Set("X-Amz-Storage-Class", sc)
			serveS3Handler(handler, putRR, putReq)
			require.Equal(t, http.StatusOK, putRR.Code, "PutObject %s", sc)

			getRR := httptest.NewRecorder()
			getReq := httptest.NewRequest(http.MethodGet, "/test-bucket-sc/"+key, nil)
			serveS3Handler(handler, getRR, getReq)
			require.Equal(t, http.StatusOK, getRR.Code)
			assert.Equal(t, sc, getRR.Header().Get("X-Amz-Storage-Class"), "GET storage class")

			headRR := httptest.NewRecorder()
			headReq := httptest.NewRequest(http.MethodHead, "/test-bucket-sc/"+key, nil)
			serveS3Handler(handler, headRR, headReq)
			require.Equal(t, http.StatusOK, headRR.Code)
			assert.Equal(t, sc, headRR.Header().Get("X-Amz-Storage-Class"), "HEAD storage class")
		})
	}
}

// TestStorageClass_DefaultIsStandard verifies objects without a storage class header default to STANDARD.
func TestStorageClass_DefaultIsStandard(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/sc-default-bucket", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/sc-default-bucket/obj",
		strings.NewReader("body"))
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/sc-default-bucket/obj", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "STANDARD", rr.Header().Get("X-Amz-Storage-Class"))
}

// TestStorageClass_ListObjectsV2 verifies ListObjectsV2 returns actual storage class.
func TestStorageClass_ListObjectsV2(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/sc-list-bucket", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/sc-list-bucket/glacierobj",
		strings.NewReader("cold"))
	req.Header.Set("X-Amz-Storage-Class", "GLACIER")
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/sc-list-bucket?list-type=2", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	body, _ := io.ReadAll(rr.Body)
	assert.Contains(t, string(body), "GLACIER", "ListObjectsV2 must reflect GLACIER storage class")
}

// TestStorageClass_CopyObject verifies X-Amz-Storage-Class is applied on CopyObject.
func TestStorageClass_CopyObject(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	for _, bucket := range []string{"sc-copy-src", "sc-copy-dst"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
		serveS3Handler(handler, rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/sc-copy-src/src",
		strings.NewReader("data"))
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/sc-copy-dst/dst", nil)
	req.Header.Set("X-Amz-Copy-Source", "/sc-copy-src/src")
	req.Header.Set("X-Amz-Storage-Class", "GLACIER")
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodHead, "/sc-copy-dst/dst", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "GLACIER", rr.Header().Get("X-Amz-Storage-Class"))
}

// TestRangeGet_SuffixZero verifies bytes=-0 returns 416 per RFC 9110 §14.1.2.
func TestRangeGet_SuffixZero(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/range-zero-bucket", nil)
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/range-zero-bucket/obj",
		strings.NewReader("hello world"))
	serveS3Handler(handler, rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/range-zero-bucket/obj", nil)
	req.Header.Set("Range", "bytes=-0")
	serveS3Handler(handler, rr, req)
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, rr.Code, "bytes=-0 must be 416")
}
