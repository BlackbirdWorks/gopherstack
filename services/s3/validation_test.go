package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/stretchr/testify/assert"
)

func TestIsValidBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{"my-bucket", true},
		{"my.bucket", true},
		{"123-bucket", true},
		{"bucket-123", true},
		{"a.b.c", true},
		{"ab", false},                         // too short
		{strings.Repeat("a", 64), false},      // too long
		{"MyBucket", false},                   // uppercase
		{"my_bucket", false},                  // underscore
		{"-mybucket", false},                  // starts with hyphen
		{"mybucket-", false},                  // ends with hyphen
		{".mybucket", false},                  // starts with dot
		{"mybucket.", false},                  // ends with dot
		{"my..bucket", false},                 // adjacent dots
		{"192.168.1.1", false},                // IP address
		{"xn--bucket", false},                 // reserved prefix
		{"sthree-bucket", false},              // reserved prefix
		{"sthree-configurator-bucket", false}, // reserved prefix
		{"bucket-s3alias", false},             // reserved suffix
		{"bucket--ol-s3", false},              // reserved suffix
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.IsValidBucketName(tt.name), "bucket name: %s", tt.name)
		})
	}
}

func TestIsValidObjectKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"valid key", "my-key", true},
		{"empty key", "", false},
		{"too long key", strings.Repeat("a", 1025), false},
		{"max length key", strings.Repeat("a", 1024), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.IsValidObjectKey(tt.key), "key: %s", tt.name)
		})
	}
}

func TestExpectedBucketOwner_Match_Passes(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "owned-bucket")

	rec := doRequest(handler, http.MethodPut, "/owned-bucket/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Expected-Bucket-Owner": "123456789012",
		})

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestExpectedBucketOwner_Mismatch_Returns403(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "owned-bucket2")

	rec := doRequest(handler, http.MethodPut, "/owned-bucket2/obj",
		strings.NewReader("data"),
		map[string]string{
			"X-Amz-Expected-Bucket-Owner": "999999999999",
		})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestExpectedBucketOwner_GetObject_Mismatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "get-owned")
	mustPutObject(t, backend, "get-owned", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodGet, "/get-owned/obj", nil, map[string]string{
		"X-Amz-Expected-Bucket-Owner": "000000000000",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestExpectedBucketOwner_HeadObject_Mismatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "head-owned")
	mustPutObject(t, backend, "head-owned", "obj", []byte("data"))

	rec := doRequest(handler, http.MethodHead, "/head-owned/obj", nil, map[string]string{
		"X-Amz-Expected-Bucket-Owner": "111111111111",
	})

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// ─── CRC64NVME checksum ───────────────────────────────────────────────────────

func TestHandler_ResolveBucketAndKey_InvalidKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{
			name:       "key longer than 1024 bytes returns 400",
			key:        strings.Repeat("a", 1025),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(http.MethodGet, "/bkt/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
