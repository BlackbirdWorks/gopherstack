package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListObjects_EncodingTypeURL verifies that encoding-type=url
// URL-encodes Key/Prefix/Delimiter in list responses (V1 and V2), so keys with
// special characters round-trip through the AWS SDK (which URL-decodes them).
func TestListObjects_EncodingTypeURL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "enc-bucket")

	// Key with characters that must be URL-encoded in the response: ampersand
	// (XML-significant) and a slash that becomes %2F under encoding-type=url.
	const rawKey = "dir/a&c.txt"
	req := httptest.NewRequest(http.MethodPut, "/enc-bucket/"+rawKey, strings.NewReader("x"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	for _, path := range []string{
		"/enc-bucket?list-type=2&encoding-type=url",
		"/enc-bucket?encoding-type=url",
	} {
		req = httptest.NewRequest(http.MethodGet, path, nil)
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code, path)

		body := rec.Body.String()
		// The encoded key (space→+, &→%26, /→%2F) must appear.
		assert.Contains(t, body, "dir%2Fa%26c.txt", "key must be URL-encoded for %s", path)
		assert.Contains(t, body, "<EncodingType>url</EncodingType>", path)
	}

	// Without encoding-type, the raw key is returned (& XML-escaped by the encoder).
	req = httptest.NewRequest(http.MethodGet, "/enc-bucket?list-type=2", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "dir%2Fa%26c.txt")
}
