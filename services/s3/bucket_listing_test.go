package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listBucketResultXML is a minimal struct for parsing ListObjects (V1) responses.
type listBucketResultXML struct {
	NextMarker  string `xml:"NextMarker"`
	IsTruncated bool   `xml:"IsTruncated"`
}

// TestListObjectsV1_NoKeyCount verifies that the ListObjects (V1) response does
// not include a KeyCount element (a ListObjectsV2-only field), while V2 does.
func TestListObjectsV1_NoKeyCount(t *testing.T) {
	t.Parallel()

	t.Run("v1_omits_key_count", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			bucket  string
			objects []string
		}{
			{
				name:    "empty bucket: no KeyCount",
				bucket:  "b2kc-empty",
				objects: nil,
			},
			{
				name:    "non-empty bucket: no KeyCount",
				bucket:  "b2kc-full",
				objects: []string{"a/obj1", "b/obj2", "c/obj3"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				handler, backend := newTestHandler(t)
				mustCreateBucket(t, backend, tt.bucket)

				for _, key := range tt.objects {
					mustPutObject(t, backend, tt.bucket, key, []byte("data"))
				}

				req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket, nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)

				require.Equal(t, http.StatusOK, rec.Code)
				assert.NotContains(t, rec.Body.String(), "KeyCount",
					"ListObjects V1 response must not contain KeyCount (V2-only field)")
			})
		}
	})

	t.Run("v2_includes_key_count", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "b2kc-v2-bkt")
		mustPutObject(t, backend, "b2kc-v2-bkt", "obj1", []byte("data"))

		req := httptest.NewRequest(http.MethodGet, "/b2kc-v2-bkt?list-type=2", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "KeyCount",
			"ListObjectsV2 response must contain KeyCount")
	})
}

// TestListObjects_NextMarker verifies that NextMarker is returned only when a
// delimiter is specified AND the response is truncated.
func TestListObjects_NextMarker(t *testing.T) {
	t.Parallel()

	t.Run("only_with_delimiter", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name           string
			delimiter      string
			wantNextMarker bool
		}{
			{
				name:           "no delimiter: NextMarker absent even when truncated",
				delimiter:      "",
				wantNextMarker: false,
			},
			{
				name:           "with delimiter: NextMarker present when truncated",
				delimiter:      "/",
				wantNextMarker: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				handler, backend := newTestHandler(t)
				mustCreateBucket(t, backend, "b2-nm-bkt")

				// Put 3 objects so max-keys=2 triggers truncation.
				for _, key := range []string{"a/obj1", "b/obj2", "c/obj3"} {
					mustPutObject(t, backend, "b2-nm-bkt", key, []byte("data"))
				}

				url := "/b2-nm-bkt?max-keys=2"
				if tt.delimiter != "" {
					url += "&delimiter=" + tt.delimiter
				}

				req := httptest.NewRequest(http.MethodGet, url, nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)

				require.Equal(t, http.StatusOK, rec.Code)

				var result listBucketResultXML
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
				assert.True(t, result.IsTruncated, "response should be truncated")

				if tt.wantNextMarker {
					assert.NotEmpty(t, result.NextMarker,
						"NextMarker must be present when delimiter is specified and truncated")
				} else {
					assert.Empty(t, result.NextMarker,
						"NextMarker must be absent when no delimiter is specified")
				}
			})
		}
	})

	t.Run("absent_when_not_truncated", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "b2-nm-complete")

		mustPutObject(t, backend, "b2-nm-complete", "key1", []byte("data"))

		req := httptest.NewRequest(http.MethodGet, "/b2-nm-complete?delimiter=/", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		var result listBucketResultXML
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
		assert.False(t, result.IsTruncated)
		assert.Empty(t, result.NextMarker, "NextMarker must be absent when not truncated")
	})
}

// TestListObjectVersions_Delimiter verifies common-prefix grouping behavior
// when a delimiter is applied to ListObjectVersions.
func TestListObjectVersions_Delimiter(t *testing.T) {
	t.Parallel()

	t.Run("groups_common_prefixes", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "lvd-bucket")
		enableVersioning(t, handler, "lvd-bucket")

		// Create objects that share a common prefix under the delimiter.
		for _, key := range []string{"logs/2024/jan.log", "logs/2024/feb.log", "data/file.csv"} {
			mustPutObject(t, backend, "lvd-bucket", key, []byte("content"))
		}

		req := httptest.NewRequest(http.MethodGet, "/lvd-bucket?versions&delimiter=/&prefix=logs/", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		// The two log/ objects share "logs/2024/" as a common prefix.
		assert.Contains(t, body, "CommonPrefixes")
	})

	t.Run("no_common_prefix", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "lvd2-bucket")
		enableVersioning(t, handler, "lvd2-bucket")

		mustPutObject(t, backend, "lvd2-bucket", "file.txt", []byte("hello"))

		// Delimiter present but no key has delimiter in its name.
		req := httptest.NewRequest(http.MethodGet, "/lvd2-bucket?versions&delimiter=/", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
	})
}

// TestListObjectVersions_KeyMarker_Pagination verifies key-marker pagination
// for ListObjectVersions.
func TestListObjectVersions_KeyMarker_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lv-pg")
	enableVersioning(t, handler, "lv-pg")

	for _, key := range []string{"aaa", "bbb", "ccc"} {
		mustPutObject(t, backend, "lv-pg", key, []byte("v"))
	}

	// List starting after "aaa".
	req := httptest.NewRequest(http.MethodGet, "/lv-pg?versions&key-marker=aaa", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<Key>aaa</Key>")
	assert.Contains(t, body, "<Key>bbb</Key>")
}
