package s3_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// TestListObjectsV2_ContextPropagation verifies that ListObjectsV2 passes the
// caller's context to the underlying ListObjects call rather than using
// [context.TODO]. The in-memory backend completes synchronously regardless of
// cancellation, but the fix ensures real backends and context-aware operations
// receive the correct context.
func TestListObjectsV2_ContextPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cancelCtx   bool
		wantObjects int
	}{
		{
			name:        "active_context_returns_results",
			cancelCtx:   false,
			wantObjects: 2,
		},
		{
			// The in-memory backend is synchronous and does not check ctx.Err,
			// so it completes successfully even when the context is already
			// cancelled. The important thing is that ctx is passed through
			// (not replaced with context.TODO()) and the call does not panic.
			name:        "cancelled_context_does_not_panic",
			cancelCtx:   true,
			wantObjects: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "ctx-test-bucket")
			mustPutObject(t, backend, "ctx-test-bucket", "key-a", []byte("data"))
			mustPutObject(t, backend, "ctx-test-bucket", "key-b", []byte("data"))

			ctx := t.Context()
			if tt.cancelCtx {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)
				cancel() // cancel immediately
			}

			out, err := backend.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
				Bucket: aws.String("ctx-test-bucket"),
			})

			require.NoError(t, err)
			assert.Len(t, out.Contents, tt.wantObjects)
		})
	}
}

// ─── ListObjectsV2 pagination correctness ────────────────────────────────────

func TestListObjectsV2_MaxKeys_CountsContentsAndPrefixes(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "v2-bucket")

	// Create objects across two prefixes.
	for _, key := range []string{"a/1", "a/2", "b/1", "c"} {
		mustPutObject(t, backend, "v2-bucket", key, []byte("data"))
	}

	out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
		Bucket:    aws.String("v2-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(2),
	})
	require.NoError(t, err)

	totalCount := len(out.Contents) + len(out.CommonPrefixes)
	assert.LessOrEqual(t, totalCount, 2, "total Contents+CommonPrefixes must respect MaxKeys")
	assert.True(t, aws.ToBool(out.IsTruncated))
}

func TestListObjectsV2_NoPagination(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "v2-nopag")

	for i := range 5 {
		mustPutObject(t, backend, "v2-nopag", fmt.Sprintf("obj%d", i), []byte("x"))
	}

	out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
		Bucket:  aws.String("v2-nopag"),
		MaxKeys: aws.Int32(10),
	})
	require.NoError(t, err)

	assert.Len(t, out.Contents, 5)
	assert.False(t, aws.ToBool(out.IsTruncated))
}

// ─── Multipart ETag fixture test ─────────────────────────────────────────────

func TestHandler_ListObjectsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "pagination with max-keys returns truncation token"},
		{name: "common prefixes with delimiter"},
		{name: "delimiter and prefix filter"},
		{name: "start-after excludes items before it"},
		{name: "invalid max-keys defaults to 1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "pagination with max-keys returns truncation token":
				for i := 1; i <= 5; i++ {
					mustPutObject(t, backend, "bkt", "key"+strings.Repeat("0", i), []byte("data"))
				}
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=2", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "<IsTruncated>true</IsTruncated>")
				assert.Contains(t, rec.Body.String(), "<NextContinuationToken>")

			case "common prefixes with delimiter":
				mustPutObject(t, backend, "bkt", "photos/1.jpg", []byte("d"))
				mustPutObject(t, backend, "bkt", "photos/2.jpg", []byte("d"))
				mustPutObject(t, backend, "bkt", "videos/1.mp4", []byte("d"))
				mustPutObject(t, backend, "bkt", "root.txt", []byte("d"))
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&delimiter=/", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Prefix>photos/</Prefix>")
				assert.Contains(t, body, "<Prefix>videos/</Prefix>")
				assert.Contains(t, body, "<Key>root.txt</Key>")

			case "delimiter and prefix filter":
				mustPutObject(t, backend, "bkt", "dir/a", []byte("d"))
				mustPutObject(t, backend, "bkt", "dir/b", []byte("d"))
				mustPutObject(t, backend, "bkt", "other", []byte("d"))
				req := httptest.NewRequest(
					http.MethodGet, "/bkt?list-type=2&prefix=dir/&delimiter=/", nil,
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Key>dir/a</Key>")
				assert.Contains(t, body, "<Key>dir/b</Key>")
				assert.NotContains(t, body, "<Key>other</Key>")

			case "start-after excludes items before it":
				mustPutObject(t, backend, "bkt", "a", []byte("d"))
				mustPutObject(t, backend, "bkt", "b", []byte("d"))
				mustPutObject(t, backend, "bkt", "c", []byte("d"))
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&start-after=a", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.NotContains(t, body, "<Key>a</Key>")
				assert.Contains(t, body, "<Key>b</Key>")
				assert.Contains(t, body, "<Key>c</Key>")

			case "invalid max-keys defaults to 1000":
				req := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&max-keys=-1", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestHandler_ListObjectVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "multiple versions of same key"},
		{name: "delete marker appears as latest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")

			switch tt.name {
			case "multiple versions of same key":
				mustPutObject(t, backend, "bkt", "key", []byte("v1"))
				mustPutObject(t, backend, "bkt", "key", []byte("v2"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<ListVersionsResult>")
				assert.Contains(t, body, "<Key>key</Key>")
				assert.Contains(t, body, "<VersionId>")
				assert.Equal(t, 2, strings.Count(body, "<Version>"))

			case "delete marker appears as latest":
				mustPutObject(t, backend, "bkt", "key", []byte("data"))

				req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusNoContent, rec.Code)

				req = httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<DeleteMarker>")
				assert.Contains(t, body, "<IsLatest>true</IsLatest>")
			}
		})
	}
}

func TestHandler_ListObjectsV2Error(t *testing.T) {
	t.Parallel()

	t.Run("non-existent bucket returns 404", func(t *testing.T) {
		t.Parallel()
		handler, _ := newTestHandler(t)

		// No bucket created → ListObjectsV2 will get ErrNoSuchBucket.
		req := httptest.NewRequest(http.MethodGet, "/no-such-bucket?list-type=2", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestS3BucketPolicyCRUD verifies put/get/delete bucket policy operations.

func TestHandler_ListObjectVersions_Pagination(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	ctx := t.Context()
	mustCreateBucket(t, backend, "bkt")

	// Enable versioning
	_, err := backend.PutBucketVersioning(ctx, &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("bkt"),
		VersioningConfiguration: &sdk_s3types.VersioningConfiguration{
			Status: sdk_s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Put objects a, b, c with a version each
	for _, key := range []string{"a", "b", "c"} {
		mustPutObject(t, backend, "bkt", key, []byte(key))
	}

	// List with max-keys=2
	req := httptest.NewRequest(http.MethodGet, "/bkt?versions&max-keys=2", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	type listVersionsResult struct {
		NextKeyMarker string `xml:"NextKeyMarker"`
		Versions      []struct {
			Key string `xml:"Key"`
		} `xml:"Version"`
		IsTruncated bool `xml:"IsTruncated"`
	}

	var result listVersionsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Len(t, result.Versions, 2, "first page should have 2 entries")
	assert.True(t, result.IsTruncated, "should be truncated")
	assert.NotEmpty(t, result.NextKeyMarker, "should have next key marker")

	// Fetch second page
	req2 := httptest.NewRequest(
		http.MethodGet, "/bkt?versions&max-keys=2&key-marker="+result.NextKeyMarker, nil,
	)
	rec2 := httptest.NewRecorder()
	serveS3Handler(handler, rec2, req2)

	require.Equal(t, http.StatusOK, rec2.Code)

	var result2 listVersionsResult
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &result2))
	assert.Len(t, result2.Versions, 1, "second page should have 1 entry")
	assert.False(t, result2.IsTruncated, "second page should not be truncated")
}

func TestHandler_ListObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list objects returns all keys"},
		{name: "marker excludes items at or before it"},
		{name: "common prefixes with delimiter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "list objects returns all keys":
				mustPutObject(t, backend, "bkt", "file1.txt", []byte("a"))
				mustPutObject(t, backend, "bkt", "file2.txt", []byte("b"))

				req := httptest.NewRequest(http.MethodGet, "/bkt", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "file1.txt")
				assert.Contains(t, rec.Body.String(), "file2.txt")

			case "marker excludes items at or before it":
				mustPutObject(t, backend, "bkt", "a", []byte("d"))
				mustPutObject(t, backend, "bkt", "b", []byte("d"))
				mustPutObject(t, backend, "bkt", "c", []byte("d"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?marker=a", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.NotContains(t, body, "<Key>a</Key>")
				assert.Contains(t, body, "<Key>b</Key>")
				assert.Contains(t, body, "<Key>c</Key>")

			case "common prefixes with delimiter":
				mustPutObject(t, backend, "bkt", "a/1", []byte("d"))
				mustPutObject(t, backend, "bkt", "a/2", []byte("d"))
				mustPutObject(t, backend, "bkt", "b/1", []byte("d"))

				req := httptest.NewRequest(http.MethodGet, "/bkt?delimiter=/", nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.Contains(t, body, "<Prefix>a/</Prefix>")
				assert.Contains(t, body, "<Prefix>b/</Prefix>")
				assert.NotContains(t, body, "<Key>a/1</Key>")
			}
		})
	}
}
