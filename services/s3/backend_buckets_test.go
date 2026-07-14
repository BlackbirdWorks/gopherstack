package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_Regions verifies S3Handler.Regions() reflects the regions of
// created buckets.
func TestHandler_Regions(t *testing.T) {
	t.Parallel()

	t.Run("empty_when_no_buckets", func(t *testing.T) {
		t.Parallel()

		handler, _ := newTestHandler(t)
		assert.Empty(t, handler.Regions())
	})

	t.Run("non_empty_with_buckets", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "reg-bucket")

		assert.NotEmpty(t, handler.Regions())
	})
}

// TestHandler_BucketsByRegion verifies S3Handler.BucketsByRegion() filters
// buckets by region, including the empty-string ("all regions") case.
func TestHandler_BucketsByRegion(t *testing.T) {
	t.Parallel()

	t.Run("empty_string_returns_all", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "byr-bucket-1")
		mustCreateBucket(t, backend, "byr-bucket-2")

		buckets := handler.BucketsByRegion("")
		assert.GreaterOrEqual(t, len(buckets), 2)
	})

	t.Run("specific_region", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "byr-specific")

		// Using a non-existent region returns empty.
		assert.Empty(t, handler.BucketsByRegion("ap-southeast-99"))

		// Default region is us-east-1.
		assert.NotEmpty(t, handler.BucketsByRegion("us-east-1"))
	})
}

// TestInMemoryBackend_Regions verifies InMemoryBackend.Regions() and
// BucketsByRegion() directly against the backend (not through the handler).
func TestInMemoryBackend_Regions(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	mustCreateBucket(t, backend, "imb-region-bucket")

	assert.NotEmpty(t, backend.Regions())
}

func TestInMemoryBackend_BucketsByRegion(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	mustCreateBucket(t, backend, "imb-byr-bucket")

	assert.NotEmpty(t, backend.BucketsByRegion(""))

	// Lookup for unknown region.
	assert.Empty(t, backend.BucketsByRegion("eu-west-99"))
}

// TestGetBucketLocation verifies that GetBucketLocation returns an empty
// LocationConstraint for the default (us-east-1) region, and the region
// string for non-default regions.
func TestGetBucketLocation(t *testing.T) {
	t.Parallel()

	t.Run("us_east_1_empty_constraint", func(t *testing.T) {
		t.Parallel()

		handler, backend := newTestHandler(t)
		mustCreateBucket(t, backend, "b2-loc-default")

		req := httptest.NewRequest(http.MethodGet, "/b2-loc-default?location", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)

		body := rec.Body.String()
		assert.Contains(t, body, "LocationConstraint")
		// For us-east-1, the constraint value must be empty (not "us-east-1").
		assert.NotContains(t, body, "us-east-1",
			"us-east-1 bucket LocationConstraint must be empty, not \"us-east-1\"")
	})

	t.Run("non_default_region_has_constraint", func(t *testing.T) {
		t.Parallel()

		handler, _ := newTestHandler(t)

		// Create bucket in eu-west-1 by setting a regional bucket name and creating
		// it via the handler with a LocationConstraint body.
		bucket := "b2-loc-euwest"
		body := `<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>`
		req := httptest.NewRequest(http.MethodPut, "/"+bucket, strings.NewReader(body))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req2 := httptest.NewRequest(http.MethodGet, "/"+bucket+"?location", nil)
		req2.Header.Set(
			"Authorization",
			"AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/s3/aws4_request",
		)
		rec2 := httptest.NewRecorder()
		serveS3Handler(handler, rec2, req2)

		require.Equal(t, http.StatusOK, rec2.Code)
		assert.Contains(t, rec2.Body.String(), "eu-west-1")
	})
}

// TestParity_ListDirectoryBuckets verifies that directory buckets (--x-s3 suffix)
// are returned by ListDirectoryBuckets and excluded from ListBuckets, matching
// the AWS partition between general-purpose and S3 Express bucket namespaces.
func TestParity_ListDirectoryBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createBuckets    []string // general-purpose buckets to create
		createDirBuckets []string // directory buckets to create (--x-s3 suffix)
		wantInList       []string // expected in ListBuckets
		wantInDirList    []string // expected in ListDirectoryBuckets
		wantNotInList    []string // must NOT appear in ListBuckets
		wantNotInDirList []string // must NOT appear in ListDirectoryBuckets
	}{
		{
			name:             "directory_bucket_excluded_from_list_buckets",
			createBuckets:    []string{"regular-bucket"},
			createDirBuckets: []string{"my-bucket--usw2-az3--x-s3"},
			wantInList:       []string{"regular-bucket"},
			wantInDirList:    []string{"my-bucket--usw2-az3--x-s3"},
			wantNotInList:    []string{"my-bucket--usw2-az3--x-s3"},
			wantNotInDirList: []string{"regular-bucket"},
		},
		{
			name:             "no_directory_buckets_returns_empty",
			createBuckets:    []string{"only-regular"},
			createDirBuckets: []string{},
			wantInList:       []string{"only-regular"},
			wantInDirList:    []string{},
			wantNotInDirList: []string{"only-regular"},
		},
		{
			name:             "multiple_directory_buckets_all_returned",
			createBuckets:    []string{},
			createDirBuckets: []string{"a--usw2-az1--x-s3", "b--usw2-az2--x-s3"},
			wantInDirList:    []string{"a--usw2-az1--x-s3", "b--usw2-az2--x-s3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			for _, name := range tt.createBuckets {
				mustCreateBucket(t, backend, name)
			}

			for _, name := range tt.createDirBuckets {
				mustCreateBucket(t, backend, name)
			}

			// Verify ListBuckets via HTTP.
			listReq := httptest.NewRequest(http.MethodGet, "/", nil)
			listRec := httptest.NewRecorder()
			serveS3Handler(handler, listRec, listReq)
			require.Equal(t, http.StatusOK, listRec.Code, "ListBuckets must return 200")
			listBody := listRec.Body.String()

			for _, name := range tt.wantInList {
				assert.Contains(t, listBody, name, "ListBuckets must contain %q", name)
			}
			for _, name := range tt.wantNotInList {
				assert.NotContains(t, listBody, name, "ListBuckets must not contain %q", name)
			}

			// Verify ListDirectoryBuckets via HTTP.
			dirReq := httptest.NewRequest(http.MethodGet, "/?list-type=directory", nil)
			dirRec := httptest.NewRecorder()
			serveS3Handler(handler, dirRec, dirReq)
			require.Equal(t, http.StatusOK, dirRec.Code, "ListDirectoryBuckets must return 200")
			dirBody := dirRec.Body.String()

			for _, name := range tt.wantInDirList {
				assert.Contains(t, dirBody, name, "ListDirectoryBuckets must contain %q", name)
			}
			for _, name := range tt.wantNotInDirList {
				assert.NotContains(
					t,
					dirBody,
					name,
					"ListDirectoryBuckets must not contain %q",
					name,
				)
			}
		})
	}
}

// TestHandler_CreateBucket_AlreadyExists verifies that creating a bucket that
// already exists returns either 204 (idempotent) or 409 Conflict.
func TestHandler_CreateBucket_AlreadyExists(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "exists-bucket")

	req := httptest.NewRequest(http.MethodPut, "/exists-bucket", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Contains(t, []int{http.StatusNoContent, http.StatusConflict}, rec.Code)
}

// TestHandler_DeleteBucket_NotEmpty verifies that deleting a non-empty bucket
// returns either 204 or 409 Conflict.
func TestHandler_DeleteBucket_NotEmpty(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "notempty-bucket")

	putReq := httptest.NewRequest(
		http.MethodPut,
		"/notempty-bucket/obj",
		strings.NewReader("data"),
	)
	putRec := httptest.NewRecorder()
	serveS3Handler(handler, putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	delReq := httptest.NewRequest(http.MethodDelete, "/notempty-bucket", nil)
	delRec := httptest.NewRecorder()
	serveS3Handler(handler, delRec, delReq)

	assert.Contains(t, []int{http.StatusNoContent, http.StatusConflict}, delRec.Code)
}

// TestHandler_ListBuckets_Empty verifies that ListBuckets returns 200 when no
// buckets exist.
func TestHandler_ListBuckets_Empty(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_HeadBucket_NoSuchBucket verifies that HeadBucket on a
// nonexistent bucket returns 404.
func TestHandler_HeadBucket_NoSuchBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/no-such-bucket-head", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
