package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestDashboardRegionScoping_ListBucketsIgnoresSelectedRegion guards against
// bd gopherstack-pejf ("S3 dashboard UI: region selector ignored, forces
// ap-southeast-1 -> 'select a region'"). The gopherstack dashboard's S3 view
// is a compiled SvelteKit SPA that talks to this handler directly via the AWS
// SDK (there is no Go-side dashboard REST endpoint for S3 — see
// dashboard/ui.go's unused S3Ops field and services/s3/dashboard.go's
// never-wired DashboardProvider), so the only Go-side surface that can drop a
// selected region is this handler's region resolution.
//
// ListBuckets is a global, account-wide operation in real S3 (and here): it
// must return buckets regardless of which region the caller's request is
// signed for, exactly like the dashboard's bucket-list view needs regardless
// of the region selector's current value.
func TestDashboardRegionScoping_ListBucketsIgnoresSelectedRegion(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	mustCreateBucketInRegion(t, handler, "dash-bucket-use1", "us-east-1")
	mustCreateBucketInRegion(t, handler, "dash-bucket-euwest1", "eu-west-1")

	// List with the "selector" set to a third region that owns no buckets.
	// Real S3 (and the dashboard's bucket overview) still expects to see
	// every bucket the account owns.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
		Region:  "ap-southeast-1",
		Account: awsmeta.DefaultAccount,
	}))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "dash-bucket-use1")
	assert.Contains(t, rec.Body.String(), "dash-bucket-euwest1")
}

// TestDashboardRegionScoping_BucketAccessHonorsSelectedRegion verifies that
// per-bucket access (the dashboard's bucket-detail/object-browser view) is
// scoped to whatever region the request actually carries: a request signed
// for the bucket's true region succeeds, and a request signed for a
// different region gets AWS's real cross-region signal (301
// PermanentRedirect + X-Amz-Bucket-Region) rather than silently failing or
// silently reading the wrong bucket.
func TestDashboardRegionScoping_BucketAccessHonorsSelectedRegion(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	mustCreateBucketInRegion(t, handler, "dash-scoped-bucket", "eu-west-1")

	t.Run("matching_region_succeeds", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/dash-scoped-bucket?location", nil)
		req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
			Region:  "eu-west-1",
			Account: awsmeta.DefaultAccount,
		}))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("mismatched_region_redirects_with_true_region", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/dash-scoped-bucket?location", nil)
		req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
			Region:  "ap-southeast-1",
			Account: awsmeta.DefaultAccount,
		}))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		require.Equal(t, http.StatusMovedPermanently, rec.Code)
		assert.Equal(t, "eu-west-1", rec.Header().Get("X-Amz-Bucket-Region"))
	})
}

// mustCreateBucketInRegion issues a CreateBucket request whose region is
// carried via the awsmeta context (matching how the dashboard's real SigV4
// requests carry region) and requires it to succeed.
func mustCreateBucketInRegion(t *testing.T, handler *s3.S3Handler, bucket, region string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
		Region:  region,
		Account: awsmeta.DefaultAccount,
	}))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
