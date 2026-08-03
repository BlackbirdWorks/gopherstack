package s3_test

import (
	"encoding/xml"
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

// listAllMyBucketsResultXML is a minimal struct for parsing ListBuckets
// responses, independent of the production ListAllMyBucketsResult/BucketXML
// shapes in model.go -- so this test actually catches a wire-shape
// regression instead of trivially agreeing with whatever the handler emits.
type listAllMyBucketsResultXML struct {
	Buckets []struct {
		Name         string `xml:"Name"`
		BucketRegion string `xml:"BucketRegion"`
	} `xml:"Buckets>Bucket"`
}

// TestDashboardRegionScoping_ListBucketsReportsEachBucketsTrueRegion is the
// actual fix for the user-reported issue behind bd gopherstack-pejf's
// surrounding investigation: ListBuckets is correctly account-global (see
// TestDashboardRegionScoping_ListBucketsIgnoresSelectedRegion above), so
// buckets created in a region other than the dashboard's currently selected
// one correctly still show up in the list -- but with nothing on screen
// saying they live somewhere else. The fix is BucketRegion on each Bucket
// element, sourced from the same per-bucket InMemoryBackend.BucketRegion
// state that already drives enforceBucketRegion's cross-region redirect, so
// the value can never drift from what actually gates bucket access.
func TestDashboardRegionScoping_ListBucketsReportsEachBucketsTrueRegion(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	mustCreateBucketInRegion(t, handler, "region-tag-use1", "us-east-1")
	mustCreateBucketInRegion(t, handler, "region-tag-apse1", "ap-southeast-1")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
		Region:  "us-east-1",
		Account: awsmeta.DefaultAccount,
	}))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result listAllMyBucketsResultXML
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result), "Body: %s", rec.Body.String())

	gotRegions := make(map[string]string, len(result.Buckets))
	for _, b := range result.Buckets {
		gotRegions[b.Name] = b.BucketRegion
	}

	// The us-east-1 bucket's region must be reported explicitly. This is
	// deliberately the OPPOSITE of GetBucketLocation's LocationConstraint,
	// which AWS blanks to "" for us-east-1 (a legacy quirk predating
	// LocationConstraint) -- BucketRegion on ListBuckets carries no such
	// quirk (confirmed against the real ListBuckets API docs' paginated
	// examples, which show <BucketRegion>us-east-1</BucketRegion> verbatim).
	assert.Equal(t, "us-east-1", gotRegions["region-tag-use1"])
	// The cross-region bucket must report ITS true region, not the region
	// the request was signed/selected for (us-east-1 above).
	assert.Equal(t, "ap-southeast-1", gotRegions["region-tag-apse1"])
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
		// Regression guard for the actual root cause of "S3 dashboard: shows
		// buckets stuck in the wrong region no matter what the selector says"
		// (the earlier "fixed" verdict on gopherstack-pejf only checked the
		// signed region, not what the browser does with the response). A 301
		// is heuristically cacheable by browsers even with zero explicit
		// caching headers -- confirmed against a running binary via
		// Playwright: fetch(url) (default cache) replayed a stale 301 from
		// several minutes earlier for a request that was, in fact, correctly
		// signed for the bucket's true region, while fetch(url, {cache:
		// 'reload'}) against the identical signed headers got a live 200.
		// The browser HTTP cache keys on method+URL, not on the Authorization
		// header, so once any request to a given bucket+query URL is signed
		// for the wrong region, every later request to that same URL --
		// including ones correctly re-signed after the user fixes the region
		// selector -- can be served the stale redirect straight out of cache
		// without ever reaching this handler again. Without Cache-Control:
		// no-store here, this line's own 301 response would be exactly such
		// a poison pill.
		assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
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
