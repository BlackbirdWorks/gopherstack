package s3control_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real S3 Control
// operation, extracted from s3control@v1.73.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestxml_serializeOp<Op>.HandleSerialize -- s3control is
// REST-XML, confirmed by the "awsRestxml_" prefix (not REST-JSON like most
// of this campaign's other services; see services/_PROTOCOLS.md). The
// account ID travels via a Host-prefix trait (X-Amz-Account-Id header in
// this handler's ExtractResource, not the URL path), so it never appears
// in any path template below. PLACEHOLDER stands in for every
// {Name}/{Bucket}/{AccessGrantId}/{AccessGrantsLocationId}/{ConfigId}/
// {JobId}/{ResourceArn+}/{RequestTokenARN+}/{Mrap+} URI label -- this
// handler's extract*Op functions (isSimplePath/isPrefixSuffix helpers
// across handler_access_grants.go, handler_access_points.go,
// handler_object_lambda.go, handler_bucket.go, handler_jobs.go,
// handler_multi_region_access_points.go, handler_storage_lens.go,
// handler_tags.go) never validate identifier shape, so the literal value
// doesn't matter here, only path depth and static segments. 97 real ops
// here, matching s3control's real op count exactly (also matches
// GetSupportedOperations's own 97 entries one-for-one, verified by diffing
// against the SDK's own serializeOp function names).
//
// A systematic check for a shared method+path across all 97 ops found zero
// collisions -- every pair of ops sharing a path template (e.g.
// CreateAccessPoint/GetAccessPoint/DeleteAccessPoint on
// "/v20180820/accesspoint/{Name}", GetMultiRegionAccessPointRoutes/
// SubmitMultiRegionAccessPointRoutes on ".../{Mrap+}/routes") is
// disambiguated by HTTP method alone, which every extract*Op function
// already switches on -- so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any
// route in this table, despite s3control being the REST-XML,
// account-id-prefixed service flagged as the likeliest place for one.
//
// One structural note found while building this table (not a routing bug):
// extractMRAPInstanceOp's own doc comment records that a synchronous DELETE
// to "/v20180820/mrap/instances/{Name+}" is unreachable by any real
// client -- the real awsRestxml_serializeOpDeleteMultiRegionAccessPoint
// hardcodes "POST /v20180820/async-requests/mrap/delete" as its only wire
// binding (an async API), which is exactly the DeleteMultiRegionAccessPoint
// entry already in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestxml_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{
			"AssociateAccessGrantsIdentityCenter",
			"POST",
			"/v20180820/accessgrantsinstance/identitycenter",
		},
		{"CreateAccessGrant", "POST", "/v20180820/accessgrantsinstance/grant"},
		{"CreateAccessGrantsInstance", "POST", "/v20180820/accessgrantsinstance"},
		{"CreateAccessGrantsLocation", "POST", "/v20180820/accessgrantsinstance/location"},
		{"CreateAccessPoint", "PUT", "/v20180820/accesspoint/PLACEHOLDER"},
		{
			"CreateAccessPointForObjectLambda",
			"PUT",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER",
		},
		{"CreateBucket", "PUT", "/v20180820/bucket/PLACEHOLDER"},
		{"CreateJob", "POST", "/v20180820/jobs"},
		{"CreateMultiRegionAccessPoint", "POST", "/v20180820/async-requests/mrap/create"},
		{"CreateStorageLensGroup", "POST", "/v20180820/storagelensgroup"},
		{"DeleteAccessGrant", "DELETE", "/v20180820/accessgrantsinstance/grant/PLACEHOLDER"},
		{"DeleteAccessGrantsInstance", "DELETE", "/v20180820/accessgrantsinstance"},
		{
			"DeleteAccessGrantsInstanceResourcePolicy",
			"DELETE",
			"/v20180820/accessgrantsinstance/resourcepolicy",
		},
		{
			"DeleteAccessGrantsLocation",
			"DELETE",
			"/v20180820/accessgrantsinstance/location/PLACEHOLDER",
		},
		{"DeleteAccessPoint", "DELETE", "/v20180820/accesspoint/PLACEHOLDER"},
		{
			"DeleteAccessPointForObjectLambda",
			"DELETE",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER",
		},
		{"DeleteAccessPointPolicy", "DELETE", "/v20180820/accesspoint/PLACEHOLDER/policy"},
		{
			"DeleteAccessPointPolicyForObjectLambda",
			"DELETE",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/policy",
		},
		{"DeleteAccessPointScope", "DELETE", "/v20180820/accesspoint/PLACEHOLDER/scope"},
		{"DeleteBucket", "DELETE", "/v20180820/bucket/PLACEHOLDER"},
		{
			"DeleteBucketLifecycleConfiguration",
			"DELETE",
			"/v20180820/bucket/PLACEHOLDER/lifecycleconfiguration",
		},
		{"DeleteBucketPolicy", "DELETE", "/v20180820/bucket/PLACEHOLDER/policy"},
		{"DeleteBucketReplication", "DELETE", "/v20180820/bucket/PLACEHOLDER/replication"},
		{"DeleteBucketTagging", "DELETE", "/v20180820/bucket/PLACEHOLDER/tagging"},
		{"DeleteJobTagging", "DELETE", "/v20180820/jobs/PLACEHOLDER/tagging"},
		{"DeleteMultiRegionAccessPoint", "POST", "/v20180820/async-requests/mrap/delete"},
		{"DeletePublicAccessBlock", "DELETE", "/v20180820/configuration/publicAccessBlock"},
		{"DeleteStorageLensConfiguration", "DELETE", "/v20180820/storagelens/PLACEHOLDER"},
		{
			"DeleteStorageLensConfigurationTagging",
			"DELETE",
			"/v20180820/storagelens/PLACEHOLDER/tagging",
		},
		{"DeleteStorageLensGroup", "DELETE", "/v20180820/storagelensgroup/PLACEHOLDER"},
		{"DescribeJob", "GET", "/v20180820/jobs/PLACEHOLDER"},
		{
			"DescribeMultiRegionAccessPointOperation",
			"GET",
			"/v20180820/async-requests/mrap/PLACEHOLDER",
		},
		{
			"DissociateAccessGrantsIdentityCenter",
			"DELETE",
			"/v20180820/accessgrantsinstance/identitycenter",
		},
		{"GetAccessGrant", "GET", "/v20180820/accessgrantsinstance/grant/PLACEHOLDER"},
		{"GetAccessGrantsInstance", "GET", "/v20180820/accessgrantsinstance"},
		{"GetAccessGrantsInstanceForPrefix", "GET", "/v20180820/accessgrantsinstance/prefix"},
		{
			"GetAccessGrantsInstanceResourcePolicy",
			"GET",
			"/v20180820/accessgrantsinstance/resourcepolicy",
		},
		{"GetAccessGrantsLocation", "GET", "/v20180820/accessgrantsinstance/location/PLACEHOLDER"},
		{"GetAccessPoint", "GET", "/v20180820/accesspoint/PLACEHOLDER"},
		{
			"GetAccessPointConfigurationForObjectLambda",
			"GET",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/configuration",
		},
		{
			"GetAccessPointForObjectLambda",
			"GET",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER",
		},
		{"GetAccessPointPolicy", "GET", "/v20180820/accesspoint/PLACEHOLDER/policy"},
		{
			"GetAccessPointPolicyForObjectLambda",
			"GET",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/policy",
		},
		{"GetAccessPointPolicyStatus", "GET", "/v20180820/accesspoint/PLACEHOLDER/policyStatus"},
		{
			"GetAccessPointPolicyStatusForObjectLambda",
			"GET",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/policyStatus",
		},
		{"GetAccessPointScope", "GET", "/v20180820/accesspoint/PLACEHOLDER/scope"},
		{"GetBucket", "GET", "/v20180820/bucket/PLACEHOLDER"},
		{
			"GetBucketLifecycleConfiguration",
			"GET",
			"/v20180820/bucket/PLACEHOLDER/lifecycleconfiguration",
		},
		{"GetBucketPolicy", "GET", "/v20180820/bucket/PLACEHOLDER/policy"},
		{"GetBucketReplication", "GET", "/v20180820/bucket/PLACEHOLDER/replication"},
		{"GetBucketTagging", "GET", "/v20180820/bucket/PLACEHOLDER/tagging"},
		{"GetBucketVersioning", "GET", "/v20180820/bucket/PLACEHOLDER/versioning"},
		{"GetDataAccess", "GET", "/v20180820/accessgrantsinstance/dataaccess"},
		{"GetJobTagging", "GET", "/v20180820/jobs/PLACEHOLDER/tagging"},
		{"GetMultiRegionAccessPoint", "GET", "/v20180820/mrap/instances/PLACEHOLDER"},
		{"GetMultiRegionAccessPointPolicy", "GET", "/v20180820/mrap/instances/PLACEHOLDER/policy"},
		{
			"GetMultiRegionAccessPointPolicyStatus",
			"GET",
			"/v20180820/mrap/instances/PLACEHOLDER/policystatus",
		},
		{"GetMultiRegionAccessPointRoutes", "GET", "/v20180820/mrap/instances/PLACEHOLDER/routes"},
		{"GetPublicAccessBlock", "GET", "/v20180820/configuration/publicAccessBlock"},
		{"GetStorageLensConfiguration", "GET", "/v20180820/storagelens/PLACEHOLDER"},
		{"GetStorageLensConfigurationTagging", "GET", "/v20180820/storagelens/PLACEHOLDER/tagging"},
		{"GetStorageLensGroup", "GET", "/v20180820/storagelensgroup/PLACEHOLDER"},
		{"ListAccessGrants", "GET", "/v20180820/accessgrantsinstance/grants"},
		{"ListAccessGrantsInstances", "GET", "/v20180820/accessgrantsinstances"},
		{"ListAccessGrantsLocations", "GET", "/v20180820/accessgrantsinstance/locations"},
		{"ListAccessPoints", "GET", "/v20180820/accesspoint"},
		{"ListAccessPointsForDirectoryBuckets", "GET", "/v20180820/accesspointfordirectory"},
		{"ListAccessPointsForObjectLambda", "GET", "/v20180820/accesspointforobjectlambda"},
		{"ListCallerAccessGrants", "GET", "/v20180820/accessgrantsinstance/caller/grants"},
		{"ListJobs", "GET", "/v20180820/jobs"},
		{"ListMultiRegionAccessPoints", "GET", "/v20180820/mrap/instances"},
		{"ListRegionalBuckets", "GET", "/v20180820/bucket"},
		{"ListStorageLensConfigurations", "GET", "/v20180820/storagelens"},
		{"ListStorageLensGroups", "GET", "/v20180820/storagelensgroup"},
		{"ListTagsForResource", "GET", "/v20180820/tags/PLACEHOLDER"},
		{
			"PutAccessGrantsInstanceResourcePolicy",
			"PUT",
			"/v20180820/accessgrantsinstance/resourcepolicy",
		},
		{
			"PutAccessPointConfigurationForObjectLambda",
			"PUT",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/configuration",
		},
		{"PutAccessPointPolicy", "PUT", "/v20180820/accesspoint/PLACEHOLDER/policy"},
		{
			"PutAccessPointPolicyForObjectLambda",
			"PUT",
			"/v20180820/accesspointforobjectlambda/PLACEHOLDER/policy",
		},
		{"PutAccessPointScope", "PUT", "/v20180820/accesspoint/PLACEHOLDER/scope"},
		{
			"PutBucketLifecycleConfiguration",
			"PUT",
			"/v20180820/bucket/PLACEHOLDER/lifecycleconfiguration",
		},
		{"PutBucketPolicy", "PUT", "/v20180820/bucket/PLACEHOLDER/policy"},
		{"PutBucketReplication", "PUT", "/v20180820/bucket/PLACEHOLDER/replication"},
		{"PutBucketTagging", "PUT", "/v20180820/bucket/PLACEHOLDER/tagging"},
		{"PutBucketVersioning", "PUT", "/v20180820/bucket/PLACEHOLDER/versioning"},
		{"PutJobTagging", "PUT", "/v20180820/jobs/PLACEHOLDER/tagging"},
		{"PutMultiRegionAccessPointPolicy", "POST", "/v20180820/async-requests/mrap/put-policy"},
		{"PutPublicAccessBlock", "PUT", "/v20180820/configuration/publicAccessBlock"},
		{"PutStorageLensConfiguration", "PUT", "/v20180820/storagelens/PLACEHOLDER"},
		{"PutStorageLensConfigurationTagging", "PUT", "/v20180820/storagelens/PLACEHOLDER/tagging"},
		{
			"SubmitMultiRegionAccessPointRoutes",
			"PATCH",
			"/v20180820/mrap/instances/PLACEHOLDER/routes",
		},
		{"TagResource", "POST", "/v20180820/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/v20180820/tags/PLACEHOLDER"},
		{
			"UpdateAccessGrantsLocation",
			"PUT",
			"/v20180820/accessgrantsinstance/location/PLACEHOLDER",
		},
		{"UpdateJobPriority", "POST", "/v20180820/jobs/PLACEHOLDER/priority"},
		{"UpdateJobStatus", "POST", "/v20180820/jobs/PLACEHOLDER/status"},
		{"UpdateStorageLensGroup", "PUT", "/v20180820/storagelensgroup/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real S3 Control op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 97 ops against s3control's real
// op count. It then drives the same request through the real Handler() and
// asserts the response does not contain the exact literal "not found" that
// this handler's two miss-fallback sites (handler.go's
// dispatchPublicAccessBlock default and handler_tags.go's
// dispatchTagDispatch default, both writeXMLErrorCode(..., "NotFound", "not
// found")) emit when no extract*/dispatch* function claims the request --
// grepped across every non-test .go file in this package and confirmed to
// appear nowhere else: every domain not-found error instead carries a
// CamelCase AWS error code as both Code and Message via handleBackendError
// (e.g. "NoSuchBucket", "NoSuchAccessPoint",
// "AccessGrantsInstanceNotExistsError", "ReplicationConfigurationNotFoundError"),
// none of which contain the space-separated literal "not found".
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(
				t,
				rec.Body.String(),
				"not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default",
				tc.method,
				tc.path,
				tc.op,
			)
		})
	}
}
