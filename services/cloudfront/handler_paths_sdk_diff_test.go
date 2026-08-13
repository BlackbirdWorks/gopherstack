package cloudfront_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real cloudfront
// (control-plane) operation, extracted from cloudfront@v1.67.4 serializers.go:
// each entry's "request.Method" and the string passed to httpbinding.SplitURI
// in that op's awsRestxml_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for any {Param} URI label -- parseCFPath does not validate ID shape, so
// the literal value doesn't matter here, only that the path matches Op.
//
// Excludes the five CloudFront KeyValueStore *data-plane* ops (GetKey,
// PutKey, DeleteKey, ListKeys, UpdateKeys): those belong to the separate
// cloudfrontkeyvaluestore SDK client/protocol (REST-JSON, no "/2020-05-31/"
// prefix) and structurally can never match this Handler's RouteMatcher --
// see TestSDKCompleteness's keyValueStoreDataPlaneOps split.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestxml_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateAlias", "PUT", "/2020-05-31/distribution/PLACEHOLDER/associate-alias"},
		{"AssociateDistributionTenantWebACL", "PUT", "/2020-05-31/distribution-tenant/PLACEHOLDER/associate-web-acl"},
		{"AssociateDistributionWebACL", "PUT", "/2020-05-31/distribution/PLACEHOLDER/associate-web-acl"},
		{"CopyDistribution", "POST", "/2020-05-31/distribution/PLACEHOLDER/copy"},
		{"CreateAnycastIpList", "POST", "/2020-05-31/anycast-ip-list"},
		{"CreateCachePolicy", "POST", "/2020-05-31/cache-policy"},
		{"CreateCloudFrontOriginAccessIdentity", "POST", "/2020-05-31/origin-access-identity/cloudfront"},
		{"CreateConnectionFunction", "POST", "/2020-05-31/connection-function"},
		{"CreateConnectionGroup", "POST", "/2020-05-31/connection-group"},
		{"CreateContinuousDeploymentPolicy", "POST", "/2020-05-31/continuous-deployment-policy"},
		{"CreateDistribution", "POST", "/2020-05-31/distribution"},
		{"CreateDistributionTenant", "POST", "/2020-05-31/distribution-tenant"},
		{"CreateDistributionWithTags", "POST", "/2020-05-31/distribution?WithTags"},
		{"CreateFieldLevelEncryptionConfig", "POST", "/2020-05-31/field-level-encryption"},
		{"CreateFieldLevelEncryptionProfile", "POST", "/2020-05-31/field-level-encryption-profile"},
		{"CreateFunction", "POST", "/2020-05-31/function"},
		{"CreateInvalidation", "POST", "/2020-05-31/distribution/PLACEHOLDER/invalidation"},
		{"CreateInvalidationForDistributionTenant", "POST", "/2020-05-31/distribution-tenant/PLACEHOLDER/invalidation"},
		{"CreateKeyGroup", "POST", "/2020-05-31/key-group"},
		{"CreateKeyValueStore", "POST", "/2020-05-31/key-value-store"},
		{"CreateMonitoringSubscription", "POST", "/2020-05-31/distributions/PLACEHOLDER/monitoring-subscription"},
		{"CreateOriginAccessControl", "POST", "/2020-05-31/origin-access-control"},
		{"CreateOriginRequestPolicy", "POST", "/2020-05-31/origin-request-policy"},
		{"CreatePublicKey", "POST", "/2020-05-31/public-key"},
		{"CreateRealtimeLogConfig", "POST", "/2020-05-31/realtime-log-config"},
		{"CreateResponseHeadersPolicy", "POST", "/2020-05-31/response-headers-policy"},
		{"CreateStreamingDistribution", "POST", "/2020-05-31/streaming-distribution"},
		{"CreateStreamingDistributionWithTags", "POST", "/2020-05-31/streaming-distribution?WithTags"},
		{"CreateTrustStore", "POST", "/2020-05-31/trust-store"},
		{"CreateVpcOrigin", "POST", "/2020-05-31/vpc-origin"},
		{"DeleteAnycastIpList", "DELETE", "/2020-05-31/anycast-ip-list/PLACEHOLDER"},
		{"DeleteCachePolicy", "DELETE", "/2020-05-31/cache-policy/PLACEHOLDER"},
		{"DeleteCloudFrontOriginAccessIdentity", "DELETE", "/2020-05-31/origin-access-identity/cloudfront/PLACEHOLDER"},
		{"DeleteConnectionFunction", "DELETE", "/2020-05-31/connection-function/PLACEHOLDER"},
		{"DeleteConnectionGroup", "DELETE", "/2020-05-31/connection-group/PLACEHOLDER"},
		{"DeleteContinuousDeploymentPolicy", "DELETE", "/2020-05-31/continuous-deployment-policy/PLACEHOLDER"},
		{"DeleteDistribution", "DELETE", "/2020-05-31/distribution/PLACEHOLDER"},
		{"DeleteDistributionTenant", "DELETE", "/2020-05-31/distribution-tenant/PLACEHOLDER"},
		{"DeleteFieldLevelEncryptionConfig", "DELETE", "/2020-05-31/field-level-encryption/PLACEHOLDER"},
		{"DeleteFieldLevelEncryptionProfile", "DELETE", "/2020-05-31/field-level-encryption-profile/PLACEHOLDER"},
		{"DeleteFunction", "DELETE", "/2020-05-31/function/PLACEHOLDER"},
		{"DeleteKeyGroup", "DELETE", "/2020-05-31/key-group/PLACEHOLDER"},
		{"DeleteKeyValueStore", "DELETE", "/2020-05-31/key-value-store/PLACEHOLDER"},
		{"DeleteMonitoringSubscription", "DELETE", "/2020-05-31/distributions/PLACEHOLDER/monitoring-subscription"},
		{"DeleteOriginAccessControl", "DELETE", "/2020-05-31/origin-access-control/PLACEHOLDER"},
		{"DeleteOriginRequestPolicy", "DELETE", "/2020-05-31/origin-request-policy/PLACEHOLDER"},
		{"DeletePublicKey", "DELETE", "/2020-05-31/public-key/PLACEHOLDER"},
		{"DeleteRealtimeLogConfig", "POST", "/2020-05-31/delete-realtime-log-config"},
		{"DeleteResourcePolicy", "POST", "/2020-05-31/delete-resource-policy"},
		{"DeleteResponseHeadersPolicy", "DELETE", "/2020-05-31/response-headers-policy/PLACEHOLDER"},
		{"DeleteStreamingDistribution", "DELETE", "/2020-05-31/streaming-distribution/PLACEHOLDER"},
		{"DeleteTrustStore", "DELETE", "/2020-05-31/trust-store/PLACEHOLDER"},
		{"DeleteVpcOrigin", "DELETE", "/2020-05-31/vpc-origin/PLACEHOLDER"},
		{"DescribeConnectionFunction", "GET", "/2020-05-31/connection-function/PLACEHOLDER/describe"},
		{"DescribeFunction", "GET", "/2020-05-31/function/PLACEHOLDER/describe"},
		{"DescribeKeyValueStore", "GET", "/2020-05-31/key-value-store/PLACEHOLDER"},
		{
			"DisassociateDistributionTenantWebACL",
			"PUT",
			"/2020-05-31/distribution-tenant/PLACEHOLDER/disassociate-web-acl",
		},
		{"DisassociateDistributionWebACL", "PUT", "/2020-05-31/distribution/PLACEHOLDER/disassociate-web-acl"},
		{"GetAnycastIpList", "GET", "/2020-05-31/anycast-ip-list/PLACEHOLDER"},
		{"GetCachePolicy", "GET", "/2020-05-31/cache-policy/PLACEHOLDER"},
		{"GetCachePolicyConfig", "GET", "/2020-05-31/cache-policy/PLACEHOLDER/config"},
		{"GetCloudFrontOriginAccessIdentity", "GET", "/2020-05-31/origin-access-identity/cloudfront/PLACEHOLDER"},
		{
			"GetCloudFrontOriginAccessIdentityConfig", "GET",
			"/2020-05-31/origin-access-identity/cloudfront/PLACEHOLDER/config",
		},
		{"GetConnectionFunction", "GET", "/2020-05-31/connection-function/PLACEHOLDER"},
		{"GetConnectionGroup", "GET", "/2020-05-31/connection-group/PLACEHOLDER"},
		{"GetConnectionGroupByRoutingEndpoint", "GET", "/2020-05-31/connection-group"},
		{"GetContinuousDeploymentPolicy", "GET", "/2020-05-31/continuous-deployment-policy/PLACEHOLDER"},
		{"GetContinuousDeploymentPolicyConfig", "GET", "/2020-05-31/continuous-deployment-policy/PLACEHOLDER/config"},
		{"GetDistribution", "GET", "/2020-05-31/distribution/PLACEHOLDER"},
		{"GetDistributionConfig", "GET", "/2020-05-31/distribution/PLACEHOLDER/config"},
		{"GetDistributionTenant", "GET", "/2020-05-31/distribution-tenant/PLACEHOLDER"},
		{"GetDistributionTenantByDomain", "GET", "/2020-05-31/distribution-tenant"},
		{"GetFieldLevelEncryption", "GET", "/2020-05-31/field-level-encryption/PLACEHOLDER"},
		{"GetFieldLevelEncryptionConfig", "GET", "/2020-05-31/field-level-encryption/PLACEHOLDER/config"},
		{"GetFieldLevelEncryptionProfile", "GET", "/2020-05-31/field-level-encryption-profile/PLACEHOLDER"},
		{
			"GetFieldLevelEncryptionProfileConfig",
			"GET",
			"/2020-05-31/field-level-encryption-profile/PLACEHOLDER/config",
		},
		{"GetFunction", "GET", "/2020-05-31/function/PLACEHOLDER"},
		{"GetInvalidation", "GET", "/2020-05-31/distribution/PLACEHOLDER/invalidation/PLACEHOLDER"},
		{
			"GetInvalidationForDistributionTenant",
			"GET",
			"/2020-05-31/distribution-tenant/PLACEHOLDER/invalidation/PLACEHOLDER",
		},
		{"GetKeyGroup", "GET", "/2020-05-31/key-group/PLACEHOLDER"},
		{"GetKeyGroupConfig", "GET", "/2020-05-31/key-group/PLACEHOLDER/config"},
		{"GetManagedCertificateDetails", "GET", "/2020-05-31/managed-certificate/PLACEHOLDER"},
		{"GetMonitoringSubscription", "GET", "/2020-05-31/distributions/PLACEHOLDER/monitoring-subscription"},
		{"GetOriginAccessControl", "GET", "/2020-05-31/origin-access-control/PLACEHOLDER"},
		{"GetOriginAccessControlConfig", "GET", "/2020-05-31/origin-access-control/PLACEHOLDER/config"},
		{"GetOriginRequestPolicy", "GET", "/2020-05-31/origin-request-policy/PLACEHOLDER"},
		{"GetOriginRequestPolicyConfig", "GET", "/2020-05-31/origin-request-policy/PLACEHOLDER/config"},
		{"GetPublicKey", "GET", "/2020-05-31/public-key/PLACEHOLDER"},
		{"GetPublicKeyConfig", "GET", "/2020-05-31/public-key/PLACEHOLDER/config"},
		{"GetRealtimeLogConfig", "POST", "/2020-05-31/get-realtime-log-config"},
		{"GetResourcePolicy", "POST", "/2020-05-31/get-resource-policy"},
		{"GetResponseHeadersPolicy", "GET", "/2020-05-31/response-headers-policy/PLACEHOLDER"},
		{"GetResponseHeadersPolicyConfig", "GET", "/2020-05-31/response-headers-policy/PLACEHOLDER/config"},
		{"GetStreamingDistribution", "GET", "/2020-05-31/streaming-distribution/PLACEHOLDER"},
		{"GetStreamingDistributionConfig", "GET", "/2020-05-31/streaming-distribution/PLACEHOLDER/config"},
		{"GetTrustStore", "GET", "/2020-05-31/trust-store/PLACEHOLDER"},
		{"GetVpcOrigin", "GET", "/2020-05-31/vpc-origin/PLACEHOLDER"},
		{"ListAnycastIpLists", "GET", "/2020-05-31/anycast-ip-list"},
		{"ListCachePolicies", "GET", "/2020-05-31/cache-policy"},
		{"ListCloudFrontOriginAccessIdentities", "GET", "/2020-05-31/origin-access-identity/cloudfront"},
		{"ListConflictingAliases", "GET", "/2020-05-31/conflicting-alias"},
		{"ListConnectionFunctions", "POST", "/2020-05-31/connection-functions"},
		{"ListConnectionGroups", "POST", "/2020-05-31/connection-groups"},
		{"ListContinuousDeploymentPolicies", "GET", "/2020-05-31/continuous-deployment-policy"},
		{"ListDistributionTenants", "POST", "/2020-05-31/distribution-tenants"},
		{"ListDistributionTenantsByCustomization", "POST", "/2020-05-31/distribution-tenants-by-customization"},
		{"ListDistributions", "GET", "/2020-05-31/distribution"},
		{"ListDistributionsByAnycastIpListId", "GET", "/2020-05-31/distributionsByAnycastIpListId/PLACEHOLDER"},
		{"ListDistributionsByCachePolicyId", "GET", "/2020-05-31/distributionsByCachePolicyId/PLACEHOLDER"},
		{"ListDistributionsByConnectionFunction", "GET", "/2020-05-31/distributionsByConnectionFunction"},
		{"ListDistributionsByConnectionMode", "GET", "/2020-05-31/distributionsByConnectionMode/PLACEHOLDER"},
		{"ListDistributionsByKeyGroup", "GET", "/2020-05-31/distributionsByKeyGroupId/PLACEHOLDER"},
		{
			"ListDistributionsByOriginRequestPolicyId", "GET",
			"/2020-05-31/distributionsByOriginRequestPolicyId/PLACEHOLDER",
		},
		{"ListDistributionsByOwnedResource", "GET", "/2020-05-31/distributionsByOwnedResource/PLACEHOLDER"},
		{"ListDistributionsByRealtimeLogConfig", "POST", "/2020-05-31/distributionsByRealtimeLogConfig"},
		{
			"ListDistributionsByResponseHeadersPolicyId", "GET",
			"/2020-05-31/distributionsByResponseHeadersPolicyId/PLACEHOLDER",
		},
		{"ListDistributionsByTrustStore", "GET", "/2020-05-31/distributionsByTrustStore"},
		{"ListDistributionsByVpcOriginId", "GET", "/2020-05-31/distributionsByVpcOriginId/PLACEHOLDER"},
		{"ListDistributionsByWebACLId", "GET", "/2020-05-31/distributionsByWebACLId/PLACEHOLDER"},
		{"ListDomainConflicts", "POST", "/2020-05-31/domain-conflicts"},
		{"ListFieldLevelEncryptionConfigs", "GET", "/2020-05-31/field-level-encryption"},
		{"ListFieldLevelEncryptionProfiles", "GET", "/2020-05-31/field-level-encryption-profile"},
		{"ListFunctions", "GET", "/2020-05-31/function"},
		{"ListInvalidations", "GET", "/2020-05-31/distribution/PLACEHOLDER/invalidation"},
		{"ListInvalidationsForDistributionTenant", "GET", "/2020-05-31/distribution-tenant/PLACEHOLDER/invalidation"},
		{"ListKeyGroups", "GET", "/2020-05-31/key-group"},
		{"ListKeyValueStores", "GET", "/2020-05-31/key-value-store"},
		{"ListOriginAccessControls", "GET", "/2020-05-31/origin-access-control"},
		{"ListOriginRequestPolicies", "GET", "/2020-05-31/origin-request-policy"},
		{"ListPublicKeys", "GET", "/2020-05-31/public-key"},
		{"ListRealtimeLogConfigs", "GET", "/2020-05-31/realtime-log-config"},
		{"ListResponseHeadersPolicies", "GET", "/2020-05-31/response-headers-policy"},
		{"ListStreamingDistributions", "GET", "/2020-05-31/streaming-distribution"},
		{"ListTagsForResource", "GET", "/2020-05-31/tagging"},
		{"ListTrustStores", "POST", "/2020-05-31/trust-stores"},
		{"ListVpcOrigins", "GET", "/2020-05-31/vpc-origin"},
		{"PublishConnectionFunction", "POST", "/2020-05-31/connection-function/PLACEHOLDER/publish"},
		{"PublishFunction", "POST", "/2020-05-31/function/PLACEHOLDER/publish"},
		{"PutResourcePolicy", "POST", "/2020-05-31/put-resource-policy"},
		{"TagResource", "POST", "/2020-05-31/tagging?Operation=Tag"},
		{"TestConnectionFunction", "POST", "/2020-05-31/connection-function/PLACEHOLDER/test"},
		{"TestFunction", "POST", "/2020-05-31/function/PLACEHOLDER/test"},
		{"UntagResource", "POST", "/2020-05-31/tagging?Operation=Untag"},
		{"UpdateAnycastIpList", "PUT", "/2020-05-31/anycast-ip-list/PLACEHOLDER"},
		{"UpdateCachePolicy", "PUT", "/2020-05-31/cache-policy/PLACEHOLDER"},
		{
			"UpdateCloudFrontOriginAccessIdentity",
			"PUT",
			"/2020-05-31/origin-access-identity/cloudfront/PLACEHOLDER/config",
		},
		{"UpdateConnectionFunction", "PUT", "/2020-05-31/connection-function/PLACEHOLDER"},
		{"UpdateConnectionGroup", "PUT", "/2020-05-31/connection-group/PLACEHOLDER"},
		{"UpdateContinuousDeploymentPolicy", "PUT", "/2020-05-31/continuous-deployment-policy/PLACEHOLDER"},
		{"UpdateDistribution", "PUT", "/2020-05-31/distribution/PLACEHOLDER/config"},
		{"UpdateDistributionTenant", "PUT", "/2020-05-31/distribution-tenant/PLACEHOLDER"},
		{"UpdateDistributionWithStagingConfig", "PUT", "/2020-05-31/distribution/PLACEHOLDER/promote-staging-config"},
		{"UpdateDomainAssociation", "POST", "/2020-05-31/domain-association"},
		{"UpdateFieldLevelEncryptionConfig", "PUT", "/2020-05-31/field-level-encryption/PLACEHOLDER/config"},
		{"UpdateFieldLevelEncryptionProfile", "PUT", "/2020-05-31/field-level-encryption-profile/PLACEHOLDER/config"},
		{"UpdateFunction", "PUT", "/2020-05-31/function/PLACEHOLDER"},
		{"UpdateKeyGroup", "PUT", "/2020-05-31/key-group/PLACEHOLDER"},
		{"UpdateKeyValueStore", "PUT", "/2020-05-31/key-value-store/PLACEHOLDER"},
		{"UpdateOriginAccessControl", "PUT", "/2020-05-31/origin-access-control/PLACEHOLDER/config"},
		{"UpdateOriginRequestPolicy", "PUT", "/2020-05-31/origin-request-policy/PLACEHOLDER"},
		{"UpdatePublicKey", "PUT", "/2020-05-31/public-key/PLACEHOLDER/config"},
		{"UpdateRealtimeLogConfig", "PUT", "/2020-05-31/realtime-log-config"},
		{"UpdateResponseHeadersPolicy", "PUT", "/2020-05-31/response-headers-policy/PLACEHOLDER"},
		{"UpdateStreamingDistribution", "PUT", "/2020-05-31/streaming-distribution/PLACEHOLDER/config"},
		{"UpdateTrustStore", "PUT", "/2020-05-31/trust-store/PLACEHOLDER"},
		{"UpdateVpcOrigin", "PUT", "/2020-05-31/vpc-origin/PLACEHOLDER"},
		{"VerifyDnsConfiguration", "POST", "/2020-05-31/verify-dns-configuration"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real cloudfront op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the "NoSuchOperation" error dispatchStubsTenantAndCerts's default case
// emits at the end of the dispatch chain (handler_dispatch.go) -- cloudfront
// shares parseCFPath between ExtractOperation and dispatch, so this mainly
// guards against an op name that resolves correctly but has no matching case
// in the dispatch switch chain (gopherstack-ey26). This is what caught
// gopherstack-o31x's routing bugs beyond the three already known: the whole
// ListDistributionsBy* family using a hyphenated path with no real-SDK
// counterpart, the monitoring-subscription trio using singular "distribution/"
// instead of plural "distributions/", GetManagedCertificateDetails nested
// under distribution-tenant instead of its own "managed-certificate/" root,
// and ListConnectionFunctions/ListConnectionGroups/GetDistributionTenantByDomain/
// GetConnectionGroupByRoutingEndpoint all swapped with their List sibling.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "NoSuchOperation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
