package eks_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real EKS
// operation, extracted from eks@v1.90.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateAccessPolicy", "POST", "/clusters/PLACEHOLDER/access-entries/PLACEHOLDER/access-policies"},
		{"AssociateEncryptionConfig", "POST", "/clusters/PLACEHOLDER/encryption-config/associate"},
		{"AssociateIdentityProviderConfig", "POST", "/clusters/PLACEHOLDER/identity-provider-configs/associate"},
		{"CancelUpdate", "POST", "/clusters/PLACEHOLDER/updates/PLACEHOLDER/cancel-update"},
		{"CreateAccessEntry", "POST", "/clusters/PLACEHOLDER/access-entries"},
		{"CreateAddon", "POST", "/clusters/PLACEHOLDER/addons"},
		{"CreateCapability", "POST", "/clusters/PLACEHOLDER/capabilities"},
		{"CreateCluster", "POST", "/clusters"},
		{"CreateEksAnywhereSubscription", "POST", "/eks-anywhere-subscriptions"},
		{"CreateFargateProfile", "POST", "/clusters/PLACEHOLDER/fargate-profiles"},
		{"CreateNodegroup", "POST", "/clusters/PLACEHOLDER/node-groups"},
		{"CreatePodIdentityAssociation", "POST", "/clusters/PLACEHOLDER/pod-identity-associations"},
		{"DeleteAccessEntry", "DELETE", "/clusters/PLACEHOLDER/access-entries/PLACEHOLDER"},
		{"DeleteAddon", "DELETE", "/clusters/PLACEHOLDER/addons/PLACEHOLDER"},
		{"DeleteCapability", "DELETE", "/clusters/PLACEHOLDER/capabilities/PLACEHOLDER"},
		{"DeleteCluster", "DELETE", "/clusters/PLACEHOLDER"},
		{"DeleteEksAnywhereSubscription", "DELETE", "/eks-anywhere-subscriptions/PLACEHOLDER"},
		{"DeleteFargateProfile", "DELETE", "/clusters/PLACEHOLDER/fargate-profiles/PLACEHOLDER"},
		{"DeleteNodegroup", "DELETE", "/clusters/PLACEHOLDER/node-groups/PLACEHOLDER"},
		{"DeletePodIdentityAssociation", "DELETE", "/clusters/PLACEHOLDER/pod-identity-associations/PLACEHOLDER"},
		{"DeregisterCluster", "DELETE", "/cluster-registrations/PLACEHOLDER"},
		{"DescribeAccessEntry", "GET", "/clusters/PLACEHOLDER/access-entries/PLACEHOLDER"},
		{"DescribeAddon", "GET", "/clusters/PLACEHOLDER/addons/PLACEHOLDER"},
		{"DescribeAddonConfiguration", "GET", "/addons/configuration-schemas"},
		{"DescribeAddonVersions", "GET", "/addons/supported-versions"},
		{"DescribeCapability", "GET", "/clusters/PLACEHOLDER/capabilities/PLACEHOLDER"},
		{"DescribeCluster", "GET", "/clusters/PLACEHOLDER"},
		{"DescribeClusterVersions", "GET", "/cluster-versions"},
		{"DescribeEksAnywhereSubscription", "GET", "/eks-anywhere-subscriptions/PLACEHOLDER"},
		{"DescribeFargateProfile", "GET", "/clusters/PLACEHOLDER/fargate-profiles/PLACEHOLDER"},
		{"DescribeIdentityProviderConfig", "POST", "/clusters/PLACEHOLDER/identity-provider-configs/describe"},
		{"DescribeInsight", "GET", "/clusters/PLACEHOLDER/insights/PLACEHOLDER"},
		{"DescribeInsightsRefresh", "GET", "/clusters/PLACEHOLDER/insights-refresh"},
		{"DescribeNodegroup", "GET", "/clusters/PLACEHOLDER/node-groups/PLACEHOLDER"},
		{"DescribePodIdentityAssociation", "GET", "/clusters/PLACEHOLDER/pod-identity-associations/PLACEHOLDER"},
		{"DescribeUpdate", "GET", "/clusters/PLACEHOLDER/updates/PLACEHOLDER"},
		{
			"DisassociateAccessPolicy", "DELETE",
			"/clusters/PLACEHOLDER/access-entries/PLACEHOLDER/access-policies/PLACEHOLDER",
		},
		{"DisassociateIdentityProviderConfig", "POST", "/clusters/PLACEHOLDER/identity-provider-configs/disassociate"},
		{"ListAccessEntries", "GET", "/clusters/PLACEHOLDER/access-entries"},
		{"ListAccessPolicies", "GET", "/access-policies"},
		{"ListAddons", "GET", "/clusters/PLACEHOLDER/addons"},
		{"ListAssociatedAccessPolicies", "GET", "/clusters/PLACEHOLDER/access-entries/PLACEHOLDER/access-policies"},
		{"ListCapabilities", "GET", "/clusters/PLACEHOLDER/capabilities"},
		{"ListClusters", "GET", "/clusters"},
		{"ListEksAnywhereSubscriptions", "GET", "/eks-anywhere-subscriptions"},
		{"ListFargateProfiles", "GET", "/clusters/PLACEHOLDER/fargate-profiles"},
		{"ListIdentityProviderConfigs", "GET", "/clusters/PLACEHOLDER/identity-provider-configs"},
		{"ListInsights", "POST", "/clusters/PLACEHOLDER/insights"},
		{"ListNodegroups", "GET", "/clusters/PLACEHOLDER/node-groups"},
		{"ListPodIdentityAssociations", "GET", "/clusters/PLACEHOLDER/pod-identity-associations"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListUpdates", "GET", "/clusters/PLACEHOLDER/updates"},
		{"RegisterCluster", "POST", "/cluster-registrations"},
		{"StartInsightsRefresh", "POST", "/clusters/PLACEHOLDER/insights-refresh"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAccessEntry", "POST", "/clusters/PLACEHOLDER/access-entries/PLACEHOLDER"},
		{"UpdateAddon", "POST", "/clusters/PLACEHOLDER/addons/PLACEHOLDER/update"},
		{"UpdateCapability", "POST", "/clusters/PLACEHOLDER/capabilities/PLACEHOLDER"},
		{"UpdateClusterConfig", "POST", "/clusters/PLACEHOLDER/update-config"},
		{"UpdateClusterVersion", "POST", "/clusters/PLACEHOLDER/updates"},
		{"UpdateEksAnywhereSubscription", "POST", "/eks-anywhere-subscriptions/PLACEHOLDER"},
		{"UpdateNodegroupConfig", "POST", "/clusters/PLACEHOLDER/node-groups/PLACEHOLDER/update-config"},
		{"UpdateNodegroupVersion", "POST", "/clusters/PLACEHOLDER/node-groups/PLACEHOLDER/update-version"},
		{"UpdatePodIdentityAssociation", "POST", "/clusters/PLACEHOLDER/pod-identity-associations/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real EKS op's authoritative
// method+path (see sdkRouteCases) through ExtractOperation and asserts the
// route table resolves it to the right op. gopherstack-jqh2 pass 3.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown operation: " ResourceNotFoundException
// that dispatch's final default emits (handler.go:515) -- guarding against
// an operation name that resolves correctly but has no matching case in any
// dispatchXxx family (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
