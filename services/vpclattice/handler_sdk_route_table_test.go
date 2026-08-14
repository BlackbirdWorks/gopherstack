package vpclattice_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real VPC Lattice
// operation, extracted from vpclattice@v1.25.5 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {...Identifier}/{resourceArn} URI label -- classifyPath
// (handler.go) does not validate ID shape, so the literal value doesn't
// matter here, only that the path matches Op.
//
// A systematic check for a shared method+path template across all 73 ops
// found zero collisions, so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any route
// in this table. ListServiceNetworkVpcEndpointAssociations/
// ListResourceEndpointAssociations/DeleteResourceEndpointAssociation are
// list/delete-only families with no Create op in the real API (real AWS
// populates them solely via EC2 CreateVpcEndpoint -- see classifyPath's
// comment in handler.go), so they are not modeled as onceCollectionRoutes
// entries; kept here as their own cases since they are still real,
// reachable ops.
//
// All 73 ops were confirmed wired into onceOpHandlers (handler.go) before
// writing this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchUpdateRule", "PATCH", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules"},
		{"CreateAccessLogSubscription", "POST", "/accesslogsubscriptions"},
		{"CreateListener", "POST", "/services/PLACEHOLDER/listeners"},
		{"CreateResourceConfiguration", "POST", "/resourceconfigurations"},
		{"CreateResourceGateway", "POST", "/resourcegateways"},
		{"CreateRule", "POST", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules"},
		{"CreateService", "POST", "/services"},
		{"CreateServiceNetwork", "POST", "/servicenetworks"},
		{"CreateServiceNetworkResourceAssociation", "POST", "/servicenetworkresourceassociations"},
		{"CreateServiceNetworkServiceAssociation", "POST", "/servicenetworkserviceassociations"},
		{"CreateServiceNetworkVpcAssociation", "POST", "/servicenetworkvpcassociations"},
		{"CreateTargetGroup", "POST", "/targetgroups"},
		{"DeleteAccessLogSubscription", "DELETE", "/accesslogsubscriptions/PLACEHOLDER"},
		{"DeleteAuthPolicy", "DELETE", "/authpolicy/PLACEHOLDER"},
		{"DeleteDomainVerification", "DELETE", "/domainverifications/PLACEHOLDER"},
		{"DeleteListener", "DELETE", "/services/PLACEHOLDER/listeners/PLACEHOLDER"},
		{"DeleteResourceConfiguration", "DELETE", "/resourceconfigurations/PLACEHOLDER"},
		{"DeleteResourceEndpointAssociation", "DELETE", "/resourceendpointassociations/PLACEHOLDER"},
		{"DeleteResourceGateway", "DELETE", "/resourcegateways/PLACEHOLDER"},
		{"DeleteResourcePolicy", "DELETE", "/resourcepolicy/PLACEHOLDER"},
		{"DeleteRule", "DELETE", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules/PLACEHOLDER"},
		{"DeleteService", "DELETE", "/services/PLACEHOLDER"},
		{"DeleteServiceNetwork", "DELETE", "/servicenetworks/PLACEHOLDER"},
		{"DeleteServiceNetworkResourceAssociation", "DELETE", "/servicenetworkresourceassociations/PLACEHOLDER"},
		{"DeleteServiceNetworkServiceAssociation", "DELETE", "/servicenetworkserviceassociations/PLACEHOLDER"},
		{"DeleteServiceNetworkVpcAssociation", "DELETE", "/servicenetworkvpcassociations/PLACEHOLDER"},
		{"DeleteTargetGroup", "DELETE", "/targetgroups/PLACEHOLDER"},
		{"DeregisterTargets", "POST", "/targetgroups/PLACEHOLDER/deregistertargets"},
		{"GetAccessLogSubscription", "GET", "/accesslogsubscriptions/PLACEHOLDER"},
		{"GetAuthPolicy", "GET", "/authpolicy/PLACEHOLDER"},
		{"GetDomainVerification", "GET", "/domainverifications/PLACEHOLDER"},
		{"GetListener", "GET", "/services/PLACEHOLDER/listeners/PLACEHOLDER"},
		{"GetResourceConfiguration", "GET", "/resourceconfigurations/PLACEHOLDER"},
		{"GetResourceGateway", "GET", "/resourcegateways/PLACEHOLDER"},
		{"GetResourcePolicy", "GET", "/resourcepolicy/PLACEHOLDER"},
		{"GetRule", "GET", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules/PLACEHOLDER"},
		{"GetService", "GET", "/services/PLACEHOLDER"},
		{"GetServiceNetwork", "GET", "/servicenetworks/PLACEHOLDER"},
		{"GetServiceNetworkResourceAssociation", "GET", "/servicenetworkresourceassociations/PLACEHOLDER"},
		{"GetServiceNetworkServiceAssociation", "GET", "/servicenetworkserviceassociations/PLACEHOLDER"},
		{"GetServiceNetworkVpcAssociation", "GET", "/servicenetworkvpcassociations/PLACEHOLDER"},
		{"GetTargetGroup", "GET", "/targetgroups/PLACEHOLDER"},
		{"ListAccessLogSubscriptions", "GET", "/accesslogsubscriptions"},
		{"ListDomainVerifications", "GET", "/domainverifications"},
		{"ListListeners", "GET", "/services/PLACEHOLDER/listeners"},
		{"ListResourceConfigurations", "GET", "/resourceconfigurations"},
		{"ListResourceEndpointAssociations", "GET", "/resourceendpointassociations"},
		{"ListResourceGateways", "GET", "/resourcegateways"},
		{"ListRules", "GET", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules"},
		{"ListServiceNetworkResourceAssociations", "GET", "/servicenetworkresourceassociations"},
		{"ListServiceNetworkServiceAssociations", "GET", "/servicenetworkserviceassociations"},
		{"ListServiceNetworkVpcAssociations", "GET", "/servicenetworkvpcassociations"},
		{"ListServiceNetworkVpcEndpointAssociations", "GET", "/servicenetworkvpcendpointassociations"},
		{"ListServiceNetworks", "GET", "/servicenetworks"},
		{"ListServices", "GET", "/services"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListTargetGroups", "GET", "/targetgroups"},
		{"ListTargets", "POST", "/targetgroups/PLACEHOLDER/listtargets"},
		{"PutAuthPolicy", "PUT", "/authpolicy/PLACEHOLDER"},
		{"PutResourcePolicy", "PUT", "/resourcepolicy/PLACEHOLDER"},
		{"RegisterTargets", "POST", "/targetgroups/PLACEHOLDER/registertargets"},
		{"StartDomainVerification", "POST", "/domainverifications"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAccessLogSubscription", "PATCH", "/accesslogsubscriptions/PLACEHOLDER"},
		{"UpdateListener", "PATCH", "/services/PLACEHOLDER/listeners/PLACEHOLDER"},
		{"UpdateResourceConfiguration", "PATCH", "/resourceconfigurations/PLACEHOLDER"},
		{"UpdateResourceGateway", "PATCH", "/resourcegateways/PLACEHOLDER"},
		{"UpdateRule", "PATCH", "/services/PLACEHOLDER/listeners/PLACEHOLDER/rules/PLACEHOLDER"},
		{"UpdateService", "PATCH", "/services/PLACEHOLDER"},
		{"UpdateServiceNetwork", "PATCH", "/servicenetworks/PLACEHOLDER"},
		{"UpdateServiceNetworkVpcAssociation", "PATCH", "/servicenetworkvpcassociations/PLACEHOLDER"},
		{"UpdateTargetGroup", "PATCH", "/targetgroups/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real VPC Lattice op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts classifyPath resolves it to the right op, all 73 ops against
// vpclattice's real op count. It then drives the same request through the
// real Handler() and asserts it did not fall through to the exact "unknown
// operation" text handleREST (handler.go) writes via c.JSON when
// onceOpHandlers() has no entry for the classified op -- distinct from every
// domain error this service writes (all via handleError, whose message is
// always err.Error() for a domain-specific error type), none of which equal
// this literal phrase.
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
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
