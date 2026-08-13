package route53_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real route53
// operation, extracted from route53@v1.65.6 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestxml_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
// GetGeoLocation carries its real ?continentcode= query filter, the load-
// bearing signal that distinguishes it from ListGeoLocations sharing the
// GeoLocation-path switch case in the handler.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestxml_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ActivateKeySigningKey", "POST", "/2013-04-01/keysigningkey/PLACEHOLDER/PLACEHOLDER/activate"},
		{"AssociateVPCWithHostedZone", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/associatevpc"},
		{"ChangeCidrCollection", "POST", "/2013-04-01/cidrcollection/PLACEHOLDER"},
		{"ChangeResourceRecordSets", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/rrset"},
		{"ChangeTagsForResource", "POST", "/2013-04-01/tags/PLACEHOLDER/PLACEHOLDER"},
		{"CreateCidrCollection", "POST", "/2013-04-01/cidrcollection"},
		{"CreateHealthCheck", "POST", "/2013-04-01/healthcheck"},
		{"CreateHostedZone", "POST", "/2013-04-01/hostedzone"},
		{"CreateKeySigningKey", "POST", "/2013-04-01/keysigningkey"},
		{"CreateQueryLoggingConfig", "POST", "/2013-04-01/queryloggingconfig"},
		{"CreateReusableDelegationSet", "POST", "/2013-04-01/delegationset"},
		{"CreateTrafficPolicy", "POST", "/2013-04-01/trafficpolicy"},
		{"CreateTrafficPolicyInstance", "POST", "/2013-04-01/trafficpolicyinstance"},
		{"CreateTrafficPolicyVersion", "POST", "/2013-04-01/trafficpolicy/PLACEHOLDER"},
		{"CreateVPCAssociationAuthorization", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/authorizevpcassociation"},
		{"DeactivateKeySigningKey", "POST", "/2013-04-01/keysigningkey/PLACEHOLDER/PLACEHOLDER/deactivate"},
		{"DeleteCidrCollection", "DELETE", "/2013-04-01/cidrcollection/PLACEHOLDER"},
		{"DeleteHealthCheck", "DELETE", "/2013-04-01/healthcheck/PLACEHOLDER"},
		{"DeleteHostedZone", "DELETE", "/2013-04-01/hostedzone/PLACEHOLDER"},
		{"DeleteKeySigningKey", "DELETE", "/2013-04-01/keysigningkey/PLACEHOLDER/PLACEHOLDER"},
		{"DeleteQueryLoggingConfig", "DELETE", "/2013-04-01/queryloggingconfig/PLACEHOLDER"},
		{"DeleteReusableDelegationSet", "DELETE", "/2013-04-01/delegationset/PLACEHOLDER"},
		{"DeleteTrafficPolicy", "DELETE", "/2013-04-01/trafficpolicy/PLACEHOLDER/PLACEHOLDER"},
		{"DeleteTrafficPolicyInstance", "DELETE", "/2013-04-01/trafficpolicyinstance/PLACEHOLDER"},
		{
			"DeleteVPCAssociationAuthorization", "POST",
			"/2013-04-01/hostedzone/PLACEHOLDER/deauthorizevpcassociation",
		},
		{"DisableHostedZoneDNSSEC", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/disable-dnssec"},
		{"DisassociateVPCFromHostedZone", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/disassociatevpc"},
		{"EnableHostedZoneDNSSEC", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/enable-dnssec"},
		{"GetAccountLimit", "GET", "/2013-04-01/accountlimit/PLACEHOLDER"},
		{"GetChange", "GET", "/2013-04-01/change/PLACEHOLDER"},
		{"GetCheckerIpRanges", "GET", "/2013-04-01/checkeripranges"},
		{"GetDNSSEC", "GET", "/2013-04-01/hostedzone/PLACEHOLDER/dnssec"},
		{"GetGeoLocation", "GET", "/2013-04-01/geolocation?continentcode=PLACEHOLDER"},
		{"GetHealthCheck", "GET", "/2013-04-01/healthcheck/PLACEHOLDER"},
		{"GetHealthCheckCount", "GET", "/2013-04-01/healthcheckcount"},
		{"GetHealthCheckLastFailureReason", "GET", "/2013-04-01/healthcheck/PLACEHOLDER/lastfailurereason"},
		{"GetHealthCheckStatus", "GET", "/2013-04-01/healthcheck/PLACEHOLDER/status"},
		{"GetHostedZone", "GET", "/2013-04-01/hostedzone/PLACEHOLDER"},
		{"GetHostedZoneCount", "GET", "/2013-04-01/hostedzonecount"},
		{"GetHostedZoneLimit", "GET", "/2013-04-01/hostedzonelimit/PLACEHOLDER/PLACEHOLDER"},
		{"GetQueryLoggingConfig", "GET", "/2013-04-01/queryloggingconfig/PLACEHOLDER"},
		{"GetReusableDelegationSet", "GET", "/2013-04-01/delegationset/PLACEHOLDER"},
		{"GetReusableDelegationSetLimit", "GET", "/2013-04-01/reusabledelegationsetlimit/PLACEHOLDER/PLACEHOLDER"},
		{"GetTrafficPolicy", "GET", "/2013-04-01/trafficpolicy/PLACEHOLDER/PLACEHOLDER"},
		{"GetTrafficPolicyInstance", "GET", "/2013-04-01/trafficpolicyinstance/PLACEHOLDER"},
		{"GetTrafficPolicyInstanceCount", "GET", "/2013-04-01/trafficpolicyinstancecount"},
		{"ListCidrBlocks", "GET", "/2013-04-01/cidrcollection/PLACEHOLDER/cidrblocks"},
		{"ListCidrCollections", "GET", "/2013-04-01/cidrcollection"},
		{"ListCidrLocations", "GET", "/2013-04-01/cidrcollection/PLACEHOLDER"},
		{"ListGeoLocations", "GET", "/2013-04-01/geolocations"},
		{"ListHealthChecks", "GET", "/2013-04-01/healthcheck"},
		{"ListHostedZones", "GET", "/2013-04-01/hostedzone"},
		{"ListHostedZonesByName", "GET", "/2013-04-01/hostedzonesbyname"},
		{"ListHostedZonesByVPC", "GET", "/2013-04-01/hostedzonesbyvpc"},
		{"ListQueryLoggingConfigs", "GET", "/2013-04-01/queryloggingconfig"},
		{"ListResourceRecordSets", "GET", "/2013-04-01/hostedzone/PLACEHOLDER/rrset"},
		{"ListReusableDelegationSets", "GET", "/2013-04-01/delegationset"},
		{"ListTagsForResource", "GET", "/2013-04-01/tags/PLACEHOLDER/PLACEHOLDER"},
		{"ListTagsForResources", "POST", "/2013-04-01/tags/PLACEHOLDER"},
		{"ListTrafficPolicies", "GET", "/2013-04-01/trafficpolicies"},
		{"ListTrafficPolicyInstances", "GET", "/2013-04-01/trafficpolicyinstances"},
		{"ListTrafficPolicyInstancesByHostedZone", "GET", "/2013-04-01/trafficpolicyinstances/hostedzone"},
		{"ListTrafficPolicyInstancesByPolicy", "GET", "/2013-04-01/trafficpolicyinstances/trafficpolicy"},
		{"ListTrafficPolicyVersions", "GET", "/2013-04-01/trafficpolicies/PLACEHOLDER/versions"},
		{"ListVPCAssociationAuthorizations", "GET", "/2013-04-01/hostedzone/PLACEHOLDER/authorizevpcassociation"},
		{"TestDNSAnswer", "GET", "/2013-04-01/testdnsanswer"},
		{"UpdateHealthCheck", "POST", "/2013-04-01/healthcheck/PLACEHOLDER"},
		{"UpdateHostedZoneComment", "POST", "/2013-04-01/hostedzone/PLACEHOLDER"},
		{"UpdateHostedZoneFeatures", "POST", "/2013-04-01/hostedzone/PLACEHOLDER/features"},
		{"UpdateTrafficPolicyComment", "POST", "/2013-04-01/trafficpolicy/PLACEHOLDER/PLACEHOLDER"},
		{"UpdateTrafficPolicyInstance", "POST", "/2013-04-01/trafficpolicyinstance/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real route53 op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-l5ir: this
// audit found and fixed one op that resolved to a plausible WRONG op instead
// of 404ing -- GetHealthCheckLastFailureReason (GET .../healthcheck/{id}/
// lastfailurereason) fell through routeHealthCheck's generic switch and
// silently returned the full HealthCheck object (GetHealthCheck's response
// shape) instead of the failure reason, exactly the "plausible wrong op"
// class of bug that a route-table diff alone (rather than this kind of
// per-op resolution test) would have missed. All other 70 ops were already
// correctly routed. ExtractOperation, previously covering roughly half of
// the 71 ops, was extended to mirror routeRequest's real dispatch tree
// op-for-op so this test exercises the real dispatch tree directly -- 71/71
// pass.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
