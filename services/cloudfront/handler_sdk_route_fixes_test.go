package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDistributionsByConnectionMode_RealClient drives the real
// aws-sdk-go-v2 client to prove ListDistributionsByConnectionMode is
// reachable. Real ConnectionMode travels as a URI label
// ("/2020-05-31/distributionsByConnectionMode/{ConnectionMode}",
// cloudfront@v1.67.4 serializers.go), not a query value -- gopherstack
// previously read it from a "ConnectionMode" query parameter a real client
// never sends, on top of routing the whole family through a hyphenated path
// no real client sends either (gopherstack-o31x).
func TestListDistributionsByConnectionMode_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	d, err := h.Backend.CreateDistribution("ref-conn-mode-real-client", "conn-mode-real-client", true,
		[]byte(`<DistributionConfig><CallerReference>ref-conn-mode-real-client</CallerReference>`+
			`<Enabled>true</Enabled><ConnectionMode>direct</ConnectionMode></DistributionConfig>`))
	require.NoError(t, err)

	direct, err := client.ListDistributionsByConnectionMode(t.Context(), &cfsdk.ListDistributionsByConnectionModeInput{
		ConnectionMode: types.ConnectionModeDirect,
	})
	require.NoError(t, err)
	require.NotNil(t, direct.DistributionList)
	found := false
	for _, item := range direct.DistributionList.Items {
		if aws.ToString(item.Id) == d.ID {
			found = true
		}
	}
	require.True(t, found, "expected distribution in direct-mode list")

	tenantOnly, err := client.ListDistributionsByConnectionMode(
		t.Context(), &cfsdk.ListDistributionsByConnectionModeInput{ConnectionMode: types.ConnectionModeTenantOnly},
	)
	require.NoError(t, err)
	for _, item := range tenantOnly.DistributionList.Items {
		require.NotEqual(t, d.ID, aws.ToString(item.Id), "direct-mode distribution should not match tenant-only filter")
	}
}

// TestListDistributionsByRealtimeLogConfig_RealClient drives the real
// aws-sdk-go-v2 client to prove ListDistributionsByRealtimeLogConfig is
// reachable. Real RealtimeLogConfigArn travels in the XML body, not a query
// value (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpDocumentListDistributionsByRealtimeLogConfigInput) --
// gopherstack previously read it from a query parameter a real client never
// sends.
func TestListDistributionsByRealtimeLogConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	out, err := client.ListDistributionsByRealtimeLogConfig(
		t.Context(),
		&cfsdk.ListDistributionsByRealtimeLogConfigInput{
			RealtimeLogConfigArn: aws.String("arn:aws:cloudfront::123456789012:realtime-log-config/rlc-real-client"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.DistributionList)
}

// TestMonitoringSubscription_RealClient drives the real aws-sdk-go-v2 client
// to prove Create/Get/DeleteMonitoringSubscription are reachable. Real paths
// use the PLURAL "distributions/{DistributionId}/monitoring-subscription"
// (cloudfront@v1.67.4 serializers.go); gopherstack previously matched only
// the singular "distribution/" prefix, so every real call 404'd
// (gopherstack-o31x).
func TestMonitoringSubscription_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	d, err := h.Backend.CreateDistribution("ref-mon-real-client", "mon-dist-real-client", true,
		minimalDistConfig("ref-mon-real-client", "mon-dist-real-client", true))
	require.NoError(t, err)

	created, err := client.CreateMonitoringSubscription(t.Context(), &cfsdk.CreateMonitoringSubscriptionInput{
		DistributionId: aws.String(d.ID),
		MonitoringSubscription: &types.MonitoringSubscription{
			RealtimeMetricsSubscriptionConfig: &types.RealtimeMetricsSubscriptionConfig{
				RealtimeMetricsSubscriptionStatus: types.RealtimeMetricsSubscriptionStatusEnabled,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.MonitoringSubscription)

	got, err := client.GetMonitoringSubscription(t.Context(), &cfsdk.GetMonitoringSubscriptionInput{
		DistributionId: aws.String(d.ID),
	})
	require.NoError(t, err)
	require.NotNil(t, got.MonitoringSubscription)

	_, err = client.DeleteMonitoringSubscription(t.Context(), &cfsdk.DeleteMonitoringSubscriptionInput{
		DistributionId: aws.String(d.ID),
	})
	require.NoError(t, err)
}

// TestGetManagedCertificateDetails_RealClient drives the real aws-sdk-go-v2
// client to prove GetManagedCertificateDetails is reachable. Real path is
// "/2020-05-31/managed-certificate/{Identifier}" (cloudfront@v1.67.4
// serializers.go), not nested under distribution-tenant -- gopherstack
// previously routed it as "distribution-tenant/{Id}/managed-certificate-details",
// which no real client sends (gopherstack-o31x).
func TestGetManagedCertificateDetails_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String("dist-mcd-real-client"),
		Name:           aws.String("mcd-real-client-tenant"),
		Domains:        []types.DomainItem{{Domain: aws.String("mcd-real-client.example.com")}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.DistributionTenant)

	out, err := client.GetManagedCertificateDetails(t.Context(), &cfsdk.GetManagedCertificateDetailsInput{
		Identifier: created.DistributionTenant.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ManagedCertificateDetails)
}

// TestDisassociateDistributionTenantWebACL_RealClient drives the real
// aws-sdk-go-v2 client to prove DisassociateDistributionTenantWebACL is
// reachable. Real path is PUT "distribution-tenant/{Id}/disassociate-web-acl"
// (cloudfront@v1.67.4 serializers.go); gopherstack previously had no route for
// it at all -- only the Associate variant was wired (gopherstack-o31x).
//
// Deliberately does not round-trip through AssociateDistributionTenantWebACL
// first (that op's own request wire-shape bug was fixed separately, see
// TestAssociateDistributionTenantWebACL_RealClient, gopherstack-4ara).
// DisassociateDistributionTenantWebACL's backend call is idempotent (map
// delete, gopherstack-o31x), so reachability is provable standalone.
func TestDisassociateDistributionTenantWebACL_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String("dist-disassoc-real-client"),
		Name:           aws.String("disassoc-real-client-tenant"),
		Domains:        []types.DomainItem{{Domain: aws.String("disassoc-real-client.example.com")}},
	})
	require.NoError(t, err)

	_, err = client.DisassociateDistributionTenantWebACL(t.Context(), &cfsdk.DisassociateDistributionTenantWebACLInput{
		Id:      created.DistributionTenant.Id,
		IfMatch: created.ETag,
	})
	require.NoError(t, err)
}

// TestGetDistributionTenantByDomain_RealClient drives the real
// aws-sdk-go-v2 client to prove GetDistributionTenantByDomain and
// ListDistributionTenants are both reachable and distinguishable. Real
// GetDistributionTenantByDomain is the bare GET "distribution-tenant" (Domain
// as a "?domain=" query value); ListDistributionTenants is POST to the plural
// "distribution-tenants" path (cloudfront@v1.67.4 serializers.go).
// gopherstack previously routed the bare GET to ListDistributionTenants
// instead, so GetDistributionTenantByDomain was unreachable (gopherstack-o31x).
func TestGetDistributionTenantByDomain_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String("dist-bydomain-real-client"),
		Name:           aws.String("bydomain-real-client-tenant"),
		Domains:        []types.DomainItem{{Domain: aws.String("bydomain-real-client.example.com")}},
	})
	require.NoError(t, err)

	byDomain, err := client.GetDistributionTenantByDomain(t.Context(), &cfsdk.GetDistributionTenantByDomainInput{
		Domain: aws.String("bydomain-real-client.example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, byDomain.DistributionTenant)
	require.Equal(t, aws.ToString(created.DistributionTenant.Id), aws.ToString(byDomain.DistributionTenant.Id))

	list, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, list.DistributionTenantList)
}

// TestGetConnectionGroupByRoutingEndpoint_RealClient drives the real
// aws-sdk-go-v2 client to prove GetConnectionGroupByRoutingEndpoint and
// ListConnectionGroups are both reachable and distinguishable. Real
// GetConnectionGroupByRoutingEndpoint is the bare GET "connection-group"
// (RoutingEndpoint as a query value); ListConnectionGroups is POST to the
// plural "connection-groups" path (cloudfront@v1.67.4 serializers.go).
// gopherstack previously swapped these -- the bare GET matched List, and a
// fictional "connection-group-by-routing-endpoint" literal path (which no
// real client sends) matched GetConnectionGroupByRoutingEndpoint
// (gopherstack-o31x).
func TestGetConnectionGroupByRoutingEndpoint_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateConnectionGroup(t.Context(), &cfsdk.CreateConnectionGroupInput{
		Name: aws.String("real-client-cg"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.ConnectionGroup)

	byEndpoint, err := client.GetConnectionGroupByRoutingEndpoint(
		t.Context(),
		&cfsdk.GetConnectionGroupByRoutingEndpointInput{
			RoutingEndpoint: created.ConnectionGroup.RoutingEndpoint,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, byEndpoint.ConnectionGroup)
	require.Equal(t, aws.ToString(created.ConnectionGroup.Id), aws.ToString(byEndpoint.ConnectionGroup.Id))

	// ListConnectionGroups (gopherstack-4ara): the response now emits a
	// top-level <ConnectionGroups> list directly, matching the real
	// deserializer (cloudfront@v1.67.4 deserializers.go:
	// awsRestxml_deserializeOpDocumentListConnectionGroupsOutput) instead of
	// the previous <ConnectionGroupList><Items> wrapper the SDK could never
	// populate a list from. Asserting the created group round-trips through
	// List is the only proof: the SDK silently decodes an empty list from a
	// wrong wrapper no matter what the raw XML holds.
	listed, err := client.ListConnectionGroups(t.Context(), &cfsdk.ListConnectionGroupsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.ConnectionGroups)

	var found bool

	for _, cg := range listed.ConnectionGroups {
		if aws.ToString(cg.Id) == aws.ToString(created.ConnectionGroup.Id) {
			found = true

			break
		}
	}

	assert.True(t, found, "created connection group must appear in ListConnectionGroups")
}

// TestListConnectionFunctions_RealClient drives the real aws-sdk-go-v2 client
// to prove ListConnectionFunctions is reachable. Real path is POST to the
// plural "connection-functions" (cloudfront@v1.67.4 serializers.go);
// gopherstack previously matched GET on the bare singular
// "connection-function", which no real client sends for List
// (gopherstack-o31x).
func TestListConnectionFunctions_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateConnectionFunction(t.Context(), &cfsdk.CreateConnectionFunctionInput{
		Name:                   aws.String("real-client-cfn"),
		ConnectionFunctionCode: []byte("function handler() {}"),
		ConnectionFunctionConfig: &types.FunctionConfig{
			Comment: aws.String("real-client test"),
			Runtime: types.FunctionRuntimeCloudfrontJs20,
		},
	})
	require.NoError(t, err)

	// ListConnectionFunctions (gopherstack-4ara): the response now emits a
	// top-level <ConnectionFunctions> list directly, matching the real
	// deserializer (cloudfront@v1.67.4 deserializers.go:
	// awsRestxml_deserializeOpDocumentListConnectionFunctionsOutput) instead
	// of the previous <ConnectionFunctionList><Items> wrapper the SDK could
	// never populate a list from. Asserting the created function round-trips
	// through List is the only proof: the SDK silently decodes an empty list
	// from a wrong wrapper no matter what the raw XML holds.
	listed, err := client.ListConnectionFunctions(t.Context(), &cfsdk.ListConnectionFunctionsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listed.ConnectionFunctions)

	var found bool

	for _, fn := range listed.ConnectionFunctions {
		if aws.ToString(fn.Id) == aws.ToString(created.ConnectionFunctionSummary.Id) {
			found = true

			break
		}
	}

	assert.True(t, found, "created connection function must appear in ListConnectionFunctions")
}
