package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreate_Tags_RoundTrip_IpamVerifiedAccess covers batch 2 of gopherstack-wjlrn: the IPAM
// and Verified Access Create handlers never called parseTagSpecification, so
// TagSpecifications were silently dropped. Each case creates a resource with a tag, then
// confirms the tag comes back on the matching Describe.
func TestCreate_Tags_RoundTrip_IpamVerifiedAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "ipam", run: testIpamCreateTags},
		{name: "ipam scope", run: testIpamScopeCreateTags},
		{name: "ipam pool", run: testIpamPoolCreateTags},
		{name: "ipam policy", run: testIpamPolicyCreateTags},
		{name: "ipam resource discovery", run: testIpamResourceDiscoveryCreateTags},
		{name: "ipam external resource verification token", run: testIpamExternalResourceVerificationTokenCreateTags},
		{name: "ipam prefix list resolver", run: testIpamPrefixListResolverCreateTags},
		{name: "ipam prefix list resolver target", run: testIpamPrefixListResolverTargetCreateTags},
		{name: "verified access instance", run: testVerifiedAccessInstanceCreateTags},
		{name: "verified access group", run: testVerifiedAccessGroupCreateTags},
		{name: "verified access endpoint", run: testVerifiedAccessEndpointCreateTags},
		{name: "verified access trust provider", run: testVerifiedAccessTrustProviderCreateTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testIpamCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpam"},
		"TagSpecification.1.ResourceType": []string{"ipam"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamId>", "</ipamId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"DescribeIpams"},
		"IpamId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func createTestIpam(t *testing.T, h *ec2.Handler) string {
	t.Helper()

	resp, err := dispatchHandler(h, url.Values{"Action": []string{"CreateIpam"}})
	require.NoError(t, err)

	id := extractBetween(t, resp, "<ipamId>", "</ipamId>")
	require.NotEmpty(t, id)

	return id
}

func testIpamScopeCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamScope"},
		"IpamId":                          []string{ipamID},
		"TagSpecification.1.ResourceType": []string{"ipam-scope"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamScopeId>", "</ipamScopeId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DescribeIpamScopes"},
		"IpamScopeId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamPoolCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamPool"},
		"IpamId":                          []string{ipamID},
		"AddressFamily":                   []string{"ipv4"},
		"TagSpecification.1.ResourceType": []string{"ipam-pool"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamPoolId>", "</ipamPoolId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":       []string{"DescribeIpamPools"},
		"IpamPoolId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamPolicyCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamPolicy"},
		"IpamId":                          []string{ipamID},
		"TagSpecification.1.ResourceType": []string{"ipam-policy"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamPolicyId>", "</ipamPolicyId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeIpamPolicies"},
		"IpamPolicyId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamResourceDiscoveryCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamResourceDiscovery"},
		"TagSpecification.1.ResourceType": []string{"ipam-resource-discovery"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamResourceDiscoveryId>", "</ipamResourceDiscoveryId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                    []string{"DescribeIpamResourceDiscoveries"},
		"IpamResourceDiscoveryId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamExternalResourceVerificationTokenCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamExternalResourceVerificationToken"},
		"IpamId":                          []string{ipamID},
		"TagSpecification.1.ResourceType": []string{"ipam-external-resource-verification-token"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(
		t, createResp, "<ipamExternalResourceVerificationTokenId>", "</ipamExternalResourceVerificationTokenId>",
	)
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeIpamExternalResourceVerificationTokens"},
		"IpamExternalResourceVerificationTokenId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamPrefixListResolverCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamPrefixListResolver"},
		"IpamId":                          []string{ipamID},
		"TagSpecification.1.ResourceType": []string{"ipam-prefix-list-resolver"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamPrefixListResolverId>", "</ipamPrefixListResolverId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                     []string{"DescribeIpamPrefixListResolvers"},
		"IpamPrefixListResolverId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testIpamPrefixListResolverTargetCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	ipamID := createTestIpam(t, h)

	resolverResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateIpamPrefixListResolver"},
		"IpamId": []string{ipamID},
	})
	require.NoError(t, err)
	resolverID := extractBetween(t, resolverResp, "<ipamPrefixListResolverId>", "</ipamPrefixListResolverId>")
	require.NotEmpty(t, resolverID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateIpamPrefixListResolverTarget"},
		"IpamPrefixListResolverId":        []string{resolverID},
		"PrefixListId":                    []string{"pl-demo12345"},
		"TagSpecification.1.ResourceType": []string{"ipam-prefix-list-resolver-target"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<ipamPrefixListResolverTargetId>", "</ipamPrefixListResolverTargetId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                           []string{"DescribeIpamPrefixListResolverTargets"},
		"IpamPrefixListResolverId":         []string{resolverID},
		"IpamPrefixListResolverTargetId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testVerifiedAccessInstanceCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVerifiedAccessInstance"},
		"TagSpecification.1.ResourceType": []string{"verified-access-instance"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<verifiedAccessInstanceId>", "</verifiedAccessInstanceId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                     []string{"DescribeVerifiedAccessInstances"},
		"VerifiedAccessInstanceId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testVerifiedAccessGroupCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	instResp, err := dispatchHandler(h, url.Values{"Action": []string{"CreateVerifiedAccessInstance"}})
	require.NoError(t, err)
	instID := extractBetween(t, instResp, "<verifiedAccessInstanceId>", "</verifiedAccessInstanceId>")
	require.NotEmpty(t, instID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVerifiedAccessGroup"},
		"VerifiedAccessInstanceId":        []string{instID},
		"TagSpecification.1.ResourceType": []string{"verified-access-group"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<verifiedAccessGroupId>", "</verifiedAccessGroupId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                  []string{"DescribeVerifiedAccessGroups"},
		"VerifiedAccessGroupId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testVerifiedAccessEndpointCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	instResp, err := dispatchHandler(h, url.Values{"Action": []string{"CreateVerifiedAccessInstance"}})
	require.NoError(t, err)
	instID := extractBetween(t, instResp, "<verifiedAccessInstanceId>", "</verifiedAccessInstanceId>")
	require.NotEmpty(t, instID)

	grpResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"CreateVerifiedAccessGroup"},
		"VerifiedAccessInstanceId": []string{instID},
	})
	require.NoError(t, err)
	grpID := extractBetween(t, grpResp, "<verifiedAccessGroupId>", "</verifiedAccessGroupId>")
	require.NotEmpty(t, grpID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVerifiedAccessEndpoint"},
		"VerifiedAccessGroupId":           []string{grpID},
		"AttachmentType":                  []string{"vpc"},
		"EndpointType":                    []string{"network-interface"},
		"TagSpecification.1.ResourceType": []string{"verified-access-endpoint"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<verifiedAccessEndpointId>", "</verifiedAccessEndpointId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                     []string{"DescribeVerifiedAccessEndpoints"},
		"VerifiedAccessEndpointId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testVerifiedAccessTrustProviderCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVerifiedAccessTrustProvider"},
		"TrustProviderType":               []string{"user"},
		"PolicyReferenceName":             []string{"policy-ref"},
		"TagSpecification.1.ResourceType": []string{"verified-access-trust-provider"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<verifiedAccessTrustProviderId>", "</verifiedAccessTrustProviderId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"DescribeVerifiedAccessTrustProviders"},
		"VerifiedAccessTrustProviderId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}
