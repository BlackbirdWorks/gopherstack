package elb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// elbCtxRegion returns a context carrying the given region under the
// region-isolation context key.
func elbCtxRegion(region string) context.Context {
	return elb.RegionContextForTest(context.Background(), region)
}

// classicLBInput returns a minimal valid CreateLoadBalancerInput for a Classic
// (EC2-Classic / AZ-based) load balancer with a single HTTP listener.
func classicLBInput(name, az string) elb.CreateLoadBalancerInput {
	return elb.CreateLoadBalancerInput{
		LoadBalancerName:  name,
		Scheme:            "internet-facing",
		AvailabilityZones: []string{az},
		Listeners: []elb.Listener{
			{Protocol: "HTTP", InstanceProtocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080},
		},
	}
}

// TestELBRegionIsolation proves that same-named Classic ELB load balancers
// created in two different regions are fully isolated: each region sees only
// its own load balancer, the ARN and DNS name embed the correct region, tags
// and policies do not leak across regions, and deleting the load balancer in
// one region leaves the other untouched.
func TestELBRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := elbCtxRegion("us-east-1")
	ctxWest := elbCtxRegion("us-west-2")

	const sharedName = "shared-lb"

	// 1. Create a load balancer with the SAME name in both regions.
	eastLB, err := backend.CreateLoadBalancer(ctxEast, classicLBInput(sharedName, "us-east-1a"))
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", eastLB.Region)
	assert.Contains(t, eastLB.ARN, "us-east-1")
	assert.Contains(t, eastLB.DNSName, "us-east-1")

	westLB, err := backend.CreateLoadBalancer(ctxWest, classicLBInput(sharedName, "us-west-2b"))
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", westLB.Region)
	assert.Contains(t, westLB.ARN, "us-west-2")
	assert.Contains(t, westLB.DNSName, "us-west-2")

	// The two LBs share a name but are region-qualified, so their ARNs differ.
	assert.NotEqual(t, eastLB.ARN, westLB.ARN)

	// 2. Each region reads back its own load balancer with the correct AZ.
	eastList, err := backend.DescribeLoadBalancers(ctxEast, []string{sharedName})
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, []string{"us-east-1a"}, eastList[0].AvailabilityZones)
	assert.Equal(t, "us-east-1", eastList[0].Region)

	westList, err := backend.DescribeLoadBalancers(ctxWest, []string{sharedName})
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, []string{"us-west-2b"}, westList[0].AvailabilityZones)
	assert.Equal(t, "us-west-2", westList[0].Region)

	// 3. Listing without a filter returns exactly one LB per region.
	eastAll, err := backend.DescribeLoadBalancers(ctxEast, nil)
	require.NoError(t, err)
	require.Len(t, eastAll, 1)

	westAll, err := backend.DescribeLoadBalancers(ctxWest, nil)
	require.NoError(t, err)
	require.Len(t, westAll, 1)

	// 4. Tags are region-scoped: a tag added in us-east-1 is invisible in us-west-2.
	require.NoError(t, backend.AddTags(ctxEast, []string{sharedName}, []tags.KV{{Key: "env", Value: "prod"}}))

	eastTags, err := backend.DescribeTags(ctxEast, []string{sharedName})
	require.NoError(t, err)
	require.Len(t, eastTags[sharedName], 1)
	assert.Equal(t, "prod", eastTags[sharedName][0].Value)

	westTags, err := backend.DescribeTags(ctxWest, []string{sharedName})
	require.NoError(t, err)
	assert.Empty(t, westTags[sharedName], "tags must not leak from us-east-1 into us-west-2")

	// 5. Policies are region-scoped: a policy created in us-west-2 is not visible
	//    when describing the same-named LB in us-east-1.
	require.NoError(t, backend.CreateAppCookieStickinessPolicy(ctxWest, sharedName, "west-pol", "SESSION"))

	westPolicies, err := backend.DescribeLoadBalancerPolicies(ctxWest, sharedName, nil)
	require.NoError(t, err)
	require.Len(t, westPolicies, 1)
	assert.Equal(t, "west-pol", westPolicies[0].PolicyName)

	eastPolicies, err := backend.DescribeLoadBalancerPolicies(ctxEast, sharedName, nil)
	require.NoError(t, err)
	assert.Empty(t, eastPolicies, "policies must not leak from us-west-2 into us-east-1")

	// 6. Deleting the load balancer in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteLoadBalancer(ctxEast, sharedName))

	_, err = backend.DescribeLoadBalancers(ctxEast, []string{sharedName})
	require.Error(t, err, "the us-east-1 load balancer must be gone")

	westStill, err := backend.DescribeLoadBalancers(ctxWest, []string{sharedName})
	require.NoError(t, err)
	require.Len(t, westStill, 1)
	assert.Equal(t, "us-west-2", westStill[0].Region)

	// The us-west-2 policy survives the us-east-1 delete.
	westPoliciesAfter, err := backend.DescribeLoadBalancerPolicies(ctxWest, sharedName, nil)
	require.NoError(t, err)
	require.Len(t, westPoliciesAfter, 1)
}

// TestELBDefaultRegionFallback verifies that a context without a region falls
// back to the backend's configured default region, and that a different region
// sees no resources.
func TestELBDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	created, err := backend.CreateLoadBalancer(context.Background(), classicLBInput("def-lb", "eu-central-1a"))
	require.NoError(t, err)
	assert.Equal(t, "eu-central-1", created.Region)

	// Reading via the explicit default region sees it.
	list, err := backend.DescribeLoadBalancers(elbCtxRegion("eu-central-1"), []string{"def-lb"})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "eu-central-1", list[0].Region)

	// A different region sees nothing.
	other, err := backend.DescribeLoadBalancers(elbCtxRegion("ap-south-1"), nil)
	require.NoError(t, err)
	assert.Empty(t, other)
}
