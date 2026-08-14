package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestSetSecurityGroups_EnforcementField_RealClient drives SetSecurityGroups
// through the real aws-sdk-go-v2 client (gopherstack-7185). The real output
// carries EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic alongside
// SecurityGroupIds (elasticloadbalancingv2@v1.58.5
// api_op_SetSecurityGroups.go); gopherstack emitted only SecurityGroupIds,
// confirmed by hand-reverting.
func TestSetSecurityGroups_EnforcementField_RealClient(t *testing.T) {
	t.Parallel()

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestELBv2Client(t, h)

	lb, err := client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("sweep1-lb"),
		Subnets: []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)
	lbArn := lb.LoadBalancers[0].LoadBalancerArn

	out, err := client.SetSecurityGroups(t.Context(), &elbv2sdk.SetSecurityGroupsInput{
		LoadBalancerArn: lbArn,
		SecurityGroups:  []string{"sg-abcdef01"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-abcdef01"}, out.SecurityGroupIds)
	assert.NotEmpty(t, out.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic,
		"EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic empty")
}

// TestSetSubnets_IPAddressTypeAndNAT_RealClient drives SetSubnets through the
// real client. The real output carries IpAddressType and
// EnablePrefixForIpv6SourceNat alongside AvailabilityZones
// (elasticloadbalancingv2@v1.58.5 api_op_SetSubnets.go); gopherstack emitted
// only AvailabilityZones, confirmed by hand-reverting. IpAddressType is
// asserted against what a subsequent SetIpAddressType call reports.
func TestSetSubnets_IPAddressTypeAndNAT_RealClient(t *testing.T) {
	t.Parallel()

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestELBv2Client(t, h)

	lb, err := client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("sweep1-lb-subnets"),
		Subnets: []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)
	lbArn := lb.LoadBalancers[0].LoadBalancerArn

	ipOut, err := client.SetIpAddressType(t.Context(), &elbv2sdk.SetIpAddressTypeInput{
		LoadBalancerArn: lbArn,
		IpAddressType:   types.IpAddressTypeDualstack,
	})
	require.NoError(t, err)
	require.Equal(t, types.IpAddressTypeDualstack, ipOut.IpAddressType)

	out, err := client.SetSubnets(t.Context(), &elbv2sdk.SetSubnetsInput{
		LoadBalancerArn: lbArn,
		Subnets:         []string{"subnet-33333333", "subnet-44444444"},
	})
	require.NoError(t, err)
	require.Len(t, out.AvailabilityZones, 2)
	assert.Equal(t, string(ipOut.IpAddressType), string(out.IpAddressType),
		"SetSubnets: IpAddressType empty or mismatched against SetIpAddressType")
	assert.NotEmpty(t, out.EnablePrefixForIpv6SourceNat, "EnablePrefixForIpv6SourceNat empty")
}
