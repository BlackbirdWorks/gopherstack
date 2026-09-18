package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func newTestELBv2Backend(t *testing.T) *elbv2sdk.Client {
	t.Helper()

	backend := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	handler := elbv2.NewHandler(backend)

	return newTestELBv2Client(t, handler)
}

// TestCreateLoadBalancer_EnablePrefixForIpv6SourceNat_RealClient covers a
// reqfielddiff tier-1 finding: CreateLoadBalancerInput.
// EnablePrefixForIpv6SourceNat was parsed nowhere, so a real client's
// request was silently dropped and DescribeLoadBalancers always reported
// nothing. SetSubnets shares the same field.
func TestCreateLoadBalancer_EnablePrefixForIpv6SourceNat_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	created, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
		Name:                         aws.String("nat-lb"),
		Type:                         elbv2types.LoadBalancerTypeEnumNetwork,
		IpAddressType:                elbv2types.IpAddressTypeDualstack,
		Subnets:                      []string{"subnet-11111111", "subnet-22222222"},
		EnablePrefixForIpv6SourceNat: elbv2types.EnablePrefixForIpv6SourceNatEnumOn,
	})
	require.NoError(t, err)
	require.Len(t, created.LoadBalancers, 1)
	assert.Equal(t, elbv2types.EnablePrefixForIpv6SourceNatEnumOn,
		created.LoadBalancers[0].EnablePrefixForIpv6SourceNat,
		"CreateLoadBalancer must apply and echo EnablePrefixForIpv6SourceNat, not silently drop it")

	lbArn := created.LoadBalancers[0].LoadBalancerArn

	setOut, err := client.SetSubnets(ctx, &elbv2sdk.SetSubnetsInput{
		LoadBalancerArn:              lbArn,
		Subnets:                      []string{"subnet-11111111"},
		EnablePrefixForIpv6SourceNat: elbv2types.EnablePrefixForIpv6SourceNatEnumOff,
	})
	require.NoError(t, err)
	assert.Equal(t, elbv2types.EnablePrefixForIpv6SourceNatEnumOff, setOut.EnablePrefixForIpv6SourceNat,
		"SetSubnets must apply and echo EnablePrefixForIpv6SourceNat, not hardcode it")
}

// TestCreateTargetGroup_IpAddressType_RealClient covers a reqfielddiff
// tier-1 finding: CreateTargetGroupInput.IpAddressType was parsed nowhere,
// so a real client requesting an ipv6 target group silently got the ipv4
// default with no way to tell.
func TestCreateTargetGroup_IpAddressType_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	created, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
		Name:          aws.String("ipv6-tg"),
		Protocol:      elbv2types.ProtocolEnumHttp,
		Port:          aws.Int32(80),
		TargetType:    elbv2types.TargetTypeEnumIp,
		IpAddressType: elbv2types.TargetGroupIpAddressTypeEnumIpv6,
	})
	require.NoError(t, err)
	require.Len(t, created.TargetGroups, 1)
	assert.Equal(t, elbv2types.TargetGroupIpAddressTypeEnumIpv6, created.TargetGroups[0].IpAddressType,
		"CreateTargetGroup must apply and echo IpAddressType, not silently default to ipv4")
}

// TestSetSecurityGroups_EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic_RealClient
// covers a reqfielddiff tier-1 finding: SetSecurityGroupsInput.
// EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic was parsed nowhere --
// SetSecurityGroups always echoed a hardcoded "off" regardless of what a
// real client requested (and regardless of the real "on" default).
func TestSetSecurityGroups_EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	created, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("sg-lb"),
		Subnets: []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)
	lbArn := created.LoadBalancers[0].LoadBalancerArn

	enforceOff := elbv2types.EnforceSecurityGroupInboundRulesOnPrivateLinkTrafficEnumOff
	out, err := client.SetSecurityGroups(ctx, &elbv2sdk.SetSecurityGroupsInput{
		LoadBalancerArn: lbArn,
		SecurityGroups:  []string{"sg-11111111"},
		EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic: enforceOff,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		elbv2types.EnforceSecurityGroupInboundRulesOnPrivateLinkTrafficEnumOff,
		out.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic,
		"SetSecurityGroups must apply and echo EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic, not hardcode it",
	)
}

// TestDescribeSSLPolicies_LoadBalancerType_RealClient covers a reqfielddiff
// tier-1 finding: DescribeSSLPoliciesInput.LoadBalancerType was parsed
// nowhere, so a caller scoping to "gateway" (which supports no TLS
// listeners at all) still got the full application/network policy catalog.
func TestDescribeSSLPolicies_LoadBalancerType_RealClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	all, err := client.DescribeSSLPolicies(ctx, &elbv2sdk.DescribeSSLPoliciesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, all.SslPolicies, "sanity: the unscoped catalog must be non-empty")

	gateway, err := client.DescribeSSLPolicies(ctx, &elbv2sdk.DescribeSSLPoliciesInput{
		LoadBalancerType: elbv2types.LoadBalancerTypeEnumGateway,
	})
	require.NoError(t, err)
	assert.Empty(t, gateway.SslPolicies,
		"DescribeSSLPolicies must apply LoadBalancerType=gateway, not ignore it -- Gateway Load "+
			"Balancers have no TLS listeners and support no SSL policies")
}
