package ociloadbalancer_test

import (
	"net/netip"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIPMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		cidr     *string
		wantCode string
		wantMode string
		wantV6   bool
	}{
		{name: "default", wantMode: "IPV4"},
		{name: "ipv4", mode: "IPV4", wantMode: "IPV4"},
		{name: "ipv6", mode: "IPV6", cidr: new("2001:db8:1::/64"), wantMode: "IPV6", wantV6: true},
		{name: "lowercase", mode: "ipv6", cidr: new("2001:db8:1::/64"), wantMode: "IPV6", wantV6: true},
		{name: "ipv6 without prefix", mode: "IPV6", wantCode: "MissingParameter"},
		{name: "ipv4 with prefix", mode: "IPV4", cidr: new("2001:db8:1::/64"), wantCode: "InvalidParameter"},
		{name: "v4 prefix", mode: "IPV6", cidr: new("10.0.0.0/24"), wantCode: "InvalidParameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := baseDetails(tc.name)
			d.IpMode = loadbalancer.CreateLoadBalancerDetailsIpModeEnum(tc.mode)
			d.Ipv6SubnetCidr = tc.cidr

			if tc.wantCode != "" {
				_, err := e.lb.CreateLoadBalancer(t.Context(), loadbalancer.CreateLoadBalancerRequest{
					CreateLoadBalancerDetails: d,
				})
				assert.Equal(t, tc.wantCode, svcErr(t, err).GetCode())

				return
			}

			id := e.mkLB(t, d)
			got := e.get(t, id)
			assert.Equal(t, tc.wantMode, string(got.IpMode))
			require.Len(t, got.IpAddresses, 1)

			addr, err := netip.ParseAddr(*got.IpAddresses[0].IpAddress)
			require.NoError(t, err)
			assert.Equal(t, tc.wantV6, addr.Is6())

			if tc.wantV6 {
				assert.True(t, netip.MustParsePrefix(*tc.cidr).Contains(addr))
			}

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			again := fresh.get(t, id)
			assert.Equal(t, got.IpMode, again.IpMode)
			assert.Equal(t, got.IpAddresses[0].IpAddress, again.IpAddresses[0].IpAddress)
		})
	}
}

func TestUpdateIPMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		cidr     *string
		wantCode string
		wantMode string
	}{
		{name: "to ipv6", mode: "IPV6", cidr: new("2001:db8:2::/64"), wantMode: "IPV6"},
		{name: "to ipv6 without prefix", mode: "IPV6", wantCode: "MissingParameter"},
		{name: "prefix on ipv4", cidr: new("2001:db8:2::/64"), wantCode: "InvalidParameter"},
		{name: "unchanged", mode: "IPV4", wantMode: "IPV4"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails(tc.name))
			before := e.get(t, id)

			_, err := e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
				LoadBalancerId: &id,
				IpMode:         loadbalancer.UpdateLoadBalancerDetailsIpModeEnum(tc.mode),
				Ipv6SubnetCidr: tc.cidr,
			})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			e.settle()

			got := e.get(t, id)
			assert.Equal(t, tc.wantMode, string(got.IpMode))

			if tc.wantMode == "IPV6" {
				assert.NotEqual(t, before.IpAddresses[0].IpAddress, got.IpAddresses[0].IpAddress)
				assert.True(t, netip.MustParseAddr(*got.IpAddresses[0].IpAddress).Is6())
			}
		})
	}
}
